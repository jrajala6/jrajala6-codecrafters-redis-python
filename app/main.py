import asyncio
import time
import logging
import sys
from pathlib import Path
logging.basicConfig(level=logging.INFO)

TYPES = {
    "<class 'str'>" : "string",
    "<class '__main__.StreamEntry'>": "stream"
}
class RedisServer:
    def __init__(self, master_host="localhost", master_port=6379):
        args = sys.argv

        self.port = 6379
        if "--port" in args:
            self.port = int(args[args.index("--port") + 1])

        self.master = None
        if "--replicaof" in args:
            self.master = args[args.index("--replicaof") + 1].split()
            self.master[1] = int(self.master[1])

        self.dir_path = None
        if "--dir" in args:
            self.dir_path = args[args.index("--dir") + 1]

        self.file_name = None
        if "--dbfilename" in args:
            self.file_name = args[args.index("--dbfilename") + 1]

        self.store = {}
        self.streams = {}
        self.repl_ports = {}
        self.replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
        self.repl_offset = 0
        self.wait_events = {}
        self.waiting_clients = {}
        self.master_connection = None
        self.ack_event = asyncio.Event()
        self.stream_event = asyncio.Event()
        self.streams = [StreamEntry("base", "0-0")]
        StreamEntry.streams = self.streams

    async def start(self):
        if self.master is not None:
            await self.send_handshake()

        server = await asyncio.start_server(self.handle_client, "localhost", self.port)
        logging.info(f"Server started on port {self.port}")
        await server.serve_forever()

    async def send_handshake(self):
        # Send PING
        reader, writer = await asyncio.open_connection(*self.master)
        self.master_connection = (reader, writer)
        await self.send_array_response(writer, ["PING"])
        await self.receive_master_response(reader)
        # Configure replication: inform listening port and assert capabilities (hardcoded --> psync2)
        await self.send_array_response(writer, ["REPLCONF", "listening-port", str(self.port)])
        await self.receive_master_response(reader)
        await self.send_array_response(writer, ["REPLCONF", "capa", "psync2"])
        await self.receive_master_response(reader)
        #replica doesn't have any data yet and needs to be fully resynchronized
        await self.send_array_response(writer, ["PSYNC", "?", "-1"])
        await self.receive_master_response(reader)
        await self.receive_rdb_file(reader)

        asyncio.create_task(self.process_master_commands(reader, writer))

    async def receive_rdb_file(self, reader):
        size_data = await reader.readline()
        if b'$' not in size_data:
            logging.error("Invalid RDB file format")
        else:
            size = int(size_data[1:-2])

        return await reader.read(size)

    async def receive_master_response(self, reader):
        return await reader.readline()

    async def process_master_commands(self, reader, writer):
        total_processed_bytes = 0
        while True:
            try:
                # Read raw data from the master
                unparsed_data = await reader.read(1024)
                if not unparsed_data:
                    break
                print(unparsed_data)
                #unparsed_data = unparsed_data.decode().split("*")[1:]
                #print(unparsed_data)
                parsed_data = {}
                remaining = unparsed_data.decode()
                while True:
                    parsed, remaining, new_processed_bytes = self.parse_input(remaining, True)
                    parsed_data[parsed] = new_processed_bytes
                    if remaining == "":
                        break

                for data in parsed_data:
                    command = data[0].upper()
                    if command == "SET":
                        expiry = None
                        if len(data) == 5:
                            expiry = int(data[4])
                        self.update_store(data[1], data[2], expiry)
                    if command == "REPLCONF" and data[1] == "GETACK":
                        await self.send_array_response(writer, ["REPLCONF", "ACK", str(total_processed_bytes)])
                    total_processed_bytes += parsed_data[data]
            except Exception as e:
                logging.error(f"Error handling client: {e}")
                break



    async def handle_client(self, reader, writer):
        """Handles communication with a single client."""
        if self.dir_path and self.file_name:
            await self.parse_rdb_file()
        while True:
            unparsed_data, data = await self.read_input(reader)
            if not data:
                break

            command = data[0].upper()
            if command == "PING":
                await self.send_simple_response(writer, "+PONG")
            if command == "ECHO":
                await self.send_string_response(writer, data[1])
            if command == "SET":
                expiry = None
                if len(data) == 5:
                    expiry = int(data[4])
                self.update_store(data[1], data[2], expiry)
                await self.send_propagation(unparsed_data)
                await self.send_simple_response(writer, "+OK")
            elif command == "GET":
                await self.handle_get_command(writer, data[1])
            elif command == "CONFIG" and data[1].upper() == "GET":
                await self.handle_config_command(writer, data[2])
            elif command == "KEYS":
                await self.send_array_response(writer, list(self.store))
            elif command == "INFO":
                if self.master is not None:
                    await self.send_string_response(writer, "role:slave")
                else:
                    await self.send_string_response(writer, f"role:master\n"
                                                            f"master_replid:{self.replid}\n"
                                                            f"master_repl_offset:{self.repl_offset}")
            elif command == "INCR":
                key = data[1]
                val = self.store.get(key, [0, None])[0]
                if type(val) == int:
                    if key in self.store:
                        self.store[key][0] += 1
                    else:
                        self.store[key] = [1, None]
                    await self.send_integer_response(writer, self.store[key][0])
                else:
                    await self.send_simple_response(writer, "-ERR value is not an integer or out of range")

            elif command == "REPLCONF":
                if data[1] == "listening-port":
                    self.repl_ports[writer] = 0
                if data[1] == "ACK":
                    self.repl_ports[writer] = int(data[2])
                    self.ack_event.set()
                if data[1] != "ACK":
                    await self.send_simple_response(writer, "+OK")

            elif command == "PSYNC":
                if data[1] == "?" and data[2] == "-1":
                    await self.send_simple_response(writer, f"+FULLRESYNC {self.replid} {self.repl_offset}") #master cannot perform incremental replication w/ replica and will start a full resynchronization
                await self.send_empty_rdbfile_response(writer)

            elif command == "WAIT":
                num_replicas_expected = int(data[1])
                if num_replicas_expected == 0:
                    await self.send_integer_response(writer, num_replicas_expected)
                else:
                    acknowledged_replicas = await self.find_all_acks(num_replicas_expected, int(data[2]))
                    await self.send_integer_response(writer, acknowledged_replicas)

            elif command == "TYPE":
                key = data[1]
                if key in self.store:
                    await self.send_simple_response(writer, f"+{TYPES[str(type(self.store[key][0]))]}")
                else:
                    await self.send_simple_response(writer, "+none")

            elif command == "XADD":
                added_entry = StreamEntry(data[1], data[2])
                added_entry.add_keyvalue_pairs(data[3:])
                is_valid = added_entry.validate_entry_id()
                if is_valid:
                    if data[1] not in self.store:
                        self.store[data[1]] = [added_entry]
                    else:
                        self.store[data[1]].append(added_entry)
                    self.streams.append(added_entry)
                    self.stream_event.set()
                    await self.send_string_response(writer, f"{added_entry.stream_id_ms()}-{added_entry.stream_id_sn()}")
                else:
                    if added_entry.stream_id() == "0-0":
                        await self.send_simple_response(writer,"-ERR The ID specified in XADD must be greater than 0-0")
                    else:
                        await self.send_simple_response(writer, "-ERR The ID specified in XADD is equal or smaller than the target stream top item")

            elif command == "XRANGE":
                contents = StreamEntry.find_range(data[1], data[2], data[3])
                result = []
                for content in contents:
                    result.append(content)
                    continue
                await self.send_array_response(writer, result)

            elif command == "XREAD":
                stream_id = None
                if data[-1] == "$":
                    stream_id = self.streams[-1].stream_id()
                if data[1] == "block":
                    start = 4
                    timeout = int(data[2])
                    if timeout == 0:
                        self.stream_event.clear()
                        await self.stream_event.wait()
                    else:
                        await asyncio.sleep(timeout / 1000)
                else:
                    start = 2
                num_keys = (len(data) - start) // 2
                output = []
                for idx in range(start, start + num_keys):
                    if stream_id:
                        contents = StreamEntry.xread(data[idx], stream_id)
                    else:
                        contents = StreamEntry.xread(data[idx], data[num_keys + idx])
                    result = [data[idx]]
                    for content in contents:
                        result.append([content])
                    output.append(result)
                if len(output[0]) == 1:
                    await self.send_simple_response(writer, "$-1")
                else:
                    await self.send_array_response(writer, output)

            elif command == "MULTI":
                self.send_simple_response(writer, "+OK")


    async def find_all_acks(self, num_replicas_expected, timeout_ms):
        start_time = time.time()

        # Ask all replicas for their latest ACKs
        for slave_writer in self.repl_ports:
            await self.send_array_response(slave_writer, ["REPLCONF", "GETACK", "*"])

        acknowledged_replicas = sum(1 for ack in self.repl_ports.values() if ack >= self.repl_offset)
        while time.time() - start_time < timeout_ms / 1000:
            try:
                # Wait until an ACK is received (or timeout)
                await asyncio.wait_for(self.ack_event.wait(), timeout=(timeout_ms / 1000) - (time.time() - start_time))
            except asyncio.TimeoutError:
                break  # Stop waiting if timeout occurs
            self.ack_event.clear()  # Reset event to wait for new ACKs

            # Count how many replicas have acknowledged the latest write
            acknowledged_replicas = sum(1 for ack in self.repl_ports.values() if ack >= self.repl_offset)

            if acknowledged_replicas >= num_replicas_expected:
                break  # Stop early if enough replicas acknowledged

        return acknowledged_replicas

    async def send_propagation(self, data):
        self.repl_offset += len(data)
        for slave_writer in self.repl_ports:
            slave_writer.write(data)
            await slave_writer.drain()


    async def send_empty_rdbfile_response(self, writer):
        file_contents = bytes.fromhex("524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2")
        response = f"${len(file_contents)}\r\n".encode() + file_contents
        writer.write(response)
        await writer.drain()

    async def send_simple_response(self, writer, message):
        response = f"{message}\r\n".encode()
        writer.write(response)
        await writer.drain()

    async def send_string_response(self, writer, message):
        response = f"${len(str(message))}\r\n{message}\r\n".encode()
        writer.write(response)
        await writer.drain()

    def encode_array_response(self, data):
        response = f"*{len(data)}\r\n".encode()
        for item in data:
            if type(item) == list:
                response += self.encode_array_response(item)
            else:
                response += f"${len(str(item))}\r\n{item}\r\n".encode()
        return response

    async def send_array_response(self, writer, data):
        """Sends an array RESP response."""
        response = self.encode_array_response(data)
        writer.write(response)
        await writer.drain()

    async def send_integer_response(self, writer, integer: int):
        response = f":{integer}\r\n".encode()
        writer.write(response)
        await writer.drain()

    async def read_input(self, reader):
        """Reads and parses RESP input from the client."""

        try:
            data = await reader.read(1024)
            if not data:
                return data, []
            return data, self.parse_input(data.decode())
        except Exception as e:
            logging.error(f"Failed to parse input: {e}")
            return data, []

    def parse_input(self, data, from_master=False):
        """Parses RESP (Redis Serialization Protocol) input."""
        try:
            input_len = int(data[1])
            start = data.index("\r\n") + 2
            data = data[start:]
            input_elements = []
            pointer = 0
            while len(input_elements) < input_len:
                #Decoding a RESP string
                if data[pointer] == "$":
                    first_cr = data.index("\r\n", pointer)  # index of first carriage return
                    string_len = int(data[pointer + 1: first_cr])
                    string = data[first_cr + 2: first_cr + 2 + string_len]
                    input_elements.append(string)
                    pointer = first_cr + string_len + 4
            if from_master:
                return tuple(input_elements), data[pointer:], start + pointer
            return input_elements
        except Exception as e:
            logging.error(f"Failed to decode input: {e}")
            return []

    def update_store(self, key, value, expiry):
        """Updates the key-value store."""
        try:
            if expiry is not None:
                time_limit = expiry / 1000
                end_time = time.time() + time_limit
            else:
                end_time = None
            try:
                value = int(value)
                self.store[key] = [value, end_time]
            except ValueError:
                self.store[key] = [value, end_time]
        except Exception as e:
            logging.error(f"Failed to update store: {e}")

    async def handle_get_command(self, writer, key):
        """Handles the GET command."""
        value_info = self.store.get(key)
        if value_info:
            value, expiry = value_info
            if (expiry is not None and time.time() <= expiry) or expiry is None:
                await self.send_string_response(writer, value_info[0])
                return
            del self.store[key]

        await self.send_simple_response(writer, "$-1")

    async def handle_config_command(self, writer, parameter):
        """Handles CONFIG commands."""
        if parameter == "dir":
            await self.send_array_response(writer, ["dir", self.dir_path])
        elif parameter == "dbfilename":
            await self.send_array_response(writer, ["dbfilename", self.file_name])

    async def parse_rdb_file(self):
        try:
            with (open(Path(self.dir_path) / Path(self.file_name), "rb") as file):
                contents = file.read()
                #find first database section
                database_start = contents.find(b"\xFE")
                database_size, int_size = self.size_encoding(contents, database_start+1)

                hash_table_info_start = contents.find(b"\xFB")
                db_header_len = 0
                hash_table_size, int_size = self.size_encoding(contents, hash_table_info_start+1)
                db_header_len += int_size
                expiry_keys_size, int_size = self.size_encoding(contents, hash_table_info_start+1+int_size)
                db_header_len += int_size


                index = hash_table_info_start + db_header_len + 1
                num_keys = 0

                def parse_key_value(index):
                    key_info = self.string_encoding(contents, index + 1)
                    val_info = self.string_encoding(contents, index + 1 + key_info[1])
                    return key_info, val_info


                while num_keys < hash_table_size and index < len(contents):
                    if contents[index:index+1] == b'\x00':
                        key, val = parse_key_value(index)
                        self.store[key[0]] = [val[0], None]
                        num_keys += 1
                        index += val[1] + key[1] + 1
                    elif contents[index:index+1] in (b'\xFC', b'\xFD'):
                        timestamp = int.from_bytes(contents[index + 1: index + 9], "little")
                        if contents[index:index+1] == b'\xFC':
                            timestamp /= 1000
                        key, val = parse_key_value(index + 9)
                        self.store[key[0]] = [val[0], timestamp]
                        num_keys += 1
                        index += val[1] + key[1] + 10

        except OSError:
            pass

    def size_encoding(self, contents, index):
        first_num = contents[index]
        first_two_bits = int((first_num & 0b11000000) >> 6)
        last_six_bits = first_num & 0b00111111
        if first_two_bits == 0b00:
            return last_six_bits, 1
        elif first_two_bits == 0b01:
            return (last_six_bits << 8) | contents[index + 1], 2
        elif first_two_bits == 0b10:
            return int.from_bytes(contents[index + 1: index + 5], "big"), 5
        elif first_two_bits == 0b11:
            self.string_encoding(contents, index)

    def string_encoding(self, contents, index):
        first_byte = contents[index: index+1]
        if first_byte == b"\xC0":
            return str(contents[index + 1]), 2
        elif first_byte == b"\xC1":
            return str(int.from_bytes(contents[index + 1: index + 3], "little")), 3
        elif first_byte == b"\xC2":
            return str(int.from_bytes(contents[index + 1: index + 5], "little")), 5
        else:
            return contents[index + 1: index + contents[index] + 1].decode(), contents[index] + 1

class StreamEntry:
    streams = []
    def __init__(self, stream_key, stream_id):
        self._stream_key = stream_key
        self._stream_id = stream_id
        self._stream_id_ms, self._stream_id_sn = self.parse_stream_id()
        self._contents = {}

    def parse_stream_id(self):
        if self._stream_id == "*":
            return self.autogenerate_stream_id()
        ms, sn = self._stream_id.split("-")
        ms = int(ms)
        if sn == "*":
            sn = self.autogenerate_sn(ms)
        sn = int(sn)
        return ms, sn

    def autogenerate_stream_id(self):
        ms = int(round(time.time() * 1000))
        sn = self.autogenerate_sn(ms)
        return ms, sn


    def autogenerate_sn(self, ms):
        for stream_entry in self.streams[-1::-1]:
            if stream_entry.stream_id_ms() == ms:
                return stream_entry.stream_id_sn() + 1
        return 0 if ms != 0 else 1


    def add_keyvalue_pairs(self, contents: list):
        for idx in range(0, len(contents), 2):
            self._contents[contents[idx]] = contents[idx + 1]

    def stream_key(self):
        return self._stream_key

    def stream_id(self):
        return self._stream_id

    def stream_id_ms(self):
        return self._stream_id_ms

    def stream_id_sn(self):
        return self._stream_id_sn

    def contents(self):
        return self._contents

    def validate_entry_id(self):
        other = self.streams[-1]
        if other.stream_id_ms() > self.stream_id_ms():
            return False
        if other.stream_id_ms() == self.stream_id_ms() and other.stream_id_sn() >= self.stream_id_sn():
            return False
        return True

    @staticmethod
    def find_range(key, start, stop):
        if start == "-":
            start_ms, start_sn = 0, 0

        elif "-" in start:
            start_ms, start_sn = start.split("-")
            start_ms, start_sn = int(start_ms), int(start_sn)
        else:
            start_ms, start_sn = int(start), 0

        if stop == "+":
            stop_ms, stop_sn = StreamEntry.streams[-1].stream_id_ms(), StreamEntry.streams[-1].stream_id_sn()

        elif "-" in stop:
            stop_ms, stop_sn = stop.split("-")
            stop_ms, stop_sn = int(stop_ms), int(stop_sn)
        else:
            stop_ms, stop_sn = int(stop), len(StreamEntry.streams)
        for stream_entry in StreamEntry.streams:
            if key == stream_entry.stream_key() and start_ms <= stream_entry.stream_id_ms() <= stop_ms:
                if stream_entry.stream_id_ms() == start_ms and stream_entry.stream_id_sn() < start_sn:
                    continue
                if stream_entry.stream_id_ms() == stop_ms and stream_entry.stream_id_sn() > stop_sn:
                    continue
                yield StreamEntry.get_output_format(stream_entry)

    @staticmethod
    def xread(key, start):
        if start == "-":
            start_ms, start_sn = 0, 0

        elif "-" in start:
            start_ms, start_sn = start.split("-")
            start_ms, start_sn = int(start_ms), int(start_sn)
        else:
            start_ms, start_sn = int(start), 0

        for stream_entry in StreamEntry.streams:
            if key == stream_entry.stream_key() and start_ms <= stream_entry.stream_id_ms():
                if stream_entry.stream_id_ms() == start_ms and stream_entry.stream_id_sn() <= start_sn:
                    continue
                yield StreamEntry.get_output_format(stream_entry)

    @staticmethod
    def get_output_format(stream_entry):
        output = [f"{stream_entry.stream_id_ms()}-{stream_entry.stream_id_sn()}"]
        output.append([item for key in stream_entry.contents() for item in (key, stream_entry.contents()[key])])
        return output









if __name__ == "__main__":
    server = RedisServer()
    asyncio.run(server.start())