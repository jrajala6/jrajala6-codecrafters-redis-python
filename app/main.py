import asyncio
import time
import logging
import sys
from pathlib import Path
logging.basicConfig(level=logging.INFO)


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

    async def start(self):
        if self.master is not None:
            await self.send_handshake()

        server = await asyncio.start_server(self.handle_client, "localhost", self.port)
        logging.info(f"Server started on port {self.port}")
        await server.serve_forever()

    async def send_handshake(self):
        # Send PING
        reader, writer = await asyncio.open_connection(*self.master)
        await self.send_array_response(writer, ["PING"])


    async def handle_client(self, reader, writer):
        """Handles communication with a single client."""
        if self.dir_path and self.file_name:
            await self.parse_rdb_file()
        print(self.store)
        while True:
            try:
                data = await self.read_client_input(reader)
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
            except Exception as e:
                logging.error(f"Error handling client: {e}")
                break

    async def send_simple_response(self, writer, message):
        response = f"{message}\r\n".encode()
        writer.write(response)
        await writer.drain()

    async def send_string_response(self, writer, message):
        response = f"${len(message)}\r\n{message}\r\n".encode()
        writer.write(response)
        await writer.drain()

    async def send_array_response(self, writer, data):
        """Sends an array RESP response."""
        response = f"*{len(data)}\r\n".encode()
        for item in data:
            response += f"${len(item)}\r\n{item}\r\n".encode()
        writer.write(response)
        await writer.drain()

    async def read_client_input(self, reader):
        """Reads and parses RESP input from the client."""
        try:
            data = await reader.read(1024)
            if not data:
                return []
            return self.parse_input(data.decode())
        except Exception as e:
            logging.error(f"Failed to parse input: {e}")
            return []

    def parse_input(self, data):
        """Parses RESP (Redis Serialization Protocol) input."""
        try:
            input_len = int(data[1])
            data = data[data.index("\r\n") + 2:]
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
                print(contents[index:])
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

                print(self.store)
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








if __name__ == "__main__":
    server = RedisServer()
    asyncio.run(server.start())