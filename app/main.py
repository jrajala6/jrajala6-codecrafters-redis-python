import asyncio
import time
import logging
import sys
from pathlib import Path
logging.basicConfig(level=logging.INFO)


class RedisServer:
    def __init__(self, port=6379, master_host="localhost", master_port=6379):
        self.port = port
        self.file_name = None
        self.dir_path = None

        args = sys.argv
        if "--dir" in args:
            self.dir_path = args[args.index("--dir") + 1]
        if "--dbfilename" in args:
            self.file_name = args[args.index("--dbfilename") + 1]

        self.master = [master_host, master_port]
        self.store = {}
        self.streams = {}
        self.repl_ports = {}
        self.replication_id = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
        self.offset = 0
        self.wait_events = {}
        self.waiting_clients = {}

    async def start(self):
        server = await asyncio.start_server(self.handle_client, "localhost", self.port)
        logging.info(f"Server started on port {self.port}")
        await server.serve_forever()

    async def handle_client(self, reader, writer):
        """Handles communication with a single client."""
        if self.dir_path and self.file_name:
            await self.parse_rdb_file()

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
            with open(Path(self.dir_path) / Path(self.file_name), "rb") as file:
                contents = file.read()
                #find first database section
                database_start = contents.find(b"\xFE")
                database_size, int_size = self.size_encoding(contents[database_start+1: database_start+10])

                hash_table_info_start = contents.find(b"\xFB")
                db_header_len = 0
                hash_table_size, int_size = self.size_encoding(contents[hash_table_info_start+1: hash_table_info_start+10])
                db_header_len += int_size
                expiry_keys_size, int_size = self.size_encoding(contents[hash_table_info_start+1+int_size: hash_table_info_start+10+int_size])
                db_header_len += int_size

                keys_count = 0
                index = hash_table_info_start + db_header_len + 1
                while keys_count < hash_table_size and index < len(contents):
                    index = contents.find(b"\x00", index)
                    key, key_len = self.string_encoding(contents[index + 1:])
                    index += key_len
                    value, value_len = self.string_encoding(contents[index + 1: index + 10])
                    keys_count += 1
                    self.store[key] = [value, None]
        except OSError:
            pass

    def size_encoding(self, encoding):
        first_num = encoding[0]
        first_two_bits = int((first_num & 0b11000000) >> 6)
        last_six_bits = first_num & 0b00111111
        if first_two_bits == 0b00:
            return last_six_bits, 1
        elif first_two_bits == 0b01:
            return (last_six_bits << 8) | encoding[1], 2
        elif first_two_bits == 0b10:
            return int.from_bytes(encoding[1:5], "big"), 5
        elif first_two_bits == 0b11:
            self.string_encoding(encoding)

    def string_encoding(self, encoding):
        first_byte = bytes(encoding[0])
        if first_byte == b"\xC0":
            return str(encoding[1]), 2
        elif first_byte == b"\xC1":
            return str(int.from_bytes(encoding[1:3], "little")), 3
        elif first_byte == b"\xC2":
            return str(int.from_bytes(encoding[1:5], "little")), 5
        else:
            return encoding[1: encoding[0] + 1].decode(), encoding[0] + 1








if __name__ == "__main__":
    server = RedisServer(port=6379)
    asyncio.run(server.start())