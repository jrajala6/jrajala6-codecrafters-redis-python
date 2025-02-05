import asyncio
import time
import logging
import sys
logging.basicConfig(level=logging.INFO)


class RedisServer:
    def __init__(self, port=6379, master_host="localhost", master_port=6379):
        self.port = port

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


if __name__ == "__main__":
    server = RedisServer(port=6379)
    asyncio.run(server.start())