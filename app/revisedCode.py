import asyncio
import time
import logging

logging.basicConfig(level=logging.INFO)


class RedisServer:
    def __init__(self, port=6379, dir_path="", file_name="", master_host="localhost", master_port=6379):
        self.port = port
        self.dir_path = dir_path
        self.file_name = file_name
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
        tasks = [server.serve_forever()]
        if self.port != self.master[1]:
            tasks.append(self.connect_to_master())
        await asyncio.gather(*tasks)

    async def connect_to_master(self):
        """Handles replication by connecting to the master server."""
        try:
            reader, writer = await asyncio.open_connection(self.master[0], self.master[1])
            await self.perform_handshake(reader, writer)
            await self.handle_replication(reader, writer)
        except Exception as e:
            logging.error(f"Failed to connect to master: {e}")

    async def perform_handshake(self, reader, writer):
        """Sends REPLCONF commands to the master server."""
        await self.send_array_response(writer, ["PING"])
        await reader.readline()
        await self.send_array_response(writer, ["REPLCONF", "listening-port", str(self.port)])
        await reader.readline()
        await self.send_array_response(writer, ["REPLCONF", "capa", "psync2"])
        await reader.readline()
        await self.send_array_response(writer, ["PSYNC", "?", "-1"])

    async def handle_replication(self, reader, writer):
        """Continuously listens to updates from the master."""
        ack_count = 0
        while True:
            contents = await reader.read(1024)
            if not contents:
                break
            if b"SET" in contents:
                commands = contents.split(b"*")
                for command in commands:
                    if b"SET" in command:
                        parsed_command = self.parse_input(command)
                        self.update_store(parsed_command)
            elif b"PING" in contents:
                ack_count += 14
            elif b"GETACK" in contents:
                await self.send_array_response(writer, ["REPLCONF", "ACK", f"{ack_count}"])
                ack_count += 37

    async def handle_client(self, reader, writer):
        """Handles communication with a single client."""
        multi = False
        queue = []
        temp_store = {}

        while True:
            try:
                data = await self.read_client_input(reader)
                if not data:
                    break

                command = data[0].upper()
                if command == "PING":
                    await self.send_simple_response(writer, "+PONG", multi, queue)
                elif command == "SET":
                    self.update_store(data, temp_store)
                    await self.send_simple_response(writer, "+OK", multi, queue)
                elif command == "GET":
                    await self.handle_get_command(writer, data, temp_store, multi, queue)
                elif command == "CONFIG":
                    await self.handle_config_command(writer, data, multi, queue)
                elif command == "KEYS":
                    await self.send_array_response(writer, list(self.store.keys()), multi, queue)
                elif command == "MULTI":
                    multi = True
                    queue.clear()
                    temp_store.clear()
                    await self.send_simple_response(writer, "+OK")
                elif command == "EXEC":
                    await self.execute_transaction(writer, temp_store, queue)
                    multi = False
                    queue.clear()
                    temp_store.clear()
                elif command == "DISCARD":
                    multi = False
                    queue.clear()
                    temp_store.clear()
                    await self.send_simple_response(writer, "+OK")
                else:
                    await self.send_simple_response(writer, f"-ERR Unknown command {command}")
            except Exception as e:
                logging.error(f"Error handling client: {e}")
                break

    async def read_client_input(self, reader):
        """Reads and parses RESP input from the client."""
        try:
            data = await reader.read(1024)
            if not data:
                return []
            return self.parse_input(data)
        except Exception as e:
            logging.error(f"Failed to parse input: {e}")
            return []

    def parse_input(self, data):
        """Parses RESP (Redis Serialization Protocol) input."""
        try:
            decoded = data.decode()
            parts = decoded.split("\r\n")
            return [parts[i + 1] for i in range(0, len(parts) - 1, 2)]
        except Exception as e:
            logging.error(f"Failed to decode input: {e}")
            return []

    def update_store(self, data, temp_store=None):
        """Updates the key-value store."""
        try:
            value = int(data[2]) if data[2].isdigit() else data[2]
            if len(data) > 3 and data[3] == "PX":
                ttl = int(data[4]) / 1000
                expiry = time.time() + ttl
                temp_store[data[1]] = [value, expiry]
            else:
                temp_store[data[1]] = [value]
        except Exception as e:
            logging.error(f"Failed to update store: {e}")

    async def handle_get_command(self, writer, data, temp_store, multi, queue):
        """Handles the GET command."""
        key = data[1]
        value = self.store.get(key) or temp_store.get(key)
        if value:
            if len(value) == 2 and time.time() > value[1]:
                del self.store[key]
                del temp_store[key]
                await self.send_simple_response(writer, "$-1", multi, queue)
            else:
                await self.send_bulk_response(writer, str(value[0]), multi, queue)
        else:
            await self.send_simple_response(writer, "$-1", multi, queue)

    async def handle_config_command(self, writer, data, multi, queue):
        """Handles CONFIG commands."""
        if data[1] == "GET":
            if data[2] == "dir":
                await self.send_array_response(writer, ["dir", self.dir_path], multi, queue)
            elif data[2] == "dbfilename":
                await self.send_array_response(writer, ["dbfilename", self.file_name], multi, queue)

    async def execute_transaction(self, writer, temp_store, queue):
        """Executes queued MULTI commands."""
        self.store.update(temp_store)
        response = b"".join(queue)
        writer.write(response)
        await writer.drain()

    async def send_simple_response(self, writer, message, multi=False, queue=None):
        """Sends a simple RESP response."""
        response = f"{message}\r\n".encode()
        if multi and queue is not None:
            queue.append(response)
        else:
            writer.write(response)
            await writer.drain()

    async def send_bulk_response(self, writer, message, multi=False, queue=None):
        """Sends a bulk RESP response."""
        response = f"${len(message)}\r\n{message}\r\n".encode()
        if multi and queue is not None:
            queue.append(response)
        else:
            writer.write(response)
            await writer.drain()

    async def send_array_response(self, writer, data, multi=False, queue=None):
        """Sends an array RESP response."""
        response = f"*{len(data)}\r\n".encode()
        for item in data:
            response += f"${len(item)}\r\n{item}\r\n".encode()
        if multi and queue is not None:
            queue.append(response)
        else:
            writer.write(response)
            await writer.drain()


if __name__ == "__main__":
    server = RedisServer(port=6379)
    asyncio.run(server.start())
