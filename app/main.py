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
        await server.serve_forever()

    def handle_client(self, reader, writer):
        pass

if __name__ == "__main__":
    server = RedisServer(port=6379)
    asyncio.run(server.start())