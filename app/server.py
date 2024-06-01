import traceback
import datetime
import socket
import selectors
import types
from enum import Enum
from .encoder import Encoder
from .utils import generate_repl_id
from .replica import Replica
from .handler import CommandHandler, ClientCommandHandler
from .store import KeyValueStore
from .master_connection import MasterConnection

sel = selectors.DefaultSelector()


class RedisServer:
    class ServerType(Enum):
        MASTER = "master"
        SLAVE = "slave"

    master_server = None
    master_port = None
    debug = True
    server_type = ServerType.MASTER
    master_connection = None
    store = None
    waiting_clients = []

    def __init__(self, port=6379, master_server=None, master_port=None, debug=True):
        self.port = port
        self.server_socket = None
        self.encoder = Encoder()
        self.master_server = master_server
        self.master_port = int(master_port) if master_port else None
        self.command_handler = CommandHandler(self)
        self.store = KeyValueStore()
        if master_server:
            self.setup_as_slave()
        else:
            self.setup_as_master()
        self.debug = debug

    def setup_as_slave(self):
        self.server_type = self.ServerType.SLAVE
        master_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        master_sock.connect_ex((self.master_server, self.master_port))
        master_sock.setblocking(False)
        events = selectors.EVENT_READ | selectors.EVENT_WRITE
        data = types.SimpleNamespace(
            addr=("master conn",),
            inb=b"",
            outb=b"",
            map_store={},
            master_connection=True,
        )
        self.master_connection = MasterConnection(self, self.port, master_sock)
        sel.register(master_sock, events, data=data)

    def setup_as_master(self):
        self.server_type = self.ServerType.MASTER
        self.repl_id = generate_repl_id()
        self.repl_offset = 0
        self.replicas = []

    def get_server_type(self):
        return self.server_type

    def log(self, message, *args):
        if self.debug:
            print(message, *args)

    def get_repl_offset(self):
        return self.repl_offset

    def get_replid(self):
        return self.repl_id

    def parse_message(self, message):
        parts = message.strip().split(b"\r\n")
        length = int(parts[0][1:])
        commands = []
        for i in range(length):
            commands.append(parts[i * 2 + 2].decode())
        return commands

    def expire_data(self):
        self.store.expire_data()

    def get_data(self, key):
        return self.store.get(key)

    def set_data(self, key, value, expiry_time=None):
        self.store.set(key, value, expiry_time)

    def add_replica(self, addr, replica_id, offset, sock):
        print(f"Adding replica {addr} {replica_id} at offset {offset}")
        replica = Replica(addr, sock, offset, replica_id)
        self.replicas.append(replica)

    def is_write_command(self, command):
        return command in ["set", "del"]

    def get_rdb_file_contents(self):
        # hex_data = open("sample_file.rdb").read()
        hex_data = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"
        return hex_data

    def get_replica_count(self):
        return len(self.replicas)

    def initialize_server(self):
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind(("localhost", self.port))
        self.server_socket.listen()
        self.log(f"Listening on port {self.port}")
        self.server_socket.setblocking(False)
        sel.register(self.server_socket, selectors.EVENT_READ, data=None)

    def handle_loop(self):
        try:
            while True:
                events = sel.select(timeout=None)
                for key, mask in events:
                    if key.data is None:
                        self.accept_wrapper(key.fileobj)
                    elif key.data.master_connection:
                        self.master_connection.service_connection(key, mask)
                    else:
                        self.service_connection(key, mask)
        finally:
            sel.close()
            self.server_socket.close()

    def accept_wrapper(self, server_socket):
        client_socket, addr = server_socket.accept()
        self.log(f"Accepted connection from {addr}")
        client_socket.setblocking(False)
        data = types.SimpleNamespace(
            addr=addr, inb=b"", outb=b"", map_store={}, master_connection=False
        )
        sel.register(
            client_socket, selectors.EVENT_READ | selectors.EVENT_WRITE, data=data
        )

    def service_connection(self, key, mask):
        sock = key.fileobj
        data = key.data
        if mask & selectors.EVENT_READ:
            recv_data = sock.recv(1024)
            if recv_data:
                data.outb += recv_data
                self.log("Received", repr(recv_data), "from", data.addr)
            else:
                self.log("Closing connection to", data.addr)
                sel.unregister(sock)
                sock.close()
        if mask & selectors.EVENT_WRITE:
            if data.outb:
                incoming = self.parse_message(data.outb)
                command = incoming[0].lower()
                response_msg = self.command_handler.handle_message(data, sock)
                self.replicate_if_required(data, command)
                self.sendall(response_msg, sock)
                data.outb = b""
                self.expire_data()

    def close_connection(self, sock):
        sel.unregister(sock)
        sock.close()
        self.log("Closed connection")

    def replicate_if_required(self, data, command):
        if self.is_write_command(command):
            for replica in self.replicas:
                # Write command
                replica.send_write_command(data.outb)
        self.check_if_client_waiting()

    def check_if_client_waiting(self):
        processed_replicas = self.processed_replicas()
        if self.waiting_clients:
            print(f"Client waiting for WAIT command: {len(self.waiting_clients)}")
        for index, (sock, min_count, expiry_time) in enumerate(self.waiting_clients):
            print(
                f"Processed replicas: f{processed_replicas}, expiry time: {expiry_time}, current: {datetime.datetime.now()}"
            )
            if (
                processed_replicas >= min_count
                or expiry_time <= datetime.datetime.now()
            ):
                self.sendall(
                    self.encoder.generate_integer_string(processed_replicas), sock
                )
                del self.waiting_clients[index]

    def add_waiter(self, sock, min_count, timeout):
        expiry_time = datetime.datetime.now() + datetime.timedelta(milliseconds=timeout)
        self.waiting_clients.append((sock, min_count, expiry_time))

    def processed_replicas(self):
        return sum([1 for i in self.replicas if i.is_processed()])

    def received_replica_offset(self, offset_count, sock):
        replica = next((i for i in self.replicas if i.socket == sock), None)
        if replica:
            print("Received offset from replica", replica.addr, offset_count)
            replica.update_processed(offset_count)
            self.log(f"Replica {replica.addr} processed {offset_count} commands")
        else:
            self.log("Received offset from unknown replica", sock.addr)

    def check_with_replicas(self):
        for replica in self.replicas:
            replica.check_processed()

    def sendall(self, message, sock):
        print(f"Sending message {message}")
        sock.sendall(message)

    def run(self):
        self.initialize_server()
        self.handle_loop()
