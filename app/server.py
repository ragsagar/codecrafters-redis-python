import os
import datetime
import socket
import selectors
import types
from enum import Enum
from .encoder import Encoder
from .utils import generate_repl_id, is_bigger_stream_id
from .replica import Replica
from .handler import CommandHandler, ClientCommandHandler
from .store import KeyValueStore
from .master_connection import MasterConnection
from .rdb.parser import RdbParser

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
    last_processed = 0
    replicas = []
    stream_blocking_clients = []

    def __init__(
        self,
        port=6379,
        master_server=None,
        master_port=None,
        rdb_dir=None,
        rdb_filename=None,
        debug=True,
    ):
        self.port = port
        self.server_socket = None
        self.encoder = Encoder()
        self.master_server = master_server
        self.master_port = int(master_port) if master_port else None
        self.store = KeyValueStore()
        self.command_handler = CommandHandler(self, store=self.store)
        self.rdb_dir = rdb_dir
        self.rdb_filename = rdb_filename
        if master_server:
            self.setup_as_slave()
        else:
            self.setup_as_master()
        self.debug = debug
        self.load_initial_data()

    def load_initial_data(self):
        if self.server_type == self.ServerType.MASTER and self.rdb_dir:
            raw_rdb_data = self.get_rdb_contents()
            if raw_rdb_data:
                rdb_data = self.parse_rdb_file(raw_rdb_data)
                self.store.load_data_from_rdb(rdb_data)

    def parse_rdb_file(self, contents):
        parser = RdbParser()
        rdb_data = parser.parse(contents)
        return rdb_data

    def get_rdb_dir(self):
        return self.rdb_dir

    def get_rdb_filename(self):
        return self.rdb_filename

    def get_rdb_filepath(self):
        return os.path.join(self.get_rdb_dir(), self.get_rdb_filename())

    def get_rdb_contents(self):
        filepath = self.get_rdb_filepath()
        if os.path.exists(filepath):
            return open(filepath, "rb").read()
        return None

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

    def get_rdb_dir(self):
        return self.rdb_dir

    def get_rdb_filename(self):
        return self.rdb_filename

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
        return command.command in ["SET", "DEL"]

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
                response_msg = self.command_handler.handle_message(data, sock)
                if response_msg:
                    self.sendall(response_msg, sock)
                data.outb = b""
                self.expire_data()
            self.periodic_checks()

    def close_connection(self, sock):
        sel.unregister(sock)
        sock.close()
        self.log("Closed connection")

    def periodic_checks(self):
        self.check_if_client_waiting()
        self.expire_stream_blocks()

    def check_if_client_waiting(self):
        processed_replicas = self.processed_replicas()
        print_debug = False
        if self.waiting_clients:
            if processed_replicas != self.last_processed:
                self.last_processed = processed_replicas
                print_debug = True
                print("Processed replicas count changed, checking with clients")
                print(f"Client waiting for WAIT command: {len(self.waiting_clients)}")
        for index, (sock, min_count, expiry_time) in enumerate(self.waiting_clients):
            if print_debug:
                print(
                    f"Processed replicas count: {processed_replicas}, expiry time: {expiry_time}, current: {datetime.datetime.now()}"
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

    def add_stream_blocking_client(self, sock, key, identifier, timeout):
        expiry_time = datetime.datetime.now() + datetime.timedelta(milliseconds=timeout)
        print(f"Set expiry time to {expiry_time} for client", sock.getpeername())
        self.stream_blocking_clients.append((sock, key, identifier, expiry_time))

    def expire_stream_blocks(self):
        for index, (sock, _, _, expiry_time) in enumerate(self.stream_blocking_clients):
            if expiry_time <= datetime.datetime.now():
                print("Expiring stream blocking client", sock.getpeername())
                self.sendall(self.encoder.generate_null_string(), sock)
                del self.stream_blocking_clients[index]
                sock.close()

    def send_data_to_stream_clients(self, key, identifier, data):
        print(
            "Sending data to stream clients",
            key,
            identifier,
            data,
            datetime.datetime.now(),
        )
        for index, (sock, stream_key, stream_identifier, _) in enumerate(
            self.stream_blocking_clients
        ):
            if stream_key == key and is_bigger_stream_id(identifier, stream_identifier):
                self.sendall(self.encoder.generate_array_string(data), sock)
                del self.stream_blocking_clients[index]
                sock.close()

    def processed_replicas(self):
        return sum([1 for i in self.replicas if i.is_processed()])

    def received_replica_offset(self, offset_count, sock):
        replica = next((i for i in self.replicas if i.socket == sock), None)
        if replica:
            print("Received offset from replica", replica.addr, offset_count)
            replica.update_processed(offset_count)
            self.log(f"Replica {replica.addr} processed {offset_count} commands")
        else:
            self.log("Received offset from unknown replica", sock.getpeername())

    def check_with_replicas(self):
        for replica in self.replicas:
            replica.check_processed()

    def sendall(self, message, sock):
        print(f"Sending message {message} to", sock.getpeername())
        sock.sendall(message)

    def run(self):
        self.initialize_server()
        self.handle_loop()
