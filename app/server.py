import socket
import selectors
import types
import datetime
import uuid
from enum import Enum
from .encoder import Encoder
from .utils import generate_repl_id
from .replica import Replica
from .handler import CommandHandler
from .store import KeyValueStore
from .parser import RespParser

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
        self.master_connection = MasterConnection(
            self, self.port, master_sock, self.command_handler
        )
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

    def replicate_if_required(self, data, command):
        if self.is_write_command(command):
            for replica in self.replicas:
                replica.send_message(data.outb)

    def sendall(self, message, sock):
        print(f"Sending message {message}")
        sock.sendall(message)

    def run(self):
        self.initialize_server()
        self.handle_loop()


class MasterConnectionState(Enum):
    INITIAL = "initial"
    WAITING_FOR_PONG = "waiting_for_pong"
    WAITING_FOR_PORT_RESPONSE = "waiting_for_port_response"
    WAITING_FOR_CAPA_RESPONSE = "waiting_for_capa_response"
    WAITING_FOR_FULLRESYNC = "waiting_for_fullresync"
    WAITING_FOR_FILE = "waiting_for_file"
    READY = "ready"


class MasterHandshakeException(Exception):
    pass


class MasterConnection:
    state = MasterConnectionState.INITIAL
    server = None
    port = None
    listening_port = None
    replica_id = "?"
    offset = -1
    offset_count = 0

    def __init__(self, server, listening_port, socket, command_handler):
        self.server = server
        self.listening_port = listening_port
        self.socket = socket
        self.command_handler = command_handler
        self.encoder = Encoder()
        self.parser = RespParser()
        self.handler = CommandHandler(server)

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
            # if self.state != MasterConnectionState.READY:
            #     self.do_handshake(data, sock)
            # elif data.outb and self.state == MasterConnectionState.READY:
            #     # commands = self.parser.parse(data.outb)
            #     # for command in commands:
            #     #     response = self.command_handler.handle_message(data, sock)
            #     #     if response:
            #     #         print("Sending", response)
            #     #         sock.sendall(response)
            #     # self.log("Expired data")
            #     response = self.command_handler.handle_message(data, sock)
            #     if response:
            #         print("Sending", response)
            #         sock.sendall(response)
            self.handle_incoming_data(data, sock)
            data.outb = b""

    def handle_incoming_data(self, data, sock):
        if self.state == MasterConnectionState.INITIAL:
            print("Sending ping to master")
            sock.sendall(self.encoder.generate_array_string(["PING"]))
            self.set_state(MasterConnectionState.WAITING_FOR_PONG)
        if data.outb:
            commands = self.parser.parse(data.outb)
            print("Received commands in slave: ", commands, self.state)
            for command in commands:
                if (
                    command.command == "PONG"
                    and self.state == MasterConnectionState.WAITING_FOR_PONG
                ):
                    print("Sending port to master")
                    sock.sendall(
                        self.encoder.generate_array_string(
                            ["REPLCONF", "listening-port", str(self.listening_port)]
                        )
                    )
                    self.set_state(MasterConnectionState.WAITING_FOR_PORT_RESPONSE)
                if (
                    command.command == "OK"
                    and self.state == MasterConnectionState.WAITING_FOR_PORT_RESPONSE
                ):
                    print("Received OK from master, Sending capa")
                    sock.sendall(
                        self.encoder.generate_array_string(
                            ["REPLCONF", "capa", "psync2"]
                        )
                    )
                    self.set_state(MasterConnectionState.WAITING_FOR_CAPA_RESPONSE)

                if (
                    command.command == "OK"
                    and self.state == MasterConnectionState.WAITING_FOR_CAPA_RESPONSE
                ):
                    print("Received OK from master, Sending psync")
                    sock.sendall(
                        self.encoder.generate_array_string(
                            ["PSYNC", self.replica_id, str(self.offset)]
                        )
                    )
                    self.set_state(MasterConnectionState.WAITING_FOR_FULLRESYNC)

                if (
                    command.command == "FULLRESYNC"
                    and self.state == MasterConnectionState.WAITING_FOR_FULLRESYNC
                ):
                    self.replica_id, self.offset = command.data
                    self.set_state(MasterConnectionState.WAITING_FOR_FILE)
                    self.log("Waiting for file")

                if (
                    command.command == "RDB"
                    and self.state == MasterConnectionState.WAITING_FOR_FILE
                ):
                    print("Received RDB file from master", command.data)
                    self.set_state(MasterConnectionState.READY)

                if command.command == "REPLCONF" and command.data[0] == b"GETACK":
                    response = self.encoder.generate_array_string(
                        ["REPLCONF", "ACK", "0"]
                    )
                    sock.sendall(response)

            if self.state == MasterConnectionState.READY:
                response = self.command_handler.handle_message(data, sock)
                if response:
                    sock.sendall(response)

    def set_state(self, state):
        print(f"Changing state from {self.state} to {state}")
        self.state = state

    def log(self, message, *args):
        print("Replica: ", message, *args)

    def parse_message(self, message):
        parts = message.strip().split(b"\r\n")
        return parts[0].decode()
