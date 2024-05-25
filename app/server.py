import socket
import selectors
import types
import datetime
import uuid
from enum import Enum
from .encoder import Encoder

sel = selectors.DefaultSelector()


class ServerType(Enum):
    MASTER = "master"
    SLAVE = "slave"


class RedisServer:
    master_server = None
    master_port = None
    debug = True
    server_type = ServerType.MASTER
    master_connection = None

    def __init__(self, port=6379, master_server=None, master_port=None, debug=True):
        self.port = port
        self.server_socket = None
        self.master_server = master_server
        self.master_port = int(master_port) if master_port else None
        self.encoder = Encoder()
        if master_server:
            self.server_type = ServerType.SLAVE
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
                self.master_server, self.master_port
            )
            sel.register(master_sock, events, data=data)
        self.debug = debug

    def get_server_type(self):
        return self.server_type

    def log(self, message, *args):
        if self.debug:
            print(message, *args)

    def get_repl_offset(self):
        return 0

    def get_replid(self):
        return uuid.uuid4().hex

    def parse_message(self, message):
        parts = message.strip().split(b"\r\n")
        length = int(parts[0][1:])
        commands = []
        for i in range(length):
            commands.append(parts[i * 2 + 2].decode())
        return commands

    def expire_data(self, data):
        current_time = datetime.datetime.now()
        self.log(f"Expiring data at time {current_time}, {data.map_store}")
        for key in list(data.map_store.keys()):
            obj = data.map_store[key]
            if obj["expiry_time"] is not None and obj["expiry_time"] < current_time:
                self.log(f"Expiring key {key}")
                del data.map_store[key]

    def accept_wrapper(self, server_socket):
        client_socket, addr = server_socket.accept()
        self.log(f"Accepted connection from {addr}")
        client_socket.setblocking(False)
        data = types.SimpleNamespace(addr=addr, inb=b"", outb=b"", map_store={})
        sel.register(
            client_socket, selectors.EVENT_READ | selectors.EVENT_WRITE, data=data
        )

    def handle_set_command(self, data, incoming):
        key = incoming[1]
        value = incoming[2]
        expiry_time = None
        if len(incoming) > 4:
            expiry_command = incoming[3]
            if expiry_command.upper() == "PX":
                expiry_value = int(incoming[4])
                expiry_time = datetime.datetime.now() + datetime.timedelta(
                    milliseconds=expiry_value
                )
        self.log(f"Setting key {key} to value {value} with expiry time {expiry_time}")
        data.map_store[key] = {"value": value, "expiry_time": expiry_time}
        data.outb = b""
        return self.encoder.generate_success_string()

    def handle_get_command(self, data, incoming):
        key = incoming[1]
        if key in data.map_store:
            response_msg = self.encoder.generate_bulkstring(
                data.map_store[key]["value"]
            )
        else:
            response_msg = self.encoder.generate_null_string()
        return response_msg

    def handle_replication_command(self, data, incoming):
        server_type = self.get_server_type()
        messages = [
            f"role:{server_type.value}",
        ]
        if server_type == ServerType.MASTER:
            messages.extend(
                [
                    f"master_replid:{self.get_replid()}",
                    f"master_repl_offset:{self.get_repl_offset()}",
                ]
            )
        response_msg = self.encoder.generate_bulkstring("\n".join(messages))
        self.log("Sending replication info", response_msg)
        return response_msg

    def _handle_get_command(self, data, incoming):
        return self.handle_get_command(data, incoming)

    def _handle_info_command(self, data, incoming):
        if incoming[1].upper() == "REPLICATION":
            response_msg = self.handle_replication_command(data, incoming)
        else:
            response_msg = self.encoder.generate_bulkstring("redis_version:0.0.1")
        return response_msg

    def _handle_set_command(self, data, incoming):
        return self.handle_set_command(data, incoming)

    def _handle_echo_command(self, data, incoming):
        echo_message = incoming[1]
        response_msg = self.encoder.generate_bulkstring(echo_message)
        return response_msg

    def _handle_ping_command(self, data, incoming):
        return self.encoder.generate_bulkstring("PONG")

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
                self.expire_data(data)
                incoming = self.parse_message(data.outb)
                command = incoming[0].lower()
                handler_func = getattr(self, f"_handle_{command}_command")
                if not handler_func:
                    response_msg = self.encoder.generate_bulkstring("Unknown command")
                else:
                    response_msg = handler_func(data, incoming)
                sock.sendall(response_msg)
                data.outb = b""

    def initialize_server(self):
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind(("localhost", self.port))
        self.server_socket.listen()
        self.log(f"Listening on port {self.port}")
        self.server_socket.setblocking(False)
        sel.register(self.server_socket, selectors.EVENT_READ, data=None)

    def parse_message_from_master(self, message):
        parts = message.strip().split(b"\r\n")
        return parts[0].decode()

    def handle_server(self):
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

    def run(self):
        self.initialize_server()
        self.handle_server()


class MasterConnectionState(Enum):
    WAITING_FOR_PING = "waiting_for_ping"
    WAITING_FOR_PORT = "waiting_for_port"
    WAITING_FOR_CAPA = "waiting_for_capa"
    READY = "ready"


class MasterConnection:
    state = MasterConnectionState.WAITING_FOR_PING
    server = None
    port = None

    def __init__(self, server, port):
        self.server = server
        self.port = port
        self.encoder = Encoder()

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
            # response_msg = self.encoder.generate_array_string(["PING"]);
            # sock.sendall(response_msg)
        if mask & selectors.EVENT_WRITE:
            if data.outb or self.state == MasterConnectionState.WAITING_FOR_PING:
                if self.state == MasterConnectionState.WAITING_FOR_PING:
                    print("Sending ping to master")
                    sock.sendall(self.encoder.generate_array_string(["PING"]))
                    self.state = MasterConnectionState.WAITING_FOR_PORT
                elif self.state == MasterConnectionState.WAITING_FOR_PORT:
                    print("Sending port to master")
                    sock.sendall(
                        self.encoder.generate_array_string(
                            ["REPLCONF", "listening-port", str(self.port)]
                        )
                    )
                    self.state = MasterConnectionState.WAITING_FOR_CAPA
                elif self.state == MasterConnectionState.WAITING_FOR_CAPA:
                    print("Sending capa to master")
                    sock.sendall(
                        self.encoder.generate_array_string(
                            ["REPLCONF", "capa", "psync2"]
                        )
                    )
                    self.state = MasterConnectionState.READY
                    print("Master connection ready")
                data.outb = b""
            # if data.outb:
            #     incoming = self.parse_message_from_master(data.outb)
            #     self.log(f"Received message from master {incoming}")
            #     response_msg = self.encoder.generate_array_string(["REPLCONF", "listening-port", str(self.port)])
            #     sock.sendall(response_msg)

    def log(self, message, *args):
        print(message, *args)
