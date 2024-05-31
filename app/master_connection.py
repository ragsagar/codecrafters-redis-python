import selectors
from .handler import ClientCommandHandler
from .encoder import Encoder
from .parser import RespParser


class MasterConnection:
    server = None
    port = None
    listening_port = None
    replica_id = "?"
    offset = -1
    offset_count = 0
    sent_ping = False

    def __init__(self, server, listening_port, socket):
        self.server = server
        self.listening_port = listening_port
        self.socket = socket
        self.encoder = Encoder()
        self.parser = RespParser()
        self.command_handler = ClientCommandHandler(server, self)

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
                self.server.close_connection(sock)
        if mask & selectors.EVENT_WRITE:
            self.handle_incoming_data(data, sock)
            data.outb = b""

    def handle_incoming_data(self, data, sock):
        if not self.sent_ping:
            print("Sending ping to master")
            sock.sendall(self.encoder.generate_array_string(["PING"]))
            self.sent_ping = True
        if data.outb:
            response = self.command_handler.handle_message(data, sock)
            print("Sending response from slave:", response)
            if response:
                sock.sendall(response)

    def log(self, message, *args):
        print("Replica: ", message, *args)

    def set_offset_and_replica(self, offset, replica_id):
        self.offset = offset
        self.replica_id = replica_id

    def get_listening_port(self):
        return self.listening_port

    def get_replica_id(self):
        return self.replica_id

    def get_offset(self):
        return self.offset


class MasterHandshakeException(Exception):
    pass
