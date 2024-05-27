from enum import Enum
from .encoder import Encoder


class ReplicaState(Enum):
    WAITING_FOR_PING = "waiting_for_ping"
    WAITING_FOR_PORT = "waiting_for_port"
    WAITING_FOR_CAPA = "waiting_for_capa"


class Replica:
    state = ReplicaState.WAITING_FOR_PING

    def __init__(self, addr, socket, offset, replica_id):
        self.addr = addr
        self.socket = socket
        self.offset = offset
        self.replid = replica_id
        self.encoder = Encoder()

    def send_message(self, message):
        print(f"Sending message to {self.addr}", message)
        self.socket.sendall(message)

    def service_connection(self):
        pass
