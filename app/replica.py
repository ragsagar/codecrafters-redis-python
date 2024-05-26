from enum import Enum
from .encoder import Encoder


class ReplicaState(Enum):
    WAITING_FOR_PING = "waiting_for_ping"
    WAITING_FOR_PORT = "waiting_for_port"
    WAITING_FOR_CAPA = "waiting_for_capa"


class ReplicaConnection:
    state = ReplicaState.WAITING_FOR_PING

    def __init__(self, socket, server, port):
        self.socket = socket
        self.server = server
        self.port = port
        self.encoder = Encoder()

    def service_connection(self):
        pass
