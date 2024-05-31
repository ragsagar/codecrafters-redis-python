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
        self.processed = True
        self.sent_count = 0

    def send_write_command(self, message):
        self.send_message(message)
        self.processed = False
        self.sent_count += len(message)

    def send_message(self, message):
        print(f"Syncing with replica {self.addr}", message)
        self.socket.sendall(message)

    def check_processed(self):
        print("Checking if replica is processed")
        self.send_message(
            self.encoder.generate_array_string(["REPLCONF", "GETACK", "*"])
        )

    def update_processed(self, offset_count):
        if self.sent_count == offset_count:
            self.processed = True
        else:
            self.processed = False
        print(
            "Replica",
            self.addr,
            "updated as ",
            self.processed,
            "with sent_count",
            self.sent_count,
        )

    def is_processed(self):
        return self.processed
