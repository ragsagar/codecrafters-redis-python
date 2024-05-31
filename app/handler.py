from enum import Enum

from .parser import RespParser
from .encoder import Encoder


class CommandHandler:
    def __init__(self, server, connection=None):
        self.server = server
        self.parser = RespParser()
        self.connection = connection
        self.encoder = Encoder()

    def get_set_success_response(self):
        return self.encoder.generate_success_string()

    def _handle_set_command(self, data, cmd, sock):
        key = cmd.data[0]
        value = cmd.data[1]
        expiry_milliseconds = None
        if len(cmd.data) > 2:
            expiry_command = cmd.data[2]
            if expiry_command.upper() == b"PX":
                expiry_milliseconds = int(cmd.data[3].decode())
        self.server.set_data(key.decode(), value.decode(), expiry_milliseconds)
        return self.get_set_success_response()

    def _handle_get_command(self, data, cmd, sock):
        key = cmd.data[0].decode()
        value = self.server.get_data(key)
        print(f"Getting value for key {key}", value)
        if value:
            response_msg = self.server.encoder.generate_bulkstring(value)
        else:
            response_msg = self.server.encoder.generate_null_string()
        return response_msg

    def handle_replication_command(self, data, cmd, sock):
        server_type = self.server.get_server_type()
        messages = [
            f"role:{server_type.value}",
        ]
        if self.server.server_type == self.server.ServerType.MASTER:
            messages.extend(
                [
                    f"master_replid:{self.server.get_replid()}",
                    f"master_repl_offset:{self.server.get_repl_offset()}",
                ]
            )
        response_msg = self.server.encoder.generate_bulkstring("\n".join(messages))
        self.server.log("Sending replication info", response_msg)
        return response_msg

    def _handle_info_command(self, data, cmd, sock):
        if cmd.data[0].upper() == b"REPLICATION":
            response_msg = self.handle_replication_command(data, cmd, sock)
        else:
            response_msg = self.server.encoder.generate_bulkstring(
                "redis_version:0.0.1"
            )
        return response_msg

    def _handle_echo_command(self, data, cmd, sock):
        echo_message = " ".join([i.decode() for i in cmd.data])
        response_msg = self.server.encoder.generate_bulkstring(echo_message)
        return response_msg

    def _handle_ping_command(self, data, cmd, sock):
        return self.server.encoder.generate_bulkstring("PONG")

    def _handle_replconf_command(self, data, cmd, sock):
        return self.server.encoder.generate_success_string()

    def _handle_psync_command(self, data, cmd, sock):
        print("Received psync command", cmd)
        self.server.add_replica(data.addr, cmd.data[0], cmd.data[1], sock)
        resync_string = (
            f"FULLRESYNC {self.server.get_replid()} {self.server.get_repl_offset()}"
        )
        resync_message = self.server.encoder.generate_simple_string(resync_string)
        file_message = self.server.encoder.generate_file_string(
            self.server.get_rdb_file_contents()
        )
        return resync_message + file_message

    def _handle_ok_command(self, data, cmd, sock):
        return None

    def parse_message(self, message):
        commands = self.parser.parse(message)
        return commands

    def handle_single_command(self, data, command, sock):
        handler_func = getattr(self, f"_handle_{command.command.lower()}_command", None)
        if not handler_func:
            response_msg = self.server.encoder.generate_bulkstring("Unknown command")
        else:
            response_msg = handler_func(data, command, sock)
        return response_msg

    def handle_message(self, data, sock):
        commands = self.parse_message(data.outb)
        print("Commands found in handler", commands)
        response_msg = b""
        for command in commands:
            response = self.handle_single_command(data, command, sock)
            if response:
                response_msg += response
        return response_msg


class ClientCommandHandler(CommandHandler):
    class State(Enum):
        INIT = 1
        WAITING_FOR_PONG = 2
        WAITING_FOR_PORT_RESPONSE = 3
        WAITING_FOR_CAPA_RESPONSE = 4
        WAITING_FOR_FULLRESYNC = 5
        WAITING_FOR_FILE = 6
        READY = 7
        RECORD_OFFSET = 8

    state = State.WAITING_FOR_PONG
    offset_count = 0

    def _handle_ping_command(self, data, cmd, sock):
        return None

    def _handle_pong_command(self, data, cmd, sock):
        if self.state == self.State.WAITING_FOR_PONG:
            self.state = self.State.WAITING_FOR_PORT_RESPONSE
            return self.encoder.generate_array_string(
                [
                    "REPLCONF",
                    "listening-port",
                    str(self.connection.get_listening_port()),
                ]
            )
        return None

    def _handle_ok_command(self, data, cmd, sock):
        if self.state == self.State.WAITING_FOR_PORT_RESPONSE:
            self.state = self.State.WAITING_FOR_CAPA_RESPONSE
            return self.encoder.generate_array_string(["REPLCONF", "capa", "psync2"])
        elif self.state == self.State.WAITING_FOR_CAPA_RESPONSE:
            self.state = self.State.WAITING_FOR_FULLRESYNC
            return self.encoder.generate_array_string(
                [
                    "PSYNC",
                    str(self.connection.get_replica_id()),
                    str(-1),
                ]
            )
        return super()._handle_ok_command(data, cmd, sock)

    def _handle_fullresync_command(self, data, cmd, sock):
        if self.state == self.State.WAITING_FOR_FULLRESYNC:
            replica_id, offset = cmd.data
            self.connection.set_offset_and_replica(offset.decode(), replica_id.decode())
            self.state = self.State.WAITING_FOR_FILE
        return None

    def _handle_rdb_command(self, data, cmd, sock):
        print("Received RDB file", cmd.data[0])
        self.state = self.State.READY
        return None

    def _handle_replconf_command(self, data, cmd, sock):
        print("Received replconf command", cmd)
        # Possible commads: listening-port, capa during handshake
        # GETACK periodically.
        if cmd.data[0] == b"GETACK":
            if self.state == self.State.READY:
                self.state = self.State.RECORD_OFFSET
            print("Sending offset count", self.offset_count)
            return self.encoder.generate_array_string(
                ["REPLCONF", "ACK", str(self.offset_count)]
            )
        return super()._handle_replconf_command(data, cmd, sock)

    def get_set_success_response(self):
        return None

    def increment_offset(self, command):
        if self.state == self.State.RECORD_OFFSET:
            self.offset_count += command.get_size()
            print(
                f"Incremented offset count to {self.offset_count} Last message: ${command}, length: {command.get_size()}"
            )

    def handle_message(self, data, sock):
        commands = self.parse_message(data.outb)
        print("Commands found in handler", commands)
        response_msg = b""
        for command in commands:
            response = self.handle_single_command(data, command, sock)
            if response:
                response_msg += response
            self.increment_offset(command)
        return response_msg
