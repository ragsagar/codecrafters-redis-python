import time
from enum import Enum

from .parser import RespParser
from .encoder import Encoder
from .store import ZeroIdentifier


class CommandHandler:
    def __init__(self, server, store, connection=None):
        self.server = server
        self.store = store
        self.parser = RespParser()
        self.connection = connection
        self.encoder = Encoder()

    def get_set_success_response(self):
        return self.encoder.generate_success_string()

    def _handle_config_command(self, data, cmd, sock):
        cmd_data = cmd.get_decoded_data()
        if cmd_data[0].upper() == "GET":
            if cmd_data[1].upper() == "DIR":
                response = ["dir", self.server.get_rdb_dir()]
                return self.encoder.generate_array_string(response)
            elif cmd_data[1].upper() == "DBFILENAME":
                response = ["dbfilename", self.server.get_rdb_filename()]
                return self.encoder.generate_array_string(response)
        return None

    def _handle_keys_command(self, data, cmd, sock):
        if cmd.data[0] == b"*":
            return self.encoder.generate_array_string(self.store.get_keys())
        return None

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
        if cmd.data[0] == b"ACK":
            offset_count = int(cmd.data[1].decode())
            self.server.received_replica_offset(offset_count, sock)
            return None
        return self.encoder.generate_success_string()

    def _handle_psync_command(self, data, cmd, sock):
        self.server.add_replica(data.addr, cmd.data[0], cmd.data[1], sock)
        resync_string = (
            f"FULLRESYNC {self.server.get_replid()} {self.server.get_repl_offset()}"
        )
        resync_message = self.server.encoder.generate_simple_string(resync_string)
        file_message = self.encoder.generate_file_string(
            self.server.get_rdb_file_contents()
        )
        return resync_message + file_message

    def _handle_ok_command(self, data, cmd, sock):
        return None

    def _handle_wait_command(self, data, cmd, sock):
        min_required = int(cmd.data[0].decode())
        timeout = int(cmd.data[1].decode())
        print("Min required", min_required, "Timeout", timeout)
        self.server.add_waiter(sock, min_required, timeout)
        self.server.check_with_replicas()
        return None

    def _handle_type_command(self, data, cmd, sock):
        key = cmd.data[0].decode()
        type_value = self.store.get_type(key)
        if type_value:
            response_msg = self.encoder.generate_bulkstring(type_value)
        else:
            response_msg = self.encoder.generate_bulkstring("none")
        return response_msg

    def _handle_xadd_command(self, data, cmd, sock):
        key = cmd.data[0].decode()
        identifier = cmd.data[1].decode()
        values = [i.decode() for i in cmd.data[2:]]
        try:
            identifier = self.store.add_stream_data(key, values, identifier)
        except ValueError as e:
            return self.encoder.generate_error_string(
                "ERR The ID specified in XADD is equal or smaller than the target stream top item"
            )
        except ZeroIdentifier as e:
            return self.encoder.generate_error_string(str(e))
        return self.encoder.generate_simple_string(identifier)

    def _handle_xrange_command(self, data, cmd, sock):
        key = cmd.data[0].decode()
        start = cmd.data[1].decode()
        end = cmd.data[2].decode()
        # count = None
        # if len(cmd.data) > 3:
        #     count = int(cmd.data[3].decode())
        messages = self.store.get_stream_range(key, start, end)
        return self.encoder.generate_array_string(messages)

    def _handle_xread_command(self, data, cmd, sock):
        type_value = cmd.data[0].decode()
        print("Type value", type_value)
        if type_value == "streams":
            messages = []
            for i in range(1, len(cmd.data), 2):
                key = cmd.data[i].decode()
                identifier = cmd.data[i + 1].decode()
                message = self.store.get_stream_read(key, identifier)
                messages.append(message)
            print("Got data", messages)
            return self.encoder.generate_array_string(messages)
        return None

    def parse_message(self, message):
        commands = self.parser.parse(message)
        return commands

    def handle_single_command(self, data, command, sock):
        handler_func = getattr(self, f"_handle_{command.command.lower()}_command", None)
        if not handler_func:
            response_msg = self.encoder.generate_bulkstring("Unknown command")
        else:
            response_msg = handler_func(data, command, sock)
        return response_msg

    def replicate_if_required(self, command):
        if self.server.is_write_command(command):
            for replica in self.server.replicas:
                replica.send_write_command(command.get_raw())

    def handle_message(self, data, sock):
        commands = self.parse_message(data.outb)
        print("Commands found in handler", commands)
        response_msg = b""
        for command in commands:
            self.replicate_if_required(command)
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
