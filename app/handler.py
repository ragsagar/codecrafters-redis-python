from .parser import RespParser


class CommandHandler:
    def __init__(self, server):
        self.server = server
        self.parser = RespParser()

    def get_set_success_response(self):
        return self.server.encoder.generate_success_string()

    def _handle_set_command(self, data, cmd, sock):
        key = cmd.data[0]
        value = cmd.data[1]
        expiry_milliseconds = None
        if len(cmd.data) > 2:
            expiry_command = cmd.data[2]
            if expiry_command.upper() == "PX":
                expiry_milliseconds = int(cmd.data[3])
        self.server.set_data(key, value, expiry_milliseconds)
        return self.get_set_success_response()

    def _handle_get_command(self, data, cmd, sock):
        key = cmd.data[0]
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
        if cmd.data[0].upper() == "REPLICATION":
            response_msg = self.handle_replication_command(data, cmd, sock)
        else:
            response_msg = self.server.encoder.generate_bulkstring(
                "redis_version:0.0.1"
            )
        return response_msg

    def _handle_echo_command(self, data, cmd, sock):
        echo_message = " ".join(cmd.data)
        response_msg = self.server.encoder.generate_bulkstring(echo_message)
        return response_msg

    def _handle_ping_command(self, data, cmd, sock):
        return self.server.encoder.generate_bulkstring("PONG")

    def _handle_replconf_command(self, data, cmd, sock):
        print("Received replconf command", cmd)
        # Possible commads: listening-port, capa during handshake
        # GETACK periodically.
        if cmd.data[0] == b"GETACK":
            # self.server.set_repl_ack()
            return self.server.encoder.generate_array_string(["REPLCONF", "ACK", "0"])
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
    def get_set_success_response(self):
        return self.server.encoder.generate_null_string()
