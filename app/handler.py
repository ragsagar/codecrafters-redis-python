import datetime


class CommandHandler:
    def __init__(self, server):
        self.server = server

    def _handle_set_command(self, data, incoming, sock):
        key = incoming[1]
        value = incoming[2]
        expiry_milliseconds = None
        if len(incoming) > 4:
            expiry_command = incoming[3]
            if expiry_command.upper() == "PX":
                expiry_milliseconds = int(incoming[4])
        self.server.set_data(key, value, expiry_milliseconds)
        return self.server.encoder.generate_success_string()

    def _handle_get_command(self, data, incoming, sock):
        key = incoming[1]
        value = self.server.get_data(key)
        if value:
            response_msg = self.server.encoder.generate_bulkstring(value)
        else:
            response_msg = self.server.encoder.generate_null_string()
        return response_msg

    def handle_replication_command(self, data, incoming, sock):
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

    def _handle_info_command(self, data, incoming, sock):
        if incoming[1].upper() == "REPLICATION":
            response_msg = self.handle_replication_command(data, incoming, sock)
        else:
            response_msg = self.server.encoder.generate_bulkstring(
                "redis_version:0.0.1"
            )
        return response_msg

    def _handle_echo_command(self, data, incoming, sock):
        echo_message = incoming[1]
        response_msg = self.server.encoder.generate_bulkstring(echo_message)
        return response_msg

    def _handle_ping_command(self, data, incoming, sock):
        return self.server.encoder.generate_bulkstring("PONG")

    def _handle_replconf_command(self, data, incoming, sock):
        print("Received replconf command", incoming)
        return self.server.encoder.generate_success_string()

    def _handle_psync_command(self, data, incoming, sock):
        print(f"Received psync command", incoming)
        self.server.add_replica(data.addr, incoming[1], incoming[2], sock)
        resync_string = (
            f"FULLRESYNC {self.server.get_replid()} {self.server.get_repl_offset()}"
        )
        resync_message = self.server.encoder.generate_simple_string(resync_string)
        file_message = self.server.encoder.generate_file_string(
            self.server.get_rdb_file_contents()
        )
        return resync_message + file_message

    def _handle_ok_command(self, data, incoming, sock):
        return None

    def parse_message(self, message):
        parts = message.strip().split(b"\r\n")
        print("Parsing message", message, "parts", parts)
        commands = []
        if len(parts) == 1:
            return [parts[0].decode()]
        else:
            length = int(parts[0][1:])
            for i in range(length):
                commands.append(parts[i * 2 + 2].decode())
        return commands

    def handle_command(self, data, socket):
        incoming = self.parse_message(data.outb)
        print("Incoming data", incoming)
        if (len(incoming) == 1 and incoming[0] == "OK") or not incoming:
            return None
        print("Incoming", incoming)
        command = incoming[0].lower()
        handler_func = getattr(self, f"_handle_{command}_command", None)
        if not handler_func:
            response_msg = self.server.encoder.generate_bulkstring("Unknown command")
        else:
            response_msg = handler_func(data, incoming, socket)
        return response_msg
