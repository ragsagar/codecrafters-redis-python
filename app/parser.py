import sys

from enum import Enum


class RespParser:
    class State:
        START = 0
        COMMAND = 1
        DATA = 2
        DONE = 3

    def __init__(self) -> None:
        self.state = self.State.START

    def read_number(self, message, cursor):
        number = 0
        while message[cursor] != b"\r":
            number = number * 10 + int(message[cursor])
            cursor += 1
        return cursor, number

    def skip_newline(self, message, cursor):
        while message[cursor] in [b"\n", b"\r"]:
            cursor += 1
        return cursor

    def read_command(self, message, cursor, part_length):
        while message[cursor] != b"$":
            cursor += 1
        cursor, command_length = self.read_number(message, cursor + 1)
        cursor = self.skip_newline(message, cursor)
        command_string = b"".join(message[cursor : cursor + command_length])
        cursor += command_length
        command = Command(command_string.decode(), [])
        part_length -= 1
        while part_length > 0:
            while message[cursor] != b"$":
                cursor += 1
            cursor, data_length = self.read_number(message, cursor + 1)
            cursor = self.skip_newline(message, cursor)
            data = b"".join(message[cursor : cursor + data_length])
            cursor += data_length
            command.add_data(data)
            part_length -= 1
        return cursor, command

    def read_pending_bytes(self, message, cursor):
        data = b""
        while message[cursor] != b"\r":
            data += message[cursor]
            cursor += 1
        return cursor, data

    def read_bytes(self, message, length, cursor):
        data = b"".join(message[cursor : cursor + length])
        cursor += length
        return cursor, data

    def parse(self, msg):
        cursor = 0
        part_length = 0
        commands = []
        message = [i.to_bytes(1, sys.byteorder) for i in msg]
        while cursor < len(message):
            if message[cursor] == b"*":
                cursor, part_length = self.read_number(message, cursor + 1)
                cursor, command = self.read_command(message, cursor, part_length)
                commands.append(command)
                cursor += 1
            elif message[cursor] == b"+":
                cursor, string = self.read_pending_bytes(message, cursor + 1)
                commands.append(Command(string.decode()))
                cursor += 1
            elif message[cursor] == b"$":
                cursor, data_length = self.read_number(message, cursor + 1)
                cursor = self.skip_newline(message, cursor)
                cursor, data = self.read_bytes(message, data_length, cursor)
                commands.append(Command("RDB", [data]))
            else:
                cursor += 1
        return commands

    def parse_old(self, message):
        tokens = message.strip()
        print(tokens)
        message_length = 0
        command = None
        commands = []
        cursor = 0
        while cursor < len(tokens):
            token = tokens[cursor]
            if token.startswith(b"$"):
                # byte length
                cursor += 1
                continue
            if self.state == self.State.START:
                if token.startswith(b"*"):
                    print(token)
                    message_length = int(token[1:])
                    self.state = self.State.COMMAND
                elif token.startswith(b"+"):
                    command = Command(token[1:].decode())
                    message_length = 0
                elif token.startswith(b"REDIS"):
                    # RDB file special case
                    command = Command("RDB", [token])
                    message_length = 0
            elif self.state == self.State.COMMAND:
                message_length -= 1
                command = Command(token.decode())
                self.state = self.State.DATA
            elif self.state == self.State.DATA:
                message_length -= 1
                command.add_data(token.decode())
            if message_length == 0:
                commands.append(command)
                self.state = self.State.START
                message_length = 0
                command = None
            cursor += 1
        return commands


class CommandType(Enum):
    SET = "SET"
    GET = "GET"
    EXPIRE = "EXPIRE"
    DELETE = "DELETE"
    PING = "PING"
    REPLCONF = "REPLCONF"
    PSYNC = "PSYNC"
    OK = "OK"


class Command:
    data = None

    def __init__(self, command, data=None):
        self.command = command
        if data:
            self.data = data
        else:
            self.data = []

    def add_data(self, data):
        self.data.append(data)

    def __str__(self):
        return f"{self.command} {self.data}"

    def __repr__(self):
        return f"{self.command} {self.data}"

    def __eq__(self, other):
        return other and self.command == other.command and self.data == other.data
