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
                cursor_start = cursor
                cursor, part_length = self.read_number(message, cursor + 1)
                cursor, command = self.read_command(message, cursor, part_length)
                size = cursor - cursor_start + 2  # 2 for \r\n
                command.set_size(size)
                command.set_raw(b"".join(message[cursor_start : cursor + 2]))
                commands.append(command)
                cursor += 1
            elif message[cursor] == b"+":
                cursor, string = self.read_pending_bytes(message, cursor + 1)
                parts = string.split()
                command = Command(parts[0].decode(), data=parts[1:])
                commands.append(command)
                cursor += 1
            elif message[cursor] == b"$":
                cursor, data_length = self.read_number(message, cursor + 1)
                cursor = self.skip_newline(message, cursor)
                cursor, data = self.read_bytes(message, data_length, cursor)
                commands.append(Command("RDB", [data]))
            else:
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

    def __init__(self, command, data=None, size=0):
        self.command = command
        if data:
            self.data = data
        else:
            self.data = []
        self.size = size
        self.raw = b""

    def add_data(self, data):
        self.data.append(data)

    def get_size(self):
        return self.size

    def set_size(self, size):
        self.size = size

    def set_raw(self, raw):
        self.raw = raw

    def get_raw(self):
        return self.raw

    def __str__(self):
        return f"{self.command} {self.data}"

    def __repr__(self):
        return f"{self.command} {self.data}"

    def __eq__(self, other):
        return other and self.command == other.command and self.data == other.data
