from enum import Enum


class RespParser:
    class State:
        START = 0
        COMMAND = 1
        DATA = 2
        DONE = 3

    def __init__(self) -> None:
        self.state = self.State.START

    def parse(self, message):
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
