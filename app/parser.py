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
        tokens = message.strip().split(b"\r\n")
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
                    message_length = int(token[1:])
                    self.state = self.State.COMMAND
                elif token.startswith(b"+"):
                    command = Command(token[1:].decode())
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


def run_tests():
    print("Running tests")
    msg1 = b"*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n"
    resp1 = RespParser().parse(msg1)
    assert resp1[0] == Command("SET", ["mykey", "myvalue"])

    msg2 = b"*3\r\n$3\r\nSET\r\n$3\r\nbar\r\n$3\r\n456\r\n*3\r\n$3\r\nSET\r\n$3\r\nbaz\r\n$3\r\n789\r\n"
    resp2 = RespParser().parse(msg2)
    print("Resp2", resp2)
    assert resp2[0] == Command("SET", ["bar", "456"])
    assert resp2[1] == Command("SET", ["baz", "789"])
    assert len(resp2) == 2

    msg3 = b"+PONG\r\n"
    resp3 = RespParser().parse(msg3)
    assert resp3[0] == Command("PONG")

    msg4 = b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n"
    resp4 = RespParser().parse(msg4)
    assert resp4[0] == Command("GET", ["foo"])

    msg5 = b"*1\r\n$4\r\nPING\r\n"
    resp5 = RespParser().parse(msg5)
    assert resp5[0] == Command("PING")

    print("All tests passed")


if __name__ == "__main__":
    run_tests()
