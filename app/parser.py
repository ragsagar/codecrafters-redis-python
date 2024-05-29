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
        parts = message.split(b"\r\n")
        message_length = 0
        command = None
        commands = []
        for part in parts:
            if message_length < 0:
                raise ValueError("Invalid message", parts)
            if part.startswith(b"$"):
                # byte length
                continue
            if self.state == self.State.START:
                if part.startswith(b"*"):
                    message_length = int(part[1:])
                    self.state = self.State.COMMAND
                elif part.startswith(b"+"):
                    command = Command(part[1:].decode())
                    commands.append(command)
                    self.state = self.State.START
                    command = None
            elif self.state == self.State.COMMAND:
                message_length -= 1
                command = Command(part.decode())
                self.state = self.State.DATA
            elif self.state == self.State.DATA:
                message_length -= 1
                command.add_data(part.decode())
                if message_length == 1:
                    self.state = self.State.DONE
            elif self.state == self.State.DONE:
                command.add_data(part.decode())
                commands.append(command)
                self.state = self.State.START
                command = None
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
        return self.command == other.command and self.data == other.data


def run_tests():
    print("Running tests")
    parser = RespParser()
    msg1 = b"*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n"
    resp1 = parser.parse(msg1)
    assert resp1[0] == Command("SET", ["mykey", "myvalue"])

    msg2 = b"*3\r\n$3\r\nSET\r\n$3\r\nbar\r\n$3\r\n456\r\n*3\r\n$3\r\nSET\r\n$3\r\nbaz\r\n$3\r\n789\r\n"
    parser = RespParser()
    resp2 = parser.parse(msg2)
    assert resp2[0] == Command("SET", ["bar", "456"])
    assert resp2[1] == Command("SET", ["baz", "789"])
    assert len(resp2) == 2

    msg3 = b"+PONG\r\n"
    parser = RespParser()
    resp3 = parser.parse(msg3)
    assert resp3[0] == Command("PONG")

    msg4 = b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n"
    resp4 = RespParser().parse(msg4)
    assert resp4[0] == Command("GET", ["foo"])

    print("All tests passed")


if __name__ == "__main__":
    run_tests()
