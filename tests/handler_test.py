import unittest
from collections import namedtuple

from app.handler import CommandHandler
from app.encoder import Encoder

DataBuffer = namedtuple("DataBuffer", ["outb"])


class DummyServer:
    def __init__(self) -> None:
        self.encoder = Encoder()

    def set_data(self, key, value, expiry):
        pass


class TestCommandHandler(unittest.TestCase):

    def setUp(self) -> None:
        self.handler = CommandHandler(DummyServer())
        self.sock = None

    def create_data(self, msg):
        return DataBuffer(outb=msg)

    def test_handle_set(self):
        msg = b"*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n"
        res = self.handler.handle_message(self.create_data(msg), self.sock)
        self.assertEqual(res, b"+OK\r\n")

    def test_handle_set_multiple(self):
        msg = b"*3\r\n$3\r\nSET\r\n$3\r\nbar\r\n$3\r\n456\r\n*3\r\n$3\r\nSET\r\n$3\r\nbaz\r\n$3\r\n789\r\n"
        res = self.handler.handle_message(self.create_data(msg), self.sock)
        self.assertEqual(res, b"+OK\r\n+OK\r\n")

    def test_handle_ping(self):
        msg = b"*1\r\n$4\r\nPING\r\n"
        res = self.handler.handle_message(self.create_data(msg), self.sock)
        self.assertEqual(res, b"$4\r\nPONG\r\n")

    def test_handle_ok(self):
        msg = b"+OK\r\n"
        res = self.handler.handle_message(self.create_data(msg), self.sock)
        self.assertEqual(res, b"")
