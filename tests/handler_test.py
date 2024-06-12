import unittest
from collections import namedtuple

from app.handler import CommandHandler
from app.encoder import Encoder
from app.store import KeyValueStore

DataBuffer = namedtuple("DataBuffer", ["outb"])


class DummyServer:
    def __init__(self) -> None:
        self.encoder = Encoder()

    def set_data(self, key, value, expiry):
        pass

    def is_write_command(self, command):
        return False

    def get_rdb_dir(self):
        return "/tmp/redis-files"

    def get_rdb_filename(self):
        return "rdbfile"


class TestCommandHandler(unittest.TestCase):

    def setUp(self) -> None:
        self.store = KeyValueStore()
        self.handler = CommandHandler(DummyServer(), store=self.store)
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

    def test_handle_config_dir(self):
        msg = b"*3\r\n$6\r\nCONFIG\r\n$3\r\nGET\r\n$3\r\ndir\r\n"
        res = self.handler.handle_message(self.create_data(msg), self.sock)
        self.assertEqual(res, b"*2\r\n$3\r\ndir\r\n$16\r\n/tmp/redis-files\r\n")

    def test_handle_config_rdbfile(self):
        msg = b"*3\r\n$6\r\nCONFIG\r\n$3\r\nGET\r\n$10\r\ndbfilename\r\n"
        res = self.handler.handle_message(self.create_data(msg), self.sock)
        self.assertEqual(res, b"*2\r\n$10\r\ndbfilename\r\n$7\r\nrdbfile\r\n")

    def test_handle_empty_keys(self):
        msg = b"*2\r\n$4\r\nKEYS\r\n$1\r\n*\r\n"
        res = self.handler.handle_message(self.create_data(msg), self.sock)
        self.assertEqual(res, b"*0\r\n")

    def test_handle_keys(self):
        msg = b"*2\r\n$4\r\nKEYS\r\n$1\r\n*\r\n"
        self.store.set("foo", "bar")
        res = self.handler.handle_message(self.create_data(msg), self.sock)
        self.assertEqual(res, b"*1\r\n$3\r\nfoo\r\n")


class HandleXRangeTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.store = KeyValueStore()
        self.handler = CommandHandler(DummyServer(), store=self.store)
        self.sock = None

    def create_data(self, msg):
        return DataBuffer(outb=msg)

    def test_xrange_returns_correct_values(self):
        self.store.add_stream_data("stream1", ["value1"], identifier="0-1")
        self.store.add_stream_data("stream1", ["value2"], identifier="0-2")
        self.store.add_stream_data("stream1", ["value3"], identifier="0-3")
        msg = b"*4\r\n$6\r\nxrange\r\n$7\r\nstream1\r\n$3\r\n0-2\r\n$3\r\n0-4\r\n"
        res = self.handler.handle_message(self.create_data(msg), self.sock)
        expected = b"*2\r\n*2\r\n$3\r\n0-2\r\n*1\r\n$6\r\nvalue2\r\n*2\r\n$3\r\n0-3\r\n*1\r\n$6\r\nvalue3\r\n"
        self.assertEqual(res, expected)
