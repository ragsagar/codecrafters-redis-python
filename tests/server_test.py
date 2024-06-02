import unittest

from app.server import RedisServer


class TestServer(unittest.TestCase):
    def setUp(self) -> None:
        self.server = RedisServer(rdb_dir="/tmp", rdb_filename="dump.rdb")

    def test_init_dir(self):
        self.assertEqual(self.server.get_rdb_dir(), "/tmp")

    def test_init_filename(self):
        self.assertEqual(self.server.get_rdb_filename(), "dump.rdb")
