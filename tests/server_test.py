import io
import unittest

from app.server import RedisServer


class TestServer(unittest.TestCase):
    def setUp(self) -> None:
        self.rdb_file_obj = io.StringIO("REDIS0009")
        self.server = RedisServer(rdb_file_obj=self.rdb_file_obj)

    # def test_init_dir(self):
    #     self.assertEqual(self.server.get_rdb_dir(), "/tmp")

    # def test_init_filename(self):
    #     self.assertEqual(self.server.get_rdb_filename(), "dump.rdb")
