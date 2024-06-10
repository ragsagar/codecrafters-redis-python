import unittest

from app.rdb.parser import RdbParser, InvalidRdbFileException


class RDBParserTest(unittest.TestCase):
    sample_rdb_data = b"REDIS0004\xfe\x00\xfc=\xd8\xc3H\x85\x01\x00\x00\x00\x14expires_ms_precision\x1b2022-12-25 10:11:12.573 UTC\xff"
    sample_invalid_rdb_data = b"REIS0004\xfe\x00\xfc=\xd8\xc3H\x85\x01\x00\x00\x00\x14expires_ms_precision\x1b2022-12-25 10:11:12.573 UTC\xff"

    def test_invalid_rdb_file(self):
        parser = RdbParser()
        self.assertRaises(
            InvalidRdbFileException,
            parser.check_magic_bytes,
            0,
            self.sample_invalid_rdb_data,
        )

    def test_valid_rdb_file(self):
        parser = RdbParser()
        cursor = parser.check_magic_bytes(0, self.sample_rdb_data)
        self.assertEqual(cursor, 5)

    def test_version(self):
        parser = RdbParser()
        cursor, version = parser.read_version(5, self.sample_rdb_data)
        self.assertEqual(cursor, 9)
        self.assertEqual(version, 4)

    def test_read_db_number(self):
        parser = RdbParser()
        cursor, db_number = parser.read_db_number(9, self.sample_rdb_data)
        self.assertEqual(cursor, 11)
        self.assertEqual(db_number, 0)

    # def test_read_milliseconds(self):
    #     parser = RdbParser()
    #     cursor, milliseconds = parser.read_milliseconds(12, self.sample_rdb_data)
    #     self.assertEqual(milliseconds, 1639887072573)
    #     self.assertEqual(cursor, 20)

    def test_read_key_with_expiry(self):
        parser = RdbParser()
        cursor, kv = parser.read_key_value(11, self.sample_rdb_data)
        self.assertEqual(kv.key, "expires_ms_precision")
        self.assertEqual(kv.value, "2022-12-25 10:11:12.573 UTC")
        self.assertEqual(kv.data_type, 0)
        # self.assertEqual(kv.expiry, 0)
        self.assertEqual(cursor, 70)

    def test_read_length(self):
        parser = RdbParser()
        cursor, length = parser.read_length(21, self.sample_rdb_data)
        self.assertEqual(cursor, 22)
        self.assertEqual(length, 20)

    def test_read_string_encoding(self):
        parser = RdbParser()
        cursor, value = parser.read_string_encoding(21, self.sample_rdb_data)
        self.assertEqual(value, "expires_ms_precision")
        self.assertEqual(cursor, 42)

    def test_parse(self):
        parser = RdbParser()
        rdb = parser.parse(self.sample_rdb_data)
        self.assertEqual(rdb.version, 4)
        kv = rdb.data[0][0]
        self.assertEqual(kv.key, "expires_ms_precision")
        self.assertEqual(kv.value, "2022-12-25 10:11:12.573 UTC")
        self.assertEqual(kv.data_type, 0)
        self.assertEqual(len(rdb.data), 1)
