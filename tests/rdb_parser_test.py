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

    def test_read_milliseconds(self):
        parser = RdbParser()
        cursor, milliseconds = parser.read_milliseconds(12, self.sample_rdb_data)
        self.assertEqual(milliseconds, 1671963072573)
        self.assertEqual(cursor, 20)

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

    def test_parse_with_one_key(self):
        sample = b"REDIS0003\xfa\tredis-ver\x057.2.0\xfa\nredis-bits\xc0@\xfe\x00\xfb\x01\x00\x00\traspberry\nstrawberry\xff\x8b\xeb\x06\x98\x9d\xd1X\xb6\n"
        parser = RdbParser()
        rdb = parser.parse(sample)
        self.assertEqual(rdb.version, 3)
        kv = rdb.data[0][0]
        self.assertEqual(kv.key, "raspberry")
        self.assertEqual(kv.value, "strawberry")
        self.assertEqual(kv.data_type, 0)
        self.assertEqual(len(rdb.data), 1)

    def test_parse_with_expiry(self):
        sample = b"REDIS0003\xfa\tredis-ver\x057.2.0\xfa\nredis-bits\xc0@\xfe\x00\xfb\x05\x05\xfc\x00\x0c(\x8a\xc7\x01\x00\x00\x00\x05grape\x05apple\xfc\x00\x9c\xef\x12~\x01\x00\x00\x00\tblueberry\traspberry\xfc\x00\x0c(\x8a\xc7\x01\x00\x00\x00\nstrawberry\x06banana\xfc\x00\x0c(\x8a\xc7\x01\x00\x00\x00\traspberry\x05grape\xfc\x00\x0c(\x8a\xc7\x01\x00\x00\x00\x04pear\x06orange\xff\xe3\x9e\x877{\xfbW\xd1\n"
        parser = RdbParser()
        rdb = parser.parse(sample)
        self.assertEqual(rdb.version, 3)
        kv = rdb.data[0][0]
        self.assertEqual(kv.key, "grape")
        self.assertEqual(kv.value, "apple")
        self.assertEqual(kv.expiry, 1956528000000)

        kv = rdb.data[0][1]
        self.assertEqual(kv.key, "blueberry")
        self.assertEqual(kv.value, "raspberry")
        self.assertEqual(kv.expiry, 1640995200000)

        kv = rdb.data[0][2]
        self.assertEqual(kv.key, "strawberry")
        self.assertEqual(kv.value, "banana")
        self.assertEqual(kv.expiry, 1956528000000)

        kv = rdb.data[0][3]
        self.assertEqual(kv.key, "raspberry")
        self.assertEqual(kv.value, "grape")
        self.assertEqual(kv.expiry, 1956528000000)

        kv = rdb.data[0][4]
        self.assertEqual(kv.key, "pear")
        self.assertEqual(kv.value, "orange")
        self.assertEqual(kv.expiry, 1956528000000)
