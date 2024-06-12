import unittest

from app.store import KeyValueStore, ZeroIdentifier
from app.rdb.parser import RdbData, KeyValue


class TestKeyValueStore(unittest.TestCase):
    def test_get_keys(self):
        store = KeyValueStore()
        store.set("key1", "value1")
        store.set("foo", "bar", 1000)
        self.assertEqual(store.get_keys(), ["key1", "foo"])

    def test_loading_from_rdb(self):
        rdb = RdbData()
        rdb.add_database(0)
        kv = KeyValue("key1", "value1")
        rdb.add_key_value(kv)
        store = KeyValueStore()
        store.load_data_from_rdb(rdb)
        self.assertEqual(store.get("key1"), "value1")

    def test_loading_from_rdb_with_expiry(self):
        rdb = RdbData()
        rdb.add_database(0)
        kv = KeyValue("key1", "value1", 1640995200000)
        rdb.add_key_value(kv)
        store = KeyValueStore()
        store.load_data_from_rdb(rdb)
        self.assertEqual(store.get("key1"), None)


class TestStreamInKVStore(unittest.TestCase):
    def test_milliseconds_part_greater(self):
        store = KeyValueStore()
        store.add_stream_data("stream1", ["value1"], identifier="123-1")
        with self.assertRaises(ValueError):
            store.add_stream_data("stream1", ["value2"], identifier="121-2")

    def test_sequence_part_greater(self):
        store = KeyValueStore()
        store.add_stream_data("stream1", ["value1"], identifier="123-4")
        with self.assertRaises(ValueError):
            store.add_stream_data("stream1", ["value2"], identifier="123-2")

    def test_identifier_is_valid_non_number(self):
        store = KeyValueStore()
        with self.assertRaises(ValueError):
            store.add_stream_data("stream1", ["value2"], identifier="1123-abc")

    def test_identifier_is_valid_same(self):
        store = KeyValueStore()
        store.add_stream_data("stream1", ["value1"], identifier="123-4")
        with self.assertRaises(ValueError):
            store.add_stream_data("stream1", ["value2"], identifier="123-4")

    def test_identifier_is_valid_no_hyphen(self):
        store = KeyValueStore()
        with self.assertRaises(ValueError):
            store.add_stream_data("stream1", ["value2"], identifier="11232323")

    def test_identifier_is_valid_zero_id(self):
        store = KeyValueStore()
        with self.assertRaises(ZeroIdentifier):
            store.add_stream_data("stream1", ["value2"], identifier="0-0")
