import unittest

from app.store import KeyValueStore
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
