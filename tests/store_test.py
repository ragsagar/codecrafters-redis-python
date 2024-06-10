import unittest

from app.store import KeyValueStore


class TestKeyValueStore(unittest.TestCase):
    def test_get_keys(self):
        store = KeyValueStore()
        store.set("key1", "value1")
        store.set("foo", "bar", 1000)
        self.assertEqual(store.get_keys(), ["key1", "foo"])
