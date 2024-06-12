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

    def test_sequence_part_with_asterisk_on_existing_stream(self):
        store = KeyValueStore()
        store.add_stream_data("stream1", ["value1"], identifier="123-4")
        res = store.add_stream_data("stream1", ["value2"], identifier="123-*")
        self.assertEqual(res, "123-5")

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

    def test_identifier_is_valid_with_seq_asterisk(self):
        store = KeyValueStore()
        res = store.add_stream_data("stream1", ["value2"], identifier="1-*")
        self.assertEqual(res, "1-0")

    def test_identifier_with_seq_asterisk(self):
        store = KeyValueStore()
        res = store.add_stream_data("stream1", ["value2"], identifier="*")
        self.assertIsNotNone(res)


class TestGenerateIdentifier(unittest.TestCase):
    def test_generate_identifier_with_seq_asterisk(self):
        store = KeyValueStore()
        res = store.generate_stream_identifier("stream1", "*")
        self.assertIsNotNone(res)
        self.assertTrue("-" in res)

    def test_generate_identifier_with_time_asterisk(self):
        # Default sequence number should be 0
        store = KeyValueStore()
        res = store.generate_stream_identifier("stream1", "123-*")
        self.assertEqual(res, "123-0")

    def test_generate_seq_with_0_millis(self):
        # Default sequence number should be 0
        store = KeyValueStore()
        res = store.generate_stream_identifier("stream1", "0-*")
        self.assertEqual(res, "0-1")

    def test_generate_increment_last_sequence(self):
        store = KeyValueStore()
        store.add_stream_data("stream1", ["value1"], identifier="123-4")
        res = store.generate_stream_identifier("stream1", "123-*")
        self.assertEqual(res, "123-5")

    def test_seq_start_from_0_for_new_milli(self):
        store = KeyValueStore()
        store.add_stream_data("stream1", ["value1"], identifier="123-4")
        res = store.generate_stream_identifier("stream1", "124-*")
        self.assertEqual(res, "124-0")

    def test_generate_millis_part(self):
        store = KeyValueStore()
        res = store.generate_stream_identifier("stream1", "*-2")
        millis, seq = res.split("-")
        self.assertTrue(millis.isdigit())
        self.assertEqual(seq, "2")


class TestXrange(unittest.TestCase):
    def test_xrange_returns_correct_values(self):
        store = KeyValueStore()
        store.add_stream_data("stream1", ["foo", "bar"], identifier="123-4")
        store.add_stream_data("stream1", ["value2"], identifier="123-5")
        store.add_stream_data("stream1", ["value3"], identifier="123-6")
        res = store.get_stream_range("stream1", "123-4", "123-5")
        self.assertEqual(res, [["123-4", ["foo", "bar"]], ["123-5", ["value2"]]])

    def test_xrange_returns_correct_values_different_millis(self):
        store = KeyValueStore()
        store.add_stream_data("stream1", ["foo", "bar"], identifier="123-4")
        store.add_stream_data("stream1", ["value2"], identifier="123-5")
        store.add_stream_data("stream1", ["value3"], identifier="124-6")
        store.add_stream_data("stream1", ["value4"], identifier="125-6")
        res = store.get_stream_range("stream1", "123-4", "124-7")
        self.assertEqual(
            res,
            [["123-4", ["foo", "bar"]], ["123-5", ["value2"]], ["124-6", ["value3"]]],
        )

    def test_xrange_returns_correct_values_empty_seq(self):
        store = KeyValueStore()
        store.add_stream_data("stream1", ["foo", "bar"], identifier="123-4")
        store.add_stream_data("stream1", ["value2"], identifier="123-5")
        store.add_stream_data("stream1", ["value3"], identifier="124-6")
        store.add_stream_data("stream1", ["value4"], identifier="125-6")
        res = store.get_stream_range("stream1", "123", "124")
        self.assertEqual(
            res,
            [["123-4", ["foo", "bar"]], ["123-5", ["value2"]], ["124-6", ["value3"]]],
        )

    def test_xrange_upstream_test(self):
        store = KeyValueStore()
        store.add_stream_data("raspberry", ["foo", "bar"], identifier="0-1")
        store.add_stream_data("raspberry", ["foo", "bar"], identifier="0-2")
        store.add_stream_data("raspberry", ["foo", "bar"], identifier="0-3")
        res = store.get_stream_range("raspberry", "0-2", "0-3")
        self.assertEqual(
            res,
            [["0-2", ["foo", "bar"]], ["0-3", ["foo", "bar"]]],
        )
