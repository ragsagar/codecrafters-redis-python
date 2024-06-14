import unittest
from app.encoder import Encoder


class TestEncoder(unittest.TestCase):
    def test_double_string(self):
        encoder = Encoder()
        self.assertEqual(encoder.generate_integer_string(3), b":3\r\n")

    def test_array_encoding(self):
        encoder = Encoder()
        self.assertEqual(
            encoder.generate_array_string(["1", "2", "3"]),
            b"*3\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n",
        )

    def test_nested_1array_encoding(self):
        encoder = Encoder()
        res = encoder.generate_array_string(["1", ["2", "3"]])
        self.assertEqual(
            res,
            b"*2\r\n$1\r\n1\r\n*2\r\n$1\r\n2\r\n$1\r\n3\r\n",
        )

    def test_xadd_array_encoding(self):
        encoder = Encoder()
        input_list = [["stream_key", [["0-2", ["temperature", "96"]]]]]
        res = encoder.generate_array_string(input_list)
        print("Res", res)
        expected = b"*1\r\n*2\r\n$10\r\nstream_key\r\n*1\r\n*2\r\n$3\r\n0-2\r\n*2\r\n$11\r\ntemperature\r\n$2\r\n96\r\n"
        self.assertEqual(res, expected)
