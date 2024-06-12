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
