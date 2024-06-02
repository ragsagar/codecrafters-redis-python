import unittest
from app.encoder import Encoder


class TestEncoder(unittest.TestCase):
    def test_double_string(self):
        encoder = Encoder()
        self.assertEqual(encoder.generate_integer_string(3), b":3\r\n")
