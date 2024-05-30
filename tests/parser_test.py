import unittest

from app.parser import RespParser, Command


class TestRespParser(unittest.TestCase):
    def test_parse_single_set_command(self):
        msg = b"*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n"
        resp = RespParser().parse(msg)
        self.assertEqual(resp, [Command("SET", ["mykey", "myvalue"])])

    def test_parse_multiple_set_command(self):
        msg = b"*3\r\n$3\r\nSET\r\n$3\r\nbar\r\n$3\r\n456\r\n*3\r\n$3\r\nSET\r\n$3\r\nbaz\r\n$3\r\n789\r\n"
        resp = RespParser().parse(msg)
        self.assertEqual(
            resp, [Command("SET", ["bar", "456"]), Command("SET", ["baz", "789"])]
        )

    def test_parse_ping_command(self):
        msg = b"*1\r\n$4\r\nPING\r\n"
        resp = RespParser().parse(msg)
        self.assertEqual(resp, [Command("PING")])

    def test_parse_pong_command(self):
        msg = b"+PONG\r\n"
        resp = RespParser().parse(msg)
        self.assertEqual(resp, [Command("PONG")])

    def test_parse_get_command(self):
        msg = b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n"
        resp = RespParser().parse(msg)
        self.assertEqual(resp, [Command("GET", ["foo"])])
