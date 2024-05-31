import unittest

from app.parser import RespParser, Command


class TestRespParser(unittest.TestCase):
    def test_parse_single_set_command(self):
        msg = b"*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n"
        resp = RespParser().parse(msg)
        self.assertEqual(resp, [Command("SET", [b"mykey", b"myvalue"])])

    def test_parse_multiple_set_command(self):
        msg = b"*3\r\n$3\r\nSET\r\n$3\r\nbar\r\n$3\r\n456\r\n*3\r\n$3\r\nSET\r\n$3\r\nbaz\r\n$3\r\n789\r\n"
        resp = RespParser().parse(msg)
        self.assertEqual(
            resp, [Command("SET", [b"bar", b"456"]), Command("SET", [b"baz", b"789"])]
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
        self.assertEqual(resp, [Command("GET", [b"foo"])])

    def test_parse_rdb_command(self):
        msg = b"$88\r\nREDIS0011\xfa\tredis-ver\x057.2.0\xfa\nredis-bits\xc0@\xfa\x05ctime\xc2m\x08\xbce\xfa\x08used-mem\xc2\xb0\xc4\x10\x00\xfa\x08aof-base\xc0\x00\xff\xf0n;\xfe\xc0\xffZ\xa2"
        resp = RespParser().parse(msg)
        print("RDB Resp", resp[0])
        self.assertEqual(
            resp,
            [
                Command(
                    "RDB",
                    [
                        b"REDIS0011\xfa\tredis-ver\x057.2.0\xfa\nredis-bits\xc0@\xfa\x05ctime\xc2m\x08\xbce\xfa\x08used-mem\xc2\xb0\xc4\x10\x00\xfa\x08aof-base\xc0\x00\xff\xf0n;\xfe\xc0\xffZ\xa2"
                    ],
                )
            ],
        )

    def test_parse_rdb_with_ack_command(self):
        msg = b"$88\r\nREDIS0011\xfa\tredis-ver\x057.2.0\xfa\nredis-bits\xc0@\xfa\x05ctime\xc2m\x08\xbce\xfa\x08used-mem\xc2\xb0\xc4\x10\x00\xfa\x08aof-base\xc0\x00\xff\xf0n;\xfe\xc0\xffZ\xa2*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n"
        resp = RespParser().parse(msg)
        self.assertEqual(
            resp,
            [
                Command(
                    "RDB",
                    [
                        b"REDIS0011\xfa\tredis-ver\x057.2.0\xfa\nredis-bits\xc0@\xfa\x05ctime\xc2m\x08\xbce\xfa\x08used-mem\xc2\xb0\xc4\x10\x00\xfa\x08aof-base\xc0\x00\xff\xf0n;\xfe\xc0\xffZ\xa2"
                    ],
                ),
                Command("REPLCONF", [b"GETACK", b"*"]),
            ],
        )

    def test_parse_fullresync_command(self):
        msg = b"+FULLRESYNC 75cd7bc10c49047e0d163660f3b90625b1af31dc 0\r\n"
        resp = RespParser().parse(msg)
        self.assertEqual(
            resp,
            [
                Command(
                    "FULLRESYNC", [b"75cd7bc10c49047e0d163660f3b90625b1af31dc", b"0"]
                )
            ],
        )
