import sys
from enum import Enum
from collections import defaultdict


class RdbData:
    version = None

    def __init__(self):
        self.data = {}
        self.current_selector = None

    def set_version(self, version):
        self.version = version

    def add_database(self, number):
        print(self.data, number)
        self.data[number] = []
        self.current_selector = number

    def add_key_value(self, kv):
        self.data[self.current_selector].append(kv)

    def __str__(self):
        return f"RDBData: {self.version} {self.data}"


class InvalidRdbFileException(Exception):
    pass


class RdbParser:
    class State(Enum):
        INIT = "init"
        READ_KEYS = "read_keys"
        DONE = "done"

    state = State.INIT
    DATABASE_SELECTOR = 0xFE
    MAGIC_BYTES = [ord("R"), ord("E"), ord("D"), ord("I"), ord("S")]
    MILLIS_EXPIRATION = 0xFC
    SECOND_EXPIRATION = 0xFD
    RDB_FILE_END = 0xFF
    STRING_TYPE = 0x0
    RESIZEDB_BYTE = 0xFB

    def check_magic_bytes(self, cursor, data):
        for byte in self.MAGIC_BYTES:
            if data[cursor] != byte:
                raise InvalidRdbFileException(f"Invalid bytes found: {data[0:cursor]}")
            cursor += 1
        return cursor

    def read_number(self, cursor, data, length):
        integer_bytes = data[cursor : cursor + length]
        integer = int("".join(chr(b) for b in integer_bytes))
        return cursor + length, integer

    def read_number_from_bytes(self, cursor, data, length):
        integer_bytes = data[cursor : cursor + length]
        integer = int.from_bytes(integer_bytes, byteorder="little")
        return cursor + length, integer

    def read_version(self, cursor, data):
        return self.read_number(cursor, data, 4)

    def read_db_number(self, cursor, data):
        cursor += 1
        print("Reading db number")
        return self.read_length(cursor, data)

    def read_seconds(self, cursor, data):
        return self.read_number_from_bytes(cursor, data, 4)

    def read_milliseconds(self, cursor, data):
        print("Read millis", cursor)
        return self.read_number_from_bytes(cursor, data, 8)
        # print("Reading milliseconds")
        # return self.read_length(cursor, data)

    def convert_sec_to_milli(self, seconds):
        return seconds * 1000

    def get_two_most_signifcant_bit(self, byte):
        return (byte & 0xC0) >> 6

    def get_last_six_bits(self, byte):
        return byte & 0x3F

    def read_length(self, cursor, data):
        decision_bits = self.get_two_most_signifcant_bit(data[cursor])
        print(
            "decision bits", decision_bits, "cursor", cursor, "data", chr(data[cursor])
        )
        if decision_bits == 0:
            # Take the next 6 bits as the length
            print("Reading lenght from next 6 bits")
            length = self.get_last_six_bits(data[cursor])
            return cursor + 1, length
        elif decision_bits == 1:
            # Take the next 14 bits as the length
            first_six_bits = self.get_last_six_bits(data[cursor]) << 8
            length = first_six_bits + int(data[cursor + 1])
            return cursor + 2, length
        elif decision_bits == 2:
            cursor += 1
            return self.read_number(cursor, data, 4)
        elif decision_bits == 3:
            print("Special decision bit", 3)
            print("Last six bits", self.get_last_six_bits(data[cursor]))
            cursor += 1
            return self.read_number(cursor, data, 8)
        raise Exception("Invalid length")

    def read_string_encoding(self, cursor, data):
        cursor, length = self.read_length(cursor, data)
        key = "".join([chr(i) for i in data[cursor : cursor + length]])
        return cursor + length, key

    def read_value(self, cursor, data, value_type):
        if value_type == self.STRING_TYPE:
            return self.read_string_encoding(cursor, data)
        return None

    def read_key_value(self, cursor, data):
        expiry_millis = None
        kv = KeyValue()
        if data[cursor] == self.SECOND_EXPIRATION:
            cursor += 1
            cursor, seconds = self.read_seconds(cursor, data)
            expiry_millis = self.convert_sec_to_milli(seconds)
            value_type = data[cursor]
            cursor, key = self.read_string_encoding(cursor, data)
            cursor += 1
            cursor, value = self.read_string_encoding(cursor, data)
            kv.set_key_value(key, value, value_type)
            kv.set_expiry_seconds(seconds)
        elif data[cursor] == self.MILLIS_EXPIRATION:
            cursor += 1
            cursor, expiry_millis = self.read_milliseconds(cursor, data)
            value_type = data[cursor]
            cursor += 1
            cursor, key = self.read_string_encoding(cursor, data)
            cursor, value = self.read_string_encoding(cursor, data)
            kv.set_key_value(key, value, value_type)
            kv.set_expiry_milliseconds(expiry_millis)
        else:
            value_type = data[cursor]
            cursor += 1
            cursor, key = self.read_string_encoding(cursor, data)
            cursor, value = self.read_string_encoding(cursor, data)
            kv.set_key_value(key, value, value_type)
        return cursor, kv

    # data is a bytes class
    def parse(self, data):
        print("Parsing data", data)
        cursor = 0
        # data_bytes = [i.to_bytes(1, sys.byteorder) for i in data]
        rdb = RdbData()
        cursor = self.check_magic_bytes(cursor, data)
        cursor, version = self.read_version(cursor, data)
        rdb.set_version(version)
        print("RDB Version:", version, cursor, data[cursor])
        state = self.State.INIT
        while cursor < len(data) and self.state != self.State.DONE:
            if data[cursor] == self.DATABASE_SELECTOR:
                cursor, db_number = self.read_db_number(cursor, data)
                print("Database number", db_number)
                rdb.add_database(db_number)
                if data[cursor] == self.RESIZEDB_BYTE:
                    print("Found resize db byte")
                    print(f"Cursor before reading length 1: {cursor}")
                    cursor, db_hash_size = self.read_length(cursor + 1, data)
                    print(f"Cursor before reading length 2: {cursor}")
                    cursor, expiry_hash_size = self.read_length(cursor, data)
                    print(
                        f"Db hash size {db_hash_size} expiry hash size {expiry_hash_size}, cursor: {cursor}"
                    )
                # Read key value pairs
                state = self.State.READ_KEYS
            elif data[cursor] == self.RDB_FILE_END:
                state = self.State.DONE
                break
            elif state == self.State.READ_KEYS:
                print("Reading keys", cursor, data[cursor])
                cursor, kv = self.read_key_value(cursor, data)
                rdb.add_key_value(kv)
            else:
                cursor += 1
        return rdb


class KeyValue:
    def __init__(self, key=None, value=None, expiry=None):
        self.key = key
        self.value = value
        self.expiry = expiry
        self.data_type = None

    def set_data_type(self, data_type):
        self.data_type = data_type

    def set_expiry_seconds(self, seconds):
        self.expiry = self.convert_sec_to_milli(seconds)

    def set_expiry_milliseconds(self, millis):
        self.expiry = millis

    def convert_sec_to_milli(self, seconds):
        return seconds * 1000

    def set_key_value(self, key, value, data_type):
        print(f"Key: {key}, Value: {value}")
        self.key = key
        self.value = value
        self.data_type = data_type

    def __str__(self):
        return f"<KV {self.key}: {self.value} ({self.expiry})>"
