import sys
from enum import Enum


class RdbData:
    version = None
    db_number = None

    def init(self):
        self.data = {}

    def set_version(self, version):
        self.version = version

    def set_db_number(self, db_number):
        self.db_number = db_number


class InvalidRdbFileException(Exception):
    pass


class RdbParser:
    class State(Enum):
        INIT = "init"
        VALID_RDB = "valid_rdb"
        READ_KEYS = "read_keys"
        DATA = "data"
        DONE = "done"

    state = State.INIT
    DATABASE_SELECTOR = 0xFD
    MAGIC_BYTES = [ord("R"), ord("E"), ord("D"), ord("I"), ord("S")]
    MILLIS_EXPIRATION = 0xFC
    SECOND_EXPIRATION = 0xFD

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
        integer = int.from_bytes(integer_bytes, byteorder="big")
        return cursor + length, integer

    def read_version(self, cursor, data):
        return self.read_number(cursor, data, 4)

    def read_db_number(self, cursor, data):
        cursor += 1
        return cursor + 1, int(data[cursor])

    def read_seconds(self, cursor, data):
        return self.read_number_from_bytes(cursor, data, 4)

    def read_milliseconds(self, cursor, data):
        return self.read_number_from_bytes(cursor, data, 8)

    def convert_sec_to_milli(self, seconds):
        return seconds * 1000

    def get_two_most_signifcant_bit(self, byte):
        return (byte & 0xC0) >> 6

    def get_last_six_bits(self, byte):
        return byte & 0x3F

    def read_length(self, cursor, data):
        decision_bits = self.get_two_most_signifcant_bit(data[cursor])
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
        raise Exception("Invalid length")

    def read_string_encoding(self, cursor, data):
        cursor, length = self.read_length(cursor, data)
        key = "".join([chr(i) for i in data[cursor : cursor + length]])
        return cursor + length, key

    def read_keys(self, cursor, data):
        if data[cursor] == self.SECOND_EXPIRATION:
            cursor += 1
            cursor, seconds = self.read_seconds(cursor, data)
            milliseconds = self.convert_sec_to_milli(seconds)
            cursor, key = self.read_string_encoding(cursor, data)
            cursor += 1
            cursor, value = self.read_string_encoding(cursor, data)
            print("Key:", key, "Value:", value)
        elif data[cursor] == self.MILLIS_EXPIRATION:
            cursor += 1
            cursor, milliseconds = self.read_milliseconds(cursor, data)
            cursor += 1
            value_type = data[cursor]
            cursor += 1
            cursor, key = self.read_string_encoding(cursor, data)
            cursor += 1
            cursor, value = self.read_string_encoding(cursor, data)
            print("Key:", key, "Value:", value, "type", value_type)
        else:
            cursor, key = self.read_string_encoding(cursor, data)
            cursor += 1
            cursor, value = self.read_string_encoding(cursor, data)
            print("Key:", key, "Value:", value)

    # data is a bytes class
    def parse(self, data):
        cursor = 0
        # data_bytes = [i.to_bytes(1, sys.byteorder) for i in data]

        rdb_data = RdbData()
        cursor = self.check_magic_bytes(cursor, data)
        cursor, version = self.read_version(cursor, data)
        print("RDB Version:", version)
        while cursor < len(data):
            if data[cursor] == self.DATABASE_SELECTOR:
                cursor, db_number = self.read_db_number(cursor, data)
                print("Database number", db_number)
                cursor, key_values = self.read_keys(cursor, data)
                # Read key value pairs
            cursor += 1
        return rdb_data

    def parse_string(self, data):
        length = int(data[0])
        string_data = data[len(str(length)) + 1 : len(str(length)) + 1 + length]
        return string_data

    def parse_integer(self, data):
        pass

    def get_key_values(self):
        pass
