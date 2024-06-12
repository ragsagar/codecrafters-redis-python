import datetime
import uuid
import sys


class ZeroIdentifier(Exception):
    pass


class KeyValueStore:
    def __init__(self):
        self.data = {}

    def get(self, key):
        return self.data.get(key)

    def set(self, key, value, expiry_milliseconds=None):
        print("Setting data in store", key, value, expiry_milliseconds)
        expiry_time = None
        if expiry_milliseconds:
            expiry_time = datetime.datetime.now() + datetime.timedelta(
                milliseconds=expiry_milliseconds
            )
        self.set_with_expiry_time(key, value, expiry_time)

    def set_with_expiry_time(self, key, value, expiry_time):
        if expiry_time:
            if expiry_time < datetime.datetime.now():
                return
        self.data[key] = {"value": value, "expiry_time": expiry_time, "type": "string"}

    def add_stream_data(self, key, values=[], identifier=None, expiry_time=None):
        self.validate_stream_identifier(key, identifier)
        identifier = self.generate_stream_identifier(key, identifier)
        if key in self.data:
            self.data[key]["value"].append(
                {"values": values, "identifier": identifier, "expiry_time": expiry_time}
            )
            self.data[key]["last_identifier"] = identifier
        else:
            self.data[key] = {
                "value": [{"values": values, "identifier": identifier}],
                "expiry_time": expiry_time,
                "type": "stream",
                "last_identifier": identifier,
            }
        return identifier

    def validate_stream_identifier(self, key, identifier):
        if identifier == "*":
            return
        if "-" not in identifier:
            raise ValueError("Invalid stream identifier")
        millisecs, sequence = identifier.split("-")
        if not (millisecs.isdigit() or millisecs == "*") or not (
            sequence.isdigit() or sequence == "*"
        ):
            raise ValueError("Identifier not valid number")
        if not millisecs or not sequence:
            raise ValueError(
                "Couldn't find identifier or sequence in stream identifier"
            )
        if millisecs == "*" and sequence == "*":
            raise ValueError("Both identifier and sequence can't be *")

        converted_millis = sys.maxsize
        if millisecs != "*":
            converted_millis = int(millisecs)

        converted_sequence = sys.maxsize
        if sequence != "*":
            converted_sequence = int(sequence)

        if converted_millis == 0 and converted_sequence == 0:
            raise ZeroIdentifier(
                "ERR The ID specified in XADD must be greater than 0-0"
            )
        if key in self.data:
            last_identifier = self.data[key]["last_identifier"]
            existing_millis, existing_seq = map(int, last_identifier.split("-"))
            if (
                converted_millis == existing_millis
                and converted_sequence == existing_seq
            ):
                raise ValueError("Identifier already exists")
            if converted_millis < existing_millis or (
                converted_millis == existing_millis
                and converted_sequence <= existing_seq
            ):
                raise ValueError("Lower than existing identifier")

    def get_timestamp_in_millis(self):
        return int(datetime.datetime.now().timestamp() * 1000)

    def generate_stream_identifier(self, key, identifier):
        if "*" not in identifier:
            return identifier

        # if identifier is *, then generate new millis part and sequence part should start from 0
        if identifier == "*":
            milli_part = self.get_timestamp_in_millis()
            seq_part = 0
            return f"{milli_part}-{seq_part}"

        given_milli_str, given_seq_str = identifier.split("-")

        # if millis part is *, generate millis from current timestamp and use given sequence part
        if given_milli_str == "*":
            milli_part = int(datetime.datetime.now().timestamp() * 1000)
            return f"{milli_part}-{given_seq_str}"

        # if sequence part is *
        # If given millis matches with last millis then sequence part should be last sequence part + 1
        # else sequence should start from 0
        # if given millis is 0, then sequence part should start from 1
        if given_seq_str == "*":
            last_millis = None
            if key in self.data:
                last_millis, last_sequence = map(
                    int, self.data[key]["last_identifier"].split("-")
                )

            millis_part = int(given_milli_str)
            if millis_part == last_millis:
                seq_part = last_sequence + 1
            elif millis_part == 0:
                seq_part = 1
            else:
                seq_part = 0

            return f"{millis_part}-{seq_part}"

    def get_stream_range(self, key, start, end):
        if key in self.data and self.data[key]["type"] == "stream":
            try:
                start_millis, start_seq = map(int, start.split("-"))
            except ValueError:
                start_millis, start_seq = int(start.split("-")[0]), -1
            try:
                end_millis, end_seq = map(int, end.split("-"))
            except ValueError:
                end_millis, end_seq = int(end.split("-")[0]), sys.maxsize
            stream_data = self.data[key]["value"]
            result = []
            for data in stream_data:
                millis, seq = map(int, data["identifier"].split("-"))
                if start_millis <= millis <= end_millis:
                    if start_millis == millis and seq < start_seq:
                        continue
                    if end_millis == millis and seq > end_seq:
                        continue
                    result.append([data["identifier"], data["values"]])
            if result:
                print("Stream result", result)
                return result
        return None

    def expire_data(self):
        for key in list(self.data.keys()):
            value = self.data[key]
            if value["expiry_time"] and value["expiry_time"] < datetime.datetime.now():
                del self.data[key]

    def get(self, key):
        self.expire_data()
        if key not in self.data:
            return None
        return self.data[key]["value"]

    def get_type(self, key):
        if key not in self.data:
            return None
        else:
            return self.data[key]["type"]

    def get_keys(self):
        return list(self.data.keys())

    def get_time_from_epoch(self, epoch):
        expiry_time = datetime.datetime.fromtimestamp(epoch / 1000)
        return expiry_time

    def load_data_from_rdb(self, rdb):
        for db in rdb.data.values():
            for kv in db:
                expiry = None
                if kv.expiry:
                    expiry = self.get_time_from_epoch(kv.expiry)
                self.set_with_expiry_time(kv.key, kv.value, expiry)
