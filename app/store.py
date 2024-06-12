import datetime
import uuid


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
        if "-" not in identifier:
            raise ValueError("Invalid stream identifier")
        millisecs, sequence = identifier.split("-")
        if not millisecs.isdigit() or not sequence.isdigit():
            raise ValueError("Identifier not valid number")
        if not millisecs or not sequence:
            raise ValueError(
                "Couldn't find identifier or sequence in stream identifier"
            )
        millisecs = int(millisecs)
        sequence = int(sequence)
        if millisecs == 0 and sequence == 0:
            raise ZeroIdentifier(
                "ERR The ID specified in XADD must be greater than 0-0"
            )
        if key in self.data:
            existing_millis, existing_seq = map(
                int, self.data[key]["last_identifier"].split("-")
            )
            if millisecs < existing_millis or (
                millisecs == existing_millis and sequence <= existing_seq
            ):
                raise ValueError("Lower than existing identifier")

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
