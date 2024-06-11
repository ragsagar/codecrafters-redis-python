import datetime
import uuid


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

    def add_stream_data(self, key, values, identifier=None, expiry_time=None):
        if not identifier:
            identifier = uuid.uuid4().hex[:10]
        self.data[key] = {
            "value": values,
            "expiry_time": expiry_time,
            "type": "stream",
            "identifier": identifier,
        }
        return identifier

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
