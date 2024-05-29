import datetime


class KeyValueStore:
    def __init__(self):
        self.data = {}

    def get(self, key):
        return self.data.get(key)

    def set(self, key, value, expiry_milliseconds=None):
        if expiry_milliseconds:
            expiry_time = datetime.datetime.now() + datetime.timedelta(
                milliseconds=expiry_milliseconds
            )
        self.data[key] = {
            "value": value,
            "expiry_time": expiry_time,
        }

    def expire_data(self):
        print("Expiring data in store")
        for key, value in self.data.items():
            if value["expiry_time"] and value["expiry_time"] < datetime.datetime.now():
                del self.data[key]
