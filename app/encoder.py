class Encoder:
    def _construct_line(self, message):
        return f"${len(message)}\r\n{message}\r\n"

    def generate_bulkstring(self, message):
        return self._construct_line(message).encode()

    def generate_simple_strings(self, messages):
        return "".join([self._construct_line(message) for message in messages]).encode()

    def generate_array_string(self, messages):
        return f"*{len(messages)}\r\n{''.join([self._construct_line(message) for message in messages])}".encode()

    def generate_null_string(self):
        return b"$-1\r\n"

    def generate_success_string(self):
        return self.generate_simple_string("OK")

    def generate_simple_string(self, message):
        return f"+{message}\r\n".encode()

    def generate_file_string(self, hex_contents):
        result_string = "".join(
            [
                chr(int(hex_contents[i : i + 2], 16))
                for i in range(0, len(hex_contents), 2)
            ]
        )
        byte_contents = bytes.fromhex(hex_contents)
        print("File contents", hex_contents)
        return f"${len(result_string)}\r\n{result_string}".encode()
