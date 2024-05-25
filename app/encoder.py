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
      return b"+OK\r\n"
