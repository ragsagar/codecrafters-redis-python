import socket
import selectors
import types
import datetime
import uuid
from enum import Enum

sel = selectors.DefaultSelector()

class ServerType(Enum):
  MASTER = 'master'
  SLAVE = 'slave'

class RedisServer:
  master_server = None
  master_port = None
  debug = True
  server_type = ServerType.MASTER
  
  def __init__(self, port=6379, master_server=None, master_port=None, debug=True):
      self.port = port
      self.server_socket = None
      self.master_server = master_server
      self.master_port = master_port
      self.server_type = ServerType.SLAVE if master_server else ServerType.MASTER
      self.debug = debug

  def get_server_type(self):
    return self.server_type
    
  def log(self, message, *args):
      if self.debug:
          print(message, *args)

  def get_repl_offset(self):
      return 0

  def get_replid(self):
      return uuid.uuid4().hex

  def parse_message(self, message):
      parts = message.strip().split(b"\r\n")
      length = int(parts[0][1:])
      commands = []
      for i in range(length):
          commands.append(parts[i*2+2].decode())
      return commands

  def _construct_line(self, message):
      return f"${len(message)}\r\n{message}\r\n"

  def encode_command(self, message):
      return self._construct_line(message).encode()
  
  def encode_commands(self, messages):
      return "".join([self._construct_line(message) for message in messages])

  def get_null_message(self):
      return b"$-1\r\n"

  def get_success_message(self):
      return b"+OK\r\n"

  def expire_data(self, data):
      current_time = datetime.datetime.now()
      self.log(f"Expiring data at time {current_time}, {data.map_store}")
      for key in list(data.map_store.keys()):
          obj = data.map_store[key]
          if obj["expiry_time"] is not None and obj["expiry_time"] < current_time:
              self.log(f"Expiring key {key}")
              del data.map_store[key]

  def accept_wrapper(self, server_socket):
      client_socket, addr = server_socket.accept()
      self.log(f"Accepted connection from {addr}")
      client_socket.setblocking(False)
      data = types.SimpleNamespace(addr=addr, inb=b"", outb=b"", map_store={})
      sel.register(client_socket, selectors.EVENT_READ | selectors.EVENT_WRITE, data=data)

  def handle_set_command(self, data, incoming):
      key = incoming[1]
      value = incoming[2]
      expiry_time = None
      if len(incoming) > 4:
          expiry_command = incoming[3]
          if expiry_command.upper() == "PX":
              expiry_value = int(incoming[4])
              expiry_time = datetime.datetime.now() + datetime.timedelta(milliseconds=expiry_value)
      self.log(f"Setting key {key} to value {value} with expiry time {expiry_time}")
      data.map_store[key] = {"value": value, "expiry_time": expiry_time}
      data.outb = b''
      return self.get_success_message()

  def handle_get_command(self, data, incoming):
      key = incoming[1]
      if key in data.map_store:
          response_msg = self.encode_command(data.map_store[key]["value"])
      else:
          response_msg = self.get_null_message()
      return response_msg

  def service_connection(self, key, mask):
      sock = key.fileobj
      data = key.data
      if mask & selectors.EVENT_READ:
          recv_data = sock.recv(1024)
          if recv_data:
              data.outb += recv_data
              self.log("Received", repr(recv_data), "from", data.addr)
          else:
              self.log("Closing connection to", data.addr)
              sel.unregister(sock)
              sock.close()
      if mask & selectors.EVENT_WRITE:
          if data.outb:
              self.expire_data(data)
              incoming = self.parse_message(data.outb)
              command = incoming[0].upper()
              if command == "PING":
                  sock.sendall(self.encode_command("PONG"))
              elif command == "ECHO":
                  echo_message = incoming[1]
                  sock.sendall(self.encode_command(echo_message))
              elif command == "GET":
                  response_msg = self.handle_get_command(data, incoming)
                  sock.sendall(response_msg)
              elif command == 'SET':
                  response_msg = self.handle_set_command(data, incoming)
                  sock.sendall(response_msg)
              elif command == 'INFO':
                  if incoming[1].upper() == "REPLICATION":
                      server_type = self.get_server_type()
                      if server_type == ServerType.MASTER:
                          messages = [
                              f"role:{server_type}",
                              f"master_replid:{self.get_replid()}",
                              f"master_repl_offset:{self.get_repl_offset()}"
                          ]
                          response_msg = self.encode_commands(messages)
                      else:
                          response_msg = self.encode_command(f"role:{server_type}")
                  else:
                      response_msg = self.encode_command("redis_version:0.0.1")
                  sock.sendall(response_msg)
              data.outb = b''

  def initialize_server(self):
      self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
      self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
      self.server_socket.bind(('localhost', self.port))
      self.server_socket.listen()
      self.log(f"Listening on port {self.port}")
      self.server_socket.setblocking(False)
      sel.register(self.server_socket, selectors.EVENT_READ, data=None)

  def handle_server(self):
      try:
          while True:
              events = sel.select(timeout=None)
              for key, mask in events:
                  if key.data is None:
                      self.accept_wrapper(key.fileobj)
                  else:
                      self.service_connection(key, mask)
      finally:
          sel.close()
          self.server_socket.close()

  def run(self):
      self.initialize_server()
      self.handle_server()