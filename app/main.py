import socket
import selectors
import types
import time

sel = selectors.DefaultSelector()

def parse_message(message):
    parts = message.strip().split(b"\r\n")
    length = int(parts[0][1:])
    commands = []
    for i in range(length):
        commands.append(parts[i*2+2].decode())
    return commands

def encode_command(message):
    return f"${len(message)}\r\n{message}\r\n".encode()

def get_null_message():
    return b"$-1\r\n"

def get_success_message():
    return b"+OK\r\n"

def expire_data(data):
    current_time = time.time()
    for key, obj in data.map_store.items():
        if obj["expiry_time"] is not None and obj["expiry_time"] < current_time:
            del data.map_store[key]

def accept_wrapper(server_socket):
    client_socket, addr = server_socket.accept()
    print(f"Accepted connection from {addr}")
    client_socket.setblocking(False)
    data = types.SimpleNamespace(addr=addr, inb=b"", outb=b"", map_store={})
    sel.register(client_socket, selectors.EVENT_READ | selectors.EVENT_WRITE, data=data)

def service_connection(key, mask):
    sock = key.fileobj
    data = key.data
    if mask & selectors.EVENT_READ:
        recv_data = sock.recv(1024)
        if recv_data:
            data.outb += recv_data
            print("Received", repr(recv_data), "from", data.addr)
        else:
            print("Closing connection to", data.addr)
            sel.unregister(sock)
            sock.close()
    if mask & selectors.EVENT_WRITE:
        if data.outb:
            expire_data(data)
            incoming = parse_message(data.outb)
            command = incoming[0].upper()
            if command == "PING":
                print("Sending PONG")
                sock.sendall(encode_command("PONG"))
            elif command == "ECHO":
                echo_message = incoming[1]
                print("Echoing message", echo_message)
                sock.sendall(encode_command(echo_message))
            elif command == "GET":
                key = incoming[1]
                if key in data.map_store:
                    sock.sendall(encode_command(data.map_store[key]["value"]))
                else:
                    sock.sendall(get_null_message())
            elif command == 'SET':
                key = incoming[1]
                value = incoming[2]
                expiry_time = None
                if len(incoming) > 4:
                    expiry_command = incoming[3]
                    if expiry_command.upper() == "PX":
                        expiry_value = int(incoming[4])
                        expiry_time = time.time() + expiry_value
                print(f"Setting key {key} to value {value} with expiry time {expiry_time}")
                data.map_store[key] = {"value": value, "expiry_time": expiry_time}
                sock.sendall(get_success_message())
            data.outb = b''

def initialize_server(port=6379):
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind(('localhost', port))
    server_socket.listen()
    print(f"Listening on port {port}")
    server_socket.setblocking(False)
    sel.register(server_socket, selectors.EVENT_READ, data=None)
    return server_socket

def main():
    print("Logs from your program will appear here!")
    server_socket = initialize_server()
    try:
        while True:
            events = sel.select(timeout=None)
            for key, mask in events:
                if key.data is None:
                    accept_wrapper(key.fileobj)
                else:
                    service_connection(key, mask)
    except:
        print("Caught keyboard interrupt, exiting")
    finally:
        sel.close()
        server_socket.close()



def run_test():
    print("Running tests")
    sample_message1 = b"*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n"
    sample_message2 = b'*1\r\n$4\r\nPING\r\n'
    exp_response = ['SET', 'mykey', 'myvalue']
    response = parse_message(sample_message1)
    assert exp_response == response
    assert parse_message(sample_message2) == ['PING']
    print("All tests passed")

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "test":
        run_test()
    else:
        main()
