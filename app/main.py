import socket
import selectors
import types
import time
import datetime

sel = selectors.DefaultSelector()

master = None

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
    current_time = datetime.datetime.now()
    print(f"Expiring data at time {current_time}, {data.map_store}")
    for key in list(data.map_store.keys()):
        obj = data.map_store[key]
        if obj["expiry_time"] is not None and obj["expiry_time"] < current_time:
            print(f"Expiring key {key}")
            del data.map_store[key]

def accept_wrapper(server_socket):
    client_socket, addr = server_socket.accept()
    print(f"Accepted connection from {addr}")
    client_socket.setblocking(False)
    data = types.SimpleNamespace(addr=addr, inb=b"", outb=b"", map_store={})
    sel.register(client_socket, selectors.EVENT_READ | selectors.EVENT_WRITE, data=data)

def handle_set_command(data, incoming):
    key = incoming[1]
    value = incoming[2]
    expiry_time = None
    if len(incoming) > 4:
        expiry_command = incoming[3]
        if expiry_command.upper() == "PX":
            expiry_value = int(incoming[4])
            expiry_time = datetime.datetime.now() + datetime.timedelta(milliseconds=expiry_value)
    print(f"Setting key {key} to value {value} with expiry time {expiry_time}")
    data.map_store[key] = {"value": value, "expiry_time": expiry_time}
    data.outb = b''
    return get_success_message()

def handle_get_command(data, incoming):
    key = incoming[1]
    if key in data.map_store:
        response_msg = encode_command(data.map_store[key]["value"])
    else:
        response_msg = get_null_message()
    return response_msg

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
                sock.sendall(encode_command("PONG"))
            elif command == "ECHO":
                echo_message = incoming[1]
                sock.sendall(encode_command(echo_message))
            elif command == "GET":
                response_msg = handle_get_command(data, incoming)
                sock.sendall(response_msg)
            elif command == 'SET':
                response_msg = handle_set_command(data, incoming)
                sock.sendall(response_msg)
            elif command == 'INFO':
                if incoming[1].upper() == "REPLICATION":
                    if master:
                        response_msg = encode_command("role:master")
                    else:
                        response_msg = encode_command("role:slave")
                else:
                    response_msg = encode_command("redis_version:0.0.1")
                sock.sendall(response_msg)
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

def handle_server(server_socket, master_server, master_port):
    print("Master server", master_server, master_port)
    global master
    master = { "server": master_server, "port": master_port }
    try:
        while True:
            events = sel.select(timeout=None)
            for key, mask in events:
                if key.data is None:
                    accept_wrapper(key.fileobj)
                else:
                    service_connection(key, mask)
    finally:
        sel.close()
        server_socket.close()

def main():
    print("Logs from your program will appear here!")
    import argparse
    parser = argparse.ArgumentParser(description="Redis server")
    parser.add_argument("--port", type=int, help="Port to run the server on")
    parser.add_argument("--test", action="store_true", help="Run tests")
    parser.add_argument("--replicaof", type=str, help="Replicate data from another server")
    args = parser.parse_args()
    replicate_server = args.replicaof.split(" ") if args.replicaof else None
    print("Replicate server", replicate_server)
    if args.test:
        run_test()
        return
    if args.port:
        server_socket = initialize_server(args.port)
    else:
        server_socket = initialize_server()
    handle_server(server_socket, *replicate_server)

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
    main()
