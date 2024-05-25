import socket
import selectors
import types

sel = selectors.DefaultSelector()

def parse_command(message):
    parts = message.strip().split(b"\r\n")
    length = int(parts[0][1:])
    commands = []
    for i in range(length):
        commands.append(parts[i*2+2].decode())
    return commands

def encode_command(message):
    return f"${len(message)}\r\n{message}\r\n".encode()

def read_message(client_socket):
    message = b""
    while True:
        chunk = client_socket.recv(1024)
        message += chunk
        if len(chunk) < 1024:
            break
    return message


def handle_client(client_socket):
    while True:
        data = read_message(client_socket)
        print(f"Received message: {data}")
        # commands = parse_commands(message)
        # print("Received commands: ", commands)
        client_socket.sendall(b"+PONG\r\n")
    client_socket.close()


def accept_wrapper(server_socket):
    client_socket, addr = server_socket.accept()
    print(f"Accepted connection from {addr}")
    client_socket.setblocking(False)
    data = types.SimpleNamespace(addr=addr, inb=b"", outb=b"")
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
        # sock.sendall(b"+PONG\r\n")
        # data.outb = data.outb[sent:]
        if data.outb:
            commands = parse_command(data.outb)
            if commands[0] == "PING":
                sent = sock.sendall(encode_command("PONG"))
                data.outb = b''
            else:
                sent = sock.sendall(encode_command(data.outb))
                data.outb = data.outb[sent:]

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
    response = parse_command(sample_message1)
    assert exp_response == response
    assert parse_command(sample_message2) == ['PING']
    print("All tests passed")

if __name__ == "__main__":
    import sys
    if sys.argv[1] == "test":
        run_test()
    else:
        main()
