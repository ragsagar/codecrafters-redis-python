import socket
import selectors
import types

sample_message1 = b"*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n"
sample_message2 = b'*1\r\n$4\r\nPING\r\n'
sel = selectors.DefaultSelector()

def parse_commands(message):
    parts = message.strip().split(b"\r\n")
    length = int(parts[0][1:])
    commands = []
    for _ in range(2, length+1*2, 2):
        commands.append(message)
    return commands

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
        sock.sendall(b"+PONG\r\n")
        # data.outb = data.outb[sent:]
        # if data.outb:
        #     print("Echoing", repr(data.outb), "to", data.addr)

def initialize_server(port=6379):
    server_socket = socket.create_server(("localhost", port), reuse_port=True)
    server_socket.listen()
    print(f"Listening on port {port}")
    server_socket.setblocking(False)
    sel.register(server_socket, selectors.EVENT_READ, data=None)

def main():
    print("Logs from your program will appear here!")
    initialize_server()
    try:
        while True:
            events = sel.select(timeout=None)
            for key, mask in events:
                if key.data is None:
                    accept_wrapper(key.fileobj)
                else:
                    service_connection(key, mask)
    except KeyboardInterrupt:
        print("Caught keyboard interrupt, exiting")
    finally:
        sel.close()




if __name__ == "__main__":
    main()
