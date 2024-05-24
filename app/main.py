import socket

sample_message1 = b"*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n"
sample_message2 = b'*1\r\n$4\r\nPING\r\n'

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

def initialize_server(port=6379):
    server_socket = socket.create_server(("localhost", port), reuse_port=True)
    client_socket, addr = server_socket.accept() # wait for client
    print(f"Received the connection from the client {addr}")
    return client_socket

def handle_client(client_socket):
    while True:
        data = client_socket.recv(1024)
        if not data:
            break
        print(f"Received message: {data}")
        # commands = parse_commands(message)
        # print("Received commands: ", commands)
        client_socket.sendall(b"+PONG\r\n")
    client_socket.close()


def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    client_socket = initialize_server()

    handle_client(client_socket)


if __name__ == "__main__":
    main()
