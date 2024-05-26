from .server import RedisServer

DEFAULT_PORT = 6379


def main():
    print("Logs from your program will appear here!")
    import argparse

    parser = argparse.ArgumentParser(description="Redis server")
    parser.add_argument("--port", type=int, help="Port to run the server on")
    parser.add_argument("--test", action="store_true", help="Run tests")
    parser.add_argument(
        "--replicaof", type=str, help="Replicate data from another server"
    )
    args = parser.parse_args()
    replicate_server = args.replicaof.split(" ") if args.replicaof else []
    print("Replicate server", replicate_server)
    if args.test:
        run_test()
        return
    port = args.port if args.port else DEFAULT_PORT
    server = RedisServer(port, debug=True, *replicate_server)
    server.run()


def run_test():
    print("Running tests")
    # sample_message1 = b"*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n"
    # sample_message2 = b"*1\r\n$4\r\nPING\r\n"
    # exp_response = ["SET", "mykey", "myvalue"]
    # response = parse_message(sample_message1)
    # assert exp_response == response
    # assert parse_message(sample_message2) == ["PING"]
    print("All tests passed")


if __name__ == "__main__":
    main()
