import os
import argparse
from .server import RedisServer

DEFAULT_PORT = 6379


def main():
    print("Logs from your program will appear here!")

    parser = argparse.ArgumentParser(description="Redis server")
    parser.add_argument("--port", type=int, help="Port to run the server on")
    parser.add_argument("--test", action="store_true", help="Run tests")
    parser.add_argument(
        "--replicaof", type=str, help="Replicate data from another server"
    )
    parser.add_argument("--dir", type=str, help="Directory where rdb file is stored.")
    parser.add_argument("--dbfilename", type=str, help="Name of the rdb file.")
    args = parser.parse_args()
    replicate_server = args.replicaof.split(" ") if args.replicaof else []
    print("Replicate server", replicate_server)
    if args.test:
        run_test()
        return
    port = args.port if args.port else DEFAULT_PORT
    rdb_file_obj = None
    if args.dbfilename:

        db_file_path = os.path.join(args.dir, args.dbfilename)
        if os.path.exists(db_file_path):
            rdb_file_obj = open(db_file_path, "rb")
        print("Using rdb file", args.dbfilename)
    server = RedisServer(
        port,
        debug=True,
        rdb_file_obj=rdb_file_obj,
        *replicate_server,
    )
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
