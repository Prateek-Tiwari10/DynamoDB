import argparse
import base64
import os
import sys

import grpc

from dynamo import dynamo_pb2 as pb
from dynamo import dynamo_pb2_grpc

EXIT_OK = 0
EXIT_NOT_FOUND = 1
EXIT_INVALID = 2
EXIT_UNREACHABLE = 3


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="kv", description="Talk to a key-value store node.")
    parser.add_argument("--node", default=os.environ.get("KV_NODE", "localhost:50051"))
    parser.add_argument("--timeout", type=float, default=2.0)
    commands = parser.add_subparsers(dest="command", required=True)

    put = commands.add_parser("put")
    put.add_argument("key")
    put.add_argument("value")
    put.add_argument("--w", type=int, default=0)

    get = commands.add_parser("get")
    get.add_argument("key")
    get.add_argument("--r", type=int, default=0)

    delete = commands.add_parser("delete")
    delete.add_argument("key")
    delete.add_argument("--w", type=int, default=0)
    return parser.parse_args(argv)


def show_value(value: bytes) -> str:
    try:
        return value.decode("utf-8")
    except UnicodeDecodeError:
        return "base64:" + base64.b64encode(value).decode("ascii")


def run(args: argparse.Namespace, stub: dynamo_pb2_grpc.KVStoreStub) -> int:
    if args.command == "put":
        request = pb.PutRequest(key=args.key, value=args.value.encode("utf-8"), w=args.w)
        response = stub.Put(request, timeout=args.timeout)
    elif args.command == "delete":
        request = pb.DeleteRequest(key=args.key, w=args.w)
        response = stub.Delete(request, timeout=args.timeout)
    else:
        response = stub.Get(pb.GetRequest(key=args.key, r=args.r), timeout=args.timeout)

    if response.status == pb.INVALID:
        print("INVALID REQUEST", file=sys.stderr)
        return EXIT_INVALID
    if response.status == pb.NOT_FOUND:
        print("NOT FOUND")
        return EXIT_NOT_FOUND
    if args.command == "get":
        print(show_value(response.values[0]))
    else:
        print("OK")
    return EXIT_OK


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    try:
        with grpc.insecure_channel(args.node) as channel:
            code = run(args, dynamo_pb2_grpc.KVStoreStub(channel))
    except grpc.RpcError:
        print(f"ERROR: cannot reach {args.node}", file=sys.stderr)
        code = EXIT_UNREACHABLE
    sys.exit(code)


if __name__ == "__main__":
    main()
