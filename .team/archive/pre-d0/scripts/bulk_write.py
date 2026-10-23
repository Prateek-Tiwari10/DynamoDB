import argparse
import sys

import grpc

from dynamo import dynamo_pb2 as pb
from dynamo import dynamo_pb2_grpc


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Write or verify many keys over one channel.")
    parser.add_argument("--node", default="localhost:50051")
    parser.add_argument("--count", type=int, default=1500)
    parser.add_argument("--prefix", default="bulk")
    parser.add_argument("--verify", action="store_true", help="read keys back instead of writing")
    return parser.parse_args()


def write_all(stub: dynamo_pb2_grpc.KVStoreStub, prefix: str, count: int) -> int:
    for i in range(count):
        response = stub.Put(pb.PutRequest(key=f"{prefix}:{i}", value=f"v{i}".encode()), timeout=5)
        if response.status != pb.OK:
            print(f"put {prefix}:{i} failed: {pb.Status.Name(response.status)}", file=sys.stderr)
            return 1
    print(f"wrote {count} keys")
    return 0


def verify_all(stub: dynamo_pb2_grpc.KVStoreStub, prefix: str, count: int) -> int:
    for i in range(count):
        response = stub.Get(pb.GetRequest(key=f"{prefix}:{i}"), timeout=5)
        expected = f"v{i}".encode()
        if response.status != pb.OK or list(response.values) != [expected]:
            print(f"mismatch at {prefix}:{i}: {pb.Status.Name(response.status)} {list(response.values)}",
                  file=sys.stderr)
            return 1
    print(f"verified {count} keys")
    return 0


def main() -> None:
    args = parse_args()
    with grpc.insecure_channel(args.node) as channel:
        stub = dynamo_pb2_grpc.KVStoreStub(channel)
        action = verify_all if args.verify else write_all
        sys.exit(action(stub, args.prefix, args.count))


if __name__ == "__main__":
    main()
