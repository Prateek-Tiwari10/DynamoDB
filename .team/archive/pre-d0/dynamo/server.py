import argparse
import logging
import os
import signal
import sys
import time
from concurrent import futures

import grpc

from dynamo import dynamo_pb2, dynamo_pb2_grpc
from dynamo.storage import Storage

MAX_KEY_BYTES = 256
MAX_VALUE_BYTES = 1024 * 1024

log = logging.getLogger("dynamo.server")


def valid_key(key: str) -> bool:
    return 0 < len(key.encode("utf-8")) <= MAX_KEY_BYTES


class KVStoreServicer(dynamo_pb2_grpc.KVStoreServicer):
    def __init__(self, storage: Storage, node_id: str) -> None:
        self.storage = storage
        self.node_id = node_id

    def Put(self, request, context) -> dynamo_pb2.PutResponse:
        start = time.perf_counter()
        if not valid_key(request.key) or len(request.value) > MAX_VALUE_BYTES:
            status = dynamo_pb2.INVALID
        else:
            self.storage.put(request.key, request.value)
            status = dynamo_pb2.OK
        self._log("PUT", request.key, status, start)
        return dynamo_pb2.PutResponse(status=status)

    def Get(self, request, context) -> dynamo_pb2.GetResponse:
        start = time.perf_counter()
        values = []
        if not valid_key(request.key):
            status = dynamo_pb2.INVALID
        else:
            value = self.storage.get(request.key)
            status = dynamo_pb2.NOT_FOUND if value is None else dynamo_pb2.OK
            values = [] if value is None else [value]
        self._log("GET", request.key, status, start)
        return dynamo_pb2.GetResponse(status=status, values=values)

    def Delete(self, request, context) -> dynamo_pb2.DeleteResponse:
        start = time.perf_counter()
        if not valid_key(request.key):
            status = dynamo_pb2.INVALID
        else:
            self.storage.delete(request.key)
            status = dynamo_pb2.OK
        self._log("DELETE", request.key, status, start)
        return dynamo_pb2.DeleteResponse(status=status)

    def _log(self, op: str, key: str, status: int, start: float) -> None:
        elapsed_ms = (time.perf_counter() - start) * 1000
        name = dynamo_pb2.Status.Name(status)
        log.info("%s %s %s %s %.1fms", self.node_id, op, key, name, elapsed_ms)


def create_server(storage: Storage, node_id: str, port: int) -> tuple[grpc.Server, int]:
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=16))
    dynamo_pb2_grpc.add_KVStoreServicer_to_server(KVStoreServicer(storage, node_id), server)
    bound = server.add_insecure_port(f"[::]:{port}")
    return server, bound


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run one key-value store node.")
    parser.add_argument("--node-id", default=os.environ.get("NODE_ID", "node1"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("PORT", "50051")))
    parser.add_argument("--data-dir", default=os.environ.get("DATA_DIR", "./data"))
    parser.add_argument(
        "--checkpoint-every",
        type=int,
        default=int(os.environ.get("CHECKPOINT_EVERY", "1000")),
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

    # Recovery runs inside Storage(), so it finishes before the port opens.
    folder = os.path.join(args.data_dir, args.node_id)
    storage = Storage(folder, checkpoint_every=args.checkpoint_every)
    server, bound = create_server(storage, args.node_id, args.port)
    server.start()
    log.info("%s listening on %d (recovered %d keys)", args.node_id, bound, len(storage.data))

    def shutdown(signum, frame) -> None:
        log.info("%s shutting down", args.node_id)
        server.stop(grace=2).wait()
        storage.close()
        sys.exit(0)

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)
    server.wait_for_termination()


if __name__ == "__main__":
    main()
