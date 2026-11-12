"""kv: command-line client.

    kv put KEY VALUE [--w N] | get KEY [--r N] | delete KEY [--w N] | info | locate KEY | local-get KEY
    options, before or after the command: [-v] [--node HOST:PORT] [--timeout S] [--context-file PATH]

Values go to stdout; messages, the sibling hint and ``-v`` details go to stderr, so scripts can
read stdout. Exit codes: 0 OK, 1 not found, 2 invalid, 3 unreachable or bad reply, 4 quorum failed.
"""

import argparse
import base64
import json
import os
import sys
import tempfile

import grpc

from dynamo import dynamo_pb2, dynamo_pb2_grpc
from dynamo.reqid import METADATA_KEY

EXIT_OK = 0
EXIT_NOT_FOUND = 1
EXIT_INVALID = 2
EXIT_ERROR = 3
EXIT_QUORUM_FAILED = 4

STATUS_EXIT = {
    dynamo_pb2.OK: EXIT_OK,
    dynamo_pb2.NOT_FOUND: EXIT_NOT_FOUND,
    dynamo_pb2.INVALID: EXIT_INVALID,
    dynamo_pb2.QUORUM_FAILED: EXIT_QUORUM_FAILED,
}
STATUS_MESSAGE = {
    dynamo_pb2.NOT_FOUND: "NOT FOUND",
    dynamo_pb2.INVALID: "INVALID: check key size, value size and --r / --w",
    dynamo_pb2.QUORUM_FAILED: "QUORUM FAILED: not enough replicas answered in time",
}


# ---------- arguments ----------

def _shared_options(at_top_level: bool) -> argparse.ArgumentParser:
    """Options accepted both before and after the command.

    The copy added to each subcommand uses SUPPRESS defaults, so it only sets a value when the
    option is really given after the command and never overwrites one given before it.
    """
    def default(value):
        return value if at_top_level else argparse.SUPPRESS

    parent = argparse.ArgumentParser(add_help=False)
    parent.add_argument("-v", "--verbose", action="store_true", default=default(False),
                        help="print replicas and the request id to stderr")
    parent.add_argument("--node", default=default(os.environ.get("KV_NODE", "localhost:50051")),
                        help="node to talk to (env KV_NODE)")
    parent.add_argument("--timeout", type=float, default=default(2.0), help="seconds per call")
    parent.add_argument("--context-file",
                        default=default(os.environ.get("KV_CONTEXT", ".kv_context.json")),
                        help="where the last context of each key is kept (env KV_CONTEXT)")
    return parent


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse the command line."""
    parser = argparse.ArgumentParser(prog="kv", parents=[_shared_options(True)])
    shared = _shared_options(False)
    commands = parser.add_subparsers(dest="command", required=True)

    put = commands.add_parser("put", parents=[shared], help="store a value")
    put.add_argument("key")
    put.add_argument("value")
    put.add_argument("--w", type=int, default=0, help="write quorum (0 = cluster default)")

    get = commands.add_parser("get", parents=[shared], help="read a value")
    get.add_argument("key")
    get.add_argument("--r", type=int, default=0, help="read quorum (0 = cluster default)")

    delete = commands.add_parser("delete", parents=[shared], help="delete a key")
    delete.add_argument("key")
    delete.add_argument("--w", type=int, default=0, help="write quorum (0 = cluster default)")

    commands.add_parser("info", parents=[shared], help="cluster view of the node")
    locate = commands.add_parser("locate", parents=[shared], help="replicas of a key")
    locate.add_argument("key")
    local_get = commands.add_parser("local-get", parents=[shared], help="what the node itself stores")
    local_get.add_argument("key")
    return parser.parse_args(argv)


# ---------- context file ----------

def _read_context_file(path: str) -> dict[str, str]:
    """The whole context file: key -> base64 context. Missing file = empty."""
    try:
        with open(path, encoding="utf-8") as file:
            return json.load(file)
    except FileNotFoundError:
        return {}


def load_context(path: str, key: str) -> bytes:
    """The saved context of ``key``; b"" when there is none."""
    return base64.b64decode(_read_context_file(path).get(key, ""))


def save_context(path: str, key: str, context: bytes) -> None:
    """Save the context of ``key``. Written to a temp file first, then renamed over the old one,
    so a crash never leaves a half-written file."""
    contexts = _read_context_file(path)
    contexts[key] = base64.b64encode(context).decode()
    folder = os.path.dirname(os.path.abspath(path))
    fd, tmp_path = tempfile.mkstemp(dir=folder, prefix=".kv_context.", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as file:
            json.dump(contexts, file, indent=2, sort_keys=True)
        os.replace(tmp_path, path)
    except BaseException:
        os.unlink(tmp_path)
        raise


# ---------- output helpers ----------

def show_value(value: bytes) -> str:
    """UTF-8 text when possible, else ``base64:...``."""
    try:
        return value.decode("utf-8")
    except UnicodeDecodeError:
        return "base64:" + base64.b64encode(value).decode()


def _request_id(call) -> str:
    """The request id from a call's trailing metadata, or "-"."""
    trailing = getattr(call, "trailing_metadata", lambda: None)() or ()
    for key, value in trailing:
        if key == METADATA_KEY:
            return value
    return "-"


def _err(text: str) -> None:
    print(text, file=sys.stderr)


def _exit_code(args: argparse.Namespace, status: int, request_id: str) -> int:
    """Exit code for a reply status; prints a message for anything but OK."""
    if status not in STATUS_EXIT:  # STATUS_UNSPECIFIED or a number we do not know
        _err(f"ERROR: bad reply from {args.node} (status {status}), request-id {request_id}")
        return EXIT_ERROR
    if status != dynamo_pb2.OK:
        _err(STATUS_MESSAGE[status])
    return STATUS_EXIT[status]


def _verbose(args: argparse.Namespace, request_id: str, **lists: list[str]) -> None:
    """``-v`` details on stderr."""
    if not args.verbose:
        return
    for name, items in lists.items():
        _err(f"{name}: {', '.join(items) or '-'}")
    _err(f"request-id: {request_id}")


def _state_name(state: int) -> str:
    try:
        return dynamo_pb2.MemberState.Name(state)
    except ValueError:
        return str(state)


# ---------- commands ----------

def _put(args, stub) -> int:
    request = dynamo_pb2.PutRequest(key=args.key, value=args.value.encode(),
                                    context=load_context(args.context_file, args.key), w=args.w)
    response, call = stub.Put.with_call(request, timeout=args.timeout)
    request_id = _request_id(call)
    code = _exit_code(args, response.status, request_id)
    if code == EXIT_OK:
        save_context(args.context_file, args.key, response.context)
        print("OK")
    _verbose(args, request_id, written_to=list(response.written_to))
    return code


def _get(args, stub) -> int:
    request = dynamo_pb2.GetRequest(key=args.key, r=args.r)
    response, call = stub.Get.with_call(request, timeout=args.timeout)
    request_id = _request_id(call)
    code = _exit_code(args, response.status, request_id)
    if code in (EXIT_OK, EXIT_NOT_FOUND):  # NOT_FOUND still carries the tombstone's context
        save_context(args.context_file, args.key, response.context)
    if code == EXIT_OK:
        values = list(response.values)
        if len(values) == 1:
            print(show_value(values[0]))
        else:
            for i, value in enumerate(values, start=1):
                print(f"[{i}] {show_value(value)}")
            _err(f"{len(values)} siblings: merge them and put again")
    _verbose(args, request_id, read_from=list(response.read_from))
    return code


def _delete(args, stub) -> int:
    request = dynamo_pb2.DeleteRequest(key=args.key, w=args.w,
                                       context=load_context(args.context_file, args.key))
    response, call = stub.Delete.with_call(request, timeout=args.timeout)
    request_id = _request_id(call)
    code = _exit_code(args, response.status, request_id)
    if code == EXIT_OK:
        print("OK")
    _verbose(args, request_id)
    return code


def _info(args, stub) -> int:
    response, call = stub.ClusterInfo.with_call(dynamo_pb2.Empty(), timeout=args.timeout)
    print(f"node {response.node_id}  N={response.n} R={response.r} W={response.w}  "
          f"vnodes {response.vnodes}  ring {response.ring_fingerprint}")
    print(f"keys: {response.live_keys} live, {response.tombstones} tombstones")
    for member in response.members:
        print(f"{member.node_id}  {member.address}  {_state_name(member.state)}  "
              f"incarnation {member.incarnation}  heartbeat {member.heartbeat}")
    _verbose(args, _request_id(call))
    return EXIT_OK


def _locate(args, stub) -> int:
    request = dynamo_pb2.LocateRequest(key=args.key)
    response, call = stub.Locate.with_call(request, timeout=args.timeout)
    print(f"position {response.position}")
    print("replicas " + ", ".join(response.nodes))
    _verbose(args, _request_id(call))
    return EXIT_OK


def _local_get(args, stub) -> int:
    request = dynamo_pb2.LocalGetRequest(key=args.key)
    response, call = stub.LocalGet.with_call(request, timeout=args.timeout)
    request_id = _request_id(call)
    code = _exit_code(args, response.status, request_id)
    for i, version in enumerate(response.versions, start=1):
        shown = "<tombstone>" if version.tombstone else show_value(version.value)
        print(f"[{i}] {shown}  clock {dict(version.clock.counters)}")
    _verbose(args, request_id, node=[response.node_id])
    return code


COMMANDS = {
    "put": _put,
    "get": _get,
    "delete": _delete,
    "info": _info,
    "locate": _locate,
    "local-get": _local_get,
}


def run(args: argparse.Namespace, stub: dynamo_pb2_grpc.KVStoreStub) -> int:
    """Run one command against ``stub`` and return the exit code."""
    try:
        return COMMANDS[args.command](args, stub)
    except grpc.RpcError as error:
        _err(f"ERROR: {args.node} unreachable or failed: {error.code().name}: {error.details()}, "
             f"request-id {_request_id(error)}")
        return EXIT_ERROR


def main(argv: list[str] | None = None) -> None:
    """Entry point for the ``kv`` command."""
    args = parse_args(argv)
    with grpc.insecure_channel(args.node) as channel:
        code = run(args, dynamo_pb2_grpc.KVStoreStub(channel))
    raise SystemExit(code)


if __name__ == "__main__":
    main()
