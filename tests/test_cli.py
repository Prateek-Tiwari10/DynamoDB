"""Unit tests for client/cli.py against a fake KVStore server."""

import base64
import json
import os
import socket
from concurrent.futures import ThreadPoolExecutor

import grpc
import pytest

from client.cli import load_context, parse_args, run, save_context, show_value
from dynamo import dynamo_pb2, dynamo_pb2_grpc
from dynamo.reqid import METADATA_KEY


class FakeKV(dynamo_pb2_grpc.KVStoreServicer):
    """Answers with whatever the test puts in ``replies`` and records every request."""

    def __init__(self) -> None:
        self.replies: dict[str, object] = {}
        self.requests: list[object] = []

    def _answer(self, name, request, context):
        self.requests.append(request)
        context.set_trailing_metadata(((METADATA_KEY, f"rid-{name}"),))
        return self.replies[name]

    def Put(self, request, context):
        return self._answer("put", request, context)

    def Get(self, request, context):
        return self._answer("get", request, context)

    def Delete(self, request, context):
        return self._answer("delete", request, context)


@pytest.fixture
def kv():
    fake = FakeKV()
    server = grpc.server(ThreadPoolExecutor(max_workers=2))
    dynamo_pb2_grpc.add_KVStoreServicer_to_server(fake, server)
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    channel = grpc.insecure_channel(f"127.0.0.1:{port}")
    yield fake, dynamo_pb2_grpc.KVStoreStub(channel), f"127.0.0.1:{port}"
    channel.close()
    server.stop(None)


def kv_run(kv, tmp_path, *argv: str) -> int:
    _, stub, address = kv
    args = parse_args([*argv, "--node", address, "--context-file", str(tmp_path / "ctx.json")])
    return run(args, stub)


# ---------- arguments ----------

def test_cli_verbose_after_subcommand():
    assert parse_args(["put", "cart:42", "book", "-v"]).verbose is True
    assert parse_args(["-v", "put", "cart:42", "book"]).verbose is True
    assert parse_args(["put", "cart:42", "book"]).verbose is False


def test_node_and_timeout_before_or_after_the_command():
    assert parse_args(["--node", "a:1", "get", "k"]).node == "a:1"
    assert parse_args(["get", "k", "--node", "b:2", "--timeout", "0.5"]).node == "b:2"
    assert parse_args(["get", "k", "--timeout", "0.5"]).timeout == 0.5


def test_defaults_from_env(monkeypatch):
    monkeypatch.setenv("KV_NODE", "node9:1234")
    monkeypatch.setenv("KV_CONTEXT", "/tmp/other.json")
    args = parse_args(["get", "k"])
    assert args.node == "node9:1234"
    assert args.context_file == "/tmp/other.json"
    assert args.timeout == 2.0


def test_defaults_without_env(monkeypatch):
    monkeypatch.delenv("KV_NODE", raising=False)
    monkeypatch.delenv("KV_CONTEXT", raising=False)
    args = parse_args(["info"])
    assert args.node == "localhost:50051"
    assert args.context_file == ".kv_context.json"


# ---------- exit codes and output ----------

@pytest.mark.parametrize("status, code", [
    (dynamo_pb2.OK, 0),
    (dynamo_pb2.NOT_FOUND, 1),
    (dynamo_pb2.INVALID, 2),
    (dynamo_pb2.QUORUM_FAILED, 4),
])
def test_exit_codes(kv, tmp_path, status, code):
    kv[0].replies["get"] = dynamo_pb2.GetResponse(status=status, values=[b"v"])
    assert kv_run(kv, tmp_path, "get", "k") == code


@pytest.mark.parametrize("status", [dynamo_pb2.STATUS_UNSPECIFIED, 99])
def test_cli_unknown_status(kv, tmp_path, capsys, status):
    kv[0].replies["get"] = dynamo_pb2.GetResponse(status=status)
    assert kv_run(kv, tmp_path, "get", "k") == 3
    assert (f"ERROR: bad reply from {kv[2]} (status {status}), request-id rid-get"
            in capsys.readouterr().err)


def test_unreachable_node_exits_3(tmp_path, capsys):
    with socket.socket() as sock:  # a port nobody listens on
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    args = parse_args(["get", "k", "--node", f"127.0.0.1:{port}", "--timeout", "0.5",
                       "--context-file", str(tmp_path / "ctx.json")])
    with grpc.insecure_channel(args.node) as channel:
        assert run(args, dynamo_pb2_grpc.KVStoreStub(channel)) == 3
    assert "unreachable" in capsys.readouterr().err


def test_get_prints_value_on_stdout(kv, tmp_path, capsys):
    kv[0].replies["get"] = dynamo_pb2.GetResponse(status=dynamo_pb2.OK, values=[b"book"])
    kv_run(kv, tmp_path, "get", "cart:42")
    assert capsys.readouterr().out == "book\n"


def test_siblings_on_stdout_hint_on_stderr(kv, tmp_path, capsys):
    kv[0].replies["get"] = dynamo_pb2.GetResponse(status=dynamo_pb2.OK, values=[b"a", b"b"])
    assert kv_run(kv, tmp_path, "get", "k") == 0
    output = capsys.readouterr()
    assert output.out == "[1] a\n[2] b\n"
    assert "2 siblings: merge them and put again" in output.err


def test_verbose_details_on_stderr(kv, tmp_path, capsys):
    kv[0].replies["put"] = dynamo_pb2.PutResponse(status=dynamo_pb2.OK,
                                                  written_to=["node1", "node2"])
    kv_run(kv, tmp_path, "put", "k", "v", "-v")
    output = capsys.readouterr()
    assert output.out == "OK\n"
    assert "written_to: node1, node2" in output.err
    assert "request-id: rid-put" in output.err


def test_show_value():
    assert show_value(b"book") == "book"
    assert show_value(b"\xff\x00") == "base64:" + base64.b64encode(b"\xff\x00").decode()


# ---------- context file ----------

def test_put_sends_saved_context_and_saves_the_new_one(kv, tmp_path):
    fake = kv[0]
    fake.replies["put"] = dynamo_pb2.PutResponse(status=dynamo_pb2.OK, context=b"\x01\x02")
    kv_run(kv, tmp_path, "put", "k", "v1")
    assert fake.requests[-1].context == b""
    kv_run(kv, tmp_path, "put", "k", "v2")
    assert fake.requests[-1].context == b"\x01\x02"
    saved = json.loads((tmp_path / "ctx.json").read_text())
    assert saved == {"k": base64.b64encode(b"\x01\x02").decode()}


def test_get_not_found_still_saves_context(kv, tmp_path):
    kv[0].replies["get"] = dynamo_pb2.GetResponse(status=dynamo_pb2.NOT_FOUND, context=b"\x07")
    assert kv_run(kv, tmp_path, "get", "gone") == 1
    assert load_context(str(tmp_path / "ctx.json"), "gone") == b"\x07"


def test_delete_sends_saved_context(kv, tmp_path):
    save_context(str(tmp_path / "ctx.json"), "k", b"\x05")
    kv[0].replies["delete"] = dynamo_pb2.DeleteResponse(status=dynamo_pb2.OK)
    assert kv_run(kv, tmp_path, "delete", "k") == 0
    assert kv[0].requests[-1].context == b"\x05"


def test_context_file_keyed_by_key_and_written_atomically(tmp_path):
    path = str(tmp_path / "ctx.json")
    save_context(path, "a", b"\x01")
    save_context(path, "b", b"\x02")
    assert load_context(path, "a") == b"\x01"
    assert load_context(path, "b") == b"\x02"
    assert load_context(path, "unknown") == b""
    assert os.listdir(tmp_path) == ["ctx.json"]  # no temp file left behind


def test_missing_context_file_means_empty_context(tmp_path):
    assert load_context(str(tmp_path / "none.json"), "k") == b""
