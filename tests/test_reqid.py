"""Unit tests for dynamo/reqid.py, using a real in-process gRPC server."""

import asyncio
import contextvars
import logging

import grpc
import pytest

from dynamo import dynamo_pb2, dynamo_pb2_grpc
from dynamo.reqid import (
    METADATA_KEY,
    REQUEST_ID,
    RequestIdFilter,
    adopt,
    outgoing,
    rpc_handler,
    start_request,
)


async def _read_request_id() -> str:
    return REQUEST_ID.get()


class FakeKV(dynamo_pb2_grpc.KVStoreServicer):
    """Put is a stub, Get breaks, Delete records the request id it sees."""

    def __init__(self) -> None:
        self.seen: list[tuple[str, str]] = []

    @rpc_handler("client")
    async def Put(self, request, context):
        raise NotImplementedError("put")

    @rpc_handler("client")
    async def Get(self, request, context):
        raise ValueError("boom")

    @rpc_handler("client")
    async def Delete(self, request, context):
        inside = REQUEST_ID.get()
        await asyncio.sleep(0.05)  # let a concurrent request run in between
        in_task = await asyncio.create_task(_read_request_id())
        self.seen.append((inside, in_task))
        return dynamo_pb2.DeleteResponse(status=dynamo_pb2.OK)


class FakeNodeService(dynamo_pb2_grpc.NodeServiceServicer):
    def __init__(self) -> None:
        self.seen: list[str] = []

    @rpc_handler("node")
    async def ReplicaGet(self, request, context):
        self.seen.append(REQUEST_ID.get())
        return dynamo_pb2.ReplicaGetResponse(node_id="fake")


@pytest.fixture
async def server():
    kv, node_service = FakeKV(), FakeNodeService()
    grpc_server = grpc.aio.server()
    dynamo_pb2_grpc.add_KVStoreServicer_to_server(kv, grpc_server)
    dynamo_pb2_grpc.add_NodeServiceServicer_to_server(node_service, grpc_server)
    port = grpc_server.add_insecure_port("127.0.0.1:0")
    await grpc_server.start()
    channel = grpc.aio.insecure_channel(f"127.0.0.1:{port}")
    yield kv, node_service, channel
    await channel.close()
    await grpc_server.stop(None)


def trailing_request_id(metadata) -> str | None:
    return dict(metadata or ()).get(METADATA_KEY)


async def test_client_request_id_kept(server):
    kv, _, channel = server
    call = dynamo_pb2_grpc.KVStoreStub(channel).Delete(
        dynamo_pb2.DeleteRequest(key="k"), metadata=((METADATA_KEY, "client-id-1"),))
    await call
    assert trailing_request_id(await call.trailing_metadata()) == "client-id-1"
    assert kv.seen == [("client-id-1", "client-id-1")]


async def test_request_id_generated_when_absent_and_inherited_by_tasks(server):
    kv, _, channel = server
    call = dynamo_pb2_grpc.KVStoreStub(channel).Delete(dynamo_pb2.DeleteRequest(key="k"))
    await call
    request_id = trailing_request_id(await call.trailing_metadata())
    assert request_id and len(request_id) == 32
    assert kv.seen == [(request_id, request_id)]


async def test_concurrent_request_ids(server):
    kv, _, channel = server
    stub = dynamo_pb2_grpc.KVStoreStub(channel)
    await asyncio.gather(
        stub.Delete(dynamo_pb2.DeleteRequest(key="a"), metadata=((METADATA_KEY, "id-a"),)),
        stub.Delete(dynamo_pb2.DeleteRequest(key="b"), metadata=((METADATA_KEY, "id-b"),)),
    )
    assert sorted(kv.seen) == [("id-a", "id-a"), ("id-b", "id-b")]


async def test_stub_rpcs_unimplemented_with_request_id(server):
    _, _, channel = server
    with pytest.raises(grpc.aio.AioRpcError) as error:
        await dynamo_pb2_grpc.KVStoreStub(channel).Put(
            dynamo_pb2.PutRequest(key="k"), metadata=((METADATA_KEY, "id-put"),))
    assert error.value.code() == grpc.StatusCode.UNIMPLEMENTED
    assert trailing_request_id(error.value.trailing_metadata()) == "id-put"


async def test_handler_internal_error(server, caplog):
    _, _, channel = server
    with pytest.raises(grpc.aio.AioRpcError) as error:
        await dynamo_pb2_grpc.KVStoreStub(channel).Get(
            dynamo_pb2.GetRequest(key="k"), metadata=((METADATA_KEY, "id-get"),))
    assert error.value.code() == grpc.StatusCode.INTERNAL
    assert trailing_request_id(error.value.trailing_metadata()) == "id-get"
    assert "Get failed" in caplog.text


async def test_node_handler_adopts_caller_id(server):
    _, node_service, channel = server
    stub = dynamo_pb2_grpc.NodeServiceStub(channel)
    await stub.ReplicaGet(dynamo_pb2.ReplicaGetRequest(key="k"), metadata=((METADATA_KEY, "id-n"),))
    await stub.ReplicaGet(dynamo_pb2.ReplicaGetRequest(key="k"))
    assert node_service.seen == ["id-n", "-"]


def test_outgoing_metadata():
    def inside() -> tuple:
        before = outgoing()
        start_request(((METADATA_KEY, "abc"),))
        return before, outgoing()

    before, after = contextvars.copy_context().run(inside)
    assert before == ()
    assert after == ((METADATA_KEY, "abc"),)


def test_adopt_without_metadata_means_no_request():
    def inside() -> str:
        REQUEST_ID.set("old")
        adopt(())
        return REQUEST_ID.get()

    assert contextvars.copy_context().run(inside) == "-"


def test_filter_adds_node_id_and_short_request_id():
    record = logging.LogRecord("x", logging.INFO, __file__, 1, "msg", None, None)

    def inside() -> None:
        start_request(((METADATA_KEY, "a3f9c2d1e5b7"),))
        RequestIdFilter("node1").filter(record)

    contextvars.copy_context().run(inside)
    assert record.node_id == "node1"
    assert record.request_id == "a3f9c2d1"


def test_filter_outside_a_request_shows_dash():
    record = logging.LogRecord("x", logging.INFO, __file__, 1, "msg", None, None)
    contextvars.copy_context().run(RequestIdFilter("node2").filter, record)
    assert record.request_id == "-"
