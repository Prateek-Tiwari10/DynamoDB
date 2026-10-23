from concurrent.futures import ThreadPoolExecutor

from dynamo import dynamo_pb2 as pb


def put(node, key: str, value: bytes) -> pb.PutResponse:
    return node.stub.Put(pb.PutRequest(key=key, value=value))


def get(node, key: str) -> pb.GetResponse:
    return node.stub.Get(pb.GetRequest(key=key))


def delete(node, key: str) -> pb.DeleteResponse:
    return node.stub.Delete(pb.DeleteRequest(key=key))


def test_put_then_get(node):
    assert put(node, "cart:42", b"book").status == pb.OK
    response = get(node, "cart:42")
    assert response.status == pb.OK
    assert list(response.values) == [b"book"]


def test_get_missing(node):
    response = get(node, "missing")
    assert response.status == pb.NOT_FOUND
    assert list(response.values) == []


def test_delete_then_get(node):
    put(node, "a", b"1")
    assert delete(node, "a").status == pb.OK
    assert get(node, "a").status == pb.NOT_FOUND


def test_delete_missing_is_ok(node):
    assert delete(node, "never-written").status == pb.OK


def test_empty_key_is_invalid(node):
    assert put(node, "", b"x").status == pb.INVALID
    assert get(node, "").status == pb.INVALID
    assert delete(node, "").status == pb.INVALID


def test_size_limits(node):
    assert put(node, "k" * 257, b"x").status == pb.INVALID
    assert put(node, "big", b"x" * (1024 * 1024 + 1)).status == pb.INVALID


def test_restart_keeps_data(node):
    put(node, "a", b"1")
    put(node, "b", b"2")
    node.restart()
    assert list(get(node, "a").values) == [b"1"]
    assert list(get(node, "b").values) == [b"2"]


def test_concurrent_puts(node):
    with ThreadPoolExecutor(max_workers=16) as pool:
        results = list(pool.map(lambda i: put(node, f"k{i}", str(i).encode()), range(100)))
    assert all(r.status == pb.OK for r in results)
    assert all(list(get(node, f"k{i}").values) == [str(i).encode()] for i in range(100))
