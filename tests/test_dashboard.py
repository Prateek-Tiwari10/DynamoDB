"""Tests for the live dashboard: JSON helpers, the write guard, and the HTTP API against the
in-process test cluster.

The test cluster runs in the test's event loop and the dashboard makes blocking gRPC calls, so
every dashboard call here runs in a thread (``asyncio.to_thread``) and the nodes keep answering.
"""

import asyncio
import json
import threading
import urllib.error
import urllib.request

import pytest

from dynamo import dynamo_pb2
from dynamo.ring import Ring
from tools import dashboard


# ---------- helpers ----------

def test_show_value_cuts_and_replaces_bad_bytes():
    assert dashboard.show_value(b"book") == "book"
    long = dashboard.show_value(b"x" * 100)
    assert len(long) == dashboard.VALUE_PREVIEW + 1 and long.endswith("…")
    assert dashboard.show_value(b"\xff") == "�"


def test_version_json_tombstone_and_clock_order():
    version = dynamo_pb2.Version(value=b"", tombstone=True)
    version.clock.counters["node2"] = 3
    version.clock.counters["node1"] = 1
    assert dashboard.version_json(version) == {"value": "", "tombstone": True,
                                               "clock": {"node1": 1, "node2": 3}}


def test_node_view():
    info = dynamo_pb2.ClusterInfoResponse(node_id="node1", live_keys=4, tombstones=1, n=3, r=2,
                                          w=2, ring_fingerprint="abc")
    info.members.add(node_id="node2", address="h:2", state=dynamo_pb2.SUSPECT, heartbeat=7,
                     incarnation=99)
    view = dashboard.node_view(info)
    assert view["answered"] and view["live_keys"] == 4 and view["ring"] == "abc"
    assert view["members"]["node2"] == {"state": "SUSPECT", "heartbeat": 7, "incarnation": 99,
                                        "address": "h:2"}


def config_of(k=5):
    from tests.conftest import cluster_config
    ids = [f"node{i}" for i in range(1, k + 1)]
    return cluster_config(ids, list(range(50001, 50001 + k)))


def test_ring_json_matches_the_nodes_ring():
    config = config_of()
    data = dashboard.ring_json(config)
    ring = Ring(list(config.nodes), config.vnodes)
    assert len(data["vnodes"]) == 5 * config.vnodes
    assert [(v["position"], v["node"]) for v in data["vnodes"]] == ring._entries


def test_placement_json_uses_the_preference_list():
    config = config_of()
    ring = Ring(list(config.nodes), config.vnodes)
    placement = dashboard.placement_json(ring, "cart:42", 3)
    assert placement["replicas"] == ring.preference_list("cart:42", 3)
    assert placement["order"][:3] == placement["replicas"] and len(placement["order"]) == 5
    assert placement["position"] == ring.position("cart:42")


def test_holdings_json_marks_nodes_that_did_not_answer():
    answer = dynamo_pb2.LocalGetResponse(status=dynamo_pb2.OK, node_id="node1")
    answer.versions.add(value=b"v")
    result = dashboard.holdings_json({"node1": answer, "node2": "DEADLINE_EXCEEDED"})
    assert result["node1"]["status"] == "OK" and result["node1"]["versions"][0]["value"] == "v"
    assert result["node2"] == {"answered": False, "error": "DEADLINE_EXCEEDED"}


@pytest.mark.parametrize("headers, allowed", [
    ({"X-Dashboard": "1", "Content-Type": "application/json"}, True),
    ({"Content-Type": "application/json"}, False),                      # cross-site form/fetch
    ({"X-Dashboard": "1", "Content-Type": "text/plain"}, False),
    ({"X-Dashboard": "0", "Content-Type": "application/json"}, False),
])
def test_allowed_post(headers, allowed):
    assert dashboard._allowed_post(headers) is allowed


# ---------- HTTP against the test cluster ----------

class Client:
    """Tiny HTTP client for the dashboard; every call runs in a thread."""

    def __init__(self, port: int) -> None:
        self.base = f"http://127.0.0.1:{port}"

    def _request(self, path, body=None, headers=None):
        data = None if body is None else json.dumps(body).encode()
        request = urllib.request.Request(self.base + path, data=data, headers=headers or {},
                                         method="GET" if body is None else "POST")
        try:
            with urllib.request.urlopen(request, timeout=10) as response:
                return response.status, json.loads(response.read())
        except urllib.error.HTTPError as error:
            return error.code, json.loads(error.read())

    async def get(self, path):
        return await asyncio.to_thread(self._request, path)

    async def post(self, path, body, headers=None):
        headers = {"X-Dashboard": "1", "Content-Type": "application/json"} \
            if headers is None else headers
        return await asyncio.to_thread(self._request, path, body, headers)


@pytest.fixture
async def dash(cluster):
    server = await asyncio.to_thread(dashboard.make_server, cluster.config, "127.0.0.1", 0, 0.2)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield Client(server.server_address[1]), server, cluster
    server.app.poller.stop()
    await asyncio.to_thread(server.shutdown)
    server.server_close()
    server.app.nodes.close()


async def all_answer(client, count=5):
    _, status = await client.get("/api/status")
    return sum(1 for v in status["nodes"].values() if v["answered"]) == count


async def test_page_and_ring_are_served(dash):
    client, server, cluster = dash
    status, ring = await client.get("/api/ring")
    assert status == 200 and len(ring["vnodes"]) == 5 * cluster.config.vnodes
    page = await asyncio.to_thread(lambda: urllib.request.urlopen(client.base + "/").read())
    assert b"Dynamo KV" in page


async def test_status_shows_every_node(dash):
    client, _, cluster = dash
    await cluster.wait_until(lambda: all_answer(client), timeout=5)
    _, status = await client.get("/api/status")
    assert set(status["nodes"]) == set(cluster.config.nodes)
    assert status["config"]["n"] == cluster.config.n


async def test_put_get_lookup_delete(dash):
    client, _, cluster = dash
    await cluster.wait_until(lambda: all_answer(client), timeout=5)
    code, put = await client.post("/api/put", {"key": "cart:1", "value": "book"})
    assert code == 200 and put["status"] == "OK" and len(put["written_to"]) >= 2

    _, got = await client.post("/api/get", {"key": "cart:1"})
    assert got["status"] == "OK" and got["values"] == ["book"]

    async def on_all_replicas():
        _, key = await client.get("/api/key?k=cart:1")
        return all(key["holders"][n]["versions"] and key["holders"][n]["versions"][0]["value"] == "book"
                   for n in key["replicas"])
    await cluster.wait_until(on_all_replicas, timeout=5)

    _, deleted = await client.post("/api/delete", {"key": "cart:1"})
    assert deleted["status"] == "OK"
    _, got = await client.post("/api/get", {"key": "cart:1"})
    assert got["status"] == "NOT_FOUND"


async def test_kept_context_replaces_and_blind_put_makes_siblings(dash):
    client, _, cluster = dash
    await cluster.wait_until(lambda: all_answer(client), timeout=5)
    _, key = await client.get("/api/key?k=doc:7")
    first, second = key["replicas"][0], key["replicas"][1]
    await client.post("/api/put", {"key": "doc:7", "value": "a", "node": first})
    await client.post("/api/put", {"key": "doc:7", "value": "b", "node": first})
    _, got = await client.post("/api/get", {"key": "doc:7", "node": first})
    assert got["values"] == ["b"]                          # context kept: b replaced a

    # A blind put sent to replica 2 is still coordinated by replica 1 (first ALIVE replica),
    # whose counter is higher, so it replaces too.
    await client.post("/api/put", {"key": "doc:7", "value": "c", "node": second, "blind": True})
    _, got = await client.post("/api/get", {"key": "doc:7", "node": first})
    assert got["values"] == ["c"]

    # With replica 1 down, replica 2 coordinates: its clock is concurrent -> siblings.
    await cluster.kill(first)

    async def first_not_alive():
        _, status = await client.get("/api/status")
        view = status["nodes"][second]
        return view["answered"] and view["members"][first]["state"] != "ALIVE"
    await cluster.wait_until(first_not_alive, timeout=5)
    _, put = await client.post("/api/put", {"key": "doc:7", "value": "d", "node": second,
                                            "blind": True})
    assert put["status"] == "OK"
    _, got = await client.post("/api/get", {"key": "doc:7", "node": second})
    assert sorted(got["values"]) == ["c", "d"]             # concurrent: both kept


async def test_writes_need_the_header_and_valid_input(dash):
    client, _, _ = dash
    code, _ = await client.post("/api/put", {"key": "k", "value": "v"},
                                headers={"Content-Type": "application/json"})
    assert code == 403
    assert (await client.post("/api/put", {"key": "", "value": "v"}))[0] == 400
    assert (await client.post("/api/put", {"key": "k", "node": "node9"}))[0] == 400
    assert (await client.post("/api/get", {"key": "k", "r": -1}))[0] == 400
    assert (await client.post("/api/kill", {"key": "k"}))[0] == 404
    assert (await client.get("/api/key"))[0] == 400


async def test_killed_node_shows_no_answer_and_others_suspect_it(dash):
    client, _, cluster = dash
    await cluster.wait_until(lambda: all_answer(client), timeout=5)
    await cluster.kill("node3")

    async def seen_down():
        _, status = await client.get("/api/status")
        if status["nodes"]["node3"]["answered"]:
            return False
        return all(v["members"]["node3"]["state"] in ("SUSPECT", "DEAD")
                   for n, v in status["nodes"].items() if n != "node3" and v["answered"])
    await cluster.wait_until(seen_down, timeout=5)

    code, put = await client.post("/api/put", {"key": "x", "value": "1", "node": "node3"})
    assert code == 200 and put["status"] == "UNREACHABLE"
