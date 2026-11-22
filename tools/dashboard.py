"""Live dashboard: node states, who sees whom, the ring, where a key is stored, put/get/delete.

    python -m tools.dashboard [--config cluster.local.yaml] [--port 8080] [--interval 1.0]

Listens on 127.0.0.1 only (the cluster has no authentication). From another machine use an ssh
tunnel:  ssh -L 8080:localhost:8080 user@host   then open http://localhost:8080

Uses only the client API of the nodes: ClusterInfo (polled), LocalGet (key lookup) and
Put / Get / Delete (the action box). Where a key belongs is computed with the same Ring code the
nodes use.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import threading
import time
from collections.abc import Callable
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

import grpc

from dynamo import dynamo_pb2, dynamo_pb2_grpc
from dynamo.config import ClusterConfig, load_config
from dynamo.ring import RING_SIZE, Ring, hash32

logger = logging.getLogger("dashboard")

POLL_TIMEOUT_S = 0.5             # per ClusterInfo / LocalGet call
ACTION_TIMEOUT_S = 3.0           # put / get / delete (more than the node's 1.5 s budget)
VALUE_PREVIEW = 60               # characters of a value shown on the page
MAX_BODY = 2 * 1024 * 1024       # largest POST body accepted
WRITE_HEADER = "X-Dashboard"     # must be "1" on every POST (see _allowed_post)
HTML_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dashboard.html")
STATE_NAMES = {dynamo_pb2.ALIVE: "ALIVE", dynamo_pb2.SUSPECT: "SUSPECT",
               dynamo_pb2.DEAD: "DEAD", dynamo_pb2.MEMBER_STATE_UNSPECIFIED: "UNKNOWN"}


# ---------- proto -> JSON ----------

def show_value(raw: bytes) -> str:
    """A value as text for the page: UTF-8 (bad bytes replaced), cut at VALUE_PREVIEW chars."""
    text = raw.decode("utf-8", errors="replace")
    return text if len(text) <= VALUE_PREVIEW else text[:VALUE_PREVIEW] + "…"


def version_json(version: dynamo_pb2.Version) -> dict:
    """One stored version: value preview, tombstone flag, vector clock."""
    return {"value": "" if version.tombstone else show_value(version.value),
            "tombstone": version.tombstone,
            "clock": dict(sorted(version.clock.counters.items()))}


def node_view(info: dynamo_pb2.ClusterInfoResponse) -> dict:
    """What one node says about itself and about every member."""
    return {"answered": True, "node_id": info.node_id, "live_keys": info.live_keys,
            "tombstones": info.tombstones, "ring": info.ring_fingerprint,
            "n": info.n, "r": info.r, "w": info.w,
            "members": {m.node_id: {"state": STATE_NAMES.get(m.state, "UNKNOWN"),
                                    "heartbeat": m.heartbeat, "incarnation": m.incarnation,
                                    "address": m.address}
                        for m in info.members}}


def ring_json(config: ClusterConfig) -> dict:
    """Every vnode's ring position and node, sorted (same formula as ``Ring.__init__``)."""
    vnodes = sorted((hash32(f"{node_id}#{i}"), node_id)
                    for node_id in sorted(config.nodes) for i in range(config.vnodes))
    return {"size": RING_SIZE, "n": config.n, "nodes": list(config.nodes),
            "vnodes": [{"position": p, "node": node_id} for p, node_id in vnodes]}


def placement_json(ring: Ring, key: str, n: int) -> dict:
    """Where a key belongs: ring position, its N replicas, and the full clockwise order
    (the nodes after the replicas are the stand-ins sloppy quorum would use, in order)."""
    order = list(ring.walk(key))
    return {"key": key, "position": ring.position(key), "replicas": order[:n], "order": order}


def holdings_json(answers: dict[str, dynamo_pb2.LocalGetResponse | str]) -> dict:
    """node -> what it stores for the key, or why it did not answer."""
    result = {}
    for node_id, answer in answers.items():
        if isinstance(answer, str):
            result[node_id] = {"answered": False, "error": answer}
        else:
            result[node_id] = {"answered": True,
                               "status": dynamo_pb2.Status.Name(answer.status),
                               "versions": [version_json(v) for v in answer.versions]}
    return result


def error_name(error: grpc.RpcError) -> str:
    """Short name of a failed call, for example DEADLINE_EXCEEDED."""
    code = error.code() if hasattr(error, "code") else None
    return code.name if code is not None else "ERROR"


# ---------- talking to the nodes ----------

class Nodes:
    """One gRPC channel per node (from the config), and the calls the dashboard makes."""

    def __init__(self, config: ClusterConfig) -> None:
        self.ids = list(config.nodes)
        self._channels = {nid: grpc.insecure_channel(addr) for nid, addr in config.nodes.items()}
        self.stubs = {nid: dynamo_pb2_grpc.KVStoreStub(ch) for nid, ch in self._channels.items()}

    def info(self, node_id: str) -> dict:
        """ClusterInfo of one node as JSON, or ``answered: false`` with the error."""
        try:
            return node_view(self.stubs[node_id].ClusterInfo(dynamo_pb2.Empty(),
                                                             timeout=POLL_TIMEOUT_S))
        except grpc.RpcError as error:
            return {"answered": False, "node_id": node_id, "error": error_name(error)}

    def local_gets(self, key: str) -> dict[str, dynamo_pb2.LocalGetResponse | str]:
        """LocalGet of ``key`` on every node; a string is the error of a node that did not answer."""
        answers: dict[str, dynamo_pb2.LocalGetResponse | str] = {}
        for node_id, stub in self.stubs.items():
            try:
                answers[node_id] = stub.LocalGet(dynamo_pb2.LocalGetRequest(key=key),
                                                 timeout=POLL_TIMEOUT_S)
            except grpc.RpcError as error:
                answers[node_id] = error_name(error)
        return answers

    def close(self) -> None:
        """Close every channel."""
        for channel in self._channels.values():
            channel.close()


class Poller:
    """Background thread: ClusterInfo of every node once per ``interval`` seconds."""

    def __init__(self, nodes: Nodes, interval: float) -> None:
        self._nodes = nodes
        self._interval = interval
        self._lock = threading.Lock()
        self._latest: dict = {"time": 0.0, "nodes": {}}
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="poller", daemon=True)

    def start(self) -> None:
        """Poll once right away, then keep polling in the background."""
        self.poll_once()
        self._thread.start()

    def stop(self) -> None:
        """Stop the thread (it finishes its current round)."""
        self._stop.set()

    def poll_once(self) -> None:
        """Ask every node now and keep the answers."""
        views = {nid: self._nodes.info(nid) for nid in self._nodes.ids}
        with self._lock:
            self._latest = {"time": time.time(), "nodes": views}

    def snapshot(self) -> dict:
        """The latest answers of all nodes."""
        with self._lock:
            return self._latest

    def first_answering(self) -> str | None:
        """A node that answered in the last round (default coordinator for actions)."""
        for node_id, view in self.snapshot()["nodes"].items():
            if view.get("answered"):
                return node_id
        return None

    def _run(self) -> None:
        while not self._stop.wait(self._interval):
            self.poll_once()


class Actions:
    """Put / get / delete sent to a chosen node, keeping each key's last context like the
    CLI's context file, so a normal put replaces what the last get or put of that key saw."""

    def __init__(self, nodes: Nodes, default_node: Callable[[], str | None]) -> None:
        self._nodes = nodes
        self._default_node = default_node
        self._contexts: dict[str, bytes] = {}
        self._lock = threading.Lock()

    def put(self, key: str, value: str, node: str = "", w: int = 0, blind: bool = False) -> dict:
        """Store ``value``. ``blind`` sends no context: that makes a sibling only when a node other
        than the last writer coordinates (puts go to the key's first ALIVE replica)."""
        context = b"" if blind else self._context(key)
        request = dynamo_pb2.PutRequest(key=key, value=value.encode(), context=context, w=w)
        return self._call(node, "Put", request, lambda reply: {
            "written_to": list(reply.written_to)})

    def get(self, key: str, node: str = "", r: int = 0) -> dict:
        """Read ``key``; more than one value means siblings."""
        request = dynamo_pb2.GetRequest(key=key, r=r)
        return self._call(node, "Get", request, lambda reply: {
            "values": [show_value(v) for v in reply.values], "read_from": list(reply.read_from)})

    def delete(self, key: str, node: str = "", w: int = 0) -> dict:
        """Delete ``key`` (writes a tombstone over what the last get or put saw)."""
        request = dynamo_pb2.DeleteRequest(key=key, context=self._context(key), w=w)
        return self._call(node, "Delete", request, lambda reply: {})

    def _context(self, key: str) -> bytes:
        with self._lock:
            return self._contexts.get(key, b"")

    def _call(self, node: str, method: str, request, details: Callable) -> dict:
        coordinator = node or self._default_node()
        if coordinator is None:
            return {"status": "UNREACHABLE", "error": "no node answers", "coordinator": ""}
        try:
            reply = getattr(self._nodes.stubs[coordinator], method)(request,
                                                                    timeout=ACTION_TIMEOUT_S)
        except grpc.RpcError as error:
            return {"status": "UNREACHABLE", "error": error_name(error),
                    "coordinator": coordinator}
        context = getattr(reply, "context", b"")
        if context and reply.status in (dynamo_pb2.OK, dynamo_pb2.NOT_FOUND):
            with self._lock:
                self._contexts[request.key] = context
        return {"status": dynamo_pb2.Status.Name(reply.status), "coordinator": coordinator,
                **details(reply)}


# ---------- HTTP ----------

class Dashboard:
    """Everything the HTTP handler needs."""

    def __init__(self, config: ClusterConfig, nodes: Nodes, poller: Poller,
                 actions: Actions) -> None:
        self.config = config
        self.nodes = nodes
        self.poller = poller
        self.actions = actions
        self.ring = Ring(list(config.nodes), config.vnodes)
        self.ring_data = ring_json(config)

    def key(self, key: str) -> dict:
        """Placement of a key plus what every node really holds for it."""
        return {**placement_json(self.ring, key, self.config.n),
                "holders": holdings_json(self.nodes.local_gets(key))}

    def action(self, name: str, body: dict) -> dict:
        """Run put / get / delete from a JSON body; ValueError for a bad request."""
        key = body.get("key")
        node = body.get("node") or ""
        if not isinstance(key, str) or not key:
            raise ValueError("key must be a non-empty string")
        if node and node not in self.config.nodes:
            raise ValueError(f"unknown node {node!r}")
        quorum = body.get("r" if name == "get" else "w") or 0
        if not isinstance(quorum, int) or quorum < 0:
            raise ValueError("r / w must be a whole number >= 0")
        if name == "put":
            value = body.get("value", "")
            if not isinstance(value, str):
                raise ValueError("value must be a string")
            return self.actions.put(key, value, node, quorum, bool(body.get("blind")))
        if name == "get":
            return self.actions.get(key, node, quorum)
        return self.actions.delete(key, node, quorum)


class Handler(BaseHTTPRequestHandler):
    """GET /, /api/status, /api/ring, /api/key?k=KEY; POST /api/put, /api/get, /api/delete."""

    server_version = "dynamo-dashboard"

    @property
    def app(self) -> Dashboard:
        return self.server.app          # type: ignore[attr-defined]

    def do_GET(self) -> None:
        url = urlparse(self.path)
        if url.path == "/":
            with open(HTML_PATH, "rb") as file:
                self._send(200, file.read(), "text/html; charset=utf-8")
        elif url.path == "/api/status":
            self._json(200, {**self.app.poller.snapshot(), "config": {
                "n": self.app.config.n, "r": self.app.config.r, "w": self.app.config.w,
                "nodes": self.app.config.nodes}})
        elif url.path == "/api/ring":
            self._json(200, self.app.ring_data)
        elif url.path == "/api/key":
            key = parse_qs(url.query).get("k", [""])[0]
            if not key:
                self._json(400, {"error": "missing ?k=KEY"})
            else:
                self._json(200, self.app.key(key))
        else:
            self._json(404, {"error": "not found"})

    def do_POST(self) -> None:
        name = urlparse(self.path).path.removeprefix("/api/")
        if name not in ("put", "get", "delete"):
            self._json(404, {"error": "not found"})
            return
        if not _allowed_post(self.headers):
            self._json(403, {"error": f"POST needs {WRITE_HEADER}: 1 and a JSON body"})
            return
        length = int(self.headers.get("Content-Length") or 0)
        if length > MAX_BODY:
            self._json(413, {"error": "body too large"})
            return
        try:
            body = json.loads(self.rfile.read(length) or b"{}")
            if not isinstance(body, dict):
                raise ValueError("body must be a JSON object")
            self._json(200, self.app.action(name, body))
        except ValueError as error:          # json.JSONDecodeError is a ValueError
            self._json(400, {"error": str(error)})

    def log_message(self, format: str, *args) -> None:
        logger.debug("%s - %s", self.address_string(), format % args)

    def _json(self, code: int, data: dict) -> None:
        self._send(code, json.dumps(data).encode(), "application/json")

    def _send(self, code: int, body: bytes, content_type: str) -> None:
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)


def _allowed_post(headers) -> bool:
    """A write must carry ``X-Dashboard: 1`` and a JSON content type.

    A page from another site open in the same browser cannot send a custom header to
    localhost without a CORS preflight, and this server never answers one, so such a page
    cannot make the dashboard write.
    """
    return (headers.get(WRITE_HEADER) == "1"
            and (headers.get("Content-Type") or "").startswith("application/json"))


def make_server(config: ClusterConfig, host: str, port: int,
                interval: float) -> ThreadingHTTPServer:
    """Build the HTTP server with its poller (started) and actions."""
    nodes = Nodes(config)
    poller = Poller(nodes, interval)
    server = ThreadingHTTPServer((host, port), Handler)
    server.app = Dashboard(config, nodes, poller,          # type: ignore[attr-defined]
                           Actions(nodes, poller.first_answering))
    poller.start()
    return server


def main(argv: list[str] | None = None) -> None:
    """Entry point."""
    parser = argparse.ArgumentParser(prog="python -m tools.dashboard",
                                     description=__doc__.split("\n")[0])
    parser.add_argument("--config", default="cluster.local.yaml",
                        help="node ids and the addresses this machine reaches them at")
    parser.add_argument("--host", default="127.0.0.1", help="listen address (keep it local)")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--interval", type=float, default=1.0, help="seconds between polls")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    server = make_server(load_config(args.config), args.host, args.port, args.interval)
    logger.info("dashboard on http://%s:%d  (from another machine: ssh -L %d:localhost:%d ...)",
                args.host, args.port, args.port, args.port)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.app.poller.stop()            # type: ignore[attr-defined]
        server.app.nodes.close()            # type: ignore[attr-defined]
        server.server_close()


if __name__ == "__main__":
    main()
