"""Live dashboard: node states, who sees whom, the ring, where a key is stored, put/get/delete.

    python -m tools.dashboard [--config cluster.local.yaml] [--port 8080] [--interval 1.0]
                              [--control docker|local] [--data-dir data] [--public]

Listens on 127.0.0.1 only (the cluster has no authentication). From another machine use an ssh
tunnel:  ssh -L 8080:localhost:8080 user@host   then open http://localhost:8080

Uses only the client API of the nodes: ClusterInfo (polled), LocalGet (key lookup) and
Put / Get / Delete (the action box). Where a key belongs is computed with the same Ring code the
nodes use. With ``--control`` the node cards also get Kill / Stop / Pause / Resume / Start buttons
(``docker compose`` for the Docker cluster, ``tools/cluster_local.sh`` for local processes).
"""

from __future__ import annotations

import argparse
import asyncio
import collections
import json
import logging
import os
import shutil
import signal
import subprocess
import tempfile
import threading
import time
from collections.abc import Callable
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

import grpc

from dynamo import dynamo_pb2, dynamo_pb2_grpc
from dynamo.config import ClusterConfig, load_config
from dynamo.ring import RING_SIZE, Ring, hash32
from dynamo.storage import HINTS_FILE, SNAPSHOT_FILE, WAL_FILE, Storage

logger = logging.getLogger("dashboard")

POLL_TIMEOUT_S = 0.5             # per ClusterInfo / LocalGet call
ACTION_TIMEOUT_S = 3.0           # put / get / delete (more than the node's 1.5 s budget)
VALUE_PREVIEW = 60               # characters of a value shown on the page
MAX_BODY = 2 * 1024 * 1024       # largest POST body accepted
WRITE_HEADER = "X-Dashboard"     # must be "1" on every POST (see _allowed_post)
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
NODE_ACTIONS = ("kill", "stop", "pause", "resume", "start")
CONTROL_TIMEOUT_S = 30
DISK_CACHE_S = 1.0               # a node's files are read at most once a second
DISK_MAX_KEYS = 200              # keys listed per node on the page
# --public guardrails (no password: anyone can use the page, so it must not stay broken)
AUTO_HEAL_S = 120                # a node downed from the page is brought back after this
MAX_DOWN = 2                     # kill / stop / pause refused when this many are already down
ACTION_GAP_S = 3.0               # at least this long between two node actions
WRITES_PER_S = 10                # put / get / delete per second, whole dashboard
PUBLIC_MAX_KEY = 100             # characters
PUBLIC_MAX_VALUE = 200
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


class Control:
    """Kill / stop / pause / resume / start one node, and read each node's process state.

    ``docker``: ``docker compose <action> NODE`` in the repo folder (the Docker cluster).
    ``local``:  ``tools/cluster_local.sh kill|stop|start NODE``; pause / resume send SIGSTOP /
    SIGCONT to the pid in ``<data_dir>/NODE.pid``.
    Every command is a fixed argument list (no shell); the node id is checked by the caller.
    """

    DOCKER = {"kill": "kill", "stop": "stop", "pause": "pause", "resume": "unpause", "start": "start"}

    def __init__(self, mode: str, root: str = REPO_ROOT, data_dir: str = "data",
                 run: Callable = subprocess.run) -> None:
        if mode not in ("docker", "local"):
            raise ValueError(f"unknown control mode {mode!r}")
        self.mode = mode
        self._root = root
        self._data_dir = os.path.join(root, data_dir)
        self._run = run

    def act(self, node_id: str, action: str) -> dict:
        """Run one action on one node; ``ok`` plus the last lines of the command's output."""
        if action not in NODE_ACTIONS:
            raise ValueError(f"unknown action {action!r}")
        if self.mode == "local" and action in ("pause", "resume"):
            return self._signal(node_id, signal.SIGSTOP if action == "pause" else signal.SIGCONT)
        if self.mode == "docker":
            argv = ["docker", "compose", self.DOCKER[action], node_id]
        else:
            argv = [os.path.join("tools", "cluster_local.sh"), action, node_id]
        try:
            done = self._run(argv, cwd=self._root, capture_output=True, text=True,
                             timeout=CONTROL_TIMEOUT_S)
        except (OSError, subprocess.TimeoutExpired) as error:
            return {"ok": False, "output": str(error)}
        output = (done.stdout + done.stderr).strip().splitlines()[-3:]
        return {"ok": done.returncode == 0, "output": "\n".join(output)}

    def states(self) -> dict[str, str]:
        """node -> process state: running, paused, exited, ... (missing = unknown)."""
        return self._docker_states() if self.mode == "docker" else self._local_states()

    def _docker_states(self) -> dict[str, str]:
        try:
            done = self._run(["docker", "compose", "ps", "-a", "--format", "json"], cwd=self._root,
                             capture_output=True, text=True, timeout=5)
        except (OSError, subprocess.TimeoutExpired):
            return {}
        states = {}
        for line in done.stdout.splitlines():          # one JSON object per line
            try:
                item = json.loads(line)
            except ValueError:
                continue
            if isinstance(item, dict) and "Service" in item:
                states[item["Service"]] = item.get("State", "?")
        return states

    def _pid(self, node_id: str) -> int | None:
        try:
            with open(os.path.join(self._data_dir, f"{node_id}.pid")) as file:
                return int(file.read().strip())
        except (OSError, ValueError):
            return None

    def _local_states(self) -> dict[str, str]:
        states = {}
        for name in os.listdir(self._data_dir) if os.path.isdir(self._data_dir) else []:
            if not name.endswith(".pid"):
                continue
            node_id = name[:-4]
            pid = self._pid(node_id)
            try:
                with open(f"/proc/{pid}/stat") as file:
                    flag = file.read().rsplit(")", 1)[1].split()[0]   # process state letter
                states[node_id] = "paused" if flag == "T" else "running"
            except (OSError, IndexError, TypeError):
                states[node_id] = "exited"
        return states

    def _signal(self, node_id: str, sig: int) -> dict:
        pid = self._pid(node_id)
        if pid is None:
            return {"ok": False, "output": f"{node_id} is not running (no pid file)"}
        try:
            os.kill(pid, sig)
        except OSError as error:
            return {"ok": False, "output": str(error)}
        return {"ok": True, "output": f"{signal.Signals(sig).name} sent to {node_id} (pid {pid})"}


class DiskReader:
    """What a node has on disk: its own keys and the hints it holds.

    No RPC returns hints or lists keys, so this reads the node's files: it copies
    ``snapshot.json``, ``hints.json`` and ``wal.log`` from ``<data_dir>/<node>/`` to a temp folder
    and loads the copy with ``Storage`` itself (same replay rules as the node). The live files are
    only read. Works only on the machine where the nodes keep their data.
    """

    FILES = (SNAPSHOT_FILE, HINTS_FILE, WAL_FILE)

    def __init__(self, config: ClusterConfig, data_dir: str) -> None:
        self._config = config
        self._data_dir = os.path.abspath(data_dir)
        self._cache: dict[str, tuple[float, dict]] = {}
        self._lock = threading.Lock()

    def available(self) -> bool:
        """True when at least one node's data folder is here."""
        return any(os.path.isdir(os.path.join(self._data_dir, nid)) for nid in self._config.nodes)

    def read(self, node_id: str) -> dict | None:
        """{"keys": {key: [Version]}, "hints": {owner: [(key, Version)]}}, or None when the node
        has no data folder here. A read that fails (file changing under us) gives "error".
        Call it from a thread without a running event loop (the HTTP handler threads are)."""
        source = os.path.join(self._data_dir, node_id)
        if not os.path.isdir(source):
            return None
        with self._lock:
            cached = self._cache.get(node_id)
            if cached and time.monotonic() - cached[0] < DISK_CACHE_S:
                return cached[1]
        result = self._load_copy(source)
        with self._lock:
            self._cache[node_id] = (time.monotonic(), result)
        return result

    def _load_copy(self, source: str) -> dict:
        copy = tempfile.mkdtemp(prefix="dashboard-")
        try:
            for name in self.FILES:
                path = os.path.join(source, name)
                if os.path.exists(path):
                    shutil.copyfile(path, os.path.join(copy, name))
            storage = Storage(copy, self._config)
            storage.load()
            result = {"keys": {key: storage.get(key) for key in storage.keys()},
                      "hints": storage.hints()}
            asyncio.run(storage.close())
            return result
        except Exception as error:          # a half-copied file, a snapshot in progress, ...
            logger.warning("reading %s failed: %s", source, error)
            return {"keys": {}, "hints": {}, "error": str(error)}
        finally:
            shutil.rmtree(copy, ignore_errors=True)


def disk_json(node_id: str, held: dict, ring: Ring, n: int) -> dict:
    """One node's stored data for the page: own keys (at most DISK_MAX_KEYS) and hints."""
    keys = sorted(held["keys"])
    return {"node": node_id, "error": held.get("error"),
            "key_count": len(keys),
            "keys": [{"key": key, "replica": node_id in ring.preference_list(key, n),
                      "versions": [version_json(v) for v in held["keys"][key]]}
                     for key in keys[:DISK_MAX_KEYS]],
            "hints": [{"owner": owner, "key": key, "version": version_json(version)}
                      for owner, items in sorted(held["hints"].items()) for key, version in items]}


class Refused(Exception):
    """A request the public guardrails turn down (answered with HTTP 429)."""


class PublicGuard:
    """Guardrails for a password-free public dashboard.

    Self-healing (nodes downed from the page come back after ``auto_heal_s``), at most
    ``max_down`` nodes not running, one node action per ``action_gap_s``, ``writes_per_s``
    put / get / delete per second, and short keys and values.
    """

    DOWN_ACTIONS = ("kill", "stop", "pause")

    def __init__(self, control: "Control | None", clock: Callable[[], float] = time.monotonic,
                 auto_heal_s: float = AUTO_HEAL_S, max_down: int = MAX_DOWN,
                 action_gap_s: float = ACTION_GAP_S, writes_per_s: int = WRITES_PER_S) -> None:
        self._control = control
        self._clock = clock
        self.auto_heal_s = auto_heal_s
        self.max_down = max_down
        self._action_gap_s = action_gap_s
        self._writes_per_s = writes_per_s
        self._downed: dict[str, float] = {}           # node -> when the page downed it
        self._last_action = -float("inf")
        self._writes: collections.deque[float] = collections.deque()
        self._lock = threading.Lock()

    def check_node_action(self, node_id: str, action: str, states: dict[str, str],
                          node_ids: list[str]) -> None:
        """Raise Refused if this node action is not allowed right now."""
        now = self._clock()
        with self._lock:
            wait = self._last_action + self._action_gap_s - now
            if wait > 0:
                raise Refused(f"one node action every {self._action_gap_s:g} s: try again in {wait:.0f} s")
            if action in self.DOWN_ACTIONS and states.get(node_id) == "running":
                down = [n for n in node_ids if states.get(n) != "running"]
                if len(down) >= self.max_down:
                    raise Refused(f"public demo: at most {self.max_down} nodes down at once "
                                  f"({', '.join(down)} already are)")
            self._last_action = now

    def record(self, node_id: str, action: str) -> None:
        """Remember nodes the page took down (for self-healing); forget them when brought back."""
        with self._lock:
            if action in self.DOWN_ACTIONS:
                self._downed.setdefault(node_id, self._clock())
            else:
                self._downed.pop(node_id, None)

    def check_write(self, key: str, value: str) -> None:
        """Raise Refused for a too long key / value or too many writes per second."""
        if len(key) > PUBLIC_MAX_KEY or len(value) > PUBLIC_MAX_VALUE:
            raise Refused(f"public demo: key at most {PUBLIC_MAX_KEY} and value at most "
                          f"{PUBLIC_MAX_VALUE} characters")
        now = self._clock()
        with self._lock:
            while self._writes and now - self._writes[0] >= 1.0:
                self._writes.popleft()
            if len(self._writes) >= self._writes_per_s:
                raise Refused(f"public demo: at most {self._writes_per_s} requests per second")
            self._writes.append(now)

    def heal_once(self, states: dict[str, str]) -> list[str]:
        """Bring back nodes downed from the page more than ``auto_heal_s`` ago. Returns what it did."""
        now = self._clock()
        with self._lock:
            due = [n for n, since in self._downed.items() if now - since >= self.auto_heal_s]
        done = []
        for node_id in due:
            state = states.get(node_id)
            if state != "running" and self._control is not None:
                action = "resume" if state == "paused" else "start"
                result = self._control.act(node_id, action)
                logger.info("self-heal: %s %s -> %s", action, node_id, result.get("ok"))
                done.append(f"{action} {node_id}")
            with self._lock:
                self._downed.pop(node_id, None)
        return done

    def info(self) -> dict:
        """For /api/status: the limits and, per downed node, seconds until it comes back."""
        now = self._clock()
        with self._lock:
            back_in = {n: max(0, round(self.auto_heal_s - (now - since)))
                       for n, since in self._downed.items()}
        return {"auto_heal_s": self.auto_heal_s, "max_down": self.max_down,
                "action_gap_s": self._action_gap_s, "writes_per_s": self._writes_per_s,
                "back_in": back_in}


class Healer:
    """Background thread: every few seconds, ``guard.heal_once(control.states())``."""

    def __init__(self, guard: PublicGuard, control: "Control", interval: float = 5.0) -> None:
        self._guard = guard
        self._control = control
        self._interval = interval
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="healer", daemon=True)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()

    def _run(self) -> None:
        while not self._stop.wait(self._interval):
            try:
                self._guard.heal_once(self._control.states())
            except Exception:                     # keep healing whatever one round did
                logger.exception("self-heal round failed")


class Poller:
    """Background thread: ClusterInfo of every node once per ``interval`` seconds."""

    def __init__(self, nodes: Nodes, interval: float,
                 process_states: Callable[[], dict[str, str]] | None = None) -> None:
        self._nodes = nodes
        self._interval = interval
        self._process_states = process_states
        self._lock = threading.Lock()
        self._latest: dict = {"time": 0.0, "nodes": {}, "processes": {}}
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
        processes = self._process_states() if self._process_states else {}
        with self._lock:
            self._latest = {"time": time.time(), "nodes": views, "processes": processes}

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
                 actions: Actions, control: Control | None = None,
                 disk: DiskReader | None = None, guard: PublicGuard | None = None) -> None:
        self.config = config
        self.control = control
        self.guard = guard
        self.disk = disk if disk is not None and disk.available() else None
        self.nodes = nodes
        self.poller = poller
        self.actions = actions
        self.ring = Ring(list(config.nodes), config.vnodes)
        self.ring_data = ring_json(config)

    def key(self, key: str) -> dict:
        """Placement of a key plus what every node really holds for it."""
        return {**placement_json(self.ring, key, self.config.n),
                "holders": holdings_json(self.nodes.local_gets(key)),
                "hints": self.key_hints(key)}

    def key_hints(self, key: str) -> dict[str, list[dict]]:
        """node -> hints it holds for ``key`` ({owner, version}); {} when disk reading is off."""
        if self.disk is None:
            return {}
        found: dict[str, list[dict]] = {}
        for node_id in self.config.nodes:
            held = self.disk.read(node_id)
            for owner, items in (held or {}).get("hints", {}).items():
                for hint_key, version in items:
                    if hint_key == key:
                        found.setdefault(node_id, []).append(
                            {"owner": owner, "version": version_json(version)})
        return found

    def disk_view(self, node_id: str) -> dict:
        """Everything one node stores; LookupError when disk reading is off or no folder."""
        if node_id not in self.config.nodes:
            raise ValueError(f"unknown node {node_id!r}")
        held = self.disk.read(node_id) if self.disk else None
        if held is None:
            raise LookupError("no data folder for this node here (start with --data-dir)")
        return disk_json(node_id, held, self.ring, self.config.n)

    def node_action(self, body: dict) -> dict:
        """Kill / stop / pause / resume / start one node; LookupError if control is off."""
        if self.control is None:
            raise LookupError("node control is off (start the dashboard with --control)")
        node, action = body.get("node"), body.get("action")
        if node not in self.config.nodes:
            raise ValueError(f"unknown node {node!r}")
        if action not in NODE_ACTIONS:
            raise ValueError(f"action must be one of {', '.join(NODE_ACTIONS)}")
        if self.guard is not None:
            self.guard.check_node_action(node, action, self.control.states(), list(self.config.nodes))
        logger.info("node control: %s %s", action, node)
        result = self.control.act(node, action)
        if self.guard is not None and result.get("ok"):
            self.guard.record(node, action)
        self.poller.poll_once()                    # show the effect at once
        return {"node": node, "action": action, **result}

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
        if self.guard is not None:
            value = body.get("value", "")
            self.guard.check_write(key, value if isinstance(value, str) else "")
        if name == "put":
            value = body.get("value", "")
            if not isinstance(value, str):
                raise ValueError("value must be a string")
            return self.actions.put(key, value, node, quorum, bool(body.get("blind")))
        if name == "get":
            return self.actions.get(key, node, quorum)
        return self.actions.delete(key, node, quorum)


class Handler(BaseHTTPRequestHandler):
    """GET /, /api/status, /api/ring, /api/key?k=KEY, /api/disk?node=NODE;
    POST /api/put, /api/get, /api/delete, /api/node (node controls, only with --control)."""

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
                "nodes": self.app.config.nodes,
                "control": self.app.control.mode if self.app.control else None,
                "disk": self.app.disk is not None,
                "public": self.app.guard.info() if self.app.guard else None}})
        elif url.path == "/api/ring":
            self._json(200, self.app.ring_data)
        elif url.path == "/api/disk":
            try:
                self._json(200, self.app.disk_view(parse_qs(url.query).get("node", [""])[0]))
            except LookupError as error:
                self._json(404, {"error": str(error)})
            except ValueError as error:
                self._json(400, {"error": str(error)})
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
        if name not in ("put", "get", "delete", "node"):
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
            if name == "node":
                self._json(200, self.app.node_action(body))
            else:
                self._json(200, self.app.action(name, body))
        except Refused as error:
            self._json(429, {"error": str(error)})
        except LookupError as error:
            self._json(404, {"error": str(error)})
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


def make_server(config: ClusterConfig, host: str, port: int, interval: float,
                control: Control | None = None, data_dir: str | None = None,
                public: bool = False) -> ThreadingHTTPServer:
    """Build the HTTP server with its poller (started), actions and optional node control."""
    nodes = Nodes(config)
    poller = Poller(nodes, interval, control.states if control else None)
    server = ThreadingHTTPServer((host, port), Handler)
    server.app = Dashboard(config, nodes, poller,          # type: ignore[attr-defined]
                           Actions(nodes, poller.first_answering), control,
                           DiskReader(config, data_dir) if data_dir else None,
                           PublicGuard(control) if public else None)
    poller.start()
    if public and control is not None:
        server.healer = Healer(server.app.guard, control)   # type: ignore[attr-defined]
        server.healer.start()                                # type: ignore[attr-defined]
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
    parser.add_argument("--control", choices=["docker", "local"],
                        help="show Kill / Stop / Pause / Resume / Start buttons (off by default)")
    parser.add_argument("--data-dir", default="data",
                        help="the nodes' data folders (DIR/<node>/), read for hints and stored keys")
    parser.add_argument("--public", action="store_true",
                        help="open to anyone: self-healing, at most 2 nodes down, rate limits")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    control = Control(args.control) if args.control else None
    server = make_server(load_config(args.config), args.host, args.port, args.interval, control,
                         args.data_dir, args.public)
    logger.info("dashboard on http://%s:%d  (from another machine: ssh -L %d:localhost:%d ...)",
                args.host, args.port, args.port, args.port)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        if hasattr(server, "healer"):
            server.healer.stop()
        server.app.poller.stop()            # type: ignore[attr-defined]
        server.app.nodes.close()            # type: ignore[attr-defined]
        server.server_close()


if __name__ == "__main__":
    main()
