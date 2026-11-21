"""Control the real 5-node cluster from outside: start, crash, stop, pause, restart, and ask
each node what it sees.

Used by the cluster tests (``tests/cluster``) and by the chaos run (``tools/chaos.py``).
Two targets, same interface:

- ``local`` (default): 5 processes on this machine via ``tools/cluster_local.sh``,
  data in ``./data-cluster``.
- ``docker``: the Docker Compose cluster (``docker compose``), data in ``./data``.

Either way the nodes answer on localhost:50051-50055 and use the real timers from the config.
"""

from __future__ import annotations

import asyncio
import logging
import os
import shutil
import signal
import sys
import time
from collections.abc import Awaitable, Callable

import grpc

from dynamo import dynamo_pb2, dynamo_pb2_grpc
from dynamo.config import load_config
from dynamo.ring import Ring

logger = logging.getLogger(__name__)

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
NODES = [f"node{i}" for i in range(1, 6)]
CALL_TIMEOUT_S = 3.0                    # every client call; a node's own request budget is 1.5 s
CHANNEL_OPTIONS = [("grpc.max_reconnect_backoff_ms", 1000)]

# config name -> (yaml for local processes, compose override for docker or None)
CONFIGS = {
    "normal": ("cluster.local.yaml", None),
    "nosloppy": ("cluster.local.nosloppy.yaml", "docker-compose.nosloppy.yml"),
}


class ClusterError(Exception):
    """The cluster could not be brought into the state a step needs."""


async def wait_until(condition: Callable[[], Awaitable[bool]], timeout: float, what: str,
                     interval: float = 0.25) -> float:
    """Poll the async ``condition`` until it is true. Returns the seconds it took.
    Raises ClusterError naming ``what`` after ``timeout`` seconds."""
    start = time.monotonic()
    while True:
        if await condition():
            return time.monotonic() - start
        if time.monotonic() - start >= timeout:
            raise ClusterError(f"timed out after {timeout:.0f} s waiting for: {what}")
        await asyncio.sleep(interval)


async def run_command(*args: str, env: dict[str, str] | None = None) -> None:
    """Run a command from the repo root; raise ClusterError with its output if it fails."""
    process = await asyncio.create_subprocess_exec(
        *args, cwd=REPO_ROOT, env={**os.environ, **(env or {})},
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
    output, _ = await process.communicate()
    if process.returncode != 0:
        raise ClusterError(f"{' '.join(args)} failed ({process.returncode}): "
                           f"{output.decode(errors='replace').strip()}")


# ---------------------------------------------------------------- backends

class LocalBackend:
    """5 local processes through ``tools/cluster_local.sh``."""

    def __init__(self, data_dir: str) -> None:
        self.data_dir = data_dir
        self.config = "normal"

    async def _script(self, command: str, *nodes: str) -> None:
        yaml_file = CONFIGS[self.config][0]
        await run_command("tools/cluster_local.sh", command, *nodes,
                          env={"DATA_DIR": self.data_dir, "CONFIG": yaml_file,
                               "PYTHON": sys.executable})

    async def start_all(self, config: str) -> None:
        self.config = config
        await self._script("start")

    async def start(self, node: str) -> None:
        await self._script("start", node)

    async def kill(self, node: str) -> None:
        self._signal(node, signal.SIGCONT)        # a frozen process must run to die cleanly
        await self._script("kill", node)

    async def stop(self, node: str) -> None:
        self._signal(node, signal.SIGCONT)        # SIGTERM waits while a process is frozen
        await self._script("stop", node)

    async def stop_all(self) -> None:
        for node in NODES:
            self._signal(node, signal.SIGCONT)
        await self._script("stop")

    async def pause(self, node: str) -> None:
        self._signal(node, signal.SIGSTOP)

    async def unpause(self, node: str) -> None:
        self._signal(node, signal.SIGCONT)

    async def wipe(self) -> None:
        shutil.rmtree(self.data_dir, ignore_errors=True)

    def _signal(self, node: str, sig: signal.Signals) -> None:
        try:
            with open(os.path.join(self.data_dir, f"{node}.pid"), encoding="ascii") as f:
                os.kill(int(f.read().strip()), sig)
        except (OSError, ValueError):
            pass                                  # not running: nothing to signal


class DockerBackend:
    """The Docker Compose cluster (``docker-compose.yml`` + an override per config)."""

    def __init__(self) -> None:
        self.config = "normal"

    def _compose(self) -> list[str]:
        files = ["-f", "docker-compose.yml"]
        override = CONFIGS[self.config][1]
        if override:
            files += ["-f", override]
        return ["docker", "compose", *files]

    async def start_all(self, config: str) -> None:
        self.config = config
        await run_command(*self._compose(), "up", "-d", "--build")   # recreates changed services

    async def start(self, node: str) -> None:
        await run_command(*self._compose(), "start", node)

    async def kill(self, node: str) -> None:
        await run_command(*self._compose(), "kill", node)

    async def stop(self, node: str) -> None:
        await run_command(*self._compose(), "stop", node)

    async def stop_all(self) -> None:
        await run_command(*self._compose(), "stop")

    async def pause(self, node: str) -> None:
        await run_command(*self._compose(), "pause", node)

    async def unpause(self, node: str) -> None:
        await run_command(*self._compose(), "unpause", node)

    async def wipe(self) -> None:
        await run_command(*self._compose(), "down")
        # Files in ./data are written by root inside the containers, so delete them from inside.
        await run_command(*self._compose(), "run", "--rm", "--no-deps", "--entrypoint", "sh",
                          "node1", "-c", "rm -rf /data/*")


# ---------------------------------------------------------------- the cluster

class RealCluster:
    """The real 5-node cluster on localhost:50051-55, controlled from outside."""

    def __init__(self, target: str = "local", data_dir: str | None = None) -> None:
        if target == "local":
            self.backend = LocalBackend(data_dir or os.path.join(REPO_ROOT, "data-cluster"))
        elif target == "docker":
            self.backend = DockerBackend()
        else:
            raise ValueError(f"unknown target {target!r}: use local or docker")
        self.target = target
        config = load_config(os.path.join(REPO_ROOT, CONFIGS["normal"][0]))
        self.addresses = dict(config.nodes)                 # localhost:50051.. for both targets
        self.ring = Ring(list(config.nodes), config.vnodes)
        self.n = config.n
        self.config: str | None = None                      # what runs now (None = nothing yet)
        self.down: set[str] = set()                         # crashed or stopped
        self.paused: set[str] = set()
        self._channels: dict[str, grpc.aio.Channel] = {}

    # ----- whole cluster

    async def ensure(self, config: str, wipe: bool = False) -> None:
        """Make all 5 nodes run with ``config`` and be ALIVE everywhere. Restarts every node only
        when a different config runs; otherwise only heals. ``wipe`` (first start only) deletes
        all data before starting."""
        if self.config == config and not wipe:
            await self.heal()
            return
        if self.config is None:
            await self._check_ports_free()
        else:
            await self.backend.stop_all()
        if wipe:
            await self.backend.wipe()
        await self.backend.start_all(config)
        self.config = config
        self.down.clear()
        self.paused.clear()
        await self._reopen_all()
        await self.wait_all_alive()

    async def heal(self) -> None:
        """Unpause and start every node that is not running; wait until all are ALIVE everywhere."""
        for node in sorted(self.paused):
            await self.unpause(node)
        for node in sorted(self.down):
            await self.start(node)
        await self.wait_all_alive()

    async def shutdown(self) -> None:
        """Stop every node (end of a test session or chaos run) and close the channels."""
        for node in sorted(self.paused):
            await self.unpause(node)
        await self.backend.stop_all()
        self.config = None
        for channel in self._channels.values():
            await channel.close()
        self._channels.clear()

    async def _check_ports_free(self) -> None:
        """Stop our own leftovers, then refuse to go on if anything else answers on our ports:
        never kill someone else's cluster."""
        await self.backend.stop_all()
        for node in NODES:
            channel = grpc.aio.insecure_channel(self.addresses[node])
            try:
                await dynamo_pb2_grpc.KVStoreStub(channel).ClusterInfo(dynamo_pb2.Empty(), timeout=0.5)
            except grpc.aio.AioRpcError:
                continue                                     # nothing there: good
            finally:
                await channel.close()
            raise ClusterError(f"something already answers on {self.addresses[node]}: stop that "
                               "cluster first (tools/cluster_local.sh stop / make down)")

    # ----- single nodes

    async def kill(self, node: str) -> None:
        """Crash: SIGKILL / docker kill."""
        await self.backend.kill(node)
        self.paused.discard(node)
        self.down.add(node)

    async def stop(self, node: str) -> None:
        """Clean stop: SIGTERM / docker stop (the node drains for up to 2 s)."""
        await self.backend.stop(node)
        self.paused.discard(node)
        self.down.add(node)

    async def start(self, node: str) -> None:
        """Start a crashed or stopped node again, with the current config."""
        await self.backend.start(node)
        self.down.discard(node)
        await self._reopen(node)                 # a fresh channel connects at once, no backoff
        await wait_until(lambda: self._answers(node), 15, f"{node} answering after start")

    async def _answers(self, node: str) -> bool:
        try:
            await self.client(node).ClusterInfo(dynamo_pb2.Empty(), timeout=CALL_TIMEOUT_S)
            return True
        except grpc.aio.AioRpcError:
            return False

    async def pause(self, node: str) -> None:
        """Freeze the node: its port stays open but it answers nothing (a very slow node)."""
        await self.backend.pause(node)
        self.paused.add(node)

    async def unpause(self, node: str) -> None:
        await self.backend.unpause(node)
        self.paused.discard(node)

    def running(self) -> list[str]:
        """Nodes that are up and not frozen."""
        return [node for node in NODES if node not in self.down and node not in self.paused]

    def up(self) -> list[str]:
        """Nodes a client would send to: not crashed or stopped (a frozen node looks up)."""
        return [node for node in NODES if node not in self.down]

    # ----- talking to nodes

    def client(self, node: str) -> dynamo_pb2_grpc.KVStoreStub:
        return dynamo_pb2_grpc.KVStoreStub(self._channel(node))

    def peer(self, node: str) -> dynamo_pb2_grpc.NodeServiceStub:
        return dynamo_pb2_grpc.NodeServiceStub(self._channel(node))

    async def owners(self, key: str) -> list[str]:
        """The key's replicas, as a running node reports them (Locate)."""
        reply = await self.client(self.running()[0]).Locate(
            dynamo_pb2.LocateRequest(key=key), timeout=CALL_TIMEOUT_S)
        return list(reply.nodes)

    def owners_local(self, key: str) -> list[str]:
        """The key's replicas from our own copy of the ring (no RPC; same ring as the nodes)."""
        return self.ring.preference_list(key, self.n)

    async def states_seen_by(self, observer: str) -> dict[str, int]:
        """node -> MemberState, as ``observer`` sees it (ClusterInfo)."""
        info = await self.client(observer).ClusterInfo(dynamo_pb2.Empty(), timeout=CALL_TIMEOUT_S)
        return {member.node_id: member.state for member in info.members}

    async def state_seen_by(self, observer: str, node: str) -> int:
        return (await self.states_seen_by(observer))[node]

    async def all_alive(self) -> bool:
        """True if every running node sees every running node ALIVE."""
        running = self.running()
        for observer in running:
            try:
                states = await self.states_seen_by(observer)
            except grpc.aio.AioRpcError:
                return False
            if any(states.get(node) != dynamo_pb2.ALIVE for node in running):
                return False
        return True

    async def wait_all_alive(self, timeout: float = 30) -> float:
        """Wait until every running node sees every running node ALIVE. Returns the seconds."""
        return await wait_until(self.all_alive, timeout,
                                f"all of {self.running()} ALIVE in every view")

    def _channel(self, node: str) -> grpc.aio.Channel:
        if node not in self._channels:
            self._channels[node] = grpc.aio.insecure_channel(self.addresses[node],
                                                             options=CHANNEL_OPTIONS)
        return self._channels[node]

    async def _reopen(self, node: str) -> None:
        old = self._channels.pop(node, None)
        if old is not None:
            await old.close()

    async def _reopen_all(self) -> None:
        for node in list(self._channels):
            await self._reopen(node)
