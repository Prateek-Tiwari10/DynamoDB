"""Shared test cluster: k real nodes in this test process, on free localhost ports.

Every node runs in the test's event loop with short timers, so a test of "a node dies and
the others notice" takes about a second. A node can be stopped cleanly, killed (crash), and
restarted with the same id, port and data folder.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import os
import socket
import time
from collections.abc import Awaitable, Callable

import grpc
import pytest

from dynamo import dynamo_pb2, dynamo_pb2_grpc
from dynamo.config import ClusterConfig
from dynamo.server import Node, build_node

logger = logging.getLogger(__name__)

TEST_TIMERS = {
    "gossip_interval_ms": 100,
    "suspect_after_ms": 500,
    "dead_after_ms": 1000,
    "rpc_timeout_ms": 200,
    "handoff_retry_ms": 300,
}

Condition = Callable[[], bool | Awaitable[bool]]


def free_ports(count: int) -> list[int]:
    """Ask the OS for ``count`` different free ports.

    All sockets stay open until every port is picked, so the ports are distinct. Another
    program could still take one before the node binds it; we accept that small race.
    """
    sockets = [socket.socket() for _ in range(count)]
    try:
        for sock in sockets:
            sock.bind(("127.0.0.1", 0))
        return [sock.getsockname()[1] for sock in sockets]
    finally:
        for sock in sockets:
            sock.close()


async def wait_until(condition: Condition, timeout: float = 5.0, interval: float = 0.05) -> None:
    """Poll ``condition`` (plain or async) until it is true. Fails the test after ``timeout`` s."""
    deadline = time.monotonic() + timeout
    while True:
        result = condition()
        if inspect.isawaitable(result):
            result = await result
        if result:
            return
        if time.monotonic() >= deadline:
            raise AssertionError(f"condition still false after {timeout} s")
        await asyncio.sleep(interval)


def cluster_config(node_ids: list[str], ports: list[int], **overrides) -> ClusterConfig:
    """Config for a test cluster: short timers, and N/R/W shrunk when there are fewer than 3 nodes."""
    n = min(3, len(node_ids))
    settings = {"n": n, "r": min(2, n), "w": min(2, n), **TEST_TIMERS, **overrides}
    nodes = {node_id: f"localhost:{port}" for node_id, port in zip(node_ids, ports)}
    return ClusterConfig(nodes=nodes, **settings)


class Cluster:
    """k nodes of one test cluster, with stop, kill and restart for single nodes."""

    def __init__(self, k: int, data_dir: str, **overrides) -> None:
        """Pick ports and build the config. Starts nothing."""
        node_ids = [f"node{i}" for i in range(1, k + 1)]
        self.ports: dict[str, int] = dict(zip(node_ids, free_ports(k)))
        self.config = cluster_config(node_ids, list(self.ports.values()), **overrides)
        self.data_dir = data_dir
        self.nodes: dict[str, Node] = {}           # running nodes by id
        self._down: list[Node] = []                # killed or stopped Node objects
        self._channels: dict[str, grpc.aio.Channel] = {}

    # ----- access

    def client(self, node_id: str) -> dynamo_pb2_grpc.KVStoreStub:
        """Client API stub for one node."""
        return dynamo_pb2_grpc.KVStoreStub(self._channel(node_id))

    def replica(self, node_id: str) -> dynamo_pb2_grpc.NodeServiceStub:
        """Node-to-node API stub for one node, as a peer would call it."""
        return dynamo_pb2_grpc.NodeServiceStub(self._channel(node_id))

    async def wait_until(self, condition: Condition, timeout: float = 5.0) -> None:
        """Same as the module-level ``wait_until``; here so tests only need the cluster."""
        await wait_until(condition, timeout)

    # ----- whole cluster

    async def start(self) -> None:
        """Start every node, then check each port is answered by the node that owns it."""
        for node_id in self.config.nodes:
            await self._start_node(node_id)
        for node_id in self.config.nodes:
            info = await self.client(node_id).ClusterInfo(dynamo_pb2.Empty(), timeout=2)
            assert info.node_id == node_id, f"port of {node_id} is answered by {info.node_id}"

    async def stop(self) -> None:
        """Stop everything: client channels, running nodes, and files left open by killed nodes.

        Safe to call twice. One failing node does not stop the others from being cleaned up.
        """
        for channel in self._channels.values():
            await channel.close()
        self._channels.clear()
        for node_id, node in list(self.nodes.items()):
            try:
                await node.stop()
            except Exception:
                logger.exception("teardown: stopping %s failed", node_id)
        self.nodes.clear()
        for node in self._down:
            await node.storage.close()   # no-op for stopped nodes; closes a killed node's log
        self._down.clear()

    # ----- single nodes

    async def stop_node(self, node_id: str) -> None:
        """Clean shutdown: the node's own ``stop()`` (drains work, closes storage)."""
        node = self.nodes.pop(node_id)
        await node.stop()
        self._down.append(node)

    async def kill(self, node_id: str) -> None:
        """Crash: stop serving at once and stop all background work, but never close storage.

        Background coordinator tasks are cancelled too, so the dead node cannot write to the
        log after a restarted copy of it has opened the same log.
        """
        node = self.nodes.pop(node_id)
        await node.server.stop(0)
        await node.membership.stop()
        await node.handoff.stop()
        await node.coordinator.drain(0)
        await node.peers.close()
        self._down.append(node)

    async def restart(self, node_id: str) -> None:
        """Bring a stopped or killed node back: same id, port and data folder, new incarnation."""
        if node_id in self.nodes:
            raise RuntimeError(f"{node_id} is still running")
        old = self._channels.pop(node_id, None)
        if old is not None:
            await old.close()        # a fresh channel connects at once, without waiting out a backoff
        await self._start_node(node_id)

    async def _start_node(self, node_id: str) -> None:
        node = build_node(self.config, node_id, self.ports[node_id], self.data_dir)
        await node.start()
        self.nodes[node_id] = node

    def _channel(self, node_id: str) -> grpc.aio.Channel:
        if node_id not in self._channels:
            self._channels[node_id] = grpc.aio.insecure_channel(f"localhost:{self.ports[node_id]}")
        return self._channels[node_id]


MakeCluster = Callable[..., Awaitable[Cluster]]


@pytest.fixture
async def make_cluster(tmp_path) -> MakeCluster:
    """Factory: ``await make_cluster(k, **config_overrides)`` starts a k-node cluster.

    Every cluster made in the test is stopped at teardown, also when the test fails.
    """
    made: list[Cluster] = []

    async def make(k: int = 5, **overrides) -> Cluster:
        data_dir = os.path.join(tmp_path, f"cluster{len(made) + 1}")
        cluster = Cluster(k, data_dir, **overrides)
        made.append(cluster)        # before start(), so a half-started cluster is cleaned up too
        await cluster.start()
        return cluster

    yield make
    for cluster in reversed(made):
        await cluster.stop()


@pytest.fixture
async def cluster(make_cluster) -> Cluster:
    """A running 5-node cluster with test timers."""
    return await make_cluster(5)
