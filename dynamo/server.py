"""One node: builds every part, serves both gRPC services on one port, stops cleanly."""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import signal
from dataclasses import dataclass

import grpc
import yaml

from dynamo import dynamo_pb2, dynamo_pb2_grpc
from dynamo.antientropy import AntiEntropy
from dynamo.config import ClusterConfig, load_config, setup_logging
from dynamo.coordinator import Coordinator, request_budget
from dynamo.handoff import Handoff
from dynamo.membership import Membership
from dynamo.peers import Peers
from dynamo.replica import NodeServiceServicer
from dynamo.reqid import RequestIdFilter, rpc_handler
from dynamo.ring import Ring
from dynamo.storage import Storage
from dynamo.wire import version_to_proto

logger = logging.getLogger(__name__)

MAX_KEY_BYTES = 256
MAX_VALUE_BYTES = 1024 * 1024
STOP_GRACE_S = 2.0


@dataclass
class Node:
    """Everything one node owns. Handlers get this object; there is no global state."""

    node_id: str
    config: ClusterConfig
    storage: Storage
    ring: Ring
    peers: Peers
    membership: Membership
    coordinator: Coordinator
    handoff: Handoff
    anti_entropy: AntiEntropy
    server: grpc.aio.Server
    port: int = 0  # asked-for port until start(); the bound port after it (0 = any free port)

    async def start(self) -> None:
        """Recover storage, then open the port, then start gossip, hint delivery and anti-entropy."""
        self.storage.load()
        bound = self.server.add_insecure_port(f"0.0.0.0:{self.port}")
        if bound == 0:
            raise RuntimeError(f"{self.node_id}: could not bind port {self.port}")
        self.port = bound
        await self.server.start()
        await self.membership.start()
        await self.handoff.start()
        await self.anti_entropy.start()

    async def stop(self) -> None:
        """Stop serving, stop the loops, finish background work, close channels, close storage."""
        await self.server.stop(STOP_GRACE_S)
        await self.membership.stop()
        await self.handoff.stop()
        await self.anti_entropy.stop()
        await self.coordinator.drain(STOP_GRACE_S)
        await self.peers.close()
        await self.storage.close()


def build_node(config: ClusterConfig, node_id: str, port: int, data_dir: str) -> Node:
    """Wire one node together. Binds nothing and creates no files; ``start()`` does that."""
    if node_id not in config.nodes:
        raise ValueError(f"node id {node_id!r} is not in the config")
    storage = Storage(os.path.join(data_dir, node_id), config)
    ring = Ring(list(config.nodes), config.vnodes)
    peers = Peers(config, node_id)
    membership = Membership(node_id, config, peers, ring.fingerprint(config.n))
    coordinator = Coordinator(node_id, config, ring, storage, peers, membership)
    handoff = Handoff(node_id, config, storage, membership, peers)
    anti_entropy = AntiEntropy(node_id, config, storage, ring, membership, peers)
    # No SO_REUSEPORT: a second node on a port that is already taken must fail to start,
    # not silently share the port with the first one.
    server = grpc.aio.server(options=[("grpc.so_reuseport", 0)])
    node = Node(node_id=node_id, config=config, storage=storage, ring=ring, peers=peers,
                membership=membership, coordinator=coordinator, handoff=handoff,
                anti_entropy=anti_entropy,
                server=server, port=port)
    dynamo_pb2_grpc.add_KVStoreServicer_to_server(KVStoreServicer(node), server)
    dynamo_pb2_grpc.add_NodeServiceServicer_to_server(NodeServiceServicer(node), server)
    return node


def validate(key: str, value: bytes | None, quorum: int, n: int) -> bool:
    """Key 1-256 bytes, value at most 1 MiB, quorum 0 (cluster default) up to N."""
    if not 1 <= len(key.encode()) <= MAX_KEY_BYTES:
        return False
    if value is not None and len(value) > MAX_VALUE_BYTES:
        return False
    return 0 <= quorum <= n


class KVStoreServicer(dynamo_pb2_grpc.KVStoreServicer):
    """The client API. Put, Get and Delete go to the coordinator; the rest is answered here."""

    def __init__(self, node: Node) -> None:
        self._node = node

    @rpc_handler("client")
    async def Put(self, request, context):
        """Store a value."""
        if not validate(request.key, request.value, request.w, self._node.config.n):
            return dynamo_pb2.PutResponse(status=dynamo_pb2.INVALID)
        return await self._node.coordinator.put(request, request_budget(context, self._node.config))

    @rpc_handler("client")
    async def Get(self, request, context):
        """Read a value (or its siblings)."""
        if not validate(request.key, None, request.r, self._node.config.n):
            return dynamo_pb2.GetResponse(status=dynamo_pb2.INVALID)
        return await self._node.coordinator.get(request, request_budget(context, self._node.config))

    @rpc_handler("client")
    async def Delete(self, request, context):
        """Write a tombstone."""
        if not validate(request.key, None, request.w, self._node.config.n):
            return dynamo_pb2.DeleteResponse(status=dynamo_pb2.INVALID)
        return await self._node.coordinator.delete(request, request_budget(context, self._node.config))

    @rpc_handler("client")
    async def ClusterInfo(self, request, context):
        """This node's view of the cluster. Local only, never forwarded."""
        node = self._node
        live_keys, tombstones = node.storage.counts()
        return dynamo_pb2.ClusterInfoResponse(
            members=node.membership.members(),
            n=node.config.n, r=node.config.r, w=node.config.w,
            node_id=node.node_id,
            vnodes=node.config.vnodes,
            ring_fingerprint=node.ring.fingerprint(node.config.n),
            live_keys=live_keys,
            tombstones=tombstones,
        )

    @rpc_handler("client")
    async def Locate(self, request, context):
        """The key's ring position and replicas, from this node's ring. Local only."""
        ring = self._node.ring
        return dynamo_pb2.LocateResponse(
            nodes=ring.preference_list(request.key, self._node.config.n),
            position=ring.position(request.key),
        )

    @rpc_handler("client")
    async def LocalGet(self, request, context):
        """What this node itself stores for the key, tombstones included. Local only."""
        node = self._node
        if not validate(request.key, None, 0, node.config.n):
            return dynamo_pb2.LocalGetResponse(status=dynamo_pb2.INVALID, node_id=node.node_id)
        versions = node.storage.get(request.key)
        status = dynamo_pb2.OK if versions else dynamo_pb2.NOT_FOUND
        return dynamo_pb2.LocalGetResponse(
            status=status,
            versions=[version_to_proto(v) for v in versions],
            node_id=node.node_id,
        )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Node flags. Every flag defaults from its env var; the node id is required from one of them."""
    parser = argparse.ArgumentParser(prog="python -m dynamo.server", description="Run one node.")
    parser.add_argument("--node-id", default=os.environ.get("NODE_ID"),
                        help="this node's id in the config (env NODE_ID)")
    parser.add_argument("--config", default=os.environ.get("CONFIG", "cluster.local.yaml"),
                        help="cluster config file (env CONFIG)")
    parser.add_argument("--port", type=int, default=os.environ.get("PORT"),
                        help="port to listen on; default: the port of this node's entry (env PORT)")
    parser.add_argument("--data-dir", default=os.environ.get("DATA_DIR", "./data"),
                        help="data folder; this node uses <data-dir>/<node-id> (env DATA_DIR)")
    args = parser.parse_args(argv)
    if not args.node_id:
        parser.error("node id missing: pass --node-id or set NODE_ID")
    return args


def _port_of(address: str) -> int:
    """The port part of ``host:port``."""
    return int(address.rsplit(":", 1)[1])


async def serve(args: argparse.Namespace) -> None:
    """Load the config, build and start the node, run until SIGTERM or SIGINT, then stop it."""
    setup_logging()
    for handler in logging.getLogger().handlers:
        handler.addFilter(RequestIdFilter(args.node_id))

    try:
        config = load_config(args.config)
    except (OSError, ValueError, yaml.YAMLError) as error:
        raise SystemExit(f"{args.node_id}: cannot load config {args.config}: {error}")
    if args.node_id not in config.nodes:
        raise SystemExit(f"{args.node_id}: not in config {args.config}")

    address = config.nodes[args.node_id]
    port = _port_of(address) if args.port is None else args.port
    data_dir = os.path.join(args.data_dir, args.node_id)

    # Installed before start(), so a SIGTERM that arrives while starting still stops cleanly.
    stop_requested = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop_requested.set)

    node = build_node(config, args.node_id, port, args.data_dir)
    await node.start()
    logger.info("%s: advertised %s, listening on 0.0.0.0:%d, data %s, ring %s",
                args.node_id, address, node.port, data_dir, node.ring.fingerprint(config.n))
    await stop_requested.wait()

    logger.info("%s: stopping", args.node_id)
    await node.stop()


def main() -> None:
    """Entry point for ``python -m dynamo.server``."""
    asyncio.run(serve(parse_args()))


if __name__ == "__main__":
    main()
