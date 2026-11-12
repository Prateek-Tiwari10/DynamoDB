"""gRPC channels to the other nodes, opened on first use and reused."""

from __future__ import annotations

from typing import TYPE_CHECKING

import grpc

from dynamo import dynamo_pb2_grpc

if TYPE_CHECKING:
    from dynamo.config import ClusterConfig

# A restarted peer is reachable again within about a second.
CHANNEL_OPTIONS = [("grpc.max_reconnect_backoff_ms", 1000)]


class Peers:
    """One cached channel and ``NodeService`` stub per peer."""

    def __init__(self, config: ClusterConfig, self_id: str) -> None:
        self._addresses = dict(config.nodes)
        self._self_id = self_id
        self._channels: dict[str, grpc.aio.Channel] = {}
        self._stubs: dict[str, dynamo_pb2_grpc.NodeServiceStub] = {}

    def stub(self, node_id: str) -> dynamo_pb2_grpc.NodeServiceStub:
        """The stub for ``node_id``; the channel is created the first time it is asked for."""
        if node_id not in self._stubs:
            channel = grpc.aio.insecure_channel(self._addresses[node_id], options=CHANNEL_OPTIONS)
            self._channels[node_id] = channel
            self._stubs[node_id] = dynamo_pb2_grpc.NodeServiceStub(channel)
        return self._stubs[node_id]

    async def close(self) -> None:
        """Close every open channel."""
        for channel in self._channels.values():
            await channel.close()
        self._channels.clear()
        self._stubs.clear()
