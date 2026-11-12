"""NodeService handlers: the calls nodes make to each other."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from dynamo import dynamo_pb2, dynamo_pb2_grpc
from dynamo.coordinator import request_budget
from dynamo.reqid import rpc_handler
from dynamo.storage import Version

if TYPE_CHECKING:
    from dynamo.server import Node

logger = logging.getLogger(__name__)


def version_to_proto(version: Version) -> dynamo_pb2.Version:
    """Stored version -> proto message."""
    return dynamo_pb2.Version(
        value=version.value,
        clock=dynamo_pb2.VectorClock(counters=version.clock),
        tombstone=version.tombstone,
    )


def version_from_proto(message: dynamo_pb2.Version) -> Version:
    """Proto message -> stored version."""
    return Version(
        value=message.value,
        clock=dict(message.clock.counters),
        tombstone=message.tombstone,
    )


class NodeServiceServicer(dynamo_pb2_grpc.NodeServiceServicer):
    """Wires every NodeService RPC to the module that handles it."""

    def __init__(self, node: Node) -> None:
        self._node = node

    @rpc_handler("node")
    async def Forward(self, request, context):
        """A request forwarded by another node: coordinate it here, never forward again."""
        budget = request_budget(context, self._node.config)
        return await self._node.coordinator.coordinate(request, budget)

    @rpc_handler("node")
    async def ReplicaPut(self, request, context):
        """Store one version. A dominated version is acknowledged but not stored."""
        if request.hint_for:
            raise NotImplementedError("hinted writes")  # D9: storage.put_hint
        await self._node.storage.apply(request.key, version_from_proto(request.version))
        return dynamo_pb2.ReplicaPutResponse(ok=True, node_id=self._node.node_id)

    @rpc_handler("node")
    async def ReplicaGet(self, request, context):
        """Every version this node holds for the key, tombstones included."""
        versions = self._node.storage.get(request.key)
        return dynamo_pb2.ReplicaGetResponse(
            node_id=self._node.node_id,
            versions=[version_to_proto(v) for v in versions],
        )

    @rpc_handler("node")
    async def Gossip(self, request, context):
        """Merge the sender's membership table and reply with ours."""
        return self._node.membership.on_gossip(request)
