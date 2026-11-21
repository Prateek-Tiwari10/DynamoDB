"""Small helpers shared by the real-cluster tests."""

from __future__ import annotations

import time

from dynamo import dynamo_pb2
from tools.cluster_control import CALL_TIMEOUT_S, RealCluster, wait_until

__all__ = ["put", "get", "local_values", "wait_until", "elapsed"]


async def put(cluster: RealCluster, node: str, key: str, value: bytes,
              context: bytes = b"") -> dynamo_pb2.PutResponse:
    return await cluster.client(node).Put(
        dynamo_pb2.PutRequest(key=key, value=value, context=context), timeout=CALL_TIMEOUT_S)


async def get(cluster: RealCluster, node: str, key: str, r: int = 0) -> dynamo_pb2.GetResponse:
    return await cluster.client(node).Get(dynamo_pb2.GetRequest(key=key, r=r), timeout=CALL_TIMEOUT_S)


async def local_values(cluster: RealCluster, node: str, key: str) -> list[bytes]:
    """What ``node`` itself stores for ``key`` (LocalGet: a local read, no read repair)."""
    reply = await cluster.client(node).LocalGet(dynamo_pb2.LocalGetRequest(key=key),
                                                timeout=CALL_TIMEOUT_S)
    return sorted(v.value for v in reply.versions if not v.tombstone)


def elapsed(start: float) -> float:
    return round(time.monotonic() - start, 1)
