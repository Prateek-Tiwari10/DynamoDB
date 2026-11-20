"""Converting stored versions to and from their proto messages.

Its own module because the coordinator, the replica handlers and the server all need it, and the
replica handlers already import the coordinator.
"""

from dynamo import dynamo_pb2
from dynamo.vclock import VectorClock
from dynamo.version import Version


def version_to_proto(version: Version) -> dynamo_pb2.Version:
    """Stored version -> proto message."""
    return dynamo_pb2.Version(
        value=version.value,
        clock=dynamo_pb2.VectorClock(counters=version.clock.counters,
                                     updated_ms=version.clock.updated_ms),
        tombstone=version.tombstone,
    )


def version_from_proto(message: dynamo_pb2.Version) -> Version:
    """Proto message -> stored version."""
    return Version(
        value=message.value,
        clock=VectorClock(counters=dict(message.clock.counters),
                          updated_ms=dict(message.clock.updated_ms)),
        tombstone=message.tombstone,
    )
