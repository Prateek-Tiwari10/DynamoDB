"""Cluster settings, read once at startup from cluster.yaml, plus the shared log setup.

Every node reads the same file, so every node builds the same ring and uses the same
timers. Per-node values (node id, port, data folder) are not here: they come from flags
and env vars in server.py.
"""

import dataclasses
import logging
import os
from dataclasses import dataclass

import yaml

# 20:15:03.412 INFO [node1] [req=a3f9c2d1] coordinator: put cart:42 acks 2/2
LOG_FORMAT = ("%(asctime)s.%(msecs)03d %(levelname)s [%(node_id)s] [req=%(request_id)s] "
              "%(module)s: %(message)s")
LOG_DATE_FORMAT = "%H:%M:%S"


@dataclass(frozen=True)
class ClusterConfig:
    """Settings shared by every node. Frozen, so no module can change them at runtime."""

    nodes: dict[str, str]          # node_id -> "host:port" (what peers use to reach it)
    n: int = 3
    r: int = 2
    w: int = 2
    vnodes: int = 32
    rpc_timeout_ms: int = 500
    request_timeout_ms: int = 1500
    gossip_interval_ms: int = 1000
    suspect_after_ms: int = 5000
    dead_after_ms: int = 10000
    handoff_retry_ms: int = 5000
    vclock_max_entries: int = 10
    snapshot_every: int = 1000
    sloppy_quorum: bool = True


def load_config(path: str) -> ClusterConfig:
    """Read the yaml file at ``path`` and check it. Raises ValueError on a bad file."""
    with open(path, encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    if not isinstance(raw, dict):
        raise ValueError(f"{path}: expected a mapping of settings")
    known = {f.name for f in dataclasses.fields(ClusterConfig)}
    unknown = sorted(set(raw) - known)
    if unknown:
        raise ValueError(f"{path}: unknown setting(s): {', '.join(unknown)}")
    if "nodes" not in raw:
        raise ValueError(f"{path}: 'nodes' is missing")
    nodes = raw["nodes"]
    if not isinstance(nodes, dict):
        raise ValueError(f"{path}: 'nodes' must map node ids to host:port")
    raw["nodes"] = {str(node_id): str(address) for node_id, address in nodes.items()}
    config = ClusterConfig(**raw)
    _validate(config, path)
    return config


def _validate(config: ClusterConfig, path: str) -> None:
    if not config.nodes:
        raise ValueError(f"{path}: at least one node is needed")
    for node_id, address in config.nodes.items():
        host, _, port = address.rpartition(":")
        if not host or not port.isdigit():
            raise ValueError(f"{path}: address of {node_id} must be host:port, got {address!r}")
    if not 1 <= config.n <= len(config.nodes):
        raise ValueError(f"{path}: n={config.n} must be between 1 and the number of nodes "
                         f"({len(config.nodes)})")
    if not 1 <= config.r <= config.n:
        raise ValueError(f"{path}: r={config.r} must be between 1 and n={config.n}")
    if not 1 <= config.w <= config.n:
        raise ValueError(f"{path}: w={config.w} must be between 1 and n={config.n}")
    if not config.suspect_after_ms < config.dead_after_ms:
        raise ValueError(f"{path}: suspect_after_ms ({config.suspect_after_ms}) must be less "
                         f"than dead_after_ms ({config.dead_after_ms})")


class _LogDefaults(logging.Filter):
    """Gives ``node_id`` and ``request_id`` a "-" value when nothing else set them.

    Meet's ``RequestIdFilter`` sets the real values; this only keeps LOG_FORMAT from failing
    on a record that did not pass through it.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        if not hasattr(record, "node_id"):
            record.node_id = "-"
        if not hasattr(record, "request_id"):
            record.request_id = "-"
        return True


def setup_logging() -> None:
    """Log to stderr in LOG_FORMAT. Level from the LOG_LEVEL env var, default INFO.

    Safe to call more than once: it replaces the root handlers each time.
    """
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(LOG_FORMAT, LOG_DATE_FORMAT))
    handler.addFilter(_LogDefaults())
    level = os.environ.get("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(level=level, handlers=[handler], force=True)
