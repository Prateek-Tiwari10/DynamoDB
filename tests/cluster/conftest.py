"""Fixtures for the tests against the real 5-node cluster (D10).

Skipped unless ``RUN_CLUSTER=1`` (``make cluster-test``). ``CLUSTER_TARGET=local`` (default:
5 local processes) or ``docker``. All tests share one cluster and one event loop for the whole
session; each test heals the cluster first and uses its own fresh keys.
"""

from __future__ import annotations

import os
import uuid
from pathlib import Path

import pytest
import pytest_asyncio

from tools.cluster_control import RealCluster

HERE = Path(__file__).parent


def pytest_collection_modifyitems(config, items) -> None:
    """Every test in this folder: skipped unless RUN_CLUSTER=1, and run on the session loop
    (the cluster's gRPC channels belong to that loop)."""
    run = os.environ.get("RUN_CLUSTER") == "1"
    for item in items:
        if HERE not in Path(item.fspath).parents:
            continue
        # append=False: in front of the function-loop marker that asyncio_mode=auto added.
        item.add_marker(pytest.mark.asyncio(loop_scope="session"), append=False)
        if not run:
            item.add_marker(pytest.mark.skip(reason="real-cluster test: run with make cluster-test"))


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def cluster():
    """The real cluster: empty data, normal config, all ALIVE. Stopped after the last test."""
    real = RealCluster(os.environ.get("CLUSTER_TARGET", "local"))
    try:
        await real.ensure("normal", wipe=True)
        yield real
    finally:
        await real.shutdown()


@pytest_asyncio.fixture(loop_scope="session", autouse=True)
async def healthy(cluster):
    """Before every test: normal config, every node running and ALIVE everywhere, so a test that
    failed half way cannot break the next one."""
    await cluster.ensure("normal")


@pytest_asyncio.fixture(loop_scope="session")
async def strict(cluster, healthy):
    """Sloppy quorum off (restarts the cluster with the no-sloppy config)."""
    await cluster.ensure("nosloppy")
    return cluster


@pytest.fixture
def fresh():
    """Fresh key names for this test: ``fresh("k")`` -> ``k-<uuid>``."""
    return lambda prefix: f"{prefix}-{uuid.uuid4().hex[:8]}"
