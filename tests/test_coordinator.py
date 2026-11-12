"""Unit tests for the parts of dynamo/coordinator.py built in D0."""

import asyncio
from types import SimpleNamespace

import pytest

from dynamo.coordinator import Coordinator, request_budget

CONFIG = SimpleNamespace(request_timeout_ms=1500)


class FakeContext:
    def __init__(self, remaining: float | None) -> None:
        self._remaining = remaining

    def time_remaining(self) -> float | None:
        return self._remaining


def make_coordinator() -> Coordinator:
    return Coordinator("node1", CONFIG, ring=None, storage=None, peers=None, membership=None)


def test_budget_without_deadline():
    assert request_budget(FakeContext(None), CONFIG) == 1.5


def test_budget_from_shorter_deadline():
    assert request_budget(FakeContext(0.4), CONFIG) == 0.4


def test_budget_capped_by_request_timeout():
    assert request_budget(FakeContext(30.0), CONFIG) == 1.5


async def test_d0_stubs_raise_not_implemented():
    coordinator = make_coordinator()
    with pytest.raises(NotImplementedError):
        await coordinator.put(None, 1.0)
    with pytest.raises(NotImplementedError):
        await coordinator.coordinate(None, 1.0)


async def test_drain_waits_for_quick_tasks_and_cancels_slow_ones():
    coordinator = make_coordinator()
    finished = []

    async def quick():
        await asyncio.sleep(0.01)
        finished.append("quick")

    async def slow():
        await asyncio.sleep(10)
        finished.append("slow")

    coordinator._spawn(quick())
    coordinator._spawn(slow())
    await coordinator.drain(0.2)
    assert finished == ["quick"]
    assert not coordinator._tasks


async def test_drain_with_no_tasks_returns_at_once():
    await make_coordinator().drain(5)


def test_key_lock_is_one_lock_per_key():
    coordinator = make_coordinator()
    assert coordinator._key_lock("a") is coordinator._key_lock("a")
    assert coordinator._key_lock("a") is not coordinator._key_lock("b")
