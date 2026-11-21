"""Hint parts of dynamo/storage.py (D6b): put_hint, hints, drop_hint, replay and snapshot."""

import json

from dynamo.config import ClusterConfig
from dynamo.storage import Storage
from dynamo.vclock import VectorClock
from dynamo.version import Version


def ver(value: bytes, counters: dict[str, int]) -> Version:
    # updated_ms filled in, as it is after a trip through the log
    return Version(value=value, clock=VectorClock(counters=counters,
                                                  updated_ms={node: 0 for node in counters}))


def open_storage(folder, snapshot_every: int = 1000) -> Storage:
    config = ClusterConfig(nodes={"node1": "localhost:50051"}, n=1, r=1, w=1,
                           snapshot_every=snapshot_every)
    storage = Storage(str(folder), config)
    storage.load()
    return storage


def log_ops(folder) -> list[str]:
    return [json.loads(line)["op"] for line in (folder / "wal.log").read_bytes().splitlines()]


A1 = ver(b"a1", {"node2": 1})
A2 = ver(b"a2", {"node2": 2})     # newer than A1
B1 = ver(b"b1", {"node4": 1})


async def test_hints_are_kept_per_owner_and_invisible_to_reads(tmp_path):
    storage = open_storage(tmp_path)
    await storage.put_hint("node3", "a", A1)
    await storage.put_hint("node5", "b", B1)
    assert storage.hints() == {"node3": [("a", A1)], "node5": [("b", B1)]}
    assert storage.get("a") == [] and storage.get("b") == []      # not this node's own data
    assert storage.counts() == (0, 0)


async def test_newer_hint_replaces_older(tmp_path):
    storage = open_storage(tmp_path)
    await storage.put_hint("node3", "a", A1)
    await storage.put_hint("node3", "a", A2)
    await storage.put_hint("node3", "a", A1)                      # stale: no change, no line
    assert storage.hints() == {"node3": [("a", A2)]}
    assert log_ops(tmp_path) == ["put", "put"]


async def test_drop_hint(tmp_path):
    storage = open_storage(tmp_path)
    await storage.put_hint("node3", "a", A1)
    await storage.put_hint("node3", "b", B1)
    await storage.drop_hint("node3", "a", A1)
    assert storage.hints() == {"node3": [("b", B1)]}
    await storage.drop_hint("node3", "b", B1)
    assert storage.hints() == {}
    await storage.drop_hint("node3", "b", B1)                     # unknown: no line
    assert log_ops(tmp_path) == ["put", "put", "drop_hint", "drop_hint"]


async def test_hints_replayed_after_restart(tmp_path):
    storage = open_storage(tmp_path)
    await storage.put_hint("node3", "a", A1)
    await storage.put_hint("node3", "b", B1)
    await storage.drop_hint("node3", "a", A1)
    await storage.apply("own", A1)
    await storage.close()

    again = open_storage(tmp_path)
    assert again.hints() == {"node3": [("b", B1)]}
    assert again.get("own") == [A1]                               # own data replayed as before
    assert again.get("b") == []                                   # the hint did not become data


async def test_lost_drop_line_means_the_hint_comes_back(tmp_path):
    storage = open_storage(tmp_path)
    await storage.put_hint("node3", "a", A1)
    await storage.drop_hint("node3", "a", A1)
    await storage.close()
    lines = (tmp_path / "wal.log").read_bytes().splitlines(keepends=True)
    (tmp_path / "wal.log").write_bytes(lines[0])                  # crash: drop line never hit disk
    assert open_storage(tmp_path).hints() == {"node3": [("a", A1)]}   # re-sent later, harmless


async def test_snapshot_keeps_hints(tmp_path):
    storage = open_storage(tmp_path, snapshot_every=3)
    await storage.put_hint("node3", "a", A1)
    await storage.put_hint("node5", "b", B1)
    await storage.apply("own", A1)                                # 3rd line: snapshot, log cleared
    assert (tmp_path / "wal.log").read_bytes() == b""
    assert json.loads((tmp_path / "snapshot.json").read_text()).keys() == {"own"}
    await storage.drop_hint("node5", "b", B1)                     # after the snapshot, in the log
    await storage.close()

    again = open_storage(tmp_path, snapshot_every=3)
    assert again.hints() == {"node3": [("a", A1)]}
    assert again.get("own") == [A1]


async def test_old_data_folder_without_hints_file_loads(tmp_path):
    storage = open_storage(tmp_path, snapshot_every=1)
    await storage.apply("own", A1)                                # snapshot written
    await storage.close()
    (tmp_path / "hints.json").unlink()                            # as before D6b
    again = open_storage(tmp_path)
    assert again.get("own") == [A1] and again.hints() == {}


async def test_crash_between_the_two_renames(tmp_path):
    storage = open_storage(tmp_path, snapshot_every=1000)
    await storage.put_hint("node3", "a", A1)
    await storage.apply("own", A1)
    storage._write_snapshot_tmp()
    (tmp_path / "snapshot.json.tmp").replace(tmp_path / "snapshot.json")   # first rename only
    await storage.close()                                         # log not cleared
    again = open_storage(tmp_path)
    assert again.hints() == {"node3": [("a", A1)]}
    assert not (tmp_path / "hints.json.tmp").exists()            # leftover removed on load
