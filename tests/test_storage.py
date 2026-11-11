"""Unit tests for dynamo/storage.py. Every test uses its own temp folder as data_dir."""

import asyncio
import json
import logging
import os
import threading
import time

import pytest

from dynamo.storage import CorruptLogError, Storage, Version, encode_line


def put(value: bytes) -> Version:
    return Version(value=value)


def tombstone() -> Version:
    return Version(tombstone=True)


def open_storage(folder, snapshot_every: int = 1000) -> Storage:
    storage = Storage(str(folder), snapshot_every)
    storage.load()
    return storage


def value_of(storage: Storage, key: str) -> bytes | None:
    """The live value of a key, or None if unknown or deleted."""
    versions = storage.get(key)
    if not versions or versions[0].tombstone:
        return None
    return versions[0].value


def log_lines(folder) -> list[bytes]:
    with open(folder / "wal.log", "rb") as f:
        return f.readlines()


def write_keys(storage: Storage, count: int, prefix: str = "k") -> None:
    async def run() -> None:
        for i in range(count):
            assert await storage.apply(f"{prefix}{i}", put(f"v{i}".encode()))
    asyncio.run(run())


# ------------------------------------------------------------------ basics

def test_put_get_and_restart(tmp_path):
    storage = open_storage(tmp_path)
    asyncio.run(storage.apply("a", put(b"\x00\xffbinary")))
    assert value_of(storage, "a") == b"\x00\xffbinary"
    assert storage.get("missing") == []

    # no close(): same as a crash right after the write was acknowledged
    assert value_of(open_storage(tmp_path), "a") == b"\x00\xffbinary"


def test_delete_stores_tombstone(tmp_path):
    storage = open_storage(tmp_path)
    asyncio.run(storage.apply("a", put(b"1")))
    asyncio.run(storage.apply("a", tombstone()))
    assert storage.get("a") == [Version(value=b"", clock={}, tombstone=True)]
    assert open_storage(tmp_path).get("a")[0].tombstone is True


def test_delete_unknown_key(tmp_path):
    storage = open_storage(tmp_path)
    assert asyncio.run(storage.apply("ghost", tombstone())) is True
    assert storage.get("ghost")[0].tombstone is True
    assert open_storage(tmp_path).get("ghost")[0].tombstone is True


def test_log_line_format(tmp_path):
    storage = open_storage(tmp_path)
    asyncio.run(storage.apply("cart:42", put(b"hi")))
    assert json.loads(log_lines(tmp_path)[0]) == {
        "op": "put",
        "key": "cart:42",
        "version": {"value": "aGk=", "clock": {}, "tombstone": False},
        "hint_for": "",
    }


def test_close_then_restart(tmp_path):
    storage = open_storage(tmp_path)
    asyncio.run(storage.apply("a", put(b"1")))
    asyncio.run(storage.close())
    assert value_of(open_storage(tmp_path), "a") == b"1"


class NodeExited(Exception):
    """Raised by the fake os._exit, so the test process itself survives."""


def test_fsync_failure_exits(tmp_path, monkeypatch, caplog):
    storage = open_storage(tmp_path)
    asyncio.run(storage.apply("a", put(b"old")))
    fsync_calls = []

    def broken_fsync(fd):
        fsync_calls.append(fd)
        raise OSError("disk full")

    def fake_exit(code):
        raise NodeExited(code)

    monkeypatch.setattr(os, "fsync", broken_fsync)
    monkeypatch.setattr(os, "_exit", fake_exit)
    with caplog.at_level(logging.CRITICAL), pytest.raises(NodeExited) as exited:
        asyncio.run(storage.apply("a", put(b"new")))
    monkeypatch.undo()

    assert exited.value.args == (1,)                  # exit code 1
    assert len(fsync_calls) == 1                      # no retry
    assert value_of(storage, "a") == b"old"           # dict untouched
    assert any(record.levelno == logging.CRITICAL for record in caplog.records)


def test_counts(tmp_path):
    storage = open_storage(tmp_path)
    assert storage.counts() == (0, 0)

    async def run() -> None:
        await storage.apply("a", put(b"1"))
        await storage.apply("b", put(b"2"))
        await storage.apply("c", put(b"3"))
        await storage.apply("c", tombstone())
        await storage.apply("ghost", tombstone())

    asyncio.run(run())
    assert storage.counts() == (2, 2)                 # a, b live; c, ghost deleted
    assert open_storage(tmp_path).counts() == (2, 2)


# ------------------------------------------------------- broken log lines

def test_torn_last_line(tmp_path):
    write_keys(open_storage(tmp_path), 3)
    with open(tmp_path / "wal.log", "ab") as f:
        f.write(b'{"op":"put","key":"k3","vers')      # crash in the middle of a line

    storage = open_storage(tmp_path)
    assert value_of(storage, "k2") == b"v2"
    assert storage.get("k3") == []
    assert len(log_lines(tmp_path)) == 3              # file was cut back


def test_torn_last_line_with_newline(tmp_path):
    write_keys(open_storage(tmp_path), 2)
    with open(tmp_path / "wal.log", "ab") as f:
        f.write(b'{"op":"put","key":\n')              # has a newline but is not valid JSON

    storage = open_storage(tmp_path)
    assert value_of(storage, "k1") == b"v1"
    assert len(log_lines(tmp_path)) == 2


def test_append_after_torn_line(tmp_path):
    write_keys(open_storage(tmp_path), 2)
    with open(tmp_path / "wal.log", "ab") as f:
        f.write(b'{"op":"put","key":"half')

    storage = open_storage(tmp_path)
    asyncio.run(storage.apply("after", put(b"ok")))   # must not glue onto the half-line

    again = open_storage(tmp_path)
    assert value_of(again, "after") == b"ok"
    assert value_of(again, "k1") == b"v1"
    assert len(log_lines(tmp_path)) == 3


def test_corrupt_middle_line(tmp_path):
    write_keys(open_storage(tmp_path), 3)
    lines = log_lines(tmp_path)
    lines[1] = b"this is not json\n"
    (tmp_path / "wal.log").write_bytes(b"".join(lines))

    with pytest.raises(CorruptLogError):
        open_storage(tmp_path)


# ------------------------------------------------------------ crash points

def test_kill_after_fsync(tmp_path, monkeypatch):
    """Crash after the log fsync but before the dict update: replay restores the write."""
    storage = open_storage(tmp_path)

    def crash(key, version):
        raise RuntimeError("killed before the dict update")

    monkeypatch.setattr(storage, "_store", crash)
    with pytest.raises(RuntimeError):
        asyncio.run(storage.apply("a", put(b"1")))
    assert storage.get("a") == []                     # never reached the dict

    assert value_of(open_storage(tmp_path), "a") == b"1"


def test_crash_during_snapshot_tmp(tmp_path, monkeypatch):
    """Crash while the temp snapshot exists: old snapshot and full log are still used."""
    storage = open_storage(tmp_path, snapshot_every=3)
    write_keys(storage, 3)                            # first snapshot, log cleared
    write_keys(storage, 2, prefix="late")

    def crash(src, dst):
        raise RuntimeError("killed before the rename")

    monkeypatch.setattr(os, "replace", crash)
    with pytest.raises(RuntimeError):
        asyncio.run(storage.apply("last", put(b"x")))
    monkeypatch.undo()
    assert (tmp_path / "snapshot.json.tmp").exists()

    again = open_storage(tmp_path, snapshot_every=3)
    assert not (tmp_path / "snapshot.json.tmp").exists()
    assert value_of(again, "k2") == b"v2"             # from the old snapshot
    assert value_of(again, "late1") == b"v1"          # from the log
    assert value_of(again, "last") == b"x"


def test_crash_between_rename_and_clear(tmp_path, monkeypatch):
    """New snapshot + old log both exist: replaying the log over it gives the same state."""
    storage = open_storage(tmp_path, snapshot_every=3)

    def crash():
        raise RuntimeError("killed before the log was cleared")

    monkeypatch.setattr(storage, "_clear_log", crash)

    async def run() -> None:
        await storage.apply("a", put(b"1"))
        await storage.apply("a", tombstone())
        await storage.apply("a", put(b"3"))           # third line triggers the snapshot

    with pytest.raises(RuntimeError):
        asyncio.run(run())
    assert (tmp_path / "snapshot.json").exists()
    assert len(log_lines(tmp_path)) == 3              # old log still there

    again = open_storage(tmp_path, snapshot_every=3)
    assert value_of(again, "a") == b"3"

    # the counter restarted at 3, so the very next write snapshots and clears the log
    asyncio.run(again.apply("b", put(b"2")))
    assert log_lines(tmp_path) == []
    final = open_storage(tmp_path, snapshot_every=3)
    assert value_of(final, "a") == b"3"
    assert value_of(final, "b") == b"2"


# --------------------------------------------------------------- snapshot

def test_snapshot_clears_log(tmp_path):
    storage = open_storage(tmp_path, snapshot_every=5)
    write_keys(storage, 5)
    assert log_lines(tmp_path) == []
    snapshot = json.loads((tmp_path / "snapshot.json").read_text())
    assert sorted(snapshot) == ["k0", "k1", "k2", "k3", "k4"]

    write_keys(storage, 2, prefix="more")
    assert len(log_lines(tmp_path)) == 2

    again = open_storage(tmp_path, snapshot_every=5)
    assert value_of(again, "k4") == b"v4"
    assert value_of(again, "more1") == b"v1"


def slow_snapshot(storage: Storage, monkeypatch) -> tuple[threading.Event, threading.Event]:
    """Make the snapshot wait inside its thread until the test lets it go."""
    started, release = threading.Event(), threading.Event()
    real = storage._write_snapshot_tmp

    def slow() -> None:
        started.set()
        release.wait(timeout=5)
        real()

    monkeypatch.setattr(storage, "_write_snapshot_tmp", slow)
    return started, release


def test_put_during_snapshot(tmp_path, monkeypatch):
    storage = open_storage(tmp_path, snapshot_every=3)
    started, release = slow_snapshot(storage, monkeypatch)

    async def run() -> None:
        await storage.apply("k0", put(b"v0"))
        await storage.apply("k1", put(b"v1"))
        trigger = asyncio.create_task(storage.apply("k2", put(b"v2")))
        while not started.is_set():
            await asyncio.sleep(0.01)

        late = asyncio.create_task(storage.apply("late", put(b"x")))
        await asyncio.sleep(0.1)
        assert not late.done()                        # waiting in the lock's queue
        assert storage.get("late") == []

        release.set()
        assert await trigger is True
        assert await late is True

    asyncio.run(run())
    assert len(log_lines(tmp_path)) == 1              # only the late put, after the snapshot
    again = open_storage(tmp_path, snapshot_every=3)
    assert value_of(again, "k2") == b"v2"
    assert value_of(again, "late") == b"x"


def test_get_during_snapshot(tmp_path, monkeypatch):
    storage = open_storage(tmp_path, snapshot_every=2)
    started, release = slow_snapshot(storage, monkeypatch)

    async def run() -> None:
        await storage.apply("a", put(b"1"))
        trigger = asyncio.create_task(storage.apply("b", put(b"2")))
        while not started.is_set():
            await asyncio.sleep(0.01)

        begin = time.perf_counter()
        assert value_of(storage, "a") == b"1"         # does not wait for the snapshot
        assert value_of(storage, "b") == b"2"
        assert time.perf_counter() - begin < 0.05
        assert not trigger.done()

        release.set()
        await trigger

    asyncio.run(run())


def test_concurrent_puts_same_key(tmp_path):
    storage = open_storage(tmp_path)

    async def run() -> None:
        await asyncio.gather(*(storage.apply("same", put(str(i).encode())) for i in range(20)))

    asyncio.run(run())
    lines = log_lines(tmp_path)
    assert len(lines) == 20
    last_logged = json.loads(lines[-1])["version"]["value"]
    in_dict = json.loads(encode_line("same", storage.get("same")[0]))["version"]["value"]
    assert in_dict == last_logged                     # log order = dict order
    assert open_storage(tmp_path).get("same") == storage.get("same")


# ---------------------------------------------------------------- restart

def test_no_requests_before_load(tmp_path):
    storage = Storage(str(tmp_path))                  # load() not called yet
    with pytest.raises(RuntimeError):
        storage.get("a")
    with pytest.raises(RuntimeError):
        asyncio.run(storage.apply("a", put(b"1")))
    with pytest.raises(RuntimeError):
        storage.counts()
    assert not (tmp_path / "wal.log").exists()        # the constructor touched nothing


def test_restart_without_snapshot(tmp_path):
    write_keys(open_storage(tmp_path), 4)
    assert not (tmp_path / "snapshot.json").exists()
    again = open_storage(tmp_path)
    assert [value_of(again, f"k{i}") for i in range(4)] == [b"v0", b"v1", b"v2", b"v3"]


def test_counter_after_restart(tmp_path):
    """Same as 600 lines with a limit of 1000, scaled down: 6 lines with a limit of 10."""
    write_keys(open_storage(tmp_path, snapshot_every=10), 6)

    storage = open_storage(tmp_path, snapshot_every=10)
    write_keys(storage, 3, prefix="b")                # 9 lines in total: no snapshot yet
    assert not (tmp_path / "snapshot.json").exists()
    write_keys(storage, 1, prefix="c")                # 10th line: snapshot
    assert (tmp_path / "snapshot.json").exists()
    assert log_lines(tmp_path) == []


def test_replay_twice(tmp_path):
    storage = open_storage(tmp_path)

    async def run() -> None:
        await storage.apply("a", put(b"1"))
        await storage.apply("a", tombstone())
        await storage.apply("a", put(b"3"))
        await storage.apply("b", put(b"2"))
        await storage.apply("b", tombstone())

    asyncio.run(run())
    log = (tmp_path / "wal.log").read_bytes()
    (tmp_path / "wal.log").write_bytes(log + log)     # every line is now in the log twice

    again = open_storage(tmp_path)
    assert value_of(again, "a") == b"3"
    assert again.get("b")[0].tombstone is True


# ----------------------------------------------------------------- timing

@pytest.mark.skipif(not os.environ.get("STORAGE_TIMING"), reason="set STORAGE_TIMING=1 to run")
def test_timing_10k_puts(tmp_path):
    """10,000 puts of 1 KB. Set STORAGE_TIMING_DIR to a folder on a real disk (not tmpfs)."""
    folder = os.environ.get("STORAGE_TIMING_DIR") or str(tmp_path)
    storage = open_storage(folder)
    value = b"x" * 1024

    async def run() -> None:
        for i in range(10_000):
            await storage.apply(f"key{i}", put(value))

    begin = time.perf_counter()
    asyncio.run(run())
    seconds = time.perf_counter() - begin
    print(f"\n10,000 puts in {seconds:.2f} s = {10_000 / seconds:.0f} writes/s  (data_dir {folder})")
    assert value_of(open_storage(folder), "key9999") == value
