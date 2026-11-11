"""Real crash tests: a child process writes keys and is killed with SIGKILL.

The child prints a key only after apply() returned for it, so every printed key was
acknowledged and must still be there after a restart.
"""

import asyncio
import os
import signal
import subprocess
import sys
import time

import pytest

from dynamo.storage import Storage, Version

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TOTAL_KEYS = 500


def value_for(key: str) -> bytes:
    return f"value-of-{key}".encode()


# ------------------------------------------------------------------ child

def pause_at(storage: Storage, point: str) -> None:
    """Make the child announce a point inside the snapshot and wait there to be killed."""
    def announce() -> None:
        print(f"AT {point}", flush=True)
        time.sleep(30)

    if point == "before-rename":
        real_write = storage._write_snapshot_tmp

        def write_then_pause() -> None:
            real_write()
            announce()

        storage._write_snapshot_tmp = write_then_pause
    elif point == "before-clear":
        storage._clear_log = announce


async def child_main(data_dir: str, snapshot_every: int, pause_point: str, pause_on: int) -> None:
    storage = Storage(data_dir, snapshot_every)
    storage.recover()
    for i in range(TOTAL_KEYS):
        if pause_point and i + 1 == pause_on * snapshot_every:
            pause_at(storage, pause_point)
        key = f"key{i}"
        await storage.apply(key, Version(value=value_for(key)))
        print(key, flush=True)


# ----------------------------------------------------------------- parent

def run_and_kill(data_dir, snapshot_every: int, kill_after: int = 0,
                 pause_point: str = "", pause_on: int = 0) -> list[str]:
    """Start the child, SIGKILL it, and return the keys it acknowledged.

    The kill lands after `kill_after` acknowledged keys, or when the child reports that
    it reached `pause_point` inside snapshot number `pause_on`.
    """
    command = [sys.executable, __file__, str(data_dir), str(snapshot_every),
               pause_point, str(pause_on)]
    env = dict(os.environ, PYTHONPATH=REPO_ROOT)
    child = subprocess.Popen(command, stdout=subprocess.PIPE, env=env)
    acked: list[str] = []
    for raw in child.stdout:
        line = raw.decode()
        if not line.endswith("\n"):
            break                                  # print was cut off by the kill
        if line.startswith("AT ") or len(acked) + 1 == kill_after:
            if not line.startswith("AT "):
                acked.append(line.strip())
            child.send_signal(signal.SIGKILL)
            break
        acked.append(line.strip())
    for raw in child.stdout:                       # keys printed before the kill landed
        line = raw.decode()
        if line.endswith("\n") and not line.startswith("AT "):
            acked.append(line.strip())
    assert child.wait() == -signal.SIGKILL
    return acked


def recover_and_check(data_dir, snapshot_every: int, acked: list[str]) -> Storage:
    storage = Storage(str(data_dir), snapshot_every)
    storage.recover()
    for key in acked:
        versions = storage.get(key)
        assert versions and versions[0].value == value_for(key), f"lost acknowledged key {key}"
    return storage


def test_kill_while_writing(tmp_path):
    acked = run_and_kill(tmp_path, snapshot_every=1000, kill_after=200)
    assert 200 <= len(acked) < TOTAL_KEYS
    recover_and_check(tmp_path, 1000, acked)


@pytest.mark.parametrize("pause_point", ["before-rename", "before-clear"])
def test_kill_during_snapshot(tmp_path, pause_point):
    # snapshot_every=50: killed inside the 4th snapshot (while key 200 is being written)
    acked = run_and_kill(tmp_path, snapshot_every=50, pause_point=pause_point, pause_on=4)
    assert len(acked) == 199
    assert (tmp_path / "snapshot.json.tmp").exists() == (pause_point == "before-rename")

    storage = recover_and_check(tmp_path, 50, acked)
    assert not (tmp_path / "snapshot.json.tmp").exists()
    # key 200 was fsynced to the log before the snapshot started, so it survived as well
    assert storage.get("key199")[0].value == value_for("key199")


def test_kill_at_many_points_with_snapshots(tmp_path):
    """Kill wherever it happens to land, several times, with a snapshot every 50 writes."""
    for round_number, kill_after in enumerate([37, 149, 150, 151, 263]):
        folder = tmp_path / str(round_number)
        acked = run_and_kill(folder, snapshot_every=50, kill_after=kill_after)
        assert len(acked) >= kill_after
        recover_and_check(folder, 50, acked)


def test_kill_then_half_written_line(tmp_path):
    acked = run_and_kill(tmp_path, snapshot_every=1000, kill_after=120)
    with open(tmp_path / "wal.log", "ab") as f:
        f.write(b'{"op":"put","key":"torn","version":{"val')   # half a line, by hand

    storage = recover_and_check(tmp_path, 1000, acked)
    assert storage.get("torn") == []

    # the node keeps working after the broken line was cut off
    asyncio.run(storage.apply("after", Version(value=b"ok")))
    again = recover_and_check(tmp_path, 1000, acked)
    assert again.get("after")[0].value == b"ok"


if __name__ == "__main__":
    asyncio.run(child_main(sys.argv[1], int(sys.argv[2]), sys.argv[3], int(sys.argv[4])))
