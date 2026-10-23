import json
import threading

from dynamo.storage import Storage, encode


def read_lines(path) -> list[str]:
    return path.read_text().splitlines()


def test_put_then_get(tmp_path):
    store = Storage(str(tmp_path))
    store.put("a", b"1")
    assert store.get("a") == b"1"


def test_get_missing_returns_none(tmp_path):
    store = Storage(str(tmp_path))
    assert store.get("missing") is None


def test_delete_then_get(tmp_path):
    store = Storage(str(tmp_path))
    store.put("a", b"1")
    store.delete("a")
    assert store.get("a") is None


def test_binary_value_round_trip(tmp_path):
    value = bytes(range(256))
    store = Storage(str(tmp_path))
    store.put("bin", value)
    assert store.get("bin") == value
    assert Storage(str(tmp_path)).get("bin") == value


def test_restart_without_close(tmp_path):
    store = Storage(str(tmp_path))
    store.put("a", b"1")
    store.put("b", b"2")
    restarted = Storage(str(tmp_path))
    assert restarted.get("a") == b"1"
    assert restarted.get("b") == b"2"


def test_delete_survives_restart(tmp_path):
    store = Storage(str(tmp_path))
    store.put("a", b"1")
    store.delete("a")
    assert Storage(str(tmp_path)).get("a") is None


def write_torn_line(tmp_path) -> None:
    store = Storage(str(tmp_path))
    store.put("a", b"1")
    store.put("b", b"2")
    store.close()
    with open(tmp_path / "wal.log", "a") as f:
        f.write('{"op": "put", "key": "c"')


def test_torn_last_line(tmp_path):
    write_torn_line(tmp_path)
    store = Storage(str(tmp_path))
    assert store.get("a") == b"1"
    assert store.get("b") == b"2"
    assert store.get("c") is None


def test_torn_line_then_new_writes(tmp_path):
    write_torn_line(tmp_path)
    store = Storage(str(tmp_path))
    store.put("d", b"4")
    restarted = Storage(str(tmp_path))
    assert restarted.get("a") == b"1"
    assert restarted.get("b") == b"2"
    assert restarted.get("d") == b"4"


def test_garbage_line_in_middle(tmp_path):
    wal = tmp_path / "wal.log"
    good = json.dumps({"op": "put", "key": "a", "value": encode(b"1")}) + "\n"
    after = json.dumps({"op": "put", "key": "b", "value": encode(b"2")}) + "\n"
    wal.write_text(good + "not json\n" + after)

    store = Storage(str(tmp_path))
    assert store.get("a") == b"1"
    assert store.get("b") is None
    assert wal.read_text() == good


def test_checkpoint_every_n_writes(tmp_path):
    store = Storage(str(tmp_path), checkpoint_every=10)
    for i in range(10):
        store.put(f"k{i}", b"v")
    assert (tmp_path / "snapshot.json").exists()
    assert (tmp_path / "wal.log").read_text() == ""

    for i in range(10, 13):
        store.put(f"k{i}", b"v")
    assert len(read_lines(tmp_path / "wal.log")) == 3

    restarted = Storage(str(tmp_path), checkpoint_every=10)
    assert all(restarted.get(f"k{i}") == b"v" for i in range(13))


def test_leftover_snapshot_tmp_removed(tmp_path):
    store = Storage(str(tmp_path))
    store.put("a", b"1")
    (tmp_path / "snapshot.tmp").write_text('{"a": "garbage", "z": "eg=="}')

    restarted = Storage(str(tmp_path))
    assert not (tmp_path / "snapshot.tmp").exists()
    assert restarted.get("a") == b"1"
    assert restarted.get("z") is None


def test_old_log_with_new_snapshot(tmp_path):
    store = Storage(str(tmp_path))
    store.put("a", b"1")
    store.put("b", b"2")
    store.delete("a")
    store.put("c", b"3")
    old_log = (tmp_path / "wal.log").read_bytes()
    store.checkpoint()
    # Simulate a crash after the snapshot rename but before the log was cleared.
    (tmp_path / "wal.log").write_bytes(old_log)

    restarted = Storage(str(tmp_path))
    assert restarted.get("a") is None
    assert restarted.get("b") == b"2"
    assert restarted.get("c") == b"3"


def test_concurrent_puts(tmp_path):
    store = Storage(str(tmp_path), checkpoint_every=150)

    def worker(thread_id: int) -> None:
        for i in range(100):
            store.put(f"t{thread_id}:{i}", f"{thread_id}-{i}".encode())

    threads = [threading.Thread(target=worker, args=(t,)) for t in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    keys = [(t, i) for t in range(8) for i in range(100)]
    assert all(store.get(f"t{t}:{i}") == f"{t}-{i}".encode() for t, i in keys)
    restarted = Storage(str(tmp_path), checkpoint_every=150)
    assert all(restarted.get(f"t{t}:{i}") == f"{t}-{i}".encode() for t, i in keys)
