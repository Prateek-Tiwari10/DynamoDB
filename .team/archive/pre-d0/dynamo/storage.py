import base64
import json
import os
import threading

WAL_FILE = "wal.log"
SNAPSHOT_FILE = "snapshot.json"
SNAPSHOT_TMP = "snapshot.tmp"


def encode(value: bytes) -> str:
    return base64.b64encode(value).decode("ascii")


def decode(text: str) -> bytes:
    return base64.b64decode(text.encode("ascii"))


def parse_record(line: bytes) -> dict | None:
    if not line.endswith(b"\n"):
        return None
    try:
        record = json.loads(line)
    except ValueError:
        return None
    if not isinstance(record, dict) or not isinstance(record.get("key"), str):
        return None
    if record.get("op") == "put" and isinstance(record.get("value"), str):
        return record
    if record.get("op") == "delete":
        return record
    return None


class Storage:
    def __init__(self, folder: str, checkpoint_every: int = 1000) -> None:
        self.folder = folder
        self.checkpoint_every = checkpoint_every
        self.wal_path = os.path.join(folder, WAL_FILE)
        self.snapshot_path = os.path.join(folder, SNAPSHOT_FILE)
        self.tmp_path = os.path.join(folder, SNAPSHOT_TMP)
        self.data: dict[str, bytes] = {}
        self.lock = threading.Lock()
        self.log = None

        os.makedirs(folder, exist_ok=True)
        if os.path.exists(self.tmp_path):
            os.remove(self.tmp_path)
        self._load_snapshot()
        self.writes = self._replay_wal()
        self.log = open(self.wal_path, "a", encoding="utf-8")

    def put(self, key: str, value: bytes) -> None:
        self._write({"op": "put", "key": key, "value": encode(value)})

    def get(self, key: str) -> bytes | None:
        return self.data.get(key)

    def delete(self, key: str) -> None:
        self._write({"op": "delete", "key": key})

    def checkpoint(self) -> None:
        with self.lock:
            self._checkpoint()

    def close(self) -> None:
        with self.lock:
            if self.log is None or self.log.closed:
                return
            self.log.flush()
            os.fsync(self.log.fileno())
            self.log.close()

    def _load_snapshot(self) -> None:
        if not os.path.exists(self.snapshot_path):
            return
        with open(self.snapshot_path, encoding="utf-8") as f:
            snapshot = json.load(f)
        self.data = {key: decode(value) for key, value in snapshot.items()}

    def _replay_wal(self) -> int:
        if not os.path.exists(self.wal_path):
            return 0
        count = 0
        good_offset = 0
        with open(self.wal_path, "rb") as f:
            for line in f:
                record = parse_record(line)
                if record is None:
                    break
                self._apply(record)
                count += 1
                good_offset += len(line)
        # Drop a torn or corrupt tail so later appends are not hidden behind it.
        if os.path.getsize(self.wal_path) > good_offset:
            os.truncate(self.wal_path, good_offset)
        return count

    def _apply(self, record: dict) -> None:
        if record["op"] == "put":
            self.data[record["key"]] = decode(record["value"])
        else:
            self.data.pop(record["key"], None)

    def _write(self, record: dict) -> None:
        with self.lock:
            self.log.write(json.dumps(record) + "\n")
            self.log.flush()
            os.fsync(self.log.fileno())
            self._apply(record)
            self.writes += 1
            if self.writes >= self.checkpoint_every:
                self._checkpoint()

    def _checkpoint(self) -> None:
        snapshot = {key: encode(value) for key, value in self.data.items()}
        with open(self.tmp_path, "w", encoding="utf-8") as f:
            json.dump(snapshot, f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(self.tmp_path, self.snapshot_path)
        self._fsync_folder()

        # The log is cleared only after the new snapshot is durable.
        self.log.close()
        with open(self.wal_path, "w", encoding="utf-8") as f:
            os.fsync(f.fileno())
        self.log = open(self.wal_path, "a", encoding="utf-8")
        self.writes = 0

    def _fsync_folder(self) -> None:
        fd = os.open(self.folder, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
