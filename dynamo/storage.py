"""Storage engine for one node: an in-memory dict backed by a write-ahead log and a snapshot.

Every change is appended to ``wal.log`` and fsynced before the dict is updated, so an
acknowledged write survives a crash. After ``snapshot_every`` log lines the whole dict is
copied to ``snapshot.json`` and the log is cleared. On startup the dict is rebuilt from the
snapshot plus the log.
"""

import asyncio
import base64
import json
import logging
import os
from dataclasses import dataclass, field

WAL_FILE = "wal.log"
SNAPSHOT_FILE = "snapshot.json"
SNAPSHOT_TMP_FILE = "snapshot.json.tmp"

logger = logging.getLogger(__name__)


class CorruptLogError(Exception):
    """A broken line was found in the middle of the log (disk corruption, not a crash)."""


@dataclass
class Version:
    """One stored version of a key."""

    value: bytes = b""                                    # empty for a tombstone
    clock: dict[str, int] = field(default_factory=dict)   # vector clock, empty until D3
    tombstone: bool = False                               # True = deleted marker


def version_to_json(version: Version) -> dict:
    """Turn a Version into a JSON-friendly dict (the value becomes base64 text)."""
    return {
        "value": base64.b64encode(version.value).decode("ascii"),
        "clock": version.clock,
        "tombstone": version.tombstone,
    }


def version_from_json(raw: dict) -> Version:
    """Build a Version from the dict made by version_to_json."""
    return Version(
        value=base64.b64decode(raw["value"]),
        clock=dict(raw["clock"]),
        tombstone=bool(raw["tombstone"]),
    )


def encode_line(key: str, version: Version, hint_for: str = "") -> bytes:
    """Build one log line, ending with a newline."""
    record = {"op": "put", "key": key, "version": version_to_json(version), "hint_for": hint_for}
    return json.dumps(record).encode("utf-8") + b"\n"


def decode_line(line: bytes) -> tuple[str, Version] | None:
    """Parse one log line. Returns None if the line is broken."""
    if not line.endswith(b"\n"):
        return None
    try:
        record = json.loads(line)
        return record["key"], version_from_json(record["version"])
    except (ValueError, KeyError, TypeError):
        return None


class Storage:
    """All keys of one node, in memory, with a write-ahead log and snapshots on disk."""

    def __init__(self, data_dir: str, snapshot_every: int = 1000) -> None:
        """Remember the settings. Does no disk work; call recover() before anything else."""
        self.data_dir = data_dir
        self.snapshot_every = snapshot_every
        self.wal_path = os.path.join(data_dir, WAL_FILE)
        self.snapshot_path = os.path.join(data_dir, SNAPSHOT_FILE)
        self.snapshot_tmp_path = os.path.join(data_dir, SNAPSHOT_TMP_FILE)
        self._data: dict[str, list[Version]] = {}
        self._lock = asyncio.Lock()
        self._log_file = None
        self._line_count = 0

    # ---------------------------------------------------------------- public

    def recover(self) -> None:
        """Rebuild the dict from disk: load the snapshot, then replay the log on top of it.

        Must finish before the server accepts requests. Raises CorruptLogError if a line
        in the middle of the log is broken.
        """
        os.makedirs(self.data_dir, exist_ok=True)
        if os.path.exists(self.snapshot_tmp_path):
            os.remove(self.snapshot_tmp_path)   # left over from a crash during a snapshot
        self._data = self._load_snapshot()
        self._line_count = self._replay_log()
        self._log_file = open(self.wal_path, "ab")
        logger.info("recovered %s: %d keys, %d log lines replayed",
                    self.data_dir, len(self._data), self._line_count)

    def get(self, key: str) -> list[Version]:
        """Return the stored versions of a key (tombstones included), or [] if unknown.

        Reads only the dict: no lock and no disk, so it never waits for a write or snapshot.
        """
        self._require_recovered()
        return list(self._data.get(key, []))

    async def apply(self, key: str, version: Version) -> bool:
        """Store a version (a put, or a tombstone for a delete).

        Returns True once the write is on disk and in the dict. Returns False if the log
        could not be written; the dict is then left unchanged.
        """
        self._require_recovered()
        line = encode_line(key, version)
        async with self._lock:
            try:
                await asyncio.to_thread(self._append_to_log, line)
            except OSError:
                logger.exception("log write failed in %s, key %r not stored", self.data_dir, key)
                return False
            self._store(key, version)
            self._line_count += 1
            if self._line_count >= self.snapshot_every:
                await asyncio.to_thread(self._snapshot)
        return True

    async def close(self) -> None:
        """Clean shutdown: wait for a running write or snapshot, then close the log file."""
        async with self._lock:
            if self._log_file is not None:
                self._log_file.close()
                self._log_file = None

    # --------------------------------------------------------------- startup

    def _require_recovered(self) -> None:
        if self._log_file is None:
            raise RuntimeError("storage is not open: call recover() first")

    def _load_snapshot(self) -> dict[str, list[Version]]:
        """Read snapshot.json. No snapshot yet means an empty dict."""
        if not os.path.exists(self.snapshot_path):
            return {}
        with open(self.snapshot_path, encoding="utf-8") as f:
            raw = json.load(f)
        return {key: [version_from_json(v) for v in versions] for key, versions in raw.items()}

    def _replay_log(self) -> int:
        """Apply every log line to the dict, in order. Returns the number of lines applied.

        A broken last line is a write that was cut off by a crash: it is skipped and the
        file is cut back to the end of the last good line.
        """
        if not os.path.exists(self.wal_path):
            return 0
        with open(self.wal_path, "rb") as f:
            lines = f.readlines()
        good_bytes = 0
        for number, line in enumerate(lines, start=1):
            parsed = decode_line(line)
            if parsed is None:
                if number < len(lines):
                    raise CorruptLogError(f"{self.wal_path}: line {number} is broken")
                logger.warning("skipped broken last line %d of %s, cutting the file back",
                               number, self.wal_path)
                os.truncate(self.wal_path, good_bytes)
                return number - 1
            key, version = parsed
            self._store(key, version)
            good_bytes += len(line)
        return len(lines)

    # ----------------------------------------------------------------- write

    def _append_to_log(self, line: bytes) -> None:
        """Append one line to the log and force it onto the disk. Runs in a thread."""
        start = self._log_file.tell()
        try:
            self._log_file.write(line)
            self._log_file.flush()
            os.fsync(self._log_file.fileno())
        except OSError:
            # Do not leave a half-written line that the next write would glue onto.
            os.truncate(self.wal_path, start)
            raise

    def _store(self, key: str, version: Version) -> None:
        """Phase 1 rule: the newest write replaces what was there (D3 swaps in reconcile)."""
        self._data[key] = [version]

    # -------------------------------------------------------------- snapshot

    def _snapshot(self) -> None:
        """Copy the dict to snapshot.json, then clear the log. Lock is held; runs in a thread."""
        self._write_snapshot_tmp()
        os.replace(self.snapshot_tmp_path, self.snapshot_path)   # atomic: old or new, never half
        self._fsync_data_dir()
        self._clear_log()
        logger.info("snapshot of %s written: %d keys, log cleared", self.data_dir, len(self._data))

    def _write_snapshot_tmp(self) -> None:
        raw = {key: [version_to_json(v) for v in versions] for key, versions in self._data.items()}
        with open(self.snapshot_tmp_path, "w", encoding="utf-8") as f:
            json.dump(raw, f)
            f.flush()
            os.fsync(f.fileno())

    def _fsync_data_dir(self) -> None:
        """Make the rename itself durable."""
        fd = os.open(self.data_dir, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

    def _clear_log(self) -> None:
        """Empty the log. Only called after the new snapshot is safely on disk."""
        self._log_file.truncate(0)
        os.fsync(self._log_file.fileno())
        self._line_count = 0
