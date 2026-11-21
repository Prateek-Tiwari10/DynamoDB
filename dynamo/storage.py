"""Storage engine for one node: an in-memory dict backed by a write-ahead log and a snapshot.

Every change is appended to ``wal.log`` and fsynced before the dict is updated, so an
acknowledged write survives a crash. After ``snapshot_every`` log lines the whole dict is
copied to ``snapshot.json`` and the log is cleared. On startup the dict is rebuilt from the
snapshot plus the log.

Hints (writes held for another node that was down, D6b) live in a separate table: they share the
log, but they are never part of this node's own data, so ``get`` and ``counts`` do not see them.
The snapshot step also copies them to ``hints.json``.
"""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
from typing import TYPE_CHECKING

from dynamo import vclock
from dynamo.version import Version, clock_key, merge_into

if TYPE_CHECKING:
    from dynamo.config import ClusterConfig

WAL_FILE = "wal.log"
SNAPSHOT_FILE = "snapshot.json"
SNAPSHOT_TMP_FILE = "snapshot.json.tmp"
HINTS_FILE = "hints.json"
HINTS_TMP_FILE = "hints.json.tmp"

logger = logging.getLogger(__name__)


class CorruptLogError(Exception):
    """A broken line was found in the middle of the log (disk corruption, not a crash)."""


def version_to_json(version: Version) -> dict:
    """Turn a Version into a JSON-friendly dict (the value becomes base64 text)."""
    return {
        "value": base64.b64encode(version.value).decode("ascii"),
        "clock": vclock.to_json(version.clock),          # {node: [counter, updated_ms]}
        "tombstone": version.tombstone,
    }


def version_from_json(raw: dict) -> Version:
    """Build a Version from the dict made by version_to_json."""
    return Version(
        value=base64.b64decode(raw["value"]),
        clock=vclock.from_json(raw["clock"]),
        tombstone=bool(raw["tombstone"]),
    )


def encode_line(key: str, version: Version, hint_for: str = "") -> bytes:
    """Build one log line, ending with a newline."""
    record = {"op": "put", "key": key, "version": version_to_json(version), "hint_for": hint_for}
    return json.dumps(record).encode("utf-8") + b"\n"


def encode_drop_line(owner: str, key: str, version: Version) -> bytes:
    """Build the log line that deletes a delivered hint (D6b)."""
    record = {"op": "drop_hint", "owner": owner, "key": key, "clock": vclock.to_json(version.clock)}
    return json.dumps(record).encode("utf-8") + b"\n"


def decode_record(line: bytes) -> tuple | None:
    """Parse one log line into one of
        ("put", key, version, hint_for)      hint_for "" = this node's own data
        ("drop_hint", owner, key, clock)
    Returns None if the line is broken."""
    if not line.endswith(b"\n"):
        return None
    try:
        record = json.loads(line)
        if record.get("op") == "drop_hint":
            return "drop_hint", record["owner"], record["key"], vclock.from_json(record["clock"])
        return "put", record["key"], version_from_json(record["version"]), record.get("hint_for", "")
    except (ValueError, KeyError, TypeError, AttributeError):
        return None


def decode_line(line: bytes) -> tuple[str, Version] | None:
    """Parse one put line into (key, version). Returns None if the line is broken."""
    record = decode_record(line)
    if record is None or record[0] != "put":
        return None
    return record[1], record[2]


class Storage:
    """All keys of one node, in memory, with a write-ahead log and snapshots on disk."""

    def __init__(self, data_dir: str, config: ClusterConfig) -> None:
        """Remember the settings. Does no disk work; call load() before anything else."""
        self.data_dir = data_dir
        self.snapshot_every = config.snapshot_every
        self.wal_path = os.path.join(data_dir, WAL_FILE)
        self.snapshot_path = os.path.join(data_dir, SNAPSHOT_FILE)
        self.snapshot_tmp_path = os.path.join(data_dir, SNAPSHOT_TMP_FILE)
        self.hints_path = os.path.join(data_dir, HINTS_FILE)
        self.hints_tmp_path = os.path.join(data_dir, HINTS_TMP_FILE)
        self._data: dict[str, list[Version]] = {}
        self._hints: dict[str, dict[str, list[Version]]] = {}   # owner -> key -> versions
        self._lock = asyncio.Lock()
        self._log_file = None
        self._line_count = 0

    # ---------------------------------------------------------------- public

    def load(self) -> None:
        """Rebuild the dict from disk: load the snapshot, then replay the log on top of it.

        Must finish before the server accepts requests. Raises CorruptLogError if a line
        in the middle of the log is broken.
        """
        os.makedirs(self.data_dir, exist_ok=True)
        for leftover in (self.snapshot_tmp_path, self.hints_tmp_path):
            if os.path.exists(leftover):
                os.remove(leftover)   # left over from a crash during a snapshot
        self._data = self._load_snapshot()
        self._hints = self._load_hints()
        self._line_count = self._replay_log()
        self._log_file = open(self.wal_path, "ab")
        logger.info("loaded %s: %d keys, %d hints, %d log lines replayed",
                    self.data_dir, len(self._data), self._hint_count(), self._line_count)

    def get(self, key: str) -> list[Version]:
        """Return the stored versions of a key (tombstones included), or [] if unknown.

        Reads only the dict: no lock and no disk, so it never waits for a write or snapshot.
        """
        self._require_loaded()
        return list(self._data.get(key, []))

    def keys(self) -> list[str]:
        """Keys of this node's own data (tombstoned keys included, hints not included)."""
        self._require_loaded()
        return list(self._data)

    async def apply(self, key: str, version: Version) -> bool:
        """Merge a version (a put, or a tombstone for a delete) into the key's stored set.

        Returns True once the changed set is on disk and in the dict. Returns False, with no
        log line and no fsync, when nothing changes (a duplicate or a stale write, see
        ``version.merge_into``). If the log cannot be written or fsynced, the node logs
        CRITICAL and exits at once: there is no retry, because a failed write may have left
        half a line in the log.
        """
        self._require_loaded()
        line = encode_line(key, version)
        async with self._lock:
            versions, changed = merge_into(self._data.get(key, []), version)
            if not changed:
                return False
            await self._write_line(line, fsync=True)
            self._store(key, versions)
            await self._count_line()
        return True

    # ----------------------------------------------------------------- hints (D6b)

    async def put_hint(self, owner: str, key: str, version: Version) -> None:
        """Keep ``version`` of ``key`` for ``owner``, a node that was down when it was written.

        Merged with the hints already held for that owner and key (a newer hint replaces an
        older one). On disk (log + fsync) before it returns, like ``apply``. Nothing new, no line.
        """
        self._require_loaded()
        line = encode_line(key, version, hint_for=owner)
        async with self._lock:
            versions, changed = merge_into(self._hints.get(owner, {}).get(key, []), version)
            if not changed:
                return
            await self._write_line(line, fsync=True)
            self._store_hint(owner, key, versions)
            await self._count_line()

    def hints(self) -> dict[str, list[tuple[str, Version]]]:
        """Every hint held: owner -> [(key, version)]. A copy, safe to use while hints change."""
        self._require_loaded()
        return {owner: [(key, v) for key, versions in keys.items() for v in versions]
                for owner, keys in self._hints.items()}

    async def drop_hint(self, owner: str, key: str, version: Version) -> None:
        """Delete a delivered hint (the one whose clock is EQUAL to ``version``'s).

        The log line is not fsynced: if it is lost in a crash, the hint comes back on replay and
        is delivered again, which the owner ignores (same clock). Unknown hint, no line.
        """
        self._require_loaded()
        async with self._lock:
            if not self._remove_hint(owner, key, version.clock):
                return
            await self._write_line(encode_drop_line(owner, key, version), fsync=False)
            await self._count_line()

    def counts(self) -> tuple[int, int]:
        """Return (live keys, tombstones). A key counts as a tombstone if all its versions are."""
        self._require_loaded()
        tombstones = sum(1 for versions in self._data.values() if all(v.tombstone for v in versions))
        return len(self._data) - tombstones, tombstones

    def _hint_count(self) -> int:
        return sum(len(versions) for keys in self._hints.values() for versions in keys.values())

    async def close(self) -> None:
        """Clean shutdown: wait for a running write or snapshot, then close the log file."""
        async with self._lock:
            if self._log_file is not None:
                self._log_file.close()
                self._log_file = None

    # --------------------------------------------------------------- startup

    def _require_loaded(self) -> None:
        if self._log_file is None:
            raise RuntimeError("storage is not open: call load() first")

    def _load_snapshot(self) -> dict[str, list[Version]]:
        """Read snapshot.json. No snapshot yet means an empty dict."""
        if not os.path.exists(self.snapshot_path):
            return {}
        with open(self.snapshot_path, encoding="utf-8") as f:
            raw = json.load(f)
        return {key: [version_from_json(v) for v in versions] for key, versions in raw.items()}

    def _load_hints(self) -> dict[str, dict[str, list[Version]]]:
        """Read hints.json (written with the snapshot). None yet means no hints."""
        if not os.path.exists(self.hints_path):
            return {}
        with open(self.hints_path, encoding="utf-8") as f:
            raw = json.load(f)
        return {owner: {key: [version_from_json(v) for v in versions]
                        for key, versions in keys.items()}
                for owner, keys in raw.items()}

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
            parsed = decode_record(line)
            if parsed is None:
                if number < len(lines):
                    raise CorruptLogError(f"{self.wal_path}: line {number} is broken")
                logger.warning("skipped broken last line %d of %s, cutting the file back",
                               number, self.wal_path)
                os.truncate(self.wal_path, good_bytes)
                return number - 1
            self._replay_record(parsed)
            good_bytes += len(line)
        return len(lines)

    def _replay_record(self, record: tuple) -> None:
        """Apply one parsed log line: own data, a hint, or the deletion of a hint."""
        if record[0] == "drop_hint":
            _, owner, key, clock = record
            self._remove_hint(owner, key, clock)
            return
        _, key, version, hint_for = record
        if hint_for:
            versions, _ = merge_into(self._hints.get(hint_for, {}).get(key, []), version)
            self._store_hint(hint_for, key, versions)
        else:
            versions, _ = merge_into(self._data.get(key, []), version)
            self._store(key, versions)

    # ----------------------------------------------------------------- write

    async def _write_line(self, line: bytes, fsync: bool) -> None:
        """Append one line to the log (lock held). A failure exits the node (DEC-06)."""
        try:
            await asyncio.to_thread(self._append_to_log, line, fsync)
        except OSError:
            logger.critical("log write failed in %s, exiting", self.data_dir, exc_info=True)
            os._exit(1)

    async def _count_line(self) -> None:
        """Count one more log line; take a snapshot every ``snapshot_every`` lines (lock held)."""
        self._line_count += 1
        if self._line_count >= self.snapshot_every:
            await asyncio.to_thread(self._snapshot)

    def _append_to_log(self, line: bytes, fsync: bool = True) -> None:
        """Append one line to the log and, if asked, force it onto the disk. Runs in a thread."""
        self._log_file.write(line)
        self._log_file.flush()
        if fsync:
            os.fsync(self._log_file.fileno())

    def _store(self, key: str, versions: list[Version]) -> None:
        """Put the key's new set of versions (from ``merge_into``) in the dict."""
        self._data[key] = versions

    def _store_hint(self, owner: str, key: str, versions: list[Version]) -> None:
        self._hints.setdefault(owner, {})[key] = versions

    def _remove_hint(self, owner: str, key: str, clock: vclock.VectorClock) -> bool:
        """Remove the hint of ``owner`` / ``key`` with an EQUAL clock. False if there is none."""
        versions = self._hints.get(owner, {}).get(key, [])
        kept = [v for v in versions if clock_key(v.clock) != clock_key(clock)]
        if len(kept) == len(versions):
            return False
        if kept:
            self._hints[owner][key] = kept
        else:
            del self._hints[owner][key]
            if not self._hints[owner]:
                del self._hints[owner]
        return True

    # -------------------------------------------------------------- snapshot

    def _snapshot(self) -> None:
        """Copy the dict to snapshot.json, then clear the log. Lock is held; runs in a thread.

        An error on the snapshot files is logged and the node keeps running: every write is
        still in the log, so nothing is lost. The log is kept but the line counter is reset,
        so the next try comes after another snapshot_every writes instead of on every write.
        Replaying the longer log is safe because merging a version that is already in the set
        changes nothing. An error while clearing the log exits the node, like a failed log write.
        """
        try:
            self._write_snapshot_tmp()
            os.replace(self.snapshot_tmp_path, self.snapshot_path)   # atomic: old or new, never half
            os.replace(self.hints_tmp_path, self.hints_path)
            self._fsync_data_dir()
        except OSError:
            logger.error("snapshot of %s failed, log kept, retry after %d more writes",
                         self.data_dir, self.snapshot_every, exc_info=True)
            self._line_count = 0
            return
        try:
            self._clear_log()
        except OSError:
            logger.critical("clearing the log in %s failed, exiting", self.data_dir, exc_info=True)
            os._exit(1)
        logger.info("snapshot of %s written: %d keys, log cleared", self.data_dir, len(self._data))

    def _write_snapshot_tmp(self) -> None:
        """Write both temp files: the data and the hints.

        A crash between the two renames leaves the new data with the old hints; that is still
        correct, because the log is only cleared after both renames and replaying it on top
        rebuilds the hints (merging the same hint twice, or dropping a missing one, does nothing).
        """
        raw = {key: [version_to_json(v) for v in versions] for key, versions in self._data.items()}
        self._write_json_fsync(self.snapshot_tmp_path, raw)
        hints = {owner: {key: [version_to_json(v) for v in versions] for key, versions in keys.items()}
                 for owner, keys in self._hints.items()}
        self._write_json_fsync(self.hints_tmp_path, hints)

    @staticmethod
    def _write_json_fsync(path: str, raw: dict) -> None:
        with open(path, "w", encoding="utf-8") as f:
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
