# dynamo-kv

A Dynamo-style distributed key-value store, built in phases. **Phase 1** is a single node: it serves `Put`, `Get` and `Delete` over gRPC, keeps every key in memory for reads, and writes each change to an fsynced write-ahead log before it acknowledges it. The node survives `kill -9` with no acknowledged write lost. Later phases add clustering, replication and quorums on top of this node.

## Prerequisites

- Python 3.11+
- Docker with the Compose plugin (only for `make up` and `make demo`)

## Quick start

```bash
python -m venv .venv && . .venv/bin/activate
make install          # dependencies + the `kv` command
make test             # generates gRPC code, runs the pytest suite
make run              # starts node1 on localhost:50051, data in ./data/node1
```

In another terminal:

```bash
kv put cart:42 book          # OK
kv get cart:42               # book
kv delete cart:42            # OK
kv get cart:42               # NOT FOUND  (exit code 1)
kv --node localhost:50051 --timeout 1 get cart:42
```

Exit codes: `0` success, `1` not found, `2` invalid request, `3` node unreachable.

Crash-recovery demo (runs the node in Docker and SIGKILLs it):

```bash
make demo             # ends with: PHASE 1 DEMO PASSED
```

## How persistence works

- **Write path:** append one JSON line to `wal.log` → `fsync` → update the in-memory dict → reply `OK`. A write is never acknowledged before it is on disk.
- **Reads** come only from the in-memory dict.
- **Snapshots:** every 1,000 writes (`--checkpoint-every`) the whole dict is written to `snapshot.tmp`, fsynced, atomically renamed to `snapshot.json`, and the folder is fsynced. Only then is `wal.log` cleared.
- **Recovery on startup:** delete any leftover `snapshot.tmp`, load `snapshot.json`, replay `wal.log` line by line, and stop at the first torn or corrupt line. The log is then truncated to the last good line, so new writes are never hidden behind a broken one. Replaying a log on top of a newer snapshot is safe because `put` and `delete` records are idempotent.

## Design

The full design for this phase is in [readme/phase1/README1.md](readme/phase1/README1.md) and the subphase READMEs next to it.
