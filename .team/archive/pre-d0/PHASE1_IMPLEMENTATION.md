# Phase 1 Implementation Task — Single-Node Key-Value Store

You are implementing Phase 1 of a Dynamo-style distributed key-value store in Python. This file is the complete spec. If the design READMEs in `phase1/` are present, read them first for background; **where they differ from this file, this file wins.**

Work through the steps in order. Finish each step's verification before starting the next one.

---

## 1. Goal

Build one node that:

- serves `Put`, `Get` and `Delete` over gRPC
- keeps all data in an in-memory dict for reads
- writes every change to an append-only write-ahead log (WAL) with `fsync` **before** acknowledging it
- takes a snapshot every 1,000 writes and then clears the log
- recovers fully after `kill -9`, including when the last log line is half-written

The final proof is a demo script. It writes keys, hard-kills the Docker container, removes it, starts a new one, and checks that every acknowledged write is still there.

## 2. Scope rules

- **Do:** everything in this file.
- **Do not:** add clustering, hashing, replication, quorum logic, vector clocks, gossip, asyncio or `grpc.aio`. Those belong to later phases.
- **Keep** the later-phase fields in the `.proto` file (`context`, `w`, `r`, `written_to`, `read_from`). Phase 1 ignores them, and they keep the API stable.
- **Dependencies:** only `grpcio`, `grpcio-tools`, `pytest`. No other third-party packages.
- **Python:** 3.11+.
- **Code style:** small functions, simple and clear variable names, minimal comments (only where the reason is not obvious), type hints on public methods.

## 3. Final folder layout

```
dynamo-kv/
  proto/
    dynamo/
      dynamo.proto
  dynamo/
    __init__.py
    dynamo_pb2.py            # generated, gitignored
    dynamo_pb2_grpc.py       # generated, gitignored
    storage.py
    server.py
  client/
    __init__.py
    cli.py
  scripts/
    gen_proto.sh
    bulk_write.py
    demo_phase1.sh
  tests/
    conftest.py
    test_storage.py
    test_server.py
    test_cli.py
  phase1/                    # design READMEs (already exist, do not delete)
  data/                      # created at runtime, gitignored
  Dockerfile
  docker-compose.yml
  .dockerignore
  .gitignore
  requirements.txt
  pyproject.toml
  Makefile
  README.md
```

The `.proto` file lives at `proto/dynamo/dynamo.proto`, not `proto/dynamo.proto`. With `-I proto`, protoc then generates `from dynamo import dynamo_pb2` in the gRPC file, so the import never needs fixing by hand. Update the folder layout in `phase1/README.md` to match.

---

## Step 1 — Project skeleton

Create:

**`requirements.txt`**
```
grpcio>=1.60
grpcio-tools>=1.60
pytest>=8.0
```

**`pyproject.toml`**: a setuptools project named `dynamo-kv` with packages `dynamo` and `client`, `requires-python = ">=3.11"`, and this script entry point:
```toml
[project.scripts]
kv = "client.cli:main"
```

**`.gitignore`**: `data/`, `__pycache__/`, `.venv/`, `*.egg-info/`, `.pytest_cache/`, `dynamo/dynamo_pb2.py`, `dynamo/dynamo_pb2_grpc.py`

**`.dockerignore`**: `data/`, `.venv/`, `__pycache__/`, `.pytest_cache/`, `.git/`

**`Makefile`** targets:

| Target | Command |
|--------|---------|
| `install` | `pip install -r requirements.txt && pip install -e .` |
| `proto` | `bash scripts/gen_proto.sh` |
| `test` | `make proto && pytest -v tests/` |
| `run` | `python -m dynamo.server --node-id node1 --port 50051 --data-dir ./data` |
| `up` | `docker compose up -d --build` |
| `down` | `docker compose down` |
| `demo` | `bash scripts/demo_phase1.sh` |

**Verify:** `make install` succeeds.

---

## Step 2 — gRPC contract (subphase 1.1)

**`proto/dynamo/dynamo.proto`**: use exactly this content.

```protobuf
syntax = "proto3";

package dynamo;

service KVStore {
  rpc Put(PutRequest) returns (PutResponse);
  rpc Get(GetRequest) returns (GetResponse);
  rpc Delete(DeleteRequest) returns (DeleteResponse);
}

enum Status {
  OK = 0;
  NOT_FOUND = 1;
  QUORUM_FAILED = 2;
  INVALID = 3;
}

message PutRequest {
  string key = 1;
  bytes value = 2;
  bytes context = 3;
  uint32 w = 4;
}

message PutResponse {
  Status status = 1;
  bytes context = 2;
  repeated string written_to = 3;
}

message GetRequest {
  string key = 1;
  uint32 r = 2;
}

message GetResponse {
  Status status = 1;
  repeated bytes values = 2;
  bytes context = 3;
  repeated string read_from = 4;
}

message DeleteRequest {
  string key = 1;
  bytes context = 2;
  uint32 w = 3;
}

message DeleteResponse {
  Status status = 1;
}
```

**`scripts/gen_proto.sh`**: run from the repo root, `set -euo pipefail`.
```bash
python -m grpc_tools.protoc -I proto --python_out=. --grpc_python_out=. proto/dynamo/dynamo.proto
```

**Verify:**
```
make proto
python -c "from dynamo import dynamo_pb2, dynamo_pb2_grpc; print('ok')"
```

---

## Step 3 — Storage engine (subphase 1.2)

**`dynamo/storage.py`**: a plain class with no gRPC imports.

### Files inside `folder`

| File | Content |
|------|---------|
| `wal.log` | One JSON object per line, `\n`-terminated |
| `snapshot.json` | `{"<key>": "<base64 value>", ...}` |
| `snapshot.tmp` | Exists only while a snapshot is being written |

### WAL record format

Values are bytes, so they are stored as base64 ASCII:
```
{"op": "put", "key": "cart:42", "value": "Ym9vaw=="}
{"op": "delete", "key": "cart:42"}
```

### Public interface

```python
class Storage:
    def __init__(self, folder: str, checkpoint_every: int = 1000) -> None: ...
    def put(self, key: str, value: bytes) -> None: ...
    def get(self, key: str) -> bytes | None: ...
    def delete(self, key: str) -> None: ...
    def checkpoint(self) -> None: ...
    def close(self) -> None: ...
```

### Recovery (in `__init__`)

1. `os.makedirs(folder, exist_ok=True)`.
2. If `snapshot.tmp` exists, delete it. It is a leftover from a crash during a checkpoint.
3. If `snapshot.json` exists, load it and base64-decode the values into `self.data`.
4. If `wal.log` exists, read it in binary mode line by line and track the byte offset of the end of the last good line. A line is good only if:
   - it ends with `\n`, and
   - it parses as JSON with a valid `op` (`put` with `key` and `value`, or `delete` with `key`).

   Apply each good line. At the first bad line, stop reading.
5. **Truncate `wal.log` to the end of the last good line** (`os.truncate(path, good_offset)`) if anything follows it. This step is required. Without it, new records appended after a torn line would be skipped on the next restart, and acknowledged writes would be lost.
6. Set the write counter to the number of records replayed.
7. Open `wal.log` in append mode for new writes.

### Write path (`put` and `delete`)

All writes run under one `threading.Lock`:

1. Build the record dict.
2. `self.log.write(json.dumps(record) + "\n")`, then `flush()`, then `os.fsync(self.log.fileno())`.
3. Apply the record to `self.data`. A delete of a missing key is a no-op on the dict, but it is still logged.
4. Add 1 to the counter. If the counter is `>= checkpoint_every`, call the internal checkpoint while still holding the lock.

`get` reads only from `self.data`, with no lock and no disk access.

### Checkpoint

Runs under the same lock (the public `checkpoint()` takes the lock; the internal version assumes it is held):

1. Write `{key: base64(value)}` for all keys to `snapshot.tmp`, then `flush()` and `fsync()`.
2. `os.replace("snapshot.tmp", "snapshot.json")`.
3. fsync the folder so the rename is durable: `fd = os.open(folder, os.O_RDONLY); os.fsync(fd); os.close(fd)`.
4. Only now close the log, reopen it with mode `"w"` (this truncates it), fsync it, then reopen in append mode. Reset the counter to 0.

**Rule:** the log is never cleared before the new snapshot is safely on disk. If the process crashes between steps 3 and 4, the old log replays on top of the new snapshot. That is safe because `put` and `delete` records are idempotent.

### `close()`

Flush, fsync and close the log file. It must be safe to call twice.

### Tests: `tests/test_storage.py` (use pytest's `tmp_path`)

- [ ] `put` then `get` returns the value
- [ ] `get` on a missing key returns `None`
- [ ] `delete` then `get` returns `None`
- [ ] Binary values round-trip (for example `bytes(range(256))`)
- [ ] Restart without calling `close()` (create a new `Storage` on the same folder): all data is present
- [ ] Deleted key stays deleted after a restart
- [ ] Torn last line: write `a`, `b`; append `{"op": "put", "key": "c"` with no newline; restart; `a` and `b` are present, `c` is not
- [ ] **Torn line then new writes:** after the previous case, `put("d")`, restart again; `a`, `b`, `d` are all present
- [ ] Garbage line in the middle stops replay there, and the log is truncated to the last good line
- [ ] With `checkpoint_every=10`, after 10 writes `snapshot.json` exists and `wal.log` is empty; after 3 more writes the log has 3 lines; restart gives all 13 keys
- [ ] A leftover `snapshot.tmp` is removed on startup and does not affect data
- [ ] Old log + new snapshot (simulate a crash after the rename, before the truncate): restart gives the correct final state
- [ ] 8 threads × 100 puts each: all 800 keys are present, before and after a restart

**Verify:** `pytest -v tests/test_storage.py` passes.

---

## Step 4 — gRPC server (subphase 1.3)

**`dynamo/server.py`**

### Settings

Use `argparse`, with environment variables as defaults:

| Flag | Env var | Default |
|------|---------|---------|
| `--node-id` | `NODE_ID` | `node1` |
| `--port` | `PORT` | `50051` |
| `--data-dir` | `DATA_DIR` | `./data` |
| `--checkpoint-every` | `CHECKPOINT_EVERY` | `1000` |

Storage folder = `<data-dir>/<node-id>`.

### Servicer

`class KVStoreServicer(dynamo_pb2_grpc.KVStoreServicer)` takes `storage` and `node_id`.

| Call | Invalid when | Action | Response |
|------|--------------|--------|----------|
| `Put` | key empty, key > 256 bytes UTF-8, or value > 1 MiB | `storage.put` | `OK` |
| `Get` | key empty or key > 256 bytes UTF-8 | `storage.get` | `OK` + `values=[value]`, or `NOT_FOUND` with an empty `values` |
| `Delete` | key empty or key > 256 bytes UTF-8 | `storage.delete` | `OK` (also when the key did not exist) |
| any | validation fails | nothing | `INVALID` |

Expected outcomes are returned as a `Status`. Never raise a gRPC error for them. An unexpected exception may propagate; gRPC turns it into `UNKNOWN`.

Log one line per request with the `logging` module at INFO level:
```
node1 PUT cart:42 OK 3.1ms
```

### Server factory (tests use it)

```python
def create_server(storage: Storage, node_id: str, port: int) -> tuple[grpc.Server, int]:
```

- `grpc.server(futures.ThreadPoolExecutor(max_workers=16))`
- register the servicer
- `bound = server.add_insecure_port(f"[::]:{port}")`. Port 0 gives a random free port.
- return `(server, bound)` without starting it

### `main()`

1. Parse settings and configure logging.
2. Create `Storage`. **Recovery must finish before the port opens.**
3. `create_server(...)`, `server.start()`, then log `node1 listening on 50051 (recovered N keys)`.
4. On SIGTERM or SIGINT: `server.stop(grace=2).wait()`, then `storage.close()`, then exit 0.
5. Otherwise `server.wait_for_termination()`.

It runs with `python -m dynamo.server`, so add `if __name__ == "__main__": main()`.

### Tests: `tests/test_server.py`

Add a fixture in `tests/conftest.py` that creates a `Storage` on `tmp_path`, calls `create_server(..., port=0)`, starts it, yields a `KVStoreStub` plus the address, then stops the server and closes the storage.

- [ ] `Put` then `Get` returns the value
- [ ] `Get` on a missing key returns `NOT_FOUND` with an empty `values`
- [ ] `Delete` then `Get` returns `NOT_FOUND`
- [ ] `Delete` on a missing key returns `OK`
- [ ] Empty key gives `INVALID` for all three calls
- [ ] 257-byte key gives `INVALID`; a value of 1 MiB + 1 byte gives `INVALID`
- [ ] Restart: stop the server, open a new `Storage` and server on the same folder, earlier keys are present
- [ ] 100 concurrent `Put` calls from a thread pool all return `OK`, and all keys are readable

**Verify:** `pytest -v tests/test_server.py` passes, and `make run` starts and logs that it is listening.

---

## Step 5 — CLI client (subphase 1.4)

**`client/cli.py`** exposes a `main()` function.

```
kv [--node HOST:PORT] [--timeout SECONDS] put KEY VALUE [--w N]
kv [--node HOST:PORT] [--timeout SECONDS] get KEY [--r N]
kv [--node HOST:PORT] [--timeout SECONDS] delete KEY [--w N]
```

- `--node` defaults to the `KV_NODE` environment variable, or `localhost:50051` if it is not set.
- `--timeout` defaults to `2.0` and is passed as the gRPC deadline.
- `--w` and `--r` are accepted and sent in the request; a single node ignores them.
- The value is sent as UTF-8 bytes. On `get`, decode as UTF-8; if that fails, print `base64:<encoded>`.

### Output and exit codes

| Situation | stdout / stderr | Exit |
|-----------|-----------------|------|
| `put` / `delete` OK | `OK` (stdout) | 0 |
| `get` found | the value (stdout) | 0 |
| `get` missing | `NOT FOUND` (stdout) | 1 |
| Server returned `INVALID` | `INVALID REQUEST` (stderr) | 2 |
| `grpc.RpcError` (unreachable, deadline) | `ERROR: cannot reach HOST:PORT` (stderr) | 3 |

`main()` must end with `sys.exit(code)`, so scripts can rely on the exit codes.

### Tests: `tests/test_cli.py`

Run the CLI with `subprocess.run([sys.executable, "-m", "client.cli", ...])` against the server fixture.

- [ ] `put a 1` prints `OK`, exit 0
- [ ] `get a` prints `1`, exit 0
- [ ] `get missing` prints `NOT FOUND`, exit 1
- [ ] `delete a`, then `get a` gives exit 1
- [ ] `put "" x` gives exit 2
- [ ] An unused port with `--timeout 0.5` gives exit 3 and finishes in under 3 seconds
- [ ] A value with spaces (`"hello world"`) round-trips

**Verify:** `pytest -v tests/` passes fully, and `kv put x 1 && kv get x` works against `make run`.

---

## Step 6 — Docker, bulk writer and demo (subphase 1.5)

### `Dockerfile`

- `FROM python:3.11-slim`, `WORKDIR /app`
- copy `requirements.txt`, `pip install --no-cache-dir -r requirements.txt`
- copy the project, run `bash scripts/gen_proto.sh`, `pip install --no-cache-dir -e .`
- `ENV PYTHONUNBUFFERED=1`, `EXPOSE 50051`
- `CMD ["python", "-m", "dynamo.server"]`

### `docker-compose.yml`

```yaml
services:
  node1:
    build: .
    environment:
      NODE_ID: node1
      PORT: "50051"
      DATA_DIR: /data
    ports:
      - "50051:50051"
    volumes:
      - ./data:/data
```

### `scripts/bulk_write.py`

```
python scripts/bulk_write.py --node localhost:50051 --count 1500 --prefix bulk
```

Writes `bulk:0 … bulk:1499` with value `v<i>` using one gRPC channel. It exits non-zero if any write is not `OK`. A separate `--verify` flag reads every key back and exits non-zero on the first mismatch. (Spawning 1,500 CLI processes would be too slow.)

### `scripts/demo_phase1.sh`

Use `set -uo pipefail` (not `-e`, because `kv get` returns 1 on purpose). Print a numbered header for each step, then:

1. `docker compose down`, `rm -rf ./data`
2. `docker compose up -d --build`; wait up to 30 s until `kv get __ping__` returns exit 0 or 1 (not 3)
3. `kv put cart:42 book`, `kv put user:7 prateek`, `kv delete user:7`
4. `python scripts/bulk_write.py --count 1500 --prefix bulk` (triggers at least one checkpoint)
5. `ls -l data/node1/` and `wc -l data/node1/wal.log` (snapshot exists, log is short)
6. **Hard crash:** `docker compose kill -s SIGKILL node1`
7. `docker compose rm -f node1`
8. `docker compose up -d`; wait for readiness as in step 2
9. Checks:
   - `kv get cart:42` prints `book`
   - `kv get user:7` exits 1 (the delete survived)
   - `python scripts/bulk_write.py --count 1500 --prefix bulk --verify` exits 0
10. Print `PHASE 1 DEMO PASSED` and exit 0, or print the first failing check and exit 1

The script needs `kv` installed on the host (`make install`).

**Verify:** `make demo` prints `PHASE 1 DEMO PASSED`.

---

## Step 7 — Root README

**`README.md`** should cover:

- one paragraph on what Phase 1 is
- prerequisites (Python 3.11+, Docker)
- quick start: `make install`, `make test`, `make run`, CLI examples, `make demo`
- a short "How persistence works" section: WAL → fsync → dict → OK, snapshot every 1,000 writes, recovery = snapshot + replay + torn-line truncation
- a link to `phase1/README.md` for the full design

---

## Definition of done

- [ ] `make test` passes, and every test listed above exists
- [ ] `make demo` prints `PHASE 1 DEMO PASSED` and uses SIGKILL, not a graceful stop
- [ ] A torn last log line never blocks startup, and writes made after it survive the next restart
- [ ] A snapshot is written every `checkpoint_every` writes, and the log is cleared only after the snapshot rename is durable
- [ ] No Phase 2+ features were added
- [ ] `phase1/README.md` folder layout matches the real layout

When finished, report:

1. a file tree
2. the full `pytest -v` output summary
3. the demo output
4. any place where you deviated from this spec, and why
