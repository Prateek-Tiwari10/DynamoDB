# Dynamo KV Store — Implementation Spec

> Copy of the agreed spec (claude.ai Doc "Dynamo KV Store — Implementation Spec", 2 Oct 2026).
> This file is the source of truth for *what* we build. *How* each part is built is decided
> in the design docs under `.team/design/`. If a design doc changes something here, record it
> in `.team/DECISIONS.md` and update this file in the same commit.

A Dynamo-style distributed key-value store in Python 3.11 + asyncio + gRPC, run as 5 nodes with
Docker Compose, built in 4 phases from a single node up to a fault-tolerant cluster.

## Capabilities

| # | Ability | Mechanism | Phase |
|---|---|---|---|
| C1 | Store, read and delete a key | `put` / `get` / `delete` API | 1 |
| C2 | Spread keys evenly across nodes | Consistent hashing with virtual nodes | 2 |
| C3 | Ask any node for any key | Coordinator forwarding | 2 |
| C4 | Keep N copies of every key | Preference list replication | 2 |
| C5 | Choose consistency per request | Quorum N, R, W | 2 |
| C6 | Detect and keep concurrent writes | Vector clocks + siblings | 3 |
| C7 | Fix stale replicas on read | Read repair | 3 |
| C8 | Know which nodes are alive without a master | Gossip membership + failure detector | 3 |
| C9 | Accept writes while a replica is down | Sloppy quorum + hinted handoff | 4 |
| C10 | Survive a node restart without losing data | Write-ahead log + snapshot | 1 |

Stretch (only if time allows): Merkle-tree anti-entropy, live dashboard, adding a node at runtime with key migration.

## Build phases

Each phase ends with something that runs and can be demoed.

**Phase 1 — Single node (C1, C10)**
- gRPC `KVStore` service with `Put`, `Get`, `Delete`
- `storage.py`: in-memory dict + append-only write-ahead log, replayed on startup
- CLI client: `kv put k v`, `kv get k`, `kv delete k`
- Demo: put a key, kill the process, restart it, the key is still there.

**Phase 2 — Static cluster with quorum (C2, C3, C4, C5)**
- `ring.py`: hash ring with MD5, 32 virtual nodes per node, `preference_list(key, n)`
- Static membership from `cluster.yaml` (all 5 nodes known at start)
- `NodeService` internal RPCs: `ReplicaPut`, `ReplicaGet`
- `coordinator.py`: forward to the first node of the preference list, fan out to N replicas, wait for W or R replies with a timeout
- Per-request R and W override
- Demo: 5 nodes in Docker Compose; write through node1, read through node4; show which 3 nodes hold the key; kill 1 node, reads and writes still succeed with N=3, R=2, W=2.

**Phase 3 — Versioning and membership (C6, C7, C8)**
- `vclock.py`: increment, compare, merge, prune to 10 entries
- Siblings: store all concurrent versions, `Get` returns all of them plus a context
- Read repair: after `Get`, push the newest version to stale replicas in the background
- `gossip.py`: every 1 s exchange membership with one random peer; ALIVE → SUSPECT (3 s) → DEAD (10 s)
- Coordinator skips DEAD nodes
- Demo: two clients write the same key with the same old context; `Get` shows two siblings; client merges and writes back. Kill a node and watch others mark it DEAD.

**Phase 4 — Failure handling and benchmarks (C9)**
- Sloppy quorum: if a preferred node is DEAD, use the next healthy node on the ring
- Hinted handoff: store the write with `hint_for = <real owner>`, retry delivery every 5 s, delete after success
- Chaos script: random kill/restart of containers under load
- Benchmark script: throughput and p50/p99 latency for several N/R/W settings
- Demo: kill a node, writes keep succeeding, restart it, the hinted data arrives; show benchmark graphs.

**Phase 5 — Stretch**
- Merkle-tree anti-entropy between replicas
- Dashboard (FastAPI page) showing the ring, node states and key counts
- Join a new node at runtime and migrate its key ranges

## Architecture

Each node is one Python process exposing two gRPC services on one port: `KVStore` for clients and
`NodeService` for other nodes. No node is special; any node can coordinate any request.

```
dynamo/
  proto/dynamo.proto      # all messages and services
  dynamo/
    server.py             # starts gRPC server, wires modules
    coordinator.py        # handles client requests, quorum logic
    replica.py            # NodeService handlers (local reads/writes)
    ring.py               # consistent hashing, preference list
    vclock.py             # vector clocks
    storage.py            # dict + write-ahead log
    membership.py         # node table, states
    gossip.py             # gossip loop, failure detector
    handoff.py            # hinted handoff store + retry loop
    config.py             # loads cluster.yaml
  client/cli.py           # kv put/get/delete
  tools/bench.py          # load generator
  tools/chaos.py          # kills/restarts containers
  tests/
  docker-compose.yml
  cluster.yaml
```

### Configuration (`cluster.yaml`)

| Setting | Default | Meaning |
|---|---|---|
| `nodes` | node1..node5, ports 50051–50055 | Seed list of node IDs and addresses |
| `N` | 3 | Copies of each key |
| `R` | 2 | Replies needed for a read |
| `W` | 2 | Acks needed for a write |
| `vnodes` | 32 | Ring positions per physical node |
| `rpc_timeout_ms` | 500 | Wait per replica call |
| `gossip_interval_ms` | 1000 | Gossip period |
| `suspect_after_ms` | 3000 | No heartbeat → SUSPECT |
| `dead_after_ms` | 10000 | No heartbeat → DEAD |
| `handoff_retry_ms` | 5000 | Hint delivery retry period |
| `vclock_max_entries` | 10 | Prune limit |
| `data_dir` | `/data/<node_id>` | WAL and snapshot location |

Hashing: `position = int(md5(key).hexdigest(), 16) % 2**32`; vnode positions use `md5(f"{node_id}#{i}")`.

## Client API

Keys are strings, values are bytes. `context` is an opaque blob (the serialized vector clock) that
the client sends back on the next write.

```protobuf
service KVStore {
  rpc Put(PutRequest) returns (PutResponse);
  rpc Get(GetRequest) returns (GetResponse);
  rpc Delete(DeleteRequest) returns (DeleteResponse);
  rpc ClusterInfo(Empty) returns (ClusterInfoResponse);
}

message PutRequest {
  string key = 1;
  bytes value = 2;
  bytes context = 3;          // empty on first write
  uint32 w = 4;               // 0 = use cluster default
}
message PutResponse {
  Status status = 1;
  bytes context = 2;          // new vector clock
  repeated string written_to = 3;
}

message GetRequest {
  string key = 1;
  uint32 r = 2;               // 0 = use cluster default
}
message GetResponse {
  Status status = 1;
  repeated bytes values = 2;  // >1 means siblings
  bytes context = 3;          // merged clock of all siblings
  repeated string read_from = 4;
}

message DeleteRequest { string key = 1; bytes context = 2; uint32 w = 3; }
message DeleteResponse { Status status = 1; }

message ClusterInfoResponse {
  repeated Member members = 1;
  uint32 n = 2; uint32 r = 3; uint32 w = 4;
}

enum Status {
  OK = 0;
  NOT_FOUND = 1;
  QUORUM_FAILED = 2;   // fewer than R/W replies before timeout
  INVALID = 3;         // e.g. W > N
}
```

Rules
- `w` and `r` must be between 1 and N, otherwise `INVALID`.
- A `Put` without `context` on an existing key creates a sibling, not an overwrite.
- To resolve siblings, the client merges the values and calls `Put` with the context from `Get`.
- `Delete` writes a tombstone; it is not an instant removal.

CLI

| Command | Calls |
|---|---|
| `kv --node node1:50051 put cart:42 book` | `Put` |
| `kv get cart:42 --r 3` | `Get` |
| `kv delete cart:42` | `Delete` |
| `kv info` | `ClusterInfo` |

The CLI stores the last context per key in a local file so `put` after `get` sends it automatically.

## Node-to-node messages

Clients never call `NodeService`. Every call carries the sender's ID so receivers can update membership as a side effect.

```protobuf
service NodeService {
  rpc Forward(ForwardRequest) returns (ForwardResponse);           // phase 2
  rpc ReplicaPut(ReplicaPutRequest) returns (ReplicaPutResponse);  // phase 2
  rpc ReplicaGet(ReplicaGetRequest) returns (ReplicaGetResponse);  // phase 2
  rpc Gossip(GossipMessage) returns (GossipMessage);               // phase 3
  rpc DeliverHint(ReplicaPutRequest) returns (ReplicaPutResponse); // phase 4
}

message VectorClock {
  map<string, uint64> counters = 1;   // node_id -> counter
  map<string, int64> updated_ms = 2;  // node_id -> last update time, for pruning
}

message Version {
  bytes value = 1;
  VectorClock clock = 2;
  bool tombstone = 3;
}

message ForwardRequest {
  string sender = 1;
  oneof op { PutRequest put = 2; GetRequest get = 3; DeleteRequest delete = 4; }
}
message ForwardResponse {
  oneof result { PutResponse put = 1; GetResponse get = 2; DeleteResponse delete = 3; }
}

message ReplicaPutRequest {
  string sender = 1;
  string key = 2;
  Version version = 3;
  string hint_for = 4;   // empty = I am a real owner; set = sloppy-quorum stand-in
}
message ReplicaPutResponse { bool ok = 1; string node_id = 2; }

message ReplicaGetRequest { string sender = 1; string key = 2; }
message ReplicaGetResponse {
  string node_id = 1;
  repeated Version versions = 2;   // empty = not found
}

message Member {
  string node_id = 1;
  string address = 2;          // host:port
  uint64 heartbeat = 3;        // incremented by the owner every gossip tick
  MemberState state = 4;
  uint64 incarnation = 5;      // bumped when a node restarts or refutes SUSPECT
}
enum MemberState { ALIVE = 0; SUSPECT = 1; DEAD = 2; }

message GossipMessage { string sender = 1; repeated Member members = 2; }
```

Gossip merge rule: for each member keep the entry with the higher `incarnation`; if equal, the higher
`heartbeat`. A node seeing itself marked SUSPECT bumps its own `incarnation` to refute it.

| RPC | Phase | Sent by | Sent to |
|---|---|---|---|
| `Forward` | 2 | Any node | First healthy node in preference list |
| `ReplicaPut` | 2 | Coordinator | Each of the N replicas |
| `ReplicaGet` | 2 | Coordinator | Each of the N replicas |
| `Gossip` | 3 | Every node, every 1 s | One random ALIVE peer |
| `DeliverHint` | 4 | Node holding a hint | The real owner, once it is ALIVE |

## Data model

Each node keeps `store: dict[str, list[Version]]` (a list because concurrent writes are siblings).
Hints live in `hints: dict[str, list[(key, Version)]]` keyed by the real owner.

| Function | Rule |
|---|---|
| `increment(clock, node_id)` | `counters[node_id] += 1`, `updated_ms[node_id] = now` |
| `compare(a, b)` | `BEFORE` if every counter in a ≤ b and one is <; `AFTER` for the reverse; `EQUAL`; otherwise `CONCURRENT` |
| `merge(a, b)` | Per node, take the max counter |
| `prune(clock)` | If more than 10 entries, drop the one with the oldest `updated_ms` |

Storing a new version on a replica
1. For each existing version: if it is `BEFORE` the new one, drop it.
2. If any existing version is `AFTER` or `EQUAL` to the new one, ignore the new one (stale).
3. Otherwise append the new one; the key now has siblings.

Coordinator write: clock = `increment(merge(context clocks), coordinator_id)`.

Tombstones: `Delete` stores a `Version` with `tombstone = true` and empty value. `Get` returns
`NOT_FOUND` if the only surviving versions are tombstones. Purged after 24 h.

On-disk format
- WAL `/data/<node_id>/wal.log`: one JSON line per write, `fsync` before acking.
- Line: `{"op":"put","key":"cart:42","version":{"value":"<base64>","clock":{"node1":3},"tombstone":false},"hint_for":""}`
- Snapshot `/data/<node_id>/snapshot.json` every 1,000 log lines; the log is then truncated.
- Startup: load snapshot, replay the log, then join gossip.

## Request flows

A request always ends at a coordinator: the first ALIVE node in the key's preference list.

Put(key, value, context)
1. Client sends `Put` to any node X.
2. X computes `preference_list(key, N)`. If X is not in it, X sends `Forward` to the first ALIVE node in the list and relays the response.
3. Coordinator C builds the new clock: `increment(merge(context), C)`.
4. C writes locally (WAL + dict); that counts as 1 ack.
5. C sends `ReplicaPut` to the other N−1 replicas in parallel with `rpc_timeout_ms`.
6. As soon as W acks arrive, C returns `OK` with the new context. Remaining replies are handled in the background.
7. Fewer than W acks before timeout → `QUORUM_FAILED`. Replicas that did write keep the value (no rollback).

Get(key)
1. Routing as in Put steps 1–2.
2. C reads locally and sends `ReplicaGet` to the other N−1 replicas in parallel.
3. When R responses arrive, merge: drop any version `BEFORE` another; the rest are siblings.
4. Return values + merged context. All tombstones → `NOT_FOUND`.
5. Read repair (phase 3): push winning version(s) to replicas that were missing/older, in the background.

When a replica is DEAD (phase 4)
1. C walks the ring past the N preferred nodes and picks the next ALIVE node H.
2. C sends `ReplicaPut` to H with `hint_for = <dead node>`.
3. H stores it in `hints` (and WAL); counts as an ack.
4. Every `handoff_retry_ms`, H checks membership; when the owner is ALIVE, H calls `DeliverHint` and deletes the hint on success.

Edge cases
- `W > number of ALIVE nodes` → `QUORUM_FAILED` fast, without waiting for the timeout.
- Coordinator crash mid-write: client gets an error/timeout and may retry; vector clocks make retries safe.
- Node restart: replay WAL, bump `incarnation`, rejoin gossip; hints flow back to it.

## Testing and benchmarks

Unit tests (pytest)
- `ring`: same key → same nodes; N distinct physical nodes; removing a node moves about 1/5 of keys.
- `vclock`: compare BEFORE/AFTER/EQUAL/CONCURRENT; merge takes max; prune keeps 10.
- `storage`: write, crash (kill without close), restart, data present.

Cluster tests (Docker Compose, 5 nodes)

| Test | Expected |
|---|---|
| Write via node1, read via node4 | Same value |
| Kill 1 node, N=3 R=2 W=2 | All reads and writes succeed |
| Kill 2 replicas of a key, W=2, no sloppy quorum | `QUORUM_FAILED` |
| Same, with sloppy quorum on | Write succeeds; hint delivered after restart |
| Two writes with the same old context | `Get` returns 2 siblings |
| Stale replica, then `Get` | Replica updated by read repair |
| Kill a node | Others mark it SUSPECT then DEAD within ~10 s |
| Chaos: random kills for 5 min under load | Zero acknowledged writes lost |

Benchmarks (`tools/bench.py`): 50/50 read/write, 1 KB values, 10,000 keys, 16 concurrent clients, 60 s.
Measure ops/s, p50, p99. Vary (N,R,W) = (3,1,1), (3,2,2), (3,3,3), (3,1,3); cluster size 1, 3, 5; with 1 node killed.
Output CSV + matplotlib graphs.

Stack: Python 3.11, asyncio, grpcio, Docker Compose, pytest, matplotlib.
