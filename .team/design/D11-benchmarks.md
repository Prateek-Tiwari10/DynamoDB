# D11 — Benchmarks + graphs

Owner: Meet · Reviewer: Prateek · Status: REVIEW · Date: 2026-11-21

## 1. Requirement
From TASKS.md / SPEC "Testing and benchmarks":
- Workload: 50/50 read/write, 1 KB values, 10,000 keys, 16 concurrent clients, 60 s.
- Measure ops/s, p50, p99.
- Vary (N,R,W) = (3,1,1), (3,2,2), (3,3,3), (3,1,3); cluster size 1, 3, 5; with 1 node killed.
- Output CSV + matplotlib graphs in `results/`, one-line takeaway per graph.

Files: `tools/bench.py`, `results/` (+ `tests/test_bench.py`).

## 2. Chosen approach (plain words)
`tools/bench.py` is one Python script with two commands: `run` and `plot`.

`run` starts its **own cluster as local processes** (`python -m dynamo.server`, the same way as
`tools/cluster_local.sh`), on ports 50061+ and in a temp data folder, so it never touches a dev
cluster you have running. For every cluster shape it writes a small yaml (nodes, N, R, W, the
normal timers), starts the nodes, waits until every node answers `ClusterInfo` and sees all others
ALIVE, then **preloads** all 10,000 keys so that reads find something.

The load generator is **closed loop**: 16 asyncio workers in one process; each one picks a random
key, flips a coin (get or put), sends the request to a random node, waits for the reply, records the
latency, and goes again. There is a 10 s **warm-up** that is not recorded, then 60 s that are.
Each scenario runs **3 times**; the CSV keeps every run and the graphs show the median run with the
min–max as an error bar.

**N needs a restart, R and W do not**: R and W are fields of each request (`GetRequest.r`,
`PutRequest.w`), so the four (N,R,W) rows run back to back on one N=3, 5-node cluster. Cluster size
1 and 3 each get their own cluster (size 1: N=R=W=1; size 3: N=3, R=W=2). The "1 node killed"
scenario uses the 5-node cluster: preload, SIGKILL one node, wait until the others see it DEAD,
then measure (steady state with a node gone, sloppy quorum on as in the real config). The
workers stop sending to a node the moment the bench kills it (like a load balancer taking it out),
so what is measured is the cluster's own reaction, not clients talking to a dead address.

One extra **timeline run** (not in the spec, good for the demo): 90 s at (3,2,2), node killed at
30 s, restarted at 60 s; ops/s and p99 per second, so the dip and the recovery are visible.

Puts carry a context like a real client: a shared `key → last context` dict, updated from every Get
and Put reply. Without it every blind put through a different coordinator would add a sibling and
values would keep growing during the run.

`plot` reads the CSVs and draws the PNGs, so graphs can be redrawn without a 30-minute rerun.
`results/README.md` holds the graphs, the takeaways and the machine (CPU, cores, RAM, Python,
"client and all nodes on one laptop").

## 3. Alternatives we rejected
| Option | Why not |
|---|---|
| Open loop (fixed request rate) | Shows queueing delay honestly, but needs a rate sweep to find the knee and a scheduler that never falls behind in Python; too much for the time left. We say in the results that closed loop hides queueing delay (coordinated omission). |
| Benchmark the Docker cluster | `cluster.yaml` is baked into the image and compose has a fixed 5 nodes; size 1/3 and N changes would need rebuilds or extra compose files. Docker networking also adds noise. We add `--existing` (below) for one confirmation run against Docker instead. |
| Shell loop over `client.cli` | One Python process start per request (~200 ms) would be the whole latency. |
| One client process per worker (multiprocessing) | Avoids the client GIL limit but more moving parts. We measure the client's own CPU instead and say so if it is the bottleneck. |
| Separate N=3 clusters for each (R,W) | R/W are per request in our API, so a restart only adds 15 s per row and changes nothing. |
| Measure right after the kill only | Mixes the SUSPECT gap (timeouts) with steady state; the timeline run shows that part separately. |

## 4. Interfaces and data structures
Uses only the public client API (`KVStore` stub: `Get`, `Put`, `ClusterInfo`) and
`python -m dynamo.server --node-id --config --data-dir`. No change to any node code or shared file
except **`requirements.txt`: add `matplotlib`** (shared file → DEC-16, Prateek to agree; SPEC
"Stack" already lists matplotlib). `bench.py run` does not need matplotlib, only `plot` does.

```python
# tools/bench.py
@dataclass(frozen=True)
class Scenario:
    name: str            # "nrw-3-2-2", "size-1", "killed-1", ...
    nodes: int           # cluster size
    n: int
    r: int               # sent in every GetRequest
    w: int               # sent in every PutRequest
    killed: int = 0      # nodes SIGKILLed before measuring

@dataclass
class RunResult:
    scenario: Scenario
    rep: int
    seconds: float
    ok: int              # OK + NOT_FOUND replies
    errors: int          # QUORUM_FAILED, timeouts, UNAVAILABLE
    latencies_ms: list[float]
    client_cpu: float    # process CPU seconds / wall seconds

def percentile(sorted_ms: list[float], p: float) -> float      # nearest-rank, p in 0..100
def make_value(size: int = 1024) -> bytes
async def worker(stubs, keys, contexts, r, w, stop_at, record) -> None
async def measure(scenario, addresses, duration_s, warmup_s) -> RunResult
def write_config(path, nodes, n, r, w, base_port) -> None
class LocalCluster:                                           # start / kill / restart / stop processes
```

CLI:
```
python tools/bench.py run  [--only NAME...] [--duration 60] [--warmup 10] [--reps 3] [--out results]
python tools/bench.py run  --existing localhost:50051,...,localhost:50055 --only nrw-3-2-2   # Docker
python tools/bench.py plot [--out results]
```

Files written:
- `results/bench.csv`: one row per run — `scenario,nodes,n,r,w,killed,rep,seconds,ok,errors,ops_per_s,p50_ms,p99_ms,client_cpu`
- `results/timeline.csv`: `second,ops,errors,p99_ms,event`
- `results/nrw.png`, `results/cluster_size.png`, `results/killed.png`, `results/timeline.png`
- `results/README.md`: graphs, takeaways, machine, how to rerun

## 5. Flow
1. For each cluster shape needed by the selected scenarios: write yaml, start nodes, wait ready
   (all answer, all ALIVE in node1's view, 1.5 s for channel backoff as in the demos).
2. Preload: 16 workers put the 10,000 keys once (default W), fill the context dict.
3. For each scenario on this shape (and each rep): kill nodes if asked and wait for DEAD;
   warm-up 10 s; measure 60 s; append the row to `bench.csv`; restart killed nodes.
4. Stop the cluster, delete the temp data folder.
5. Timeline run last.
6. `plot` draws the four graphs from the CSVs.

Expected total: 7 scenarios × 3 reps × 70 s + startup/preload ≈ 30 min. `--duration 5 --reps 1`
for a quick check.

## 6. Edge cases
| Case | What happens | Test |
|---|---|---|
| Ports 50061+ already in use | Node fails to start (no SO_REUSEPORT); bench stops with a clear message | manual |
| Dev cluster on 50051–55 running | Not touched (different ports, own data dir) | manual |
| Request fails (QUORUM_FAILED / timeout) | Counted in `errors`, latency still recorded (a failed request costs time too); ops/s counts only `ok` | unit (fake stub) |
| Get returns NOT_FOUND (key not preloaded yet) | Counts as ok; should not happen after preload | unit |
| Siblings in a Get | Context stored, all values ignored; next put merges them | unit |
| (3,3,3) with a node killed | Not run: R=3 cannot be met by design; only (3,2,2) is run with a kill | — |
| Size 1 | N=R=W=1; no replication, so it is the "one machine" baseline | run |
| Bench interrupted (Ctrl-C) | Nodes are stopped in a `finally`, temp folder removed, rows written so far kept | manual |
| Client CPU near 100 % | Written in the CSV; README says the client may be the bottleneck for that row | run |
| Sloppy quorum during "killed" | On (as in the real config): writes keep W=2 via a stand-in; reads stay strict | run |

## 7. Tests
- Unit (`tests/test_bench.py`): `percentile` (nearest rank, 1 sample, p50/p99), `make_value` size,
  `write_config` loads with `load_config`, `worker` against a fake stub (counts ok / errors,
  stores contexts, uses r/w from the scenario), CSV row format.
- In-process cluster: one short `measure` (2 s) against the `cluster` fixture → ok > 0, errors = 0.
- Real: full `run` once locally; one `--existing` run against Docker for (3,2,2).

## 8. Interview questions on this part
1. Q: Why is (3,1,1) faster than (3,3,3)?  A: The coordinator replies after the R-th/W-th fastest
   replica; with 3 it waits for the slowest one, so p99 follows the slowest replica.
2. Q: What is coordinated omission?  A: In a closed loop a slow reply stops that client from sending,
   so the requests that would have queued during the stall are never measured; p99 looks better
   than what users at a fixed rate would see.
3. Q: Why does a 5-node cluster not give 5× the throughput of 1 node?  A: Every op does N=3 replica
   calls instead of 1, and here all nodes and the client share one laptop's CPUs.
4. Q: What does the timeline show after the kill?  A: Probably only a small dip: with N=3 and
   R=W=2 the two live owners still make the quorum, so the coordinator does not wait for the dead
   one's 500 ms timeout. That is the point of N > W. After DEAD the fan-out skips it; after restart
   hints are handed back. (Whatever we see, we report it as measured.)
5. Q: Why preload?  A: Otherwise half the reads early on are NOT_FOUND, which are cheaper and make
   the numbers look better.

## 9. As built
Built as designed (`tools/bench.py`, `tests/test_bench.py` 18 tests, results in `results/`). Changes:
1. Run as `python -m tools.bench`, not `python tools/bench.py`: the script imports the `dynamo`
   package, which needs the project root on the path.
2. Node data goes to `~/.cache/dynamo-bench` (`--work-dir`), not the system temp folder: `/tmp` is
   tmpfs on this laptop, where fsync is free (1 node: ~3000 ops/s on tmpfs vs ~1300 on the SSD).
   `./data` was not usable either: Docker created it as root.
3. Nodes run with `LOG_LEVEL=WARNING` so logging does not take part in the measurement.
4. Killed nodes' client channels are closed only at the end: closing one at once cancelled
   requests still in flight on it (CancelledError, not an RPC error) and crashed the first full run's
   timeline. Test added.
5. matplotlib is only in my venv until DEC-16 is agreed; `plot` says how to install it if missing.

Results (`results/README.md`): p50 follows the quorum size (5.3 ms at R/W=1, ~10–11 ms at 2–3);
throughput differences between quorum settings are inside the laptop's run-to-run noise (±25 %);
only R/W=3 settings had QUORUM_FAILED; 1 node is the fastest (no replication); with 1 of 5 nodes
DEAD, (3,2,2) serves everything at ~10 % lower throughput. **Found:** the restart of a killed node
drops throughput from ~800 to 30–200 ops/s for ~20 s with p99 1–3 s (likely hinted handoff + read
repair + anti-entropy all landing on it at once, one fsync each; cause not isolated; REVIEW-LATER #19).
