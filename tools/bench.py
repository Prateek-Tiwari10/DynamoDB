"""Benchmark: throughput and p50/p99 latency of the key-value store.

    python -m tools.bench run  [--only NAME ...] [--duration 60] [--warmup 10] [--reps 3] [--out results]
    python -m tools.bench run  --existing localhost:50051,...,localhost:50055 [--only nrw-3-2-2]
    python -m tools.bench plot [--out results]

``run`` starts its own cluster as local processes on ports 50061+, so a cluster already running
on 50051-50055 is not touched. Node data goes to a temp folder under ~/.cache/dynamo-bench: a
real disk, so the log's fsync costs what it really costs (/tmp is often tmpfs). ``--existing`` measures a running cluster instead
(for example the Docker one); it never starts or kills nodes there.

Workload (SPEC): 50/50 get/put, 1 KB values, 10,000 keys, 16 clients, closed loop: each client
sends one request, waits for the reply, then sends the next. Closed loop hides queueing delay
("coordinated omission"): a client stuck on a slow reply sends nothing meanwhile.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import os
import platform
import random
import shutil
import subprocess
import sys
import tempfile
import time
from collections.abc import Callable
from dataclasses import dataclass, field

import grpc
import yaml

from dynamo import dynamo_pb2, dynamo_pb2_grpc

KEY_COUNT = 10_000
VALUE_SIZE = 1024
CLIENTS = 16
REQUEST_TIMEOUT_S = 3.0          # more than the node's own 1.5 s request budget
BASE_PORT = 50061                # 50061.. so a cluster on 50051-50055 is never touched
BASE_CONFIG = "cluster.local.yaml"
TIMELINE_KILL_AT_S = 30
TIMELINE_RESTART_AT_S = 60
TIMELINE_DURATION_S = 90
KILLED_NODE = "node5"
CSV_FIELDS = ["scenario", "nodes", "n", "r", "w", "killed", "rep", "seconds", "ok", "errors",
              "ops_per_s", "p50_ms", "p99_ms", "client_cpu"]


@dataclass(frozen=True)
class Scenario:
    """One benchmark setting: a cluster shape plus the R/W sent with every request."""
    name: str
    nodes: int
    n: int
    r: int
    w: int
    killed: int = 0              # nodes SIGKILLed before measuring


SCENARIOS = [
    Scenario("nrw-3-1-1", nodes=5, n=3, r=1, w=1),
    Scenario("nrw-3-2-2", nodes=5, n=3, r=2, w=2),   # also "cluster size 5"
    Scenario("nrw-3-3-3", nodes=5, n=3, r=3, w=3),
    Scenario("nrw-3-1-3", nodes=5, n=3, r=1, w=3),
    Scenario("killed-1", nodes=5, n=3, r=2, w=2, killed=1),
    Scenario("size-3", nodes=3, n=3, r=2, w=2),
    Scenario("size-1", nodes=1, n=1, r=1, w=1),
]
TIMELINE = Scenario("timeline", nodes=5, n=3, r=2, w=2)


# ---------- small helpers ----------

def percentile(sorted_ms: list[float], p: float) -> float:
    """Nearest-rank percentile of an already sorted list (p from 0 to 100); 0.0 for no samples."""
    if not sorted_ms:
        return 0.0
    rank = max(1, -(-len(sorted_ms) * p // 100))    # ceil(len * p / 100), at least 1
    return sorted_ms[int(rank) - 1]


def make_value(seq: int, size: int = VALUE_SIZE) -> bytes:
    """A value of exactly ``size`` bytes that differs from write to write."""
    head = f"v{seq}:".encode()
    return (head + b"x" * size)[:size]


def key_names(count: int = KEY_COUNT) -> list[str]:
    """The benchmark's key space: bench:00000 .. bench:09999."""
    return [f"bench:{i:05d}" for i in range(count)]


def node_ids(count: int) -> list[str]:
    """node1 .. nodeN."""
    return [f"node{i}" for i in range(1, count + 1)]


def write_config(path: str, nodes: int, n: int, base_port: int = BASE_PORT,
                 base_config: str = BASE_CONFIG) -> None:
    """Write a cluster yaml: the timers of ``base_config``, ``nodes`` nodes from ``base_port`` on.

    The cluster-wide R and W are min(2, n); the benchmark sends its own R/W with each request.
    """
    with open(base_config) as file:
        settings = yaml.safe_load(file)
    settings["nodes"] = {node_id: f"localhost:{base_port + i}"
                         for i, node_id in enumerate(node_ids(nodes))}
    settings["n"] = n
    settings["r"] = settings["w"] = min(2, n)
    with open(path, "w") as file:
        yaml.safe_dump(settings, file, sort_keys=False)


# ---------- recording ----------

@dataclass
class Recorder:
    """Collects the result of every request finished after ``measure_from`` (a monotonic time)."""
    measure_from: float
    ok: int = 0
    errors: int = 0
    latencies_ms: list[float] = field(default_factory=list)
    per_second: dict[int, list[tuple[bool, float]]] = field(default_factory=dict)

    def record(self, ok: bool, latency_ms: float, now: float) -> None:
        """Count one finished request (warm-up requests are ignored)."""
        if now < self.measure_from:
            return
        if ok:
            self.ok += 1
        else:
            self.errors += 1
        self.latencies_ms.append(latency_ms)
        second = int(now - self.measure_from)
        self.per_second.setdefault(second, []).append((ok, latency_ms))


@dataclass
class RunResult:
    """One measured run of one scenario."""
    scenario: Scenario
    rep: int
    seconds: float
    ok: int                  # OK and NOT_FOUND replies
    errors: int              # QUORUM_FAILED, INVALID, timeouts, unreachable
    p50_ms: float
    p99_ms: float
    client_cpu: float        # this process's CPU seconds / wall seconds (1.0 = one core busy)

    @property
    def ops_per_s(self) -> float:
        """Successful requests per second."""
        return self.ok / self.seconds if self.seconds else 0.0

    def row(self) -> dict[str, object]:
        """One CSV row."""
        s = self.scenario
        return {"scenario": s.name, "nodes": s.nodes, "n": s.n, "r": s.r, "w": s.w,
                "killed": s.killed, "rep": self.rep, "seconds": round(self.seconds, 2),
                "ok": self.ok, "errors": self.errors, "ops_per_s": round(self.ops_per_s, 1),
                "p50_ms": round(self.p50_ms, 2), "p99_ms": round(self.p99_ms, 2),
                "client_cpu": round(self.client_cpu, 2)}


# ---------- load ----------

async def one_request(stub, key: str, contexts: dict[str, bytes], r: int, w: int,
                      rng: random.Random, seq: int) -> bool:
    """Send one get or put (coin flip). Keeps the reply's context. True when it succeeded."""
    try:
        if rng.random() < 0.5:
            reply = await stub.Get(dynamo_pb2.GetRequest(key=key, r=r), timeout=REQUEST_TIMEOUT_S)
            ok = reply.status in (dynamo_pb2.OK, dynamo_pb2.NOT_FOUND)
        else:
            request = dynamo_pb2.PutRequest(key=key, value=make_value(seq),
                                            context=contexts.get(key, b""), w=w)
            reply = await stub.Put(request, timeout=REQUEST_TIMEOUT_S)
            ok = reply.status == dynamo_pb2.OK
    except grpc.aio.AioRpcError:
        return False
    if ok:
        contexts[key] = reply.context     # a put with this context merges any siblings
    return ok


async def worker(targets: list, keys: list[str], contexts: dict[str, bytes], r: int, w: int,
                 stop_at: float, record: Callable[[bool, float, float], None],
                 rng: random.Random) -> None:
    """Closed loop: random key, random node from ``targets``, wait for the reply, repeat.

    ``targets`` is read on every request, so nodes can be taken out and put back while it runs.
    """
    seq = 0
    while time.monotonic() < stop_at:
        if not targets:
            await asyncio.sleep(0.01)
            continue
        seq += 1
        start = time.monotonic()
        ok = await one_request(rng.choice(targets), rng.choice(keys), contexts, r, w, rng, seq)
        now = time.monotonic()
        record(ok, (now - start) * 1000, now)


async def run_load(targets: list, keys: list[str], contexts: dict[str, bytes], r: int, w: int,
                   warmup_s: float, duration_s: float, clients: int = CLIENTS,
                   seed: int | None = None) -> tuple[Recorder, float]:
    """Run ``clients`` workers for warm-up + duration. Returns the recorder and client CPU use."""
    rng = random.Random(seed)
    start = time.monotonic()
    recorder = Recorder(measure_from=start + warmup_s)
    stop_at = recorder.measure_from + duration_s
    tasks = [asyncio.create_task(worker(targets, keys, contexts, r, w, stop_at, recorder.record,
                                        random.Random(rng.random())))
             for _ in range(clients)]
    await asyncio.sleep(max(0.0, recorder.measure_from - time.monotonic()))
    cpu_before = time.process_time()
    await asyncio.gather(*tasks)
    wall = time.monotonic() - recorder.measure_from
    cpu = (time.process_time() - cpu_before) / wall if wall > 0 else 0.0
    return recorder, cpu


def summarize(scenario: Scenario, rep: int, recorder: Recorder, seconds: float,
              cpu: float) -> RunResult:
    """Turn a recorder into one RunResult."""
    ordered = sorted(recorder.latencies_ms)
    return RunResult(scenario, rep, seconds, recorder.ok, recorder.errors,
                     percentile(ordered, 50), percentile(ordered, 99), cpu)


async def preload(targets: list, keys: list[str], contexts: dict[str, bytes]) -> None:
    """Write every key once (cluster default W) so reads find data. Retries a key 3 times."""
    queue = list(keys)

    async def loader(index: int) -> None:
        stub = targets[index % len(targets)]
        while queue:
            key = queue.pop()
            for _ in range(3):
                request = dynamo_pb2.PutRequest(key=key, value=make_value(0))
                try:
                    reply = await stub.Put(request, timeout=REQUEST_TIMEOUT_S)
                except grpc.aio.AioRpcError:
                    continue
                if reply.status == dynamo_pb2.OK:
                    contexts[key] = reply.context
                    break

    await asyncio.gather(*(loader(i) for i in range(CLIENTS)))
    missing = len(keys) - len(contexts)
    if missing:
        print(f"  warning: {missing} keys could not be preloaded")


# ---------- cluster ----------

class LocalCluster:
    """Nodes as local processes (``python -m dynamo.server``) with a generated config."""

    def __init__(self, nodes: int, n: int, work_dir: str, base_port: int = BASE_PORT) -> None:
        self.work_dir = work_dir
        self.config_path = os.path.join(work_dir, "cluster.yaml")
        write_config(self.config_path, nodes, n, base_port)
        self.addresses = {node_id: f"localhost:{base_port + i}"
                          for i, node_id in enumerate(node_ids(nodes))}
        self._procs: dict[str, subprocess.Popen] = {}
        self._channels: dict[str, grpc.aio.Channel] = {}
        self._retired: list[grpc.aio.Channel] = []   # channels of killed nodes, closed in stop()

    def start(self, node_id: str | None = None) -> None:
        """Start one node, or all of them."""
        for nid in [node_id] if node_id else list(self.addresses):
            log = open(os.path.join(self.work_dir, f"{nid}.log"), "ab")
            env = {**os.environ, "LOG_LEVEL": "WARNING", "PYTHONUNBUFFERED": "1"}
            self._procs[nid] = subprocess.Popen(
                [sys.executable, "-m", "dynamo.server", "--node-id", nid,
                 "--config", self.config_path, "--data-dir", os.path.join(self.work_dir, "data")],
                stdout=log, stderr=subprocess.STDOUT, env=env)
            log.close()

    def kill(self, node_id: str) -> None:
        """Crash one node (SIGKILL)."""
        proc = self._procs.pop(node_id)
        proc.kill()
        proc.wait()

    async def stop(self) -> None:
        """Close client channels, SIGTERM every node, SIGKILL any still running after 10 s."""
        for channel in [*self._channels.values(), *self._retired]:
            await channel.close()
        self._channels.clear()
        self._retired.clear()
        for proc in self._procs.values():
            proc.terminate()
        for proc in self._procs.values():
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()
        self._procs.clear()

    def stub(self, node_id: str) -> dynamo_pb2_grpc.KVStoreStub:
        """Client stub for one node. A fresh channel after a restart (no reconnect backoff)."""
        if node_id not in self._channels:
            self._channels[node_id] = grpc.aio.insecure_channel(self.addresses[node_id])
        return dynamo_pb2_grpc.KVStoreStub(self._channels[node_id])

    def forget_channel(self, node_id: str) -> None:
        """Stop handing out the channel of a killed node; a restart gets a fresh one.

        The old channel is closed only in ``stop()``: closing it now would cancel requests that
        workers still have in flight on it, and a cancelled call raises CancelledError, not an
        RPC error.
        """
        channel = self._channels.pop(node_id, None)
        if channel is not None:
            self._retired.append(channel)

    def running(self) -> list[str]:
        """Ids of the nodes whose process is running."""
        return [nid for nid, proc in self._procs.items() if proc.poll() is None]

    def check_alive(self) -> None:
        """Fail with the log tail if a started node has exited (for example: port in use)."""
        for nid, proc in self._procs.items():
            if proc.poll() is not None:
                with open(os.path.join(self.work_dir, f"{nid}.log"), errors="replace") as file:
                    tail = file.read()[-800:]
                raise RuntimeError(f"{nid} exited with code {proc.returncode}:\n{tail}")


async def member_states(stub) -> dict[str, int] | None:
    """node_id -> state in that node's member table; None if it does not answer."""
    try:
        info = await stub.ClusterInfo(dynamo_pb2.Empty(), timeout=1)
    except grpc.aio.AioRpcError:
        return None
    return {m.node_id: m.state for m in info.members}


async def wait_for(condition: Callable, what: str, timeout_s: float,
                   check: Callable[[], None] = lambda: None) -> None:
    """Poll the async ``condition`` every 0.2 s until it is true; fail after ``timeout_s``."""
    deadline = time.monotonic() + timeout_s
    while not await condition():
        check()
        if time.monotonic() > deadline:
            raise RuntimeError(f"timed out after {timeout_s} s waiting for {what}")
        await asyncio.sleep(0.2)


async def all_see(stubs: dict[str, object], node_id: str, state: int) -> bool:
    """True when every node in ``stubs`` sees ``node_id`` in ``state``."""
    for stub in stubs.values():
        states = await member_states(stub)
        if states is None or states.get(node_id) != state:
            return False
    return True


async def wait_ready(stubs: dict[str, object], check: Callable[[], None] = lambda: None) -> None:
    """Every node answers and sees every node in ``stubs`` as ALIVE."""
    async def ready() -> bool:
        for nid in stubs:
            if not await all_see(stubs, nid, dynamo_pb2.ALIVE):
                return False
        return True
    await wait_for(ready, "all nodes ALIVE", 60, check)
    await asyncio.sleep(1.5)   # node-to-node channels refused during startup reconnect (1 s backoff)


# ---------- scenarios ----------

async def measure_scenario(scenario: Scenario, targets: list, contexts: dict[str, bytes],
                           args: argparse.Namespace) -> list[RunResult]:
    """All repetitions of one scenario on a ready cluster."""
    results = []
    keys = key_names()
    for rep in range(1, args.reps + 1):
        recorder, cpu = await run_load(targets, keys, contexts, scenario.r, scenario.w,
                                       args.warmup, args.duration)
        result = summarize(scenario, rep, recorder, args.duration, cpu)
        print(f"  {scenario.name} rep {rep}: {result.ops_per_s:.0f} ops/s, "
              f"p50 {result.p50_ms:.1f} ms, p99 {result.p99_ms:.1f} ms, "
              f"errors {result.errors}, client cpu {result.client_cpu:.2f}")
        results.append(result)
    return results


async def run_shape(nodes: int, n: int, scenarios: list[Scenario], args: argparse.Namespace,
                    work_root: str) -> None:
    """Start one cluster shape, preload, run its scenarios, save each one's rows, stop."""
    work_dir = tempfile.mkdtemp(prefix=f"bench-{nodes}n-", dir=work_root)
    cluster = LocalCluster(nodes, n, work_dir)
    try:
        print(f"cluster: {nodes} node(s), N={n}  (logs in {work_dir})")
        cluster.start()
        stubs = {nid: cluster.stub(nid) for nid in cluster.addresses}
        await wait_ready(stubs, cluster.check_alive)
        targets = list(stubs.values())
        contexts: dict[str, bytes] = {}
        await preload(targets, key_names(), contexts)
        for scenario in scenarios:
            if scenario.name == TIMELINE.name:
                await run_timeline(cluster, stubs, targets, contexts, args)
                continue
            if scenario.killed:
                await kill_and_wait_dead(cluster, stubs, targets, KILLED_NODE)
            save_rows(os.path.join(args.out, "bench.csv"), scenario.name,
                      [r.row() for r in await measure_scenario(scenario, targets, contexts, args)])
    finally:
        await cluster.stop()
        shutil.rmtree(work_dir, ignore_errors=True)


async def kill_and_wait_dead(cluster: LocalCluster, stubs: dict, targets: list,
                             node_id: str) -> None:
    """SIGKILL ``node_id``, take it out of the clients' targets, wait until the rest see DEAD."""
    targets.remove(stubs.pop(node_id))
    cluster.kill(node_id)
    cluster.forget_channel(node_id)
    started = time.monotonic()
    await wait_for(lambda: all_see(stubs, node_id, dynamo_pb2.DEAD), f"{node_id} DEAD", 30)
    print(f"  {node_id} killed, seen as DEAD by all after {time.monotonic() - started:.1f} s")


async def run_timeline(cluster: LocalCluster, stubs: dict, targets: list,
                       contexts: dict[str, bytes], args: argparse.Namespace) -> None:
    """(3,2,2) for TIMELINE_DURATION_S: kill a node at 30 s, start it again at 60 s.

    Per second: successful ops, errors and p99, written to timeline.csv with the events.
    """
    events: dict[int, str] = {}
    measure_start = time.monotonic() + args.warmup

    async def script() -> None:
        await asyncio.sleep(max(0.0, measure_start + TIMELINE_KILL_AT_S - time.monotonic()))
        targets.remove(stubs.pop(KILLED_NODE))
        cluster.kill(KILLED_NODE)
        cluster.forget_channel(KILLED_NODE)
        events[TIMELINE_KILL_AT_S] = f"kill {KILLED_NODE}"
        await asyncio.sleep(max(0.0, measure_start + TIMELINE_RESTART_AT_S - time.monotonic()))
        cluster.start(KILLED_NODE)
        events[TIMELINE_RESTART_AT_S] = f"start {KILLED_NODE}"
        stub = cluster.stub(KILLED_NODE)
        while await member_states(stub) is None:        # clients use it once it answers
            await asyncio.sleep(0.2)
        events.setdefault(int(time.monotonic() - measure_start), f"{KILLED_NODE} answers")
        stubs[KILLED_NODE] = stub
        targets.append(stub)

    print(f"  timeline: {TIMELINE_DURATION_S} s, kill {KILLED_NODE} at {TIMELINE_KILL_AT_S} s, "
          f"start at {TIMELINE_RESTART_AT_S} s")
    events_task = asyncio.create_task(script())
    recorder, _ = await run_load(targets, key_names(), contexts, TIMELINE.r, TIMELINE.w,
                                 args.warmup, TIMELINE_DURATION_S)
    await events_task
    rows = []
    for second in range(TIMELINE_DURATION_S):
        samples = recorder.per_second.get(second, [])
        ordered = sorted(ms for _, ms in samples)
        rows.append({"second": second, "ops": sum(1 for ok, _ in samples if ok),
                     "errors": sum(1 for ok, _ in samples if not ok),
                     "p99_ms": round(percentile(ordered, 99), 2), "event": events.get(second, "")})
    write_csv(os.path.join(args.out, "timeline.csv"), rows,
              ["second", "ops", "errors", "p99_ms", "event"])
    print(f"  timeline: events {events}")


async def run_existing(addresses: list[str], scenarios: list[Scenario],
                       args: argparse.Namespace) -> None:
    """Measure an already running cluster (no start, no kill). Rows are named <scenario>-existing."""
    channels = [grpc.aio.insecure_channel(a) for a in addresses]
    try:
        targets = [dynamo_pb2_grpc.KVStoreStub(c) for c in channels]
        info = await targets[0].ClusterInfo(dynamo_pb2.Empty(), timeout=2)
        contexts: dict[str, bytes] = {}
        print(f"existing cluster: {len(addresses)} address(es), N={info.n}")
        await preload(targets, key_names(), contexts)
        for scenario in scenarios:
            if scenario.killed or scenario.n != info.n or scenario.name == TIMELINE.name:
                print(f"  skipping {scenario.name}: needs its own cluster")
                continue
            named = Scenario(f"{scenario.name}-existing", len(addresses), info.n,
                             scenario.r, scenario.w)
            save_rows(os.path.join(args.out, "bench.csv"), named.name,
                      [r.row() for r in await measure_scenario(named, targets, contexts, args)])
    finally:
        for channel in channels:
            await channel.close()


# ---------- files ----------

def read_csv(path: str) -> list[dict[str, str]]:
    """All rows of a CSV file; [] if it does not exist."""
    if not os.path.exists(path):
        return []
    with open(path, newline="") as file:
        return list(csv.DictReader(file))


def write_csv(path: str, rows: list[dict], fields: list[str]) -> None:
    """Write rows to a CSV file (temp file, then rename)."""
    tmp = path + ".tmp"
    with open(tmp, "w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)
    os.replace(tmp, path)


def save_rows(path: str, scenario_name: str, rows: list[dict]) -> None:
    """Replace one scenario's rows in bench.csv, keep every other scenario's rows."""
    kept = [row for row in read_csv(path) if row["scenario"] != scenario_name]
    write_csv(path, kept + rows, CSV_FIELDS)


def write_machine(path: str) -> None:
    """What the numbers were measured on."""
    cpu = platform.processor() or "?"
    try:
        with open("/proc/cpuinfo") as file:
            cpu = next(line.split(":", 1)[1].strip() for line in file if line.startswith("model name"))
    except (OSError, StopIteration):
        pass
    try:
        ram_gb = os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES") / 2**30
    except (ValueError, OSError):
        ram_gb = 0.0
    with open(path, "w") as file:
        file.write(f"cpu: {cpu}\ncores: {os.cpu_count()}\nram: {ram_gb:.1f} GiB\n"
                   f"os: {platform.platform()}\npython: {platform.python_version()}\n"
                   f"grpcio: {grpc.__version__}\n"
                   "setup: load generator and all nodes on this one machine (local processes)\n")


# ---------- graphs ----------

def median_runs(rows: list[dict[str, str]]) -> dict[str, dict]:
    """scenario -> the run with the median ops/s, plus min/max ops/s over its runs."""
    by_name: dict[str, list[dict[str, str]]] = {}
    for row in rows:
        by_name.setdefault(row["scenario"], []).append(row)
    result = {}
    for name, runs in by_name.items():
        runs = sorted(runs, key=lambda row: float(row["ops_per_s"]))
        middle = dict(runs[(len(runs) - 1) // 2])
        middle["ops_min"] = float(runs[0]["ops_per_s"])
        middle["ops_max"] = float(runs[-1]["ops_per_s"])
        result[name] = middle
    return result


def plot_bars(plt, rows: dict[str, dict], names: list[tuple[str, str]], title: str,
              path: str) -> None:
    """Two panels: ops/s (median run, min-max error bar) and p50/p99 latency, one group per name."""
    present = [(name, label) for name, label in names if name in rows]
    if not present:
        print(f"  no data for {path}")
        return
    labels = [label for _, label in present]
    picked = [rows[name] for name, _ in present]
    ops = [float(r["ops_per_s"]) for r in picked]
    low = [o - r["ops_min"] for o, r in zip(ops, picked)]
    high = [r["ops_max"] - o for o, r in zip(ops, picked)]
    xs = range(len(present))
    fig, (left, right) = plt.subplots(1, 2, figsize=(11, 4))
    left.bar(xs, ops, yerr=[low, high], capsize=4, color="#4C72B0")
    left.set_xticks(list(xs), labels)
    left.set_ylabel("successful ops/s")
    left.set_title("Throughput (median of runs, bar = min-max)")
    width = 0.38
    right.bar([x - width / 2 for x in xs], [float(r["p50_ms"]) for r in picked], width,
              label="p50", color="#55A868")
    right.bar([x + width / 2 for x in xs], [float(r["p99_ms"]) for r in picked], width,
              label="p99", color="#C44E52")
    right.set_xticks(list(xs), labels)
    right.set_ylabel("latency (ms)")
    right.set_title("Latency")
    right.legend()
    fig.suptitle(title)
    fig.tight_layout()
    fig.savefig(path, dpi=120)
    plt.close(fig)
    print(f"  wrote {path}")


def plot_timeline(plt, rows: list[dict[str, str]], path: str) -> None:
    """ops/s and p99 per second, with the kill and restart marked."""
    if not rows:
        print(f"  no data for {path}")
        return
    seconds = [int(r["second"]) for r in rows]
    fig, ops_axis = plt.subplots(figsize=(11, 4))
    ops_axis.plot(seconds, [int(r["ops"]) for r in rows], color="#4C72B0", label="successful ops/s")
    ops_axis.bar(seconds, [int(r["errors"]) for r in rows], color="#C44E52", label="errors/s")
    ops_axis.set_xlabel("second")
    ops_axis.set_ylabel("per second")
    p99_axis = ops_axis.twinx()
    p99_axis.plot(seconds, [float(r["p99_ms"]) for r in rows], color="#DD8452", linestyle="--",
                  label="p99 (ms)")
    p99_axis.set_ylabel("p99 latency (ms)")
    for r in rows:
        if r["event"]:
            ops_axis.axvline(int(r["second"]), color="grey", linestyle=":")
            ops_axis.annotate(r["event"], (int(r["second"]), 0), xytext=(3, 5),
                              textcoords="offset points", rotation=90, fontsize=8)
    lines = ops_axis.get_legend_handles_labels()
    more = p99_axis.get_legend_handles_labels()
    ops_axis.legend(lines[0] + more[0], lines[1] + more[1], loc="lower left")
    ops_axis.set_title("N=3 R=2 W=2, 5 nodes: one node killed, then started again")
    fig.tight_layout()
    fig.savefig(path, dpi=120)
    plt.close(fig)
    print(f"  wrote {path}")


def plot_all(out: str) -> None:
    """Draw every graph from the CSVs in ``out`` and print a summary table."""
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        raise SystemExit("plot needs matplotlib: pip install matplotlib")
    rows = median_runs(read_csv(os.path.join(out, "bench.csv")))
    plot_bars(plt, rows, [("nrw-3-1-1", "(3,1,1)"), ("nrw-3-2-2", "(3,2,2)"),
                          ("nrw-3-3-3", "(3,3,3)"), ("nrw-3-1-3", "(3,1,3)")],
              "5 nodes: varying (N,R,W)", os.path.join(out, "nrw.png"))
    plot_bars(plt, rows, [("size-1", "1 node\n(1,1,1)"), ("size-3", "3 nodes\n(3,2,2)"),
                          ("nrw-3-2-2", "5 nodes\n(3,2,2)")],
              "Cluster size", os.path.join(out, "cluster_size.png"))
    plot_bars(plt, rows, [("nrw-3-2-2", "5 nodes, all up"), ("killed-1", "1 node killed")],
              "(3,2,2) with one node killed (after it is DEAD)", os.path.join(out, "killed.png"))
    plot_timeline(plt, read_csv(os.path.join(out, "timeline.csv")),
                  os.path.join(out, "timeline.png"))
    print(f"\n{'scenario':<22}{'ops/s':>8}{'p50 ms':>9}{'p99 ms':>9}{'errors':>8}{'cpu':>6}")
    for name, r in rows.items():
        print(f"{name:<22}{float(r['ops_per_s']):>8.0f}{float(r['p50_ms']):>9.1f}"
              f"{float(r['p99_ms']):>9.1f}{r['errors']:>8}{r['client_cpu']:>6}")


# ---------- main ----------

def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse the command line."""
    parser = argparse.ArgumentParser(prog="python -m tools.bench", description=__doc__.split("\n")[0])
    commands = parser.add_subparsers(dest="command", required=True)
    run = commands.add_parser("run", help="run the benchmarks, write CSVs")
    names = [s.name for s in SCENARIOS] + [TIMELINE.name]
    run.add_argument("--only", nargs="+", choices=names, help="run only these scenarios")
    run.add_argument("--duration", type=float, default=60, help="measured seconds per run")
    run.add_argument("--warmup", type=float, default=10, help="unmeasured seconds before each run")
    run.add_argument("--reps", type=int, default=3, help="runs per scenario")
    run.add_argument("--existing", help="comma-separated host:port list of a running cluster")
    run.add_argument("--out", default="results")
    run.add_argument("--work-dir", default=os.path.expanduser("~/.cache/dynamo-bench"),
                     help="node data and logs go here (a real disk: on tmpfs fsync costs nothing)")
    plot = commands.add_parser("plot", help="draw graphs from the CSVs")
    plot.add_argument("--out", default="results")
    return parser.parse_args(argv)


def selected(only: list[str] | None) -> list[Scenario]:
    """The scenarios to run, in SCENARIOS order, timeline last."""
    every = SCENARIOS + [TIMELINE]
    return [s for s in every if only is None or s.name in only]


def group_by_shape(scenarios: list[Scenario]) -> dict[tuple[int, int], list[Scenario]]:
    """(nodes, n) -> scenarios, so each cluster shape is started once.

    The timeline gets its own 5-node cluster (key ("timeline", ...)) so a killed node from
    ``killed-1`` does not carry over into it.
    """
    shapes: dict[tuple, list[Scenario]] = {}
    for s in scenarios:
        key = ("timeline", s.nodes, s.n) if s.name == TIMELINE.name else (s.nodes, s.n)
        shapes.setdefault(key, []).append(s)
    return shapes


async def run_all(args: argparse.Namespace) -> None:
    """The ``run`` command."""
    os.makedirs(args.out, exist_ok=True)
    write_machine(os.path.join(args.out, "machine.txt"))
    scenarios = selected(args.only)
    if args.existing:
        await run_existing(args.existing.split(","), scenarios, args)
        return
    os.makedirs(args.work_dir, exist_ok=True)
    work_root = tempfile.mkdtemp(prefix="bench-", dir=args.work_dir)
    try:
        for group in group_by_shape(scenarios).values():
            await run_shape(group[0].nodes, group[0].n, group, args, work_root)
    finally:
        shutil.rmtree(work_root, ignore_errors=True)


def main(argv: list[str] | None = None) -> None:
    """Entry point."""
    args = parse_args(argv)
    sys.stdout.reconfigure(line_buffering=True)    # progress lines show up at once in a log file
    if args.command == "plot":
        plot_all(args.out)
    else:
        asyncio.run(run_all(args))


if __name__ == "__main__":
    main()
