"""Chaos run: clients write and read non-stop while one node at a time is crashed, stopped or
frozen. At the end, prove that no write the cluster acknowledged was lost and that all replicas
agree.

    python -m tools.chaos --seconds 300 [--seed 81723] [--target local|docker]

Workload (8 clients, each request to a random node that is up):
- write-once keys ``once-<client>-<seq>``: written once, empty context;
- overwrite keys ``ow-<client>-<j>`` (25 per client, no other client touches them):
  Get -> remember the values seen -> Put a new value with that context;
- some plain Gets.
Every Put is logged as ``{key, value, outcome, seen}``; outcome is ok, failed (QUORUM_FAILED) or
unknown (RPC error or timeout: it may or may not be stored).

No-loss check, per key, newest write first: a value is *accounted for* if it is in the final read,
or a later write of the same key (any outcome) saw it and is itself accounted for. LOST = an
acknowledged value that is not accounted for. The report goes to ``results/``.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import random
import sys
import time
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass, field
from datetime import datetime

import grpc

from dynamo import dynamo_pb2
from dynamo.version import fingerprint
from dynamo.wire import version_from_proto
from tools.cluster_control import CALL_TIMEOUT_S, REPO_ROOT, ClusterError, RealCluster

logger = logging.getLogger(__name__)

CLIENTS = 8
OWN_KEYS = 25                  # overwrite keys per client
VALUE_SIZE = 100               # bytes (C5: small values)
PAUSE_S = 0.05                 # between two requests of one client
SETTLE_TIMEOUT_S = 120
FAULTS = ["crash", "stop", "pause"]
FAULT_WEIGHTS = [60, 25, 15]


@dataclass
class Write:
    """One Put the load made."""

    seq: int                    # global order of the Puts
    key: str
    value: str
    outcome: str                # ok | failed | unknown
    seen: list[str]             # values the writer had read for this key (overwrite keys)


@dataclass
class Fault:
    """One fault the chaos loop applied."""

    at_s: float                 # seconds since the start
    node: str
    kind: str                   # crash | stop | pause
    down_s: float
    recovered_s: float = -1.0   # seconds from restore until ALIVE everywhere (-1: never)


@dataclass
class Load:
    """What the clients did."""

    writes: list[Write] = field(default_factory=list)
    reads: Counter = field(default_factory=Counter)        # ok | not_found | failed | unknown
    errors: Counter = field(default_factory=Counter)       # gRPC code names of failed calls


def make_value(client: int, seq: int) -> str:
    return f"{client}:{seq}:".ljust(VALUE_SIZE, "x")


# ---------------------------------------------------------------- load

class Client:
    """One load client. Owns its overwrite keys, so only it ever writes them."""

    def __init__(self, number: int, cluster: RealCluster, load: Load, rng: random.Random,
                 counter: list[int]) -> None:
        self.number = number
        self.cluster = cluster
        self.load = load
        self.rng = rng
        self.counter = counter                         # shared [next seq] across clients
        self.own_keys = [f"ow-{number}-{j}" for j in range(OWN_KEYS)]
        self.once_keys: list[str] = []

    async def run(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            roll = self.rng.random()
            if roll < 0.4:
                await self.write_once()
            elif roll < 0.8:
                await self.overwrite()
            else:
                await self.plain_get()
            await asyncio.sleep(PAUSE_S)

    def _stub(self):
        return self.cluster.client(self.rng.choice(self.cluster.up()))

    def _next_seq(self) -> int:
        self.counter[0] += 1
        return self.counter[0]

    async def write_once(self) -> None:
        seq = self._next_seq()
        key = f"once-{self.number}-{seq}"
        self.once_keys.append(key)
        await self._put(seq, key, make_value(self.number, seq), b"", seen=[])

    async def overwrite(self) -> None:
        key = self.rng.choice(self.own_keys)
        reply = await self._get(key)
        if reply is None or reply.status not in (dynamo_pb2.OK, dynamo_pb2.NOT_FOUND):
            return                                     # no context to write with: skip this turn
        seen = [v.decode() for v in reply.values]
        seq = self._next_seq()
        await self._put(seq, key, make_value(self.number, seq), reply.context, seen)

    async def plain_get(self) -> None:
        keys = self.own_keys + self.once_keys[-50:]
        await self._get(self.rng.choice(keys))

    async def _put(self, seq: int, key: str, value: str, context: bytes, seen: list[str]) -> None:
        try:
            reply = await self._stub().Put(
                dynamo_pb2.PutRequest(key=key, value=value.encode(), context=context),
                timeout=CALL_TIMEOUT_S)
            outcome = "ok" if reply.status == dynamo_pb2.OK else "failed"
        except grpc.aio.AioRpcError as error:
            self.load.errors[error.code().name] += 1
            outcome = "unknown"
        self.load.writes.append(Write(seq, key, value, outcome, seen))

    async def _get(self, key: str) -> dynamo_pb2.GetResponse | None:
        try:
            reply = await self._stub().Get(dynamo_pb2.GetRequest(key=key), timeout=CALL_TIMEOUT_S)
        except grpc.aio.AioRpcError as error:
            self.load.errors[error.code().name] += 1
            self.load.reads["unknown"] += 1
            return None
        name = {dynamo_pb2.OK: "ok", dynamo_pb2.NOT_FOUND: "not_found"}.get(reply.status, "failed")
        self.load.reads[name] += 1
        return reply


# ---------------------------------------------------------------- chaos loop

async def apply_fault(cluster: RealCluster, node: str, kind: str) -> None:
    if kind == "crash":
        await cluster.kill(node)
    elif kind == "stop":
        await cluster.stop(node)
    else:
        await cluster.pause(node)


async def restore(cluster: RealCluster, node: str, kind: str) -> None:
    if kind == "pause":
        await cluster.unpause(node)
    else:
        await cluster.start(node)


async def chaos_loop(cluster: RealCluster, seconds: float, rng: random.Random,
                     faults: list[Fault], start: float) -> tuple[bool, int]:
    """Apply one fault at a time until ``seconds`` have passed (C2: next fault only after the
    cluster is ALIVE everywhere again + 5 s). Returns (aborted, max nodes down at once)."""
    max_down = 0
    while time.monotonic() - start < seconds:
        node = rng.choice(cluster.running())
        kind = rng.choices(FAULTS, weights=FAULT_WEIGHTS)[0]
        down_s = rng.uniform(3, 8) if kind == "pause" else rng.uniform(5, 20)
        fault = Fault(round(time.monotonic() - start, 1), node, kind, round(down_s, 1))
        faults.append(fault)
        logger.info("fault %d: %s %s for %.1f s", len(faults), kind, node, down_s)
        try:
            await apply_fault(cluster, node, kind)
            max_down = max(max_down, len(cluster.down | cluster.paused))
            await asyncio.sleep(down_s)
            await restore(cluster, node, kind)
            fault.recovered_s = round(await cluster.wait_all_alive(timeout=60), 1)
        except ClusterError as error:
            logger.error("ABORTED: %s", error)
            return True, max_down
        await asyncio.sleep(5)
    return False, max_down


# ---------------------------------------------------------------- final checks

async def converge(cluster: RealCluster, keys: list[str], timeout: float) -> tuple[bool, float, int]:
    """Poll until every owner of every key stores the same set of versions (LocalGet).
    Returns (converged, seconds taken, keys still different)."""
    pending = list(keys)
    start = time.monotonic()
    limit = asyncio.Semaphore(32)

    async def same_everywhere(key: str) -> bool:
        async with limit:
            prints = set()
            for node in cluster.owners_local(key):
                try:
                    reply = await cluster.client(node).LocalGet(
                        dynamo_pb2.LocalGetRequest(key=key), timeout=CALL_TIMEOUT_S)
                except grpc.aio.AioRpcError:
                    return False
                prints.add(fingerprint([version_from_proto(v) for v in reply.versions]))
            return len(prints) == 1

    while True:
        results = await asyncio.gather(*(same_everywhere(key) for key in pending))
        pending = [key for key, same in zip(pending, results) if not same]
        took = time.monotonic() - start
        if not pending:
            return True, took, 0
        if took >= timeout:
            return False, took, len(pending)
        logger.info("converging: %d keys still differ", len(pending))
        await asyncio.sleep(2)


async def final_values(cluster: RealCluster, keys: list[str]) -> tuple[dict[str, set[str]], int]:
    """Get(key, r=3) for every key: all 3 owners must answer. Returns (values per key, keys that
    could not be read)."""
    limit = asyncio.Semaphore(32)
    values: dict[str, set[str]] = {}
    unreadable = 0

    async def read(key: str) -> None:
        nonlocal unreadable
        async with limit:
            for _ in range(3):
                try:
                    reply = await cluster.client(random.choice(cluster.running())).Get(
                        dynamo_pb2.GetRequest(key=key, r=3), timeout=CALL_TIMEOUT_S)
                except grpc.aio.AioRpcError:
                    continue
                if reply.status in (dynamo_pb2.OK, dynamo_pb2.NOT_FOUND):
                    values[key] = {v.decode() for v in reply.values}
                    return
            unreadable += 1

    await asyncio.gather(*(read(key) for key in keys))
    return values, unreadable


def check_no_loss(writes: list[Write], final: dict[str, set[str]]) -> dict:
    """The no-loss proof (D10 §6, chain rule). Returns the counts and the lost writes."""
    by_key: dict[str, list[Write]] = defaultdict(list)
    for write in writes:
        by_key[write.key].append(write)
    present = superseded = 0
    lost: list[dict] = []
    last_acked_ok = True
    invented: list[dict] = []
    for key, key_writes in by_key.items():
        key_writes.sort(key=lambda w: w.seq)
        values = final.get(key, set())
        accounted: dict[int, bool] = {}
        for i in range(len(key_writes) - 1, -1, -1):          # newest first
            write = key_writes[i]
            if write.value in values:
                accounted[write.seq] = True
                if write.outcome == "ok":
                    present += 1
                continue
            replaced = any(write.value in later.seen and accounted[later.seq]
                           for later in key_writes[i + 1:])
            accounted[write.seq] = replaced
            if write.outcome == "ok":
                if replaced:
                    superseded += 1
                else:
                    lost.append(asdict(write))
        acked = [w for w in key_writes if w.outcome == "ok"]
        if acked and not accounted[acked[-1].seq]:
            last_acked_ok = False
        written = {w.value for w in key_writes}
        invented += [{"key": key, "value": v} for v in values - written]
    return {"acked": sum(1 for w in writes if w.outcome == "ok"), "present": present,
            "superseded": superseded, "lost": len(lost), "lost_writes": lost[:50],
            "last_acked_accounted": last_acked_ok, "invented": len(invented),
            "invented_values": invented[:50]}


# ---------------------------------------------------------------- the run

async def run(cluster: RealCluster, seconds: float, seed: int | None = None,
              out_dir: str = os.path.join(REPO_ROOT, "results")) -> dict:
    """One chaos run on a cluster that is already running (all ALIVE). Returns the report."""
    seed = seed if seed is not None else random.randrange(100_000)
    rng = random.Random(seed)
    logger.info("chaos: seed %d, %d s, target %s", seed, seconds, cluster.target)
    started_at = datetime.now()
    load = Load()
    counter = [0]
    clients = [Client(i, cluster, load, random.Random(seed * 100 + i), counter)
               for i in range(1, CLIENTS + 1)]
    stop = asyncio.Event()
    tasks = [asyncio.create_task(client.run(stop)) for client in clients]
    faults: list[Fault] = []
    start = time.monotonic()
    try:
        aborted, max_down = await chaos_loop(cluster, seconds, rng, faults, start)
    finally:
        stop.set()
        await asyncio.wait(tasks, timeout=10)              # in-flight calls end within 3 s each
    duration = time.monotonic() - start

    logger.info("settling: heal, wait ALIVE, wait for replicas to agree")
    try:
        await cluster.heal()
    except ClusterError as error:
        logger.error("heal failed: %s", error)
        aborted = True
    keys = sorted({write.key for write in load.writes})
    converged, converge_s, differ = await converge(cluster, keys, SETTLE_TIMEOUT_S)
    final, unreadable = await final_values(cluster, keys)
    proof = check_no_loss(load.writes, final)

    outcomes = Counter(write.outcome for write in load.writes)
    report = {
        "started": started_at.strftime("%Y-%m-%d %H:%M"), "target": cluster.target, "seed": seed,
        "seconds_asked": seconds, "duration_s": round(duration, 1), "aborted": aborted,
        "faults": [asdict(f) for f in faults], "fault_counts": dict(Counter(f.kind for f in faults)),
        "max_nodes_down": max_down,
        "writes": {"attempted": len(load.writes), "ok": outcomes["ok"],
                   "failed": outcomes["failed"], "unknown": outcomes["unknown"]},
        "reads": dict(load.reads), "errors": dict(load.errors),
        "keys": len(keys), "unreadable_keys": unreadable,
        "converged": converged, "converge_s": round(converge_s, 1), "keys_differing": differ,
        **proof,
    }
    report["passed"] = (not aborted and converged and unreadable == 0 and proof["lost"] == 0
                        and proof["last_acked_accounted"] and proof["invented"] == 0)
    write_report(report, load.writes, out_dir, started_at)
    return report


def write_report(report: dict, writes: list[Write], out_dir: str, started_at: datetime) -> None:
    """results/chaos-<date>.json, .md and .writes.jsonl (the last one is not committed)."""
    os.makedirs(out_dir, exist_ok=True)
    base = os.path.join(out_dir, f"chaos-{started_at.strftime('%Y%m%d-%H%M')}")
    with open(base + ".json", "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)
    with open(base + ".writes.jsonl", "w", encoding="utf-8") as f:
        for write in writes:
            f.write(json.dumps(asdict(write)) + "\n")
    with open(base + ".md", "w", encoding="utf-8") as f:
        f.write(render_markdown(report))
    report["files"] = [base + ".md", base + ".json"]
    logger.info("report: %s.md", base)


def render_markdown(r: dict) -> str:
    w, reads = r["writes"], r["reads"]
    counts = r["fault_counts"]
    availability = 100 * w["ok"] / w["attempted"] if w["attempted"] else 0
    reads_total = sum(reads.values())
    lines = [
        f"# Chaos run {r['started']}",
        "",
        "```",
        f"Chaos run {r['started']}   target: {r['target']}   seed: {r['seed']}   "
        f"duration: {r['duration_s']:.0f} s{'   ABORTED' if r['aborted'] else ''}",
        f"Faults: {len(r['faults'])} (crash {counts.get('crash', 0)}, stop {counts.get('stop', 0)}, "
        f"pause {counts.get('pause', 0)})   max nodes down at once: {r['max_nodes_down']}",
        f"Writes: {w['attempted']:,} attempted · {w['ok']:,} ok · {w['failed']:,} failed · "
        f"{w['unknown']:,} unknown   (availability {availability:.1f} %)",
        f"Reads:  {reads_total:,} attempted · {reads.get('ok', 0) + reads.get('not_found', 0):,} ok",
        f"Final check: {r['acked']:,} acked → {r['present']:,} present, "
        f"{r['superseded']:,} superseded, LOST {r['lost']}",
        f"Last acked write accounted for on every key: {'yes' if r['last_acked_accounted'] else 'NO'}"
        f"   Unknown values: {r['invented']}   Unreadable keys: {r['unreadable_keys']}",
        f"Converged: {'yes' if r['converged'] else 'NO'}, {r['converge_s']} s after the last fault "
        f"(all owners identical for {r['keys'] - r['keys_differing']:,} of {r['keys']:,} keys)",
        f"Result: {'PASSED' if r['passed'] else 'FAILED'}",
        "```",
        "",
        "Failed calls by gRPC code: " + (", ".join(f"{k} {v}" for k, v in sorted(r["errors"].items()))
                                         or "none"),
        "",
        "## Faults",
        "",
        "| # | at (s) | node | type | down (s) | ALIVE everywhere after (s) |",
        "|---|---|---|---|---|---|",
    ]
    lines += [f"| {i} | {f['at_s']} | {f['node']} | {f['kind']} | {f['down_s']} | "
              f"{f['recovered_s'] if f['recovered_s'] >= 0 else 'never'} |"
              for i, f in enumerate(r["faults"], 1)]
    if r["lost_writes"]:
        lines += ["", "## Lost writes (first 50)", ""]
        lines += [f"- `{x['key']}` = `{x['value'][:20]}…` (seq {x['seq']})" for x in r["lost_writes"]]
    return "\n".join(lines) + "\n"


async def main_async(args: argparse.Namespace) -> int:
    cluster = RealCluster(args.target)
    try:
        await cluster.ensure("normal", wipe=True)   # empty data; refuses a cluster we did not start
        report = await run(cluster, args.seconds, args.seed)
    finally:
        await cluster.shutdown()
    print(render_markdown(report))
    return 0 if report["passed"] else 1


def main() -> None:
    parser = argparse.ArgumentParser(prog="python -m tools.chaos", description=__doc__.split("\n")[0])
    parser.add_argument("--seconds", type=float, default=300, help="how long to inject faults")
    parser.add_argument("--seed", type=int, default=None, help="random seed (printed in the report)")
    parser.add_argument("--target", default=os.environ.get("CLUSTER_TARGET", "local"),
                        choices=["local", "docker"], help="local processes or docker compose")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                        datefmt="%H:%M:%S")
    sys.exit(asyncio.run(main_async(args)))


if __name__ == "__main__":
    main()
