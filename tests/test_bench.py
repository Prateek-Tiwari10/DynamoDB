"""Tests for the benchmark tool: the pure helpers, the load loop on a fake stub, and one short
run against the in-process test cluster."""

import random
import time

import grpc

from dynamo import dynamo_pb2
from dynamo.config import load_config
from tools import bench


# ---------- helpers ----------

def test_percentile_nearest_rank():
    data = [float(i) for i in range(1, 101)]          # 1 .. 100
    assert bench.percentile(data, 50) == 50.0
    assert bench.percentile(data, 99) == 99.0
    assert bench.percentile(data, 100) == 100.0


def test_percentile_small_lists():
    assert bench.percentile([], 99) == 0.0
    assert bench.percentile([7.0], 50) == 7.0
    assert bench.percentile([7.0], 99) == 7.0
    assert bench.percentile([1.0, 2.0], 50) == 1.0


def test_make_value_has_the_size_and_changes():
    assert len(bench.make_value(1)) == bench.VALUE_SIZE
    assert len(bench.make_value(123456, size=10)) == 10
    assert bench.make_value(1) != bench.make_value(2)


def test_key_names():
    keys = bench.key_names()
    assert len(keys) == bench.KEY_COUNT == len(set(keys))
    assert keys[0] == "bench:00000"


def test_write_config_loads(tmp_path):
    path = tmp_path / "c.yaml"
    bench.write_config(str(path), nodes=3, n=3, base_port=50071)
    config = load_config(str(path))
    assert config.nodes == {"node1": "localhost:50071", "node2": "localhost:50072",
                            "node3": "localhost:50073"}
    assert (config.n, config.r, config.w) == (3, 2, 2)
    assert config.dead_after_ms == load_config(bench.BASE_CONFIG).dead_after_ms


def test_write_config_single_node(tmp_path):
    path = tmp_path / "c.yaml"
    bench.write_config(str(path), nodes=1, n=1)
    config = load_config(str(path))
    assert (config.n, config.r, config.w) == (1, 1, 1)


def test_recorder_ignores_warmup():
    recorder = bench.Recorder(measure_from=100.0)
    recorder.record(True, 5.0, now=99.0)
    recorder.record(True, 6.0, now=100.5)
    recorder.record(False, 7.0, now=101.2)
    assert (recorder.ok, recorder.errors) == (1, 1)
    assert recorder.latencies_ms == [6.0, 7.0]
    assert recorder.per_second == {0: [(True, 6.0)], 1: [(False, 7.0)]}


def test_summarize_and_row():
    recorder = bench.Recorder(measure_from=0.0)
    for ms in (1.0, 2.0, 3.0, 4.0):
        recorder.record(True, ms, now=1.0)
    scenario = bench.Scenario("nrw-3-2-2", nodes=5, n=3, r=2, w=2)
    result = bench.summarize(scenario, 1, recorder, seconds=2.0, cpu=0.5)
    assert result.ops_per_s == 2.0
    assert (result.p50_ms, result.p99_ms) == (2.0, 4.0)
    row = result.row()
    assert list(row) == bench.CSV_FIELDS
    assert row["scenario"] == "nrw-3-2-2" and row["errors"] == 0


def test_save_rows_replaces_only_that_scenario(tmp_path):
    path = str(tmp_path / "bench.csv")
    row = {field: 0 for field in bench.CSV_FIELDS}
    bench.save_rows(path, "a", [{**row, "scenario": "a", "rep": 1}])
    bench.save_rows(path, "b", [{**row, "scenario": "b", "rep": 1}])
    bench.save_rows(path, "a", [{**row, "scenario": "a", "rep": 9}])
    rows = bench.read_csv(path)
    assert sorted((r["scenario"], r["rep"]) for r in rows) == [("a", "9"), ("b", "1")]


def test_median_runs_picks_the_middle_run():
    rows = [{"scenario": "x", "ops_per_s": str(v), "rep": str(i)}
            for i, v in enumerate([300, 100, 200])]
    picked = bench.median_runs(rows)["x"]
    assert picked["ops_per_s"] == "200"
    assert (picked["ops_min"], picked["ops_max"]) == (100.0, 300.0)


def test_group_by_shape_starts_each_shape_once_and_timeline_alone():
    groups = bench.group_by_shape(bench.selected(None))
    assert [s.name for s in groups[(5, 3)]] == ["nrw-3-1-1", "nrw-3-2-2", "nrw-3-3-3",
                                                 "nrw-3-1-3", "killed-1"]
    assert [s.name for s in groups[("timeline", 5, 3)]] == ["timeline"]
    assert len(groups) == 4                          # 5 nodes, timeline, 3 nodes, 1 node


def test_selected_keeps_order():
    assert [s.name for s in bench.selected(["size-1", "nrw-3-1-1"])] == ["nrw-3-1-1", "size-1"]


# ---------- load loop on a fake stub ----------

class FakeStub:
    """Answers every Get/Put at once; can be told to fail Puts."""

    def __init__(self, fail_puts: bool = False) -> None:
        self.fail_puts = fail_puts
        self.gets: list[dynamo_pb2.GetRequest] = []
        self.puts: list[dynamo_pb2.PutRequest] = []

    async def Get(self, request, timeout=None):
        self.gets.append(request)
        return dynamo_pb2.GetResponse(status=dynamo_pb2.OK, values=[b"v"], context=b"ctx-get")

    async def Put(self, request, timeout=None):
        self.puts.append(request)
        if self.fail_puts:
            raise grpc.aio.AioRpcError(grpc.StatusCode.UNAVAILABLE, None, None, "down")
        return dynamo_pb2.PutResponse(status=dynamo_pb2.OK, context=b"ctx-put")


async def test_worker_sends_r_and_w_and_keeps_contexts():
    stub = FakeStub()
    contexts: dict[str, bytes] = {}
    recorder = bench.Recorder(measure_from=0.0)
    await bench.worker([stub], ["k1"], contexts, 2, 3, time.monotonic() + 0.05,
                       recorder.record, random.Random(1))
    assert stub.gets and stub.puts
    assert all(g.r == 2 for g in stub.gets) and all(p.w == 3 for p in stub.puts)
    assert all(len(p.value) == bench.VALUE_SIZE for p in stub.puts)
    assert contexts["k1"] in (b"ctx-get", b"ctx-put")
    # every put after the first request carries the context of the reply before it
    assert any(p.context in (b"ctx-get", b"ctx-put") for p in stub.puts[1:])
    assert recorder.errors == 0 and recorder.ok == len(stub.gets) + len(stub.puts)


async def test_worker_counts_failures_as_errors():
    stub = FakeStub(fail_puts=True)
    recorder = bench.Recorder(measure_from=0.0)
    await bench.worker([stub], ["k1"], {}, 1, 1, time.monotonic() + 0.05,
                       recorder.record, random.Random(2))
    assert recorder.errors == len(stub.puts) > 0
    assert recorder.ok == len(stub.gets)


async def test_run_load_measures_only_after_warmup():
    stub = FakeStub()
    recorder, cpu = await bench.run_load([stub], ["k1", "k2"], {}, 1, 1, warmup_s=0.1,
                                         duration_s=0.1, clients=2, seed=3)
    total = len(stub.gets) + len(stub.puts)
    assert 0 < recorder.ok < total                    # warm-up requests were not counted
    assert cpu >= 0.0


async def test_forget_channel_keeps_the_old_channel_open_until_stop(tmp_path):
    cluster = bench.LocalCluster(1, 1, str(tmp_path))
    cluster.stub("node1")
    old = cluster._channels["node1"]
    cluster.forget_channel("node1")
    cluster.stub("node1")
    assert cluster._channels["node1"] is not old       # a fresh channel for the restarted node
    assert cluster._retired == [old]                   # in-flight calls on the old one survive
    await cluster.stop()
    assert cluster._retired == []


# ---------- against the in-process cluster ----------

async def test_short_run_on_the_test_cluster(cluster):
    targets = [cluster.client(node_id) for node_id in cluster.config.nodes]
    keys = bench.key_names(200)
    contexts: dict[str, bytes] = {}
    await bench.preload(targets, keys, contexts)
    assert len(contexts) == len(keys)
    recorder, _ = await bench.run_load(targets, keys, contexts, 2, 2, warmup_s=0.2,
                                       duration_s=1.0, clients=4, seed=4)
    assert recorder.ok > 0
    assert recorder.errors == 0


async def test_all_see_alive_then_dead_on_the_test_cluster(cluster):
    stubs = {node_id: cluster.client(node_id) for node_id in cluster.config.nodes}
    await cluster.wait_until(lambda: bench.all_see(stubs, "node3", dynamo_pb2.ALIVE), timeout=5)
    await cluster.kill("node3")
    del stubs["node3"]
    await cluster.wait_until(lambda: bench.all_see(stubs, "node3", dynamo_pb2.DEAD), timeout=5)
