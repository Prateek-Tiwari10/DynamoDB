"""Real-cluster test 8 (D10 §5–§8): the chaos run. CHAOS_SECONDS (default 300) of faults under
load, then the no-loss proof and the convergence check. The report goes to results/."""

import os

from tools import chaos


async def test_8_chaos_no_acked_write_lost(cluster):
    seconds = float(os.environ.get("CHAOS_SECONDS", "300"))
    report = await chaos.run(cluster, seconds)
    print("\n" + chaos.render_markdown(report))
    assert not report["aborted"]
    assert report["lost"] == 0, report["lost_writes"]
    assert report["last_acked_accounted"]
    assert report["invented"] == 0
    assert report["unreadable_keys"] == 0
    assert report["converged"]
    assert report["passed"]
