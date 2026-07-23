"""Spawn orchestrator: N fresh processes per case, aggregate cold-start /
first-run rows. Mirrors runners/node/coldstart.mjs."""

import json
import subprocess
import sys
from pathlib import Path

from manifest import cases_for_metric
from stats import compute_stats

_CHILD = str(Path(__file__).resolve().parent / "coldstart_child.py")


def _spawn_sample(corpus_dir, case_id):
    """Spawn one fresh process and parse its single JSON line."""
    out = subprocess.check_output([sys.executable, _CHILD, str(corpus_dir), case_id], text=True)
    line = out.strip().split("\n")[-1]
    obj = json.loads(line)
    return obj["initMs"], obj["firstRunMs"]


def run_cold_start_and_first_run(manifest, sample_fn=_spawn_sample, samples_override=None):
    # Unique case ids that declare cold-start and/or first-run, preserving order.
    ids = []
    for c in cases_for_metric(manifest, "cold-start") + cases_for_metric(manifest, "first-run"):
        if c["id"] not in ids:
            ids.append(c["id"])

    rows = []
    for cid in ids:
        c = next(x for x in manifest["cases"] if x["id"] == cid)
        n = samples_override if samples_override is not None \
            else c.get("iterations", {}).get("samples", 20)
        inits, firsts = [], []
        for _ in range(n):
            init_ms, first_ms = sample_fn(manifest["corpusDir"], cid)
            inits.append(init_ms)
            firsts.append(first_ms)
        if "cold-start" in c["metrics"]:
            rows.append({"id": cid, "metric": "cold-start", "unit": "ms",
                         "stats": compute_stats(inits), "iterations": n})
        if "first-run" in c["metrics"]:
            rows.append({"id": cid, "metric": "first-run", "unit": "ms",
                         "stats": compute_stats(firsts), "iterations": n})
    return rows
