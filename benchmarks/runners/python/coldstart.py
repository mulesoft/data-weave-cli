"""Spawn orchestrator: N fresh processes per case, aggregate cold-start /
first-run rows. Mirrors runners/node/coldstart.mjs."""

import json
import subprocess
import sys
import time
from pathlib import Path

from manifest import cases_for_metric
from stats import compute_stats

_CHILD = str(Path(__file__).resolve().parent / "coldstart_child.py")


def _spawn_sample(corpus_dir, case_id):
    """Spawn one fresh process and measure true cold-start: wall-clock from just
    before spawn to the child's "READY" marker (process launch + interpreter
    start + dlopen(dwlib) + isolate creation). first-run is timed in-process by
    the child. Raises on a non-zero exit or a missing READY/JSON line so a failed
    sample never records a bogus timing. Returns (coldStartMs, firstRunMs)."""
    t0 = time.perf_counter_ns()
    p = subprocess.Popen(
        [sys.executable, _CHILD, str(corpus_dir), case_id],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )
    cold_start_ms = None
    result_line = None
    for line in p.stdout:
        line = line.rstrip("\n")
        if line == "READY":
            # Stamp the moment the runtime reported ready; everything before this
            # (launch, dlopen, init) is the cold-start cost.
            cold_start_ms = (time.perf_counter_ns() - t0) / 1e6
        elif line:
            result_line = line
    stderr = p.stderr.read()
    code = p.wait()
    if code != 0:
        raise RuntimeError(f"coldstart child failed for '{case_id}' (exit {code})\n{stderr}")
    if cold_start_ms is None:
        raise RuntimeError(f"coldstart child for '{case_id}' never printed READY\n{stderr}")
    if not result_line:
        raise RuntimeError(f"coldstart child for '{case_id}' printed no result line\n{stderr}")
    return cold_start_ms, json.loads(result_line)["firstRunMs"]


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
        colds, firsts = [], []
        for _ in range(n):
            cold_ms, first_ms = sample_fn(manifest["corpusDir"], cid)
            colds.append(cold_ms)
            firsts.append(first_ms)
        if "cold-start" in c["metrics"]:
            rows.append({"id": cid, "metric": "cold-start", "unit": "ms",
                         "stats": compute_stats(colds), "iterations": n})
        if "first-run" in c["metrics"]:
            rows.append({"id": cid, "metric": "first-run", "unit": "ms",
                         "stats": compute_stats(firsts), "iterations": n})
    return rows
