"""Collector/main: run cold-start/first-run (fresh processes) + warm/streaming
(in-process), validate ids, stamp env, write results/python-<ts>.json. Emit-only —
does NOT render the report (root :benchmarkCompare renders once over all runners)."""

import json
from datetime import datetime, timezone
from pathlib import Path

from coldstart import run_cold_start_and_first_run
from env import gather_env
from manifest import load_manifest, validate_result_ids
from warm_bench import run_warm_and_streaming
from wrapper import load_wrapper

# benchmarks/runners/python -> benchmarks
_BENCH_DIR = Path(__file__).resolve().parents[2]
CORPUS = _BENCH_DIR / "corpus"
RESULTS_DIR = _BENCH_DIR / "results"


def build_result(env, cases):
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    return {
        "schemaVersion": "1.0",
        "runner": env["runner"],
        "env": env,
        "timestamp": ts,
        "cases": cases,
    }


def main():
    manifest = load_manifest(CORPUS)

    # Cold-start / first-run first (fresh processes), then warm/streaming in-process.
    cold_rows = run_cold_start_and_first_run(manifest)

    api = load_wrapper()
    dw = api.DataWeave()
    dw.initialize()
    try:
        warm_rows = run_warm_and_streaming(dw, manifest)
    finally:
        dw.cleanup()

    cases = cold_rows + warm_rows
    validate_result_ids(manifest, [c["id"] for c in cases])  # fail-fast on orphan ids

    env = gather_env()
    result = build_result(env, cases)

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    stamp = result["timestamp"].replace(":", "-").replace(".", "-")
    out = RESULTS_DIR / f"python-{stamp}.json"
    out.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(f"wrote {out} ({len(cases)} rows)")
    return out


if __name__ == "__main__":
    main()
