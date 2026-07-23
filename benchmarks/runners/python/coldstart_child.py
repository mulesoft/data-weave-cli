"""Fresh-process worker. Measures a cold engine init + a cold (first) compile+exec
for one case, then prints a single JSON line. Spawned by coldstart.py — the honest
cold path (process launch + interpreter start + dlopen(dwlib) + isolate creation +
first compile+exec)."""

import json
import sys
import time

from manifest import load_manifest, read_script, resolve_inputs
from wrapper import load_wrapper


def _to_inputs(resolved):
    inputs = {}
    for name, v in resolved.items():
        inputs[name] = {
            "content": v["bytes"],
            "mimeType": v["mimeType"],
            "charset": v["charset"] or "utf-8",
        }
    return inputs


def main(argv):
    corpus_dir, case_id = argv[1], argv[2]
    manifest = load_manifest(corpus_dir)
    c = next((x for x in manifest["cases"] if x["id"] == case_id), None)
    if c is None:
        raise SystemExit(f"unknown case: {case_id}")
    script = read_script(manifest, c)
    inputs = _to_inputs(resolve_inputs(manifest, c))

    api = load_wrapper()
    dw = api.DataWeave()

    init_start = time.perf_counter_ns()
    dw.initialize()
    init_ms = (time.perf_counter_ns() - init_start) / 1e6

    run_start = time.perf_counter_ns()
    result = dw.run(script, inputs)
    first_run_ms = (time.perf_counter_ns() - run_start) / 1e6
    if not result.success:
        raise SystemExit(f"first run failed: {result.error}")

    dw.cleanup()
    sys.stdout.write(json.dumps({"initMs": init_ms, "firstRunMs": first_run_ms}) + "\n")


if __name__ == "__main__":
    main(sys.argv)
