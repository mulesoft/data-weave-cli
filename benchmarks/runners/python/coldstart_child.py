"""Fresh-process worker for one case. Prints a "READY" marker the instant the
runtime is initialized, then a JSON line with the in-process first-run timing.
The PARENT (coldstart.py) measures cold-start as wall-clock from spawn to the
READY marker, so process launch + interpreter start + dlopen(dwlib) + isolate
creation are all included — not just the in-process initialize() call."""

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

    dw.initialize()
    # Runtime is ready: flush the marker now so the parent's clock stops here.
    # flush() guarantees the byte is on the pipe before the timed run begins.
    sys.stdout.write("READY\n")
    sys.stdout.flush()

    run_start = time.perf_counter_ns()
    result = dw.run(script, inputs)
    first_run_ms = (time.perf_counter_ns() - run_start) / 1e6
    if not result.success:
        raise SystemExit(f"first run failed: {result.error}")

    dw.cleanup()
    sys.stdout.write(json.dumps({"firstRunMs": first_run_ms}) + "\n")


if __name__ == "__main__":
    main(sys.argv)
