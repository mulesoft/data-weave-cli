"""In-process metrics: warm steady-state and streaming throughput. Timing mirrors
the Node runner: perf_counter_ns -> ms. NO JIT warmup floor (CPython has no JIT),
matching Node — unlike the engine's 2000-iter floor."""

import time

from manifest import cases_for_metric, read_script, resolve_inputs
from stats import compute_stats, to_mbps


def _to_inputs(resolved):
    inputs = {}
    for name, v in resolved.items():
        inputs[name] = {"content": v["bytes"], "mimeType": v["mimeType"],
                        "charset": v["charset"] or "utf-8"}
    return inputs


def _chunked(data, size=65536):
    for i in range(0, len(data), size):
        yield data[i:i + size]


def _assert_ok(result):
    if not result.success:
        raise RuntimeError(f"run failed: {result.error}")


def run_warm_and_streaming(api, manifest, warmup_cap=None, warm_cap=None, streaming_cap=None):
    rows = []

    for c in cases_for_metric(manifest, "warm"):
        script = read_script(manifest, c)
        inputs = _to_inputs(resolve_inputs(manifest, c))
        warmup = warmup_cap if warmup_cap is not None \
            else c.get("iterations", {}).get("warmup", 10)
        iters = warm_cap if warm_cap is not None \
            else c.get("iterations", {}).get("warm", 100)

        for _ in range(warmup):
            _assert_ok(api.run(script, inputs))
        samples = []
        for _ in range(iters):
            start = time.perf_counter_ns()
            _assert_ok(api.run(script, inputs))
            samples.append((time.perf_counter_ns() - start) / 1e6)
        rows.append({"id": c["id"], "metric": "warm", "unit": "ms",
                     "stats": compute_stats(samples), "iterations": iters})

    for c in cases_for_metric(manifest, "streaming"):
        script = read_script(manifest, c)
        resolved = resolve_inputs(manifest, c)
        primary_name, primary = next(iter(resolved.items()))
        iters = streaming_cap if streaming_cap is not None \
            else c.get("iterations", {}).get("streaming", 10)

        mbps = []
        for _ in range(iters):
            start = time.perf_counter_ns()
            stream = api.run_transform(
                script, _chunked(primary["bytes"]),
                input_name=primary_name, input_mime_type=primary["mimeType"],
                input_charset=primary["charset"],
            )
            for _chunk in stream:
                pass
            meta = stream.metadata
            if meta is None or not meta.success:
                raise RuntimeError(f"stream failed: {getattr(meta, 'error', 'no metadata')}")
            elapsed_ms = (time.perf_counter_ns() - start) / 1e6
            mbps.append(to_mbps(len(primary["bytes"]), elapsed_ms))
        rows.append({"id": c["id"], "metric": "streaming", "unit": "MB/s",
                     "stats": compute_stats(mbps), "iterations": iters})

    return rows
