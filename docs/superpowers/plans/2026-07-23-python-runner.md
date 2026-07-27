# Python Runner (native-lib wrapper benchmark) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the Python benchmark runner — a stdlib-only set of scripts at `benchmarks/runners/python/` that drives the DataWeave Python binding over the shared corpus and emits a schema-conformant `results/python-<ts>.json`, auto-registered into the `benchmarkCompare` aggregator, so `report/report.mjs` produces Python-vs-Node-vs-engine deltas at the same weave version.

**Architecture:** Pure Python (stdlib only), structured to mirror the **Node** runner file-for-file (Python is Node's true peer: both are dynamic-language FFI wrappers over the *same* staged `dwlib`). Timing is hand-rolled `time.perf_counter_ns()` → ms for parity with Node (`process.hrtime.bigint()`) and the engine (`System.nanoTime()`). `stats.py`/`manifest.py`/`env.py` reimplement `benchmarks/lib/*.mjs` self-contained (no runtime `node` except the shared input generator). Cold-start/first-run spawn a fresh `python` per sample (`coldstart_child.py`); warm/streaming run in-process (`warm_bench.py`). A parity test (`test_bench.py`) guards the reimplemented math against drift and is wired always-on into `native-lib:test`.

**Tech Stack:** Python 3.9+ (stdlib `unittest`, `subprocess`, `hashlib`, `time.perf_counter_ns`, `json`, `pathlib`), the `dataweave` binding at `native-lib/python/src`, Gradle `Exec` tasks invoking `python3` directly (no venv/pip), the existing Node `corpus/gen-inputs.mjs` for the shared large input. Reuses `benchmarks/corpus/`, `benchmarks/schema/`, and `report/report.mjs` unchanged.

## Global Constraints

- **Stdlib only.** No third-party Python packages; no venv/pip step. The runner is invoked as `python3 runners/python/<file>.py`.
- **Timing methodology (every metric):** `time.perf_counter_ns()`; convert to ms as `ns / 1e6` (float). Identical in spirit to Node's `process.hrtime.bigint()` → ms and the engine's `System.nanoTime()`.
- **No JIT warmup floor.** CPython has no JIT — `warm` uses `iterations.warmup` verbatim (like the Node runner), NOT the engine's `max(warmup, 2000)` floor. Do not add a floor.
- **Nearest-rank percentiles, byte-compatible with `lib/stats.mjs`:** `pct(p) = sorted[min(n-1, max(0, ceil(p/100 * n) - 1))]`; `mean = sum/n`. Throughput `to_mbps = total_bytes / 1e6 / (elapsed_ms/1000)` (decimal MB).
- **`id` is the immutable join key.** Emit each case `id` verbatim from the manifest; never invent or rename.
- **Fail-fast on orphan ids.** `emit.py` MUST abort before writing output if any emitted `id` is absent from the manifest.
- **Explicit metrics.** Run only the metrics a case declares in `metrics[]` (`cases_for_metric`).
- **Correctness guard.** A case whose `run()` returns `success == False`, or whose streaming `.metadata.success` is false, aborts that case with a clear error — never record a bogus fast timing.
- **Schema conformance (mandatory `env` fields).** Every result validates against `benchmarks/schema/result.schema.json`: top-level `schemaVersion:"1.0"`, `runner`, `env`, `timestamp`, `cases[]`; each case a flat `{id, metric, unit, stats, iterations}` row with no extra keys; `env` includes `os`, `cpu`, `runtimeVersion`, `weaveVersion`, `commit`, `dwlibBuildId` (all required).
- **`runner` is `"python-wrapper"`** — the report column name and dedupe key, symmetric with Node's `"node-wrapper"`. (`report.mjs` still auto-selects `engine` as the delta baseline.)
- **Real `dwlibBuildId`:** `"dwlib-" + sha256(str(size) + first-64KB-of-file).hexdigest()[:8]`, pointed at the Python staging path (`native-lib/python/src/dataweave/native/dwlib.*`), honoring a `DATAWEAVE_NATIVE_LIB` override. Same formula as `lib/env.mjs`.
- **Units per row:** `ms` for latency (cold-start, first-run, warm), `MB/s` for streaming.
- **Results are local-only.** Output to `benchmarks/results/` (already gitignored). Do not commit result files or generated inputs.
- **Deterministic large inputs.** Generated via the existing `corpus/gen-inputs.mjs` (Node), byte-identical to other runners — the Python runner consumes that file, never regenerates it differently.
- **All always-on unit tests are dwlib-free.** The Python binding's `dwlib` requires a multi-minute native build, so every test in `test_bench.py` uses dependency injection (fake api / fake `sample_fn`), synthetic manifests, or temp files. The real dwlib integration is verified only by the opt-in `benchmarkPython` end-to-end run (Task 8).

---

### Task 1: `stats.py` + parity test scaffold

**Files:**
- Create: `benchmarks/runners/python/stats.py`
- Test: `benchmarks/runners/python/test_bench.py`

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `stats.compute_stats(samples) -> {"min","median","p90","p99","mean"}` — nearest-rank percentiles on a sorted copy; raises `ValueError` on empty input.
  - `stats.to_mbps(total_bytes, elapsed_ms) -> float` — decimal-MB throughput; raises `ValueError` if `elapsed_ms <= 0`.
  - `test_bench.py` — the always-on parity harness (stdlib `unittest`), extended by later tasks.

- [ ] **Step 1: Write the failing test (creates `test_bench.py`)**

Create `benchmarks/runners/python/test_bench.py`. These expected values are the exact outputs of `benchmarks/lib/stats.mjs` for the same inputs:

```python
"""Always-on parity guard for the Python runner: nearest-rank stats vs
lib/stats.mjs, manifest parsing, env stamp, and the pure emit/aggregation
logic. Pure stdlib — no dwlib, no venv. Run: python3 runners/python/test_bench.py"""

import unittest
from pathlib import Path

# benchmarks/runners/python -> benchmarks -> corpus
CORPUS = Path(__file__).resolve().parents[2] / "corpus"

import stats


class TestStats(unittest.TestCase):
    def test_matches_lib_stats_on_1_to_100(self):
        s = stats.compute_stats([float(i) for i in range(1, 101)])
        self.assertEqual(s["min"], 1.0)
        self.assertEqual(s["median"], 50.0)  # ceil(0.5*100)-1 = 49 -> sorted[49] = 50
        self.assertEqual(s["p90"], 90.0)     # ceil(0.9*100)-1 = 89 -> 90
        self.assertEqual(s["p99"], 99.0)     # ceil(0.99*100)-1 = 98 -> 99
        self.assertEqual(s["mean"], 50.5)

    def test_sorts_before_ranking(self):
        s = stats.compute_stats([5.0, 1.0, 3.0, 2.0, 4.0])
        self.assertEqual(s["min"], 1.0)
        self.assertEqual(s["median"], 3.0)   # ceil(0.5*5)-1 = 2 -> sorted[2] = 3
        self.assertEqual(s["p90"], 5.0)       # ceil(0.9*5)-1 = 4 -> 5
        self.assertEqual(s["mean"], 3.0)

    def test_rejects_empty(self):
        with self.assertRaises(ValueError):
            stats.compute_stats([])

    def test_to_mbps(self):
        self.assertEqual(stats.to_mbps(1_000_000, 1000.0), 1.0)
        self.assertEqual(stats.to_mbps(500_000, 250.0), 2.0)
        with self.assertRaises(ValueError):
            stats.to_mbps(10, 0.0)


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd benchmarks && python3 runners/python/test_bench.py`
Expected: FAIL — `ModuleNotFoundError: No module named 'stats'` (module not yet created).

- [ ] **Step 3: Implement `stats.py`**

Create `benchmarks/runners/python/stats.py`:

```python
"""Percentile + throughput math kept behavior-compatible with
benchmarks/lib/stats.mjs so the Python runner's deltas compare cleanly against
the Node and engine runners. Any divergence is caught by test_bench.py."""

import math


def compute_stats(samples):
    """Nearest-rank percentiles on a sorted copy; mean is the arithmetic mean.
    Mirrors lib/stats.mjs:computeStats."""
    if not samples:
        raise ValueError("compute_stats requires a non-empty sequence of numbers")
    ordered = sorted(samples)
    n = len(ordered)

    def pct(p):
        idx = min(n - 1, max(0, math.ceil(p / 100 * n) - 1))
        return ordered[idx]

    return {
        "min": ordered[0],
        "median": pct(50),
        "p90": pct(90),
        "p99": pct(99),
        "mean": sum(ordered) / n,
    }


def to_mbps(total_bytes, elapsed_ms):
    """Throughput in decimal megabytes per second (1 MB = 1e6 bytes).
    Mirrors lib/stats.mjs:toMBps."""
    if elapsed_ms <= 0:
        raise ValueError("elapsed_ms must be > 0")
    return total_bytes / 1e6 / (elapsed_ms / 1000)
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cd benchmarks && python3 runners/python/test_bench.py`
Expected: PASS — `Ran 4 tests ... OK`.

- [ ] **Step 5: Commit**

```bash
git add benchmarks/runners/python/stats.py benchmarks/runners/python/test_bench.py
git commit -m "W-23545283: Add Python runner stats with lib/stats.mjs parity test"
```

---

### Task 2: `manifest.py` + manifest parsing tests

**Files:**
- Create: `benchmarks/runners/python/manifest.py`
- Modify: `benchmarks/runners/python/test_bench.py`

**Interfaces:**
- Consumes: the real corpus at `benchmarks/corpus/`.
- Produces:
  - `manifest.METRICS` — the allowed metric tuple.
  - `manifest.load_manifest(corpus_dir) -> {"corpusDir": str, "cases": list, "ids": set}` — parse + validate `manifest.json` (mirrors `lib/manifest.mjs:loadManifest`: non-empty unique ids, metrics from the allowed set, script exists, non-generated input files exist). Raises `ValueError` on any violation.
  - `manifest.cases_for_metric(manifest, metric) -> list`
  - `manifest.resolve_inputs(manifest, case_obj) -> {name: {"bytes": bytes, "mimeType": str, "charset": str|None}}`
  - `manifest.read_script(manifest, case_obj) -> str`
  - `manifest.validate_result_ids(manifest, result_ids) -> None` — raises `ValueError` on any id not in `manifest["ids"]`.

- [ ] **Step 1: Write the failing test**

Add `import manifest` beneath `import stats` in `test_bench.py`, and insert this class above the `if __name__` guard:

```python
class TestManifest(unittest.TestCase):
    def setUp(self):
        self.m = manifest.load_manifest(CORPUS)

    def test_loads_all_corpus_ids(self):
        for cid in ("trivial", "object-transform", "map-scale", "xml-to-csv",
                    "json-stream", "compile-heavy"):
            self.assertIn(cid, self.m["ids"])

    def test_cases_for_metric_filters(self):
        streaming = [c["id"] for c in manifest.cases_for_metric(self.m, "streaming")]
        self.assertIn("map-scale", streaming)
        self.assertIn("json-stream", streaming)
        cold = [c["id"] for c in manifest.cases_for_metric(self.m, "cold-start")]
        self.assertIn("trivial", cold)

    def test_read_script(self):
        trivial = next(c for c in self.m["cases"] if c["id"] == "trivial")
        self.assertIn("2 + 2", manifest.read_script(self.m, trivial))

    def test_resolve_inputs_reads_bytes_mime_charset(self):
        xml = next(c for c in self.m["cases"] if c["id"] == "xml-to-csv")
        resolved = manifest.resolve_inputs(self.m, xml)
        self.assertEqual(list(resolved.keys()), ["payload"])
        self.assertEqual(resolved["payload"]["mimeType"], "application/xml")
        self.assertEqual(resolved["payload"]["charset"], "UTF-16")
        self.assertGreater(len(resolved["payload"]["bytes"]), 0)

    def test_validate_result_ids_rejects_orphan(self):
        with self.assertRaises(ValueError):
            manifest.validate_result_ids(self.m, ["trivial", "not-a-case"])
        manifest.validate_result_ids(self.m, ["trivial"])  # no raise
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd benchmarks && python3 runners/python/test_bench.py`
Expected: FAIL — `ModuleNotFoundError: No module named 'manifest'`.

- [ ] **Step 3: Implement `manifest.py`**

Create `benchmarks/runners/python/manifest.py`:

```python
"""Parse and validate corpus/manifest.json, resolve scripts + inputs.
Mirrors benchmarks/lib/manifest.mjs."""

import json
from pathlib import Path

# The only metrics a case may declare.
METRICS = ("cold-start", "first-run", "warm", "streaming")


def load_manifest(corpus_dir):
    corpus_dir = Path(corpus_dir)
    raw = json.loads((corpus_dir / "manifest.json").read_text(encoding="utf-8"))
    cases = raw.get("cases")
    if not isinstance(cases, list):
        raise ValueError("manifest.cases must be an array")
    ids = set()
    for c in cases:
        cid = c.get("id")
        if not cid:
            raise ValueError("manifest case is missing an id")
        if cid in ids:
            raise ValueError(f"duplicate case id: {cid}")
        ids.add(cid)
        metrics = c.get("metrics")
        if not isinstance(metrics, list) or not metrics:
            raise ValueError(f"case {cid} must declare a non-empty metrics[]")
        for m in metrics:
            if m not in METRICS:
                raise ValueError(f"case {cid} has unknown metric: {m}")
        script = c.get("script")
        if not script or not (corpus_dir / script).exists():
            raise ValueError(f"case {cid} script not found: {script}")
        for name, inp in (c.get("inputs") or {}).items():
            if inp.get("file") and not inp.get("generated") \
                    and not (corpus_dir / inp["file"]).exists():
                raise ValueError(f"case {cid} input '{name}' file not found: {inp['file']}")
    return {"corpusDir": str(corpus_dir), "cases": cases, "ids": ids}


def cases_for_metric(manifest, metric):
    return [c for c in manifest["cases"] if metric in c["metrics"]]


def resolve_inputs(manifest, case_obj):
    """Read a case's declared inputs into bytes."""
    corpus_dir = Path(manifest["corpusDir"])
    out = {}
    for name, inp in (case_obj.get("inputs") or {}).items():
        data = (corpus_dir / inp["file"]).read_bytes()
        out[name] = {"bytes": data, "mimeType": inp["mimeType"], "charset": inp.get("charset")}
    return out


def read_script(manifest, case_obj):
    return (Path(manifest["corpusDir"]) / case_obj["script"]).read_text(encoding="utf-8")


def validate_result_ids(manifest, result_ids):
    """Fail-fast: raise if any result id is not present in the manifest."""
    for rid in result_ids:
        if rid not in manifest["ids"]:
            raise ValueError(f"result contains orphan id not in manifest: {rid}")
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cd benchmarks && python3 runners/python/test_bench.py`
Expected: PASS — `Ran 9 tests ... OK`.

- [ ] **Step 5: Commit**

```bash
git add benchmarks/runners/python/manifest.py benchmarks/runners/python/test_bench.py
git commit -m "W-23545283: Add Python runner manifest parser + input resolution"
```

---

### Task 3: `env.py` — env stamp with real `dwlibBuildId`

**Files:**
- Create: `benchmarks/runners/python/env.py`
- Modify: `benchmarks/runners/python/test_bench.py`

**Interfaces:**
- Consumes: `gradle.properties` (weaveVersion), `git` (commit), the staged/overridden `dwlib` (build id).
- Produces:
  - `env.gather_env() -> {"runner","os","cpu","runtimeVersion","weaveVersion","commit","dwlibBuildId"}` — `runner="python-wrapper"`; `os="<sys.platform>-<normalized arch>"`; `cpu` best-effort; `runtimeVersion="python <X.Y.Z>"`; `weaveVersion` from `gradle.properties`; `commit` from git (fallback `"unknown"`); `dwlibBuildId` real (sha256 formula) or `"unknown"` if no lib found.

- [ ] **Step 1: Write the failing test**

Add `import env as envmod` beneath `import manifest` in `test_bench.py`, add `import hashlib`, `import os`, `import tempfile` beneath the existing stdlib imports, and insert this class above the `if __name__` guard:

```python
class TestEnv(unittest.TestCase):
    def test_gather_env_has_required_keys(self):
        e = envmod.gather_env()
        for k in ("os", "cpu", "runtimeVersion", "weaveVersion", "commit", "dwlibBuildId"):
            self.assertIn(k, e)
        self.assertEqual(e["runner"], "python-wrapper")
        self.assertTrue(e["runtimeVersion"].startswith("python "))
        self.assertTrue(e["weaveVersion"])  # non-empty (read from gradle.properties)

    def test_dwlib_build_id_formula(self):
        data = b"hello world" * 10
        with tempfile.NamedTemporaryFile(suffix=".dylib", delete=False) as f:
            f.write(data)
            path = f.name
        os.environ["DATAWEAVE_NATIVE_LIB"] = path
        try:
            e = envmod.gather_env()
            size = os.path.getsize(path)
            h = hashlib.sha256()
            h.update(str(size).encode())
            h.update(data[:65536])
            self.assertEqual(e["dwlibBuildId"], "dwlib-" + h.hexdigest()[:8])
        finally:
            del os.environ["DATAWEAVE_NATIVE_LIB"]
            os.unlink(path)
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd benchmarks && python3 runners/python/test_bench.py`
Expected: FAIL — `ModuleNotFoundError: No module named 'env'`.

- [ ] **Step 3: Implement `env.py`**

Create `benchmarks/runners/python/env.py`:

```python
"""Env stamp for the Python runner, mirroring benchmarks/lib/env.mjs. Includes a
REAL dwlibBuildId — the Python runner wraps the same staged dwlib as Node, so an
unchanged lib yields the same id in both runners' result files."""

import hashlib
import os
import platform
import re
import subprocess
import sys
from pathlib import Path

# benchmarks/runners/python -> repo root
_REPO_ROOT = Path(__file__).resolve().parents[3]
_ENV_NATIVE_LIB = "DATAWEAVE_NATIVE_LIB"


def _read_weave_version():
    txt = (_REPO_ROOT / "gradle.properties").read_text(encoding="utf-8")
    m = re.search(r"^weaveVersion=(.+)$", txt, re.MULTILINE)
    if not m:
        raise RuntimeError("weaveVersion not found in gradle.properties")
    return m.group(1).strip()


def _read_commit():
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=_REPO_ROOT, stderr=subprocess.DEVNULL,
        ).decode().strip()
    except Exception:
        return "unknown"


def _normalize_arch(machine):
    # Match Node's process.arch vocabulary so labels read identically across runners.
    return {"x86_64": "x64", "amd64": "x64"}.get(machine, machine)


def _cpu_model():
    try:
        if sys.platform == "darwin":
            return subprocess.check_output(
                ["sysctl", "-n", "machdep.cpu.brand_string"], stderr=subprocess.DEVNULL,
            ).decode().strip()
        if sys.platform.startswith("linux"):
            for line in Path("/proc/cpuinfo").read_text().splitlines():
                if line.startswith("model name"):
                    return line.split(":", 1)[1].strip()
    except Exception:
        pass
    return platform.machine() or "unknown"


def _dwlib_path():
    override = (os.environ.get(_ENV_NATIVE_LIB) or "").strip()
    if override:
        return Path(override)
    base = _REPO_ROOT / "native-lib" / "python" / "src" / "dataweave" / "native"
    for ext in (".dylib", ".so", ".dll"):
        p = base / f"dwlib{ext}"
        if p.exists():
            return p
    return None


def _read_dwlib_build_id():
    # sha256 over (size + first 64KB), same formula as lib/env.mjs.
    p = _dwlib_path()
    if p and p.exists():
        size = p.stat().st_size
        head = p.read_bytes()[:65536]
        h = hashlib.sha256()
        h.update(str(size).encode())
        h.update(head)
        return "dwlib-" + h.hexdigest()[:8]
    return "unknown"


def gather_env():
    return {
        "runner": "python-wrapper",
        "os": f"{sys.platform}-{_normalize_arch(platform.machine())}",
        "cpu": _cpu_model(),
        "runtimeVersion": f"python {platform.python_version()}",
        "weaveVersion": _read_weave_version(),
        "commit": _read_commit(),
        "dwlibBuildId": _read_dwlib_build_id(),
    }
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cd benchmarks && python3 runners/python/test_bench.py`
Expected: PASS — `Ran 11 tests ... OK`.

- [ ] **Step 5: Commit**

```bash
git add benchmarks/runners/python/env.py benchmarks/runners/python/test_bench.py
git commit -m "W-23545283: Add Python runner env stamp with real dwlibBuildId"
```

---

### Task 4: `wrapper.py` — locate + import the binding

**Files:**
- Create: `benchmarks/runners/python/wrapper.py`
- Modify: `benchmarks/runners/python/test_bench.py`

**Interfaces:**
- Consumes: the `dataweave` binding at `native-lib/python/src`.
- Produces:
  - `wrapper.load_wrapper(src=None) -> module` — put `native-lib/python/src` (or `src` override) on `sys.path` and `import dataweave`; raise `RuntimeError` with a build hint if it cannot be imported. Importing the binding is dwlib-free (the lib loads lazily on `DataWeave().initialize()`).

- [ ] **Step 1: Write the failing test**

Add `import wrapper` beneath `import env as envmod` in `test_bench.py`, and insert this class above the `if __name__` guard:

```python
class TestWrapper(unittest.TestCase):
    def test_load_wrapper_exposes_api(self):
        api = wrapper.load_wrapper()
        for attr in ("DataWeave", "run", "run_transform", "run_streaming"):
            self.assertTrue(hasattr(api, attr), f"binding missing {attr}")
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd benchmarks && python3 runners/python/test_bench.py`
Expected: FAIL — `ModuleNotFoundError: No module named 'wrapper'`.

- [ ] **Step 3: Implement `wrapper.py`**

Create `benchmarks/runners/python/wrapper.py`:

```python
"""Locate and import the DataWeave Python binding (native-lib/python). The
binding loads the staged dwlib lazily on DataWeave().initialize(), so importing
the module itself is dwlib-free."""

import sys
from pathlib import Path

# benchmarks/runners/python -> repo root -> native-lib/python/src
_SRC = Path(__file__).resolve().parents[3] / "native-lib" / "python" / "src"


def load_wrapper(src=None):
    src = Path(src) if src is not None else _SRC
    if str(src) not in sys.path:
        sys.path.insert(0, str(src))
    try:
        import dataweave
    except ImportError as e:
        raise RuntimeError(
            f"DataWeave Python binding not importable from {src}. "
            f"Run: ./gradlew native-lib:stagePythonNativeLib  ({e})"
        )
    return dataweave
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cd benchmarks && python3 runners/python/test_bench.py`
Expected: PASS — `Ran 12 tests ... OK`.

- [ ] **Step 5: Commit**

```bash
git add benchmarks/runners/python/wrapper.py benchmarks/runners/python/test_bench.py
git commit -m "W-23545283: Add Python runner binding loader"
```

---

### Task 5: `coldstart_child.py` + `coldstart.py` — fresh-process spawn harness

**Files:**
- Create: `benchmarks/runners/python/coldstart_child.py`
- Create: `benchmarks/runners/python/coldstart.py`
- Modify: `benchmarks/runners/python/test_bench.py`

**Interfaces:**
- Consumes: `manifest` (`load_manifest`, `cases_for_metric`, `read_script`, `resolve_inputs`), `wrapper` (child only), `stats.compute_stats`.
- Produces:
  - `coldstart_child.py` — a script (`python3 coldstart_child.py <corpusDir> <caseId>`) that times `DataWeave()` construction + `.initialize()` (`initMs`) and the first `.run()` (`firstRunMs`), exits non-zero on failure, and prints one JSON line `{"initMs":..,"firstRunMs":..}`. dwlib-dependent — never imported by tests, only spawned in Task 8.
  - `coldstart.run_cold_start_and_first_run(manifest, sample_fn=<spawn>, samples_override=None) -> list[row]` — for each case declaring cold-start and/or first-run: call `sample_fn(corpusDir, caseId)` N times (N = `samples_override` or `iterations.samples` or 20), aggregate into `cold-start`/`first-run` rows (only the metrics the case declares). `sample_fn` is injectable for dwlib-free testing.

- [ ] **Step 1: Write the failing test**

Add `import coldstart` beneath `import wrapper` in `test_bench.py`, and insert this class above the `if __name__` guard. It injects a fake `sample_fn`, so it needs no dwlib and no input files:

```python
class TestColdstartAggregation(unittest.TestCase):
    def _manifest(self):
        return {
            "corpusDir": str(CORPUS),
            "cases": [
                {"id": "trivial", "script": "scripts/trivial.dwl",
                 "metrics": ["cold-start", "first-run"], "iterations": {"samples": 5}},
                {"id": "object-transform", "script": "scripts/object-transform.dwl",
                 "metrics": ["first-run"], "iterations": {"samples": 5}},
            ],
            "ids": {"trivial", "object-transform"},
        }

    def test_aggregates_injected_samples(self):
        calls = []

        def fake_sample(corpus_dir, case_id):
            calls.append(case_id)
            return (1.0, 2.0)  # (initMs, firstRunMs)

        rows = coldstart.run_cold_start_and_first_run(
            self._manifest(), sample_fn=fake_sample, samples_override=3)

        by = {(r["id"], r["metric"]): r for r in rows}
        self.assertIn(("trivial", "cold-start"), by)
        self.assertIn(("trivial", "first-run"), by)
        self.assertIn(("object-transform", "first-run"), by)
        self.assertNotIn(("object-transform", "cold-start"), by)

        self.assertEqual(by[("trivial", "cold-start")]["unit"], "ms")
        self.assertEqual(by[("trivial", "cold-start")]["stats"]["median"], 1.0)
        self.assertEqual(by[("trivial", "first-run")]["stats"]["median"], 2.0)
        self.assertEqual(by[("trivial", "cold-start")]["iterations"], 3)
        # 2 cases sampled 3 times each.
        self.assertEqual(len(calls), 6)
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd benchmarks && python3 runners/python/test_bench.py`
Expected: FAIL — `ModuleNotFoundError: No module named 'coldstart'`.

- [ ] **Step 3: Implement `coldstart_child.py`**

Create `benchmarks/runners/python/coldstart_child.py`:

```python
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
```

- [ ] **Step 4: Implement `coldstart.py`**

Create `benchmarks/runners/python/coldstart.py`:

```python
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
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `cd benchmarks && python3 runners/python/test_bench.py`
Expected: PASS — `Ran 13 tests ... OK`.

- [ ] **Step 6: Commit**

```bash
git add benchmarks/runners/python/coldstart_child.py benchmarks/runners/python/coldstart.py \
  benchmarks/runners/python/test_bench.py
git commit -m "W-23545283: Add Python runner fresh-process cold-start/first-run harness"
```

---

### Task 6: `warm_bench.py` — in-process warm + streaming

**Files:**
- Create: `benchmarks/runners/python/warm_bench.py`
- Modify: `benchmarks/runners/python/test_bench.py`

**Interfaces:**
- Consumes: `manifest` (`cases_for_metric`, `read_script`, `resolve_inputs`), `stats` (`compute_stats`, `to_mbps`), and an injected `api` object exposing `run(script, inputs)` and `run_transform(script, input_stream, input_name=, input_mime_type=, input_charset=)`.
- Produces:
  - `warm_bench.run_warm_and_streaming(api, manifest, warmup_cap=None, warm_cap=None, streaming_cap=None) -> list[row]` — for each `warm` case: `warmup` runs (uncapped default `iterations.warmup` or 10 — NO JIT floor), then `iters` timed `api.run` reps → a `warm`/`ms` row. For each `streaming` case: `iters` reps of `api.run_transform` over the 64KB-chunked primary input, drained + `.metadata.success`-guarded → a `streaming`/`MB/s` row via `to_mbps(len(primary_bytes), elapsed_ms)`. Raises `RuntimeError` on any non-success result (correctness guard).

- [ ] **Step 1: Write the failing test**

Add `import warm_bench` beneath `import coldstart` in `test_bench.py`, and insert this class above the `if __name__` guard. It injects a fake `api` and uses a synthetic manifest pointing at the committed `person-record.json`, so it needs no dwlib and no generated inputs:

```python
class _FakeResult:
    def __init__(self, success=True, error=None):
        self.success, self.error = success, error


class _FakeMeta:
    def __init__(self, success=True, error=None):
        self.success, self.error = success, error


class _FakeStream:
    def __init__(self, chunks, meta):
        self._chunks = iter(chunks)
        self.metadata = meta

    def __iter__(self):
        return self

    def __next__(self):
        return next(self._chunks)


class _FakeApi:
    def __init__(self, run_ok=True):
        self.run_ok = run_ok

    def run(self, script, inputs=None):
        return _FakeResult(self.run_ok, None if self.run_ok else "boom")

    def run_transform(self, script, input_stream, input_name="payload",
                      input_mime_type="application/json", input_charset=None):
        for _ in input_stream:  # consume, mimicking the real read side
            pass
        return _FakeStream([b"out-chunk"], _FakeMeta(True))


class TestWarmBench(unittest.TestCase):
    def _warm_manifest(self):
        return {
            "corpusDir": str(CORPUS),
            "cases": [{"id": "trivial", "script": "scripts/trivial.dwl",
                       "metrics": ["warm"], "iterations": {}}],
            "ids": {"trivial"},
        }

    def _streaming_manifest(self):
        return {
            "corpusDir": str(CORPUS),
            "cases": [{"id": "object-transform", "script": "scripts/object-transform.dwl",
                       "inputs": {"payload": {"file": "inputs/person-record.json",
                                              "mimeType": "application/json"}},
                       "metrics": ["streaming"], "iterations": {}}],
            "ids": {"object-transform"},
        }

    def test_warm_rows(self):
        rows = warm_bench.run_warm_and_streaming(
            _FakeApi(), self._warm_manifest(), warmup_cap=1, warm_cap=3)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["metric"], "warm")
        self.assertEqual(rows[0]["unit"], "ms")
        self.assertEqual(rows[0]["iterations"], 3)
        self.assertGreaterEqual(rows[0]["stats"]["median"], 0.0)

    def test_streaming_rows(self):
        rows = warm_bench.run_warm_and_streaming(
            _FakeApi(), self._streaming_manifest(), streaming_cap=2)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["metric"], "streaming")
        self.assertEqual(rows[0]["unit"], "MB/s")
        self.assertGreater(rows[0]["stats"]["median"], 0.0)

    def test_warm_guard_raises_on_failure(self):
        with self.assertRaises(RuntimeError):
            warm_bench.run_warm_and_streaming(
                _FakeApi(run_ok=False), self._warm_manifest(), warmup_cap=1, warm_cap=1)
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd benchmarks && python3 runners/python/test_bench.py`
Expected: FAIL — `ModuleNotFoundError: No module named 'warm_bench'`.

- [ ] **Step 3: Implement `warm_bench.py`**

Create `benchmarks/runners/python/warm_bench.py`:

```python
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
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cd benchmarks && python3 runners/python/test_bench.py`
Expected: PASS — `Ran 16 tests ... OK`.

- [ ] **Step 5: Commit**

```bash
git add benchmarks/runners/python/warm_bench.py benchmarks/runners/python/test_bench.py
git commit -m "W-23545283: Add Python runner warm + streaming (no JIT floor)"
```

---

### Task 7: `emit.py` — collector + result serializer

**Files:**
- Create: `benchmarks/runners/python/emit.py`
- Modify: `benchmarks/runners/python/test_bench.py`

**Interfaces:**
- Consumes: `coldstart.run_cold_start_and_first_run`, `warm_bench.run_warm_and_streaming`, `env.gather_env`, `manifest` (`load_manifest`, `validate_result_ids`), `wrapper.load_wrapper`.
- Produces:
  - `emit.build_result(env, cases) -> dict` — pure assembler: `{schemaVersion:"1.0", runner, env, timestamp, cases}`. `timestamp` is ISO-8601 UTC millis with `Z` suffix.
  - `emit.main() -> Path` — run cold-start/first-run (fresh processes) + warm/streaming (in-process, one initialized `DataWeave`), fail-fast id-validate, stamp env, write `results/python-<ts>.json`. dwlib-dependent; exercised in Task 8.
  - `emit.CORPUS`, `emit.RESULTS_DIR` — resolved from `__file__`.

- [ ] **Step 1: Write the failing test**

Add `import emit` beneath `import warm_bench` in `test_bench.py`, and insert this class above the `if __name__` guard. It tests only the pure `build_result` (like Node's `emit.test.mjs`), so no dwlib:

```python
class TestEmit(unittest.TestCase):
    def test_build_result_shape(self):
        env = {"runner": "python-wrapper", "os": "darwin-arm64", "cpu": "x",
               "runtimeVersion": "python 3.9.6", "weaveVersion": "2.12.0-x",
               "commit": "abc", "dwlibBuildId": "dwlib-x"}
        cases = [{"id": "trivial", "metric": "warm", "unit": "ms",
                  "stats": {"median": 1.0}, "iterations": 3}]
        r = emit.build_result(env, cases)
        self.assertEqual(r["schemaVersion"], "1.0")
        self.assertEqual(r["runner"], "python-wrapper")
        self.assertEqual(r["env"], env)
        self.assertEqual(r["cases"], cases)
        self.assertTrue(r["timestamp"].endswith("Z"))
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd benchmarks && python3 runners/python/test_bench.py`
Expected: FAIL — `ModuleNotFoundError: No module named 'emit'`.

- [ ] **Step 3: Implement `emit.py`**

Create `benchmarks/runners/python/emit.py`:

```python
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
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cd benchmarks && python3 runners/python/test_bench.py`
Expected: PASS — `Ran 17 tests ... OK`.

- [ ] **Step 5: Commit**

```bash
git add benchmarks/runners/python/emit.py benchmarks/runners/python/test_bench.py
git commit -m "W-23545283: Add Python runner emit collector + result serializer"
```

---

### Task 8: Gradle wiring (3 tasks) + README + end-to-end verification

**Files:**
- Modify: `native-lib/build.gradle` (add three tasks after `benchmarkNode`; extend the `test` block)
- Modify: `benchmarks/README.md`
- (Verification only) `benchmarks/runners/python/emit.py`, `report/report.mjs`, `schema/result.schema.json`

**Interfaces:**
- Consumes: `emit.py` (Task 7), the existing `report/report.mjs` and `schema/result.schema.json`, the existing `pythonExe` def (`native-lib/build.gradle:116`) and `stagePythonNativeLib` task.
- Produces: `native-lib:benchmarkPython` (tagged `ext.benchmarkRunner=true`, opt-in, emit-only), `native-lib:benchmarkPythonStatsTest` (always-on), `native-lib:benchmarkJsUnitTest` (always-on), both test tasks wired into `native-lib:test`; corrected README.

- [ ] **Step 1: Add the three tasks to `native-lib/build.gradle`**

Insert the following immediately after the `benchmarkNode` task (which ends at the closing `}` on line 235, before `tasks.named('test')`). `pythonExe` is the script-level `def` from line 116 and is in scope here:

```groovy
// --- Python runner benchmark tasks ---

// The Python runner as an aggregator-registered runner: emits its result file
// but does NOT render the report (root :benchmarkCompare renders once over all
// runners). Tagged `benchmarkRunner` so :benchmarkCompare discovers it.
tasks.register('benchmarkPython', Exec) {
  onlyIf { project.findProperty('benchmark')?.toString()?.toBoolean() == true }
  ext.benchmarkRunner = true

  dependsOn tasks.named('stagePythonNativeLib')
  workingDir("${rootDir}/benchmarks")

  // Generate the shared input via the Node generator (idempotent, deterministic,
  // byte-identical to other runners) then emit; no report here.
  def script = 'node corpus/gen-inputs.mjs && ' + pythonExe + ' runners/python/emit.py'
  if (System.getProperty('os.name').toLowerCase().contains('windows')) {
    commandLine('cmd', '/c', script)
  } else {
    commandLine('bash', '-c', script)
  }
}

// Always-on parity guard: pure-stdlib stats + manifest + env tests (no dwlib, no
// venv). Wired into `test` so drift vs lib/stats.mjs fails the normal build.
tasks.register('benchmarkPythonStatsTest', Exec) {
  if (project.findProperty('skipPythonTests')?.toString()?.toBoolean() == true) {
    enabled = false
  }
  workingDir("${rootDir}/benchmarks")
  commandLine(pythonExe, 'runners/python/test_bench.py')
}

// Always-on parity guard for the dwlib-free JS harness tests (shared lib + report
// + node emit builder). Excludes the dwlib-dependent warm-bench/coldstart tests.
tasks.register('benchmarkJsUnitTest', Exec) {
  if (project.findProperty('skipNodeTests')?.toString()?.toBoolean() == true) {
    enabled = false
  }
  workingDir("${rootDir}/benchmarks")
  def files = 'lib/stats.test.mjs lib/manifest.test.mjs lib/env.test.mjs ' +
    'report/report.test.mjs runners/node/emit.test.mjs'
  def script = 'node --test ' + files
  if (System.getProperty('os.name').toLowerCase().contains('windows')) {
    commandLine('cmd', '/c', script)
  } else {
    commandLine('bash', '-c', script)
  }
}
```

- [ ] **Step 2: Wire both always-on tests into `native-lib:test`**

In `native-lib/build.gradle`, replace the existing `test` block (lines 237–240):

```groovy
tasks.named('test') {
  dependsOn tasks.named('pythonTest')
  dependsOn tasks.named('nodeTest')
}
```

with:

```groovy
tasks.named('test') {
  dependsOn tasks.named('pythonTest')
  dependsOn tasks.named('nodeTest')
  dependsOn tasks.named('benchmarkPythonStatsTest')
  dependsOn tasks.named('benchmarkJsUnitTest')
}
```

- [ ] **Step 3: Verify the always-on tests run under `native-lib:test`'s new deps**

Run: `./gradlew native-lib:benchmarkPythonStatsTest native-lib:benchmarkJsUnitTest`
Expected: both PASS — the Python parity suite prints `Ran 17 tests ... OK`; `node --test` reports all 5 JS files passing with a non-zero test count and exit 0.

- [ ] **Step 4: Fix the README Layout claim**

In `benchmarks/README.md`, the Layout bullet currently reads (lines 12–14):

```
- `runners/node/` — the Node reference runner. `runners/engine/` is the JVM baseline
  (Scala/Gradle subproject `:benchmarks-engine`, depends on `org.mule.weave:runtime` at
  the same `weaveVersion` the native image is built from). `runners/python/` is a follow-up.
```

Replace those three lines verbatim with:

```
- `runners/node/` — the Node reference runner. `runners/engine/` is the JVM baseline
  (Scala/Gradle subproject `:benchmarks-engine`, depends on `org.mule.weave:runtime` at
  the same `weaveVersion` the native image is built from). `runners/python/` is the
  Python runner (stdlib scripts under `native-lib`, wrapping the same staged `dwlib` as Node).
```

- [ ] **Step 5: Add the Python invocation to the README Running section**

In `benchmarks/README.md`, under "Single-runner options", the block currently reads:

```
    ./gradlew native-lib:benchmark -Pbenchmark=true              # Node only: build wrapper, run, report
    ./gradlew benchmarks-engine:benchmarkEngine -Pbenchmark=true # engine (JVM) only: writes results/engine-<ts>.json
```

Replace it verbatim with:

```
    ./gradlew native-lib:benchmark -Pbenchmark=true              # Node only: build wrapper, run, report
    ./gradlew benchmarks-engine:benchmarkEngine -Pbenchmark=true # engine (JVM) only: writes results/engine-<ts>.json
    ./gradlew native-lib:benchmarkPython -Pbenchmark=true        # Python only: writes results/python-<ts>.json
```

- [ ] **Step 6: Commit the wiring + docs**

```bash
git add native-lib/build.gradle benchmarks/README.md
git commit -m "W-23545283: Wire Python runner + always-on parity tests into Gradle"
```

- [ ] **Step 7: Set the GraalVM env for the native build**

The Python runner's end-to-end run needs the staged `dwlib`, which needs a native build under GraalVM. On this machine:

Run:
```bash
sdk use java 21.0.11-graal
export GRAALVM_HOME="$JAVA_HOME"
```
Expected: `java -version` reports GraalVM; `$GRAALVM_HOME` and `$JAVA_HOME` both point at the graal candidate.

- [ ] **Step 8: Run the Python runner end-to-end (opt-in, dwlib-dependent)**

Run: `./gradlew native-lib:benchmarkPython -Pbenchmark=true`
Expected: `stagePythonNativeLib` stages `dwlib.*` into `native-lib/python/src/dataweave/native/`, `gen-inputs.mjs` writes `corpus/inputs/generated/records-large.json`, then `emit.py` prints `wrote .../benchmarks/results/python-<ts>.json (<N> rows)`. This is real timing — it takes a few minutes (native build + fresh Python process per cold-start/first-run sample). If `native-lib:nativeCompile` has not run yet, this triggers it first (longer).

- [ ] **Step 9: Verify the emitted result conforms to the schema shape**

From the repo root (pure Node built-ins, no new deps):

Run:
```bash
node -e '
const {readFileSync,readdirSync}=require("node:fs");
const dir="benchmarks/results";
const f=readdirSync(dir).filter(n=>n.startsWith("python-")&&n.endsWith(".json")).sort().pop();
const r=JSON.parse(readFileSync(dir+"/"+f,"utf8"));
const need=(o,k)=>{if(!(k in o))throw new Error("missing "+k);};
["schemaVersion","runner","env","timestamp","cases"].forEach(k=>need(r,k));
if(r.schemaVersion!=="1.0")throw new Error("schemaVersion");
if(r.runner!=="python-wrapper")throw new Error("runner "+r.runner);
["os","cpu","runtimeVersion","weaveVersion","commit","dwlibBuildId"].forEach(k=>need(r.env,k));
if(!r.env.dwlibBuildId.startsWith("dwlib-"))throw new Error("dwlibBuildId "+r.env.dwlibBuildId);
for(const c of r.cases){["id","metric","unit","stats","iterations"].forEach(k=>need(c,k));
  if(!["cold-start","first-run","warm","streaming"].includes(c.metric))throw new Error("metric "+c.metric);
  if(!["ms","MB/s"].includes(c.unit))throw new Error("unit "+c.unit);
  need(c.stats,"median");}
console.log("OK "+f+" ("+r.cases.length+" rows)");
'
```
Expected: `OK python-<ts>.json (<N> rows)`. Any missing key or bad enum throws. Note the real `dwlibBuildId` (starts with `dwlib-`), unlike the engine's `n/a-engine`.

- [ ] **Step 10: Verify the report renders the Python column**

Run: `cd benchmarks && node report/report.mjs results/*.json`
Expected: a markdown table grouped by case × metric with a `python-wrapper` column (alongside any `node-wrapper`/`engine` results present) and a `Δ vs engine` column when the engine result exists; **no** `⚠️ WEAVE VERSION SKEW` banner (all runners share the same `weaveVersion` from `gradle.properties`).

- [ ] **Step 11: (Optional) Verify aggregator auto-discovery**

Run: `./gradlew benchmarkCompare -Pbenchmark=true`
Expected: the root aggregator discovers `benchmarkPython` (via its `ext.benchmarkRunner = true` tag) alongside `benchmarkNode`, runs both, and renders the report once. Confirms the runner self-registered with no edit to the `benchmarkCompare` task. (Long-running: builds the wrapper and runs both runners.)

- [ ] **Step 12: Final commit (if any verification-driven tweaks were needed)**

```bash
git add -A
git commit -m "W-23545283: Verify Python runner end-to-end (benchmarkPython + report + aggregator)"
```

---

## Self-Review

**1. Spec coverage** (against `docs/superpowers/specs/2026-07-23-python-runner-design.md`):
- Pure stdlib, `benchmarks/runners/python/`, Node-mirroring structure → Tasks 1–7. ✓
- `perf_counter_ns()` → ms timing parity → coldstart_child (Task 5), warm_bench (Task 6). ✓
- **No JIT warmup floor** (verbatim `iterations.warmup`) → Task 6 `warm_bench.run_warm_and_streaming` + Global Constraints. ✓
- Metric methodology table (cold-start/first-run fresh process; warm/streaming in-process) → Tasks 5, 6. ✓
- Correctness guard (run/stream non-success aborts) → Task 5 child `SystemExit`, Task 6 `_assert_ok`/metadata guard + test. ✓
- Explicit-dict input form + UTF-16 charset path → `_to_inputs` in Tasks 5/6; `resolve_inputs` charset in Task 2. ✓
- Env: `runner:"python-wrapper"`, os `<platform>-<arch>` (x86_64→x64), cpu, `python <ver>`, weaveVersion, commit, **real dwlibBuildId** at the Python staging path w/ env override → Task 3 + parity test. ✓
- Nearest-rank stats + to_mbps reimpl of lib/stats.mjs → Task 1 + parity test. ✓
- Fail-fast id validation + schema conformance + `python-<ts>.json` → Task 2 `validate_result_ids`, Task 7 `emit`. ✓
- Three Gradle tasks (`benchmarkPython` tagged/opt-in/emit-only; `benchmarkPythonStatsTest` always-on; `benchmarkJsUnitTest` always-on dwlib-free JS) + `test` wiring → Task 8. ✓
- Testing: always-on parity (stats + manifest + env + wrapper + pure aggregation/emit), dwlib integration via end-to-end run → Tasks 1–7 (`test_bench.py`), Task 8 Steps 8–11. ✓
- Docs (README Layout + Running) → Task 8 Steps 4–5. ✓
- Out of scope honored: no corpus/schema/report.mjs/engine/Node-source changes; no `settings.gradle` change (scripts under existing module); dwlib-dependent Node tests left unwired (explicit file list in `benchmarkJsUnitTest`). ✓

**2. Placeholder scan:** No "TBD"/"TODO"/"handle edge cases" — every code step has complete, runnable code; every command has expected output. ✓

**3. Type/name consistency across tasks:** `compute_stats`/`to_mbps` (Task 1) used in Tasks 5/6; `load_manifest`/`cases_for_metric`/`resolve_inputs`/`read_script`/`validate_result_ids` (Task 2) used in Tasks 5/6/7; `gather_env` (Task 3) used in Task 7; `load_wrapper` (Task 4) used in Tasks 5/7; `run_cold_start_and_first_run(manifest, sample_fn=, samples_override=)` (Task 5) called in Task 7; `run_warm_and_streaming(api, manifest, warmup_cap=, warm_cap=, streaming_cap=)` (Task 6) called in Task 7; `build_result(env, cases)` (Task 7) used by `main` + test. Row shape `{id, metric, unit, stats, iterations}` identical in coldstart/warm_bench/emit. Result key `corpusDir` consistent between `load_manifest` output and all consumers. ✓
