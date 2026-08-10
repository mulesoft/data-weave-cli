# Python runner (native-lib wrapper benchmark) — design

**Date:** 2026-07-23
**Status:** Approved (design)
**Parent spec:** [`2026-07-22-native-lib-benchmarks-design.md`](./2026-07-22-native-lib-benchmarks-design.md) — the corpus/schema/report contract every runner shares, which lists the Python runner as an explicit follow-up.
**Sibling precedent:** [`2026-07-23-engine-runner-design.md`](./2026-07-23-engine-runner-design.md) — the closest structural template; this mirrors its self-contained-emit-with-parity-test playbook, one language over.

> **Superseded by current implementation:** This document records the original
> Python runner design. For current task wiring, external-artifact overrides, and
> test coverage, use [`benchmarks/README.md`](../../benchmarks/README.md) and
> [`benchmarks/runners/python/`](../../benchmarks/runners/python/). Retain this
> document for its original decisions and rationale.

## Purpose

Build the **Python runner** — the third benchmark surface, alongside the Node wrapper and the JVM engine baseline. It drives the DataWeave **Python** binding (`native-lib/python`, which wraps the same staged `dwlib` the Node wrapper does) over the **same shared corpus** and emits the **same JSON schema**, so `report/report.mjs` joins its results and produces cross-binding deltas: **Python wrapper vs. Node wrapper vs. JVM engine** — all at the same `weaveVersion`, all through the aggregator (`benchmarkCompare`).

Parity with the existing runners is load-bearing: same four metrics, same monotonic-clock→ms timing methodology, same nearest-rank percentile math as `lib/stats.mjs`, same flat `(case, metric)` result rows. A divergence in any of these silently corrupts the cross-runner deltas, so the design pins them with an always-on parity test.

## Decisions resolved

The parent spec left four open questions for the Python runner. Resolved:

1. **Reuse a bench library (pytest-benchmark) or hand-roll?** → **Hand-roll**, exactly as the engine runner did and for the same reason: `pytest-benchmark` brings its own timer, calibration, and percentile math, which would diverge from `lib/stats.mjs` and break clean cross-runner deltas — plus it requires a venv/pip step. Timing is `time.perf_counter_ns()` → ms (`ns / 1e6`), the direct analog of Node's `process.hrtime.bigint()` and the engine's `System.nanoTime()`.
2. **How does the spawn harness work in Python?** → **Fresh `python` process per sample** for cold-start/first-run (`coldstart_child.py`), spawned by `coldstart.py`, mirroring the Node `coldstart-child.mjs`/`coldstart.mjs` pair and the engine's `EngineChild`. This captures the honest cold path — process launch + interpreter start + `dlopen(dwlib)` + GraalVM isolate creation + first compile/exec.
3. **Self-contained, or shell out to the shared JS lib?** → **Self-contained** (like the engine): `stats.py`, `manifest.py`, `env.py` reimplement `lib/*.mjs` in pure Python. No `node` dependency at runtime except the shared input generator (see Gradle wiring). Drift risk is contained by the parity test.
4. **How does Gradle drive it — venv/pip vs. the existing `pythonTest` task?** → **Like `pythonTest`**: `python3` invoked directly against `runners/python/*.py` with the staged `dwlib` on the package's native dir. **No venv, no pip** — the runner is stdlib-only.

## Language, placement, structure

- **Language: pure Python, standard library only**, 3.9+ compatible (the binding already targets ≥3.9 — it uses PEP-585 `list[Path]` generics). No third-party packages, so no venv/pip step in Gradle.
- **Placement:** `benchmarks/runners/python/`, exactly where the parent spec's layout diagram puts it — a self-contained consumer of the shared corpus + schema.
- **Structure:** mirrors the **Node** runner file-for-file rather than the engine's Scala object layout. Python is Node's true peer here: both are dynamic-language FFI wrappers over the *same* staged `dwlib`, both split cold-start (fresh-process spawn) from warm/streaming (in-process), and both carry a **real** `dwlibBuildId`. The engine, by contrast, is a JVM runner with a `"n/a-engine"` sentinel and a JIT warmup floor — neither of which applies here.

```
benchmarks/runners/python/
  stats.py            # nearest-rank percentiles + toMBps — reimpl of lib/stats.mjs
  manifest.py         # parse manifest.json; casesForMetric, resolveInputs, validateResultIds
  env.py              # env stamp incl. REAL dwlibBuildId (sha256 of staged dwlib)
  wrapper.py          # locate + import the `dataweave` binding; clear error if unbuilt
  coldstart_child.py  # fresh-process worker: init + first run, prints one JSON line
  coldstart.py        # spawn orchestrator: N children per case -> cold-start/first-run rows
  warm_bench.py       # in-process: warm + streaming rows
  emit.py             # collector/main: merge rows, validate ids, stamp env, write result JSON
  test_bench.py       # always-on parity test (stats + manifest), stdlib unittest
```

`benchmarks/runners/` is the parent spec's extension point: one subdirectory per surface, each self-contained. The Python runner adds one such subdirectory and touches nothing in the corpus, schema, `report.mjs`, Node runner, or engine runner.

## 1. Metric methodology (timing parity)

Timing is `time.perf_counter_ns()` → ms as `ns / 1e6` throughout — a monotonic clock, matching the Node (`process.hrtime.bigint()`) and engine (`System.nanoTime()`) methodology. Which metrics run for a case is read from that case's `metrics[]` in the manifest; unlisted metrics are skipped (`cases_for_metric`), exactly like the other runners.

| Metric | Where | How |
|---|---|---|
| **cold-start** | fresh `python` per sample | `coldstart_child.py` times `DataWeave()` construction + `.initialize()` → `initMs`. Samples = `iterations.samples`. |
| **first-run** | fresh `python` per sample | The same child times the first `.run(script, inputs)` (compile + exec) → `firstRunMs`. Samples = `iterations.samples`. |
| **warm** | in-process | Warmup = `iterations.warmup` **verbatim** (no floor — see below), then `iterations.warm` timed reps of `dw.run(script, inputs)` → min/median/p90/p99/mean. |
| **streaming** | in-process | `iterations.streaming` reps of `dw.run_transform(script, chunked(input), ...)`; drain the output, assert `.metadata.success`; MB/s = `to_mbps(len(input_bytes), elapsedMs)`. |

**Deliberate divergence from the engine — no JIT warmup floor.** The engine floored warmup at `max(manifest.warmup, 2000)` because HotSpot's C2 compiler needs thousands of iterations to reach steady state. **CPython has no JIT**, so a warmup floor would be meaningless work that only distorts the comparison. Python therefore honors `iterations.warmup` verbatim — exactly like the Node runner (`c.iterations?.warmup ?? 10`). This is the correct parity choice against Node (the like-for-like wrapper comparison); it is called out here so it is not "corrected" to match the engine.

**Streaming input chunking.** The primary declared input is read fully into bytes, then chunked at 64 KiB (`chunked(buf, 65536)`) and fed to `run_transform` as an iterable — matching the Node runner's chunk size. Throughput is measured against the full input byte length: `to_mbps(len(primary_bytes), elapsedMs)`.

**Correctness guard.** A case whose `run()` returns `success == False`, or whose `run_transform` stream ends with `.metadata.success == False`, **aborts that case with a clear error** rather than recording a bogus fast timing — an errored script cannot be benchmarked. Mirrors the Node runner's `assertOk`/`drain` failure check and the engine's guard.

## 2. Input resolution, binding usage

`manifest.py` reads each case's declared input files into raw bytes and builds the binding's **explicit-dict** input form:

```python
{ name: {"content": <bytes>, "mimeType": mime, "charset": charset or "utf-8"} }
```

The binding (`native-lib/python/src/dataweave/__init__.py`) base64-encodes the raw `bytes` and passes `mimeType`/`charset` through to the native side. This is exactly the path the UTF-16 `xml-to-csv` case needs — the `charset: "UTF-16"` from the manifest flows into the binding so the XML decodes correctly, the same path the Node runner exercises. No-input cases (`trivial`, `compile-heavy`) pass `{}`.

- **Buffered runs** (`first-run`, `warm`, cold-start child) use `dw.run(script, inputs)` → `ExecutionResult`; guard on `result.success`.
- **Streaming runs** use `dw.run_transform(script, chunked(primary), input_name=<name>, input_mime_type=<mime>, input_charset=<charset>)` → a `Stream`; consume all chunks, then guard on `stream.metadata.success`.

The runner uses the explicit `DataWeave()` class (not the module-level singleton) so the cold-start child controls `initialize()`/`cleanup()` lifecycle precisely and the in-process bench reuses one initialized instance across warm/streaming cases.

## 3. Env stamp + schema conformance

`env.py` produces the mandatory `env` block, symmetric with `lib/env.mjs`:

- **`runner: "python-wrapper"`** — the report column name and dedupe key, symmetric with the Node `"node-wrapper"`.
- **`os: "<sys.platform>-<machine>"`** — e.g. `darwin-arm64`. Normalize `platform.machine()` to Node's `process.arch` vocabulary (map `x86_64`→`x64`; `arm64` is already common) so the label reads identically across runners. The report does not join on `os` (it joins only on `weaveVersion`, for skew) — this is purely for consistent attribution labels, per the parent spec's future time-series intent (`env` fields as series dimensions).
- **`cpu`** — best-effort: `sysctl -n machdep.cpu.brand_string` on macOS, first `model name` from `/proc/cpuinfo` on Linux, else `platform.machine()`.
- **`runtimeVersion: "python <X.Y.Z>"`** — e.g. `python 3.9.6`.
- **`weaveVersion`** — parsed from `gradle.properties` (`^weaveVersion=`), same source as `lib/env.mjs`/`Env.scala`.
- **`commit`** — `git rev-parse --short HEAD` (fallback `"unknown"`).
- **`dwlibBuildId` — a REAL id** (this is a dwlib-based runner, unlike the engine's `"n/a-engine"` sentinel). Same formula as `lib/env.mjs`: `"dwlib-" + sha256(str(size) + first-64KB-of-file).hexdigest()[:8]`. Pointed at the **Python** staging path `native-lib/python/src/dataweave/native/dwlib.{dylib,so,dll}`, and honoring a `DATAWEAVE_NATIVE_LIB` override first so the id reflects the lib actually loaded. The differing staged path (vs. Node's `native-lib/node/native/`) is a legitimate per-runner detail; the identity *formula* is byte-for-byte identical, so an unchanged `dwlib` yields the same id across the Node and Python runners.

**Fail-fast id validation.** Every emitted `id` is checked against the manifest; the run aborts before writing output on any orphan id (matching `validateResultIds`), so the result file always left-joins cleanly in the report.

**Schema conformance.** `emit.py` hand-writes JSON matching `schema/result.schema.json` exactly: top-level `schemaVersion:"1.0"`, `runner`, `env`, `timestamp` (ISO-8601), `cases[]`; each case a flat `{id, metric, unit, stats, iterations}` row with `stats` = `{min, median, p90, p99, mean}`; no stray keys (`additionalProperties:false` respected). Units per row: `ms` for latency (cold-start, first-run, warm), `MB/s` for streaming. Output path: `benchmarks/results/python-<ISO-timestamp>.json` (`results/` is gitignored).

## 4. Stats — nearest-rank percentiles (reimpl of `lib/stats.mjs`)

`stats.py` reimplements `lib/stats.mjs` byte-for-byte in behavior:

- `compute_stats(samples) -> {min, median, p90, p99, mean}` — nearest-rank on a sorted copy: `pct(p) = sorted[min(n-1, max(0, ceil(p/100 * n) - 1))]`; `mean = sum/n`. Raises on empty input.
- `to_mbps(total_bytes, elapsed_ms) -> float` — `total_bytes / 1e6 / (elapsed_ms / 1000)` (decimal MB); raises if `elapsed_ms <= 0`.

This is the one component that can silently drift and corrupt deltas, so it is guarded by an always-on parity test (§6).

## 5. Gradle wiring (three tasks, all in `native-lib/build.gradle`)

`native-lib/build.gradle` already owns the Python toolchain tasks (`stagePythonNativeLib`, `pythonTest`, `buildPythonWheel`) and `benchmarkNode`, so all three new tasks live there.

1. **`benchmarkPython`** — the runner, aggregator-registered.
   - `ext.benchmarkRunner = true` so the root `benchmarkCompare` auto-discovers it (no edit to that task — the parent spec's "Adding a runner" contract).
   - Opt-in only: `onlyIf { project.findProperty('benchmark')?.toString()?.toBoolean() == true }`.
   - `dependsOn tasks.named('stagePythonNativeLib')` — the runner needs the staged `dwlib` at `python/src/dataweave/native/`.
   - Runs `node corpus/gen-inputs.mjs && python3 runners/python/emit.py` from `benchmarks/`. It shells to the **Node** input generator for the shared deterministic large input — the same choice the engine runner made; reimplementing generation in Python risks byte drift and thus skewed MB/s. This is the runner's only runtime `node` touch.
   - **Emit-only:** it does **not** render the report. `benchmarkCompare` runs the report once over all runners.
2. **`benchmarkPythonStatsTest`** — always-on parity guard.
   - `python3 runners/python/test_bench.py` (stdlib `unittest`; **no** `dwlib`, no venv/pip — pure math + manifest parsing).
   - Wired into `native-lib:test` (`tasks.named('test') { dependsOn 'benchmarkPythonStatsTest' }`), gated by the existing `-PskipPythonTests` flag for symmetry with `pythonTest`.
   - Drift in the Python percentile math vs. `lib/stats.mjs` **fails the normal build** — the engine's "any divergence fails the build" stance, one language over.
3. **`benchmarkJsUnitTest`** — always-on Node-harness parity guard (closes the pre-existing gap where the shared JS `lib/` was untested in CI; see §7).
   - `node --test` over the **dwlib-free** JS test files only, listed explicitly: `lib/stats.test.mjs`, `lib/manifest.test.mjs`, `lib/env.test.mjs`, `report/report.test.mjs`, `runners/node/emit.test.mjs`. (Verified: none of these call `loadWrapper()`; `emit.test.mjs` uses `buildResult` with a mock env.)
   - The two **dwlib-dependent** Node tests — `runners/node/warm-bench.test.mjs` and `coldstart.test.mjs` (both call `loadWrapper()`) — are **excluded**; they remain unwired, exercised only in the opt-in benchmark flow where a wrapper is built. Explicit file list (not directory discovery) enforces this boundary.
   - Wired into `native-lib:test`, gated by `-PskipNodeTests` for symmetry with `nodeTest`.

## 6. Testing

- **`test_bench.py`** (always-on, the load-bearing guard):
  - **Stats parity** vs. `lib/stats.mjs` on fixed vectors: `1..100 → {min:1, median:50, p90:90, p99:99, mean:50.5}`; `[5,1,3,2,4] → {min:1, median:3, p90:5, mean:3}` (asserts sort-before-rank); empty input raises; `to_mbps(1_000_000, 1000) == 1.0`, `to_mbps(500_000, 250) == 2.0`, `to_mbps(10, 0)` raises. These expected values are the exact outputs of `lib/stats.mjs` — any divergence fails.
  - **Manifest parsing** against the real corpus: loads all case ids (`trivial`, `object-transform`, `map-scale`, `xml-to-csv`, `json-stream`, `compile-heavy`); `cases_for_metric("streaming")` yields `map-scale` + `json-stream`; `resolve_inputs` on `xml-to-csv` returns one `payload` input with `mimeType application/xml`, `charset UTF-16`, non-empty bytes; `validate_result_ids` raises on an orphan id.
- **dwlib-dependent integration** is the `benchmarkPython` end-to-end run itself (deferred to the opt-in flow, not an always-on test, because it needs a multi-minute native build): confirm it writes `results/python-<ts>.json`, that the file conforms to `schema/result.schema.json`, and that `report.mjs results/*.json` renders a `python-wrapper` column with no skew banner (same `weaveVersion` as Node/engine). Mirrors the engine plan's end-to-end verification step.

## 7. Docs

- **`benchmarks/README.md`:** flip `runners/python/` from "a follow-up" to a built runner in the **Layout** section; add the `benchmarkPython` invocation under **Running**; note the two always-on test tasks. Update **Adding a runner** only if the Python task reveals a gap in the existing instructions (it should slot into the documented two-step contract unchanged).

## Out of scope

- **Any results datastore, dashboard, or CI benchmark upload** — deferred per the parent spec (the `report.mjs --emit` seam remains the future push point).
- **Changes to corpus, schema, `report.mjs`, the Node runner source, or the engine runner** — the Python runner slots into all of them unchanged. The only shared-file edits are `benchmarks/README.md`, `native-lib/build.gradle` (the three tasks + two `test` dependsOn lines), and no `settings.gradle` change (the runner is scripts under the existing `native-lib` module, not a new Gradle subproject — unlike the engine).
- **Wiring the two dwlib-dependent Node tests** (`warm-bench`, `coldstart`) into an always-on task — they need a built wrapper, so they stay in the opt-in path.

## Non-obvious context

- **No JIT warmup floor** (see §1) — the single most important place the Python runner intentionally diverges from the engine. Correct because CPython has no JIT; it keeps Python's `warm` methodology identical to Node's, which is the like-for-like wrapper comparison the headline delta wants.
- **`runner: "python-wrapper"`** is symmetric with `"node-wrapper"`; `report.mjs` dedupes by `runner`, keeping only the latest result per runner, so re-runs never duplicate the Python column. The engine remains the auto-selected delta baseline (`report.mjs:92`).
- **Real `dwlibBuildId` from the Python staging path** — because Python and Node wrap the *same* `dwlib`, an unchanged lib produces the same id in both runners' result files. That is the intended attribution: the two wrappers are benchmarking identical native code.
- **`benchmarkJsUnitTest` is a scope addition** beyond "add the Python runner," folded into this PR because the Python parity test pins against `lib/stats.mjs`, and that shared lib was itself untested in CI. Making the dwlib-free JS harness tests build-gated closes the gap for both runners at once. The dwlib-dependent Node tests are deliberately left out of the always-on set.
