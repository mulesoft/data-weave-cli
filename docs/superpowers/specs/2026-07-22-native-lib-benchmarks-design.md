# Native-lib wrapper benchmarks — design

**Date:** 2026-07-22
**Status:** Approved (design)
**Scope of first deliverable:** Node runner + common JSON schema + report script. Python and Scala-engine runners are follow-up specs; the schema and corpus are designed to accommodate them without change.

> **Superseded by current implementation:** This is the original harness design record.
> Node, Python, engine, and CLI runners are now implemented, and task wiring has
> evolved. Use [`benchmarks/README.md`](../../benchmarks/README.md) and the
> current runner sources for operational documentation; retain this document for
> its original decisions and rationale.

## Purpose

Benchmark the DataWeave native-lib wrappers to serve, from one harness:

- **CI regression gating** — repeatable numbers, comparable across runs.
- **Local perf characterization** — where time goes (wrapper overhead vs. native runtime).
- **Wrapper-overhead / cross-binding comparison** — Node vs. Python later.
- **Native-wrapper vs. current engine tradeoff** — the headline goal: compare the native-image wrappers against the Scala DataWeave engine (`DataWeaveScriptingEngine`).

The comparison is decoupled by construction: every surface runs the **same shared corpus** and emits the **same JSON schema**, and a report script joins the results.

> **Correction (post-spec):** an earlier draft assumed the engine harness had to live in a separate `data-weave` repo at a different version (`2.13.0-SNAPSHOT`), forcing a permanent version-skew caveat. This is wrong. **This repo already depends on the engine runtime as a Maven artifact** — `native-lib/build.gradle` and `native-cli/build.gradle` both declare `api org.mule.weave:runtime:${weaveVersion}`, so `DataWeaveScriptingEngine` is on the JVM classpath here. The engine runner can therefore live in **this** repo and, crucially, run the **same `weaveVersion` (`2.12.0-20260413`) that the native image is compiled from** — eliminating the skew and isolating purely the native-image-vs-JVM axis. The skew banner (below) remains as a safety net for an accidental mismatch, not a standing condition. See "Engine runner" under the layout and the out-of-scope note.

## Metrics (all four)

1. **Cold start / init** — time from a fresh process launch to runtime-ready, before any script runs. The native-image headline vs. JVM startup. Measured via a spawn harness (fresh process per sample), not in-process.
2. **First-run (compile + exec)** — latency of the first execution of a script, including DataWeave compilation. Matters for short-lived / CLI-style usage.
3. **Warm / steady-state exec** — repeated execution of an already-compiled script; report min / median / p90 / p99 / mean.
4. **Streaming throughput** — MB/s for `runStreaming` / `runTransform` over large inputs; exercises the streaming plumbing.

Which metrics apply is declared **explicitly per case** in the manifest (see §2). A runner skips any metric a case does not list.

## Architecture & layout

```
benchmarks/
  corpus/
    manifest.json          # single source of truth: every case, its files, and applicable metrics
    scripts/               # .dwl files, shared by ALL runners (incl. future engine harness)
    inputs/                # committed small input files
    gen-inputs.mjs         # deterministically generates large inputs (not committed)
  schema/
    result.schema.json     # JSON Schema for a runner's output; versioned
  runners/
    node/                  # THIS deliverable — reference runner
      warm-bench.ts        # vitest bench: first-run, warm, streaming
      coldstart.ts         # spawn harness: cold-start / init
      emit.ts              # collect both -> schema-conformant JSON, validate ids, stamp env
    python/                # FOLLOW-UP: same corpus + schema, pytest-benchmark/custom timer + spawn harness
    engine/                # FOLLOW-UP: JVM harness IN THIS REPO — depends on org.mule.weave:runtime (already a
                           #            build dep), drives DataWeaveScriptingEngine over THIS corpus, emits THIS schema
  report/
    report.mjs             # ingest N result JSONs -> markdown/console comparison table
  results/                 # gitignored; per-runner per-run JSON output
  README.md
```

`benchmarks/runners/` is the extension point: one subdirectory per surface, each a self-contained consumer of the shared corpus + schema. Adding Python later means dropping in `runners/python/`, consuming the corpus unchanged, emitting schema-conformant JSON — no changes to Node, corpus, schema, or report. The engine surface is the same story: it lives in **this** repo as a JVM harness under `runners/engine/`, because this repo already depends on the engine runtime (`api org.mule.weave:runtime:${weaveVersion}` in both `native-lib` and `native-cli` build files), so `DataWeaveScriptingEngine` is on the classpath. It runs the **same `weaveVersion` the native image is built from**, reads this repo's corpus, and emits this schema — so the node-vs-engine comparison is on identical runtime code, isolating the native-image-vs-JVM axis.

## 1. Shared corpus

Pure data, no runner code. The corpus is the contract every runner consumes.

- **Scripts** in `corpus/scripts/*.dwl`, shared verbatim across runners.
- **Small inputs** committed in `corpus/inputs/`. **Large inputs** generated deterministically (fixed seed/size params) by `corpus/gen-inputs.mjs` so runs are comparable across machines and runners, and the repo stays lean.
- **Starter case set** (~6–8), chosen to exercise each metric without ballooning runtime:
  - trivial expression (`2 + 2`) — cold-start / first-run / warm
  - object transform — first-run / warm
  - array `map` at scale — first-run / warm / streaming
  - XML → CSV with a non-default charset (UTF-16) — first-run / warm (exercises the charset path)
  - JSON → JSON streaming over a large generated input — first-run / warm / streaming
  - a compile-heavy script — first-run (emphasizes compilation cost)

## 2. Manifest

`corpus/manifest.json` is the single source that maps a case `id` to its files and metrics. `id` is a stable **logical name**, not a file path — it is the join key between corpus and result files.

```json
{
  "cases": [
    {
      "id": "map-transform-large",
      "script": "scripts/map-transform.dwl",
      "inputs": {
        "payload": { "file": "inputs/records-50mb.json", "mimeType": "application/json" }
      },
      "metrics": ["first-run", "warm", "streaming"],
      "iterations": { "warm": 100, "warmup": 20 }
    }
  ]
}
```

- `script` / `inputs[].file` resolve relative to `corpus/`.
- `metrics` — explicit list of which of the four metrics apply to this case.
- `iterations` — per-metric sample/warmup counts (defaults applied when omitted).

**`id` immutability rule:** case ids are permanent. Deprecate, never rename. Renaming silently forks any future historical series and breaks cross-run joins.

## 3. Common JSON schema

One file per runner per run, validated against `schema/result.schema.json`.

```json
{
  "schemaVersion": "1.0",
  "runner": "node-wrapper",
  "env": {
    "os": "darwin-arm64",
    "cpu": "Apple M1 Pro",
    "runtimeVersion": "node v20.11.0",
    "weaveVersion": "2.12.0-20260413",
    "commit": "33ba19c",
    "dwlibBuildId": "<staged dwlib identity>"
  },
  "timestamp": "2026-07-22T12:00:00Z",
  "cases": [
    {
      "id": "map-transform-large",
      "metric": "warm",
      "unit": "ms",
      "stats": { "min": 1.2, "median": 1.4, "p90": 1.7, "p99": 2.1, "mean": 1.5 },
      "iterations": 100
    },
    {
      "id": "map-transform-large",
      "metric": "streaming",
      "unit": "MB/s",
      "stats": { "median": 320.5 },
      "iterations": 10
    },
    {
      "id": "trivial-expr",
      "metric": "cold-start",
      "unit": "ms",
      "stats": { "min": 40, "median": 45, "p90": 52, "p99": 60, "mean": 46 },
      "iterations": 30
    }
  ]
}
```

Rules:
- Each `(case, metric)` pair is one flat row — trivially groupable by the report and a natural time-series point.
- `unit` is per-row (`ms` for latency, `MB/s` for throughput) so latency and throughput coexist.
- **`env.weaveVersion` is mandatory** — the report uses it to flag engine-vs-wrapper skew.
- **`env.commit`** (data-weave-cli SHA) and **`env.dwlibBuildId`** are recorded now for future attributable history, even though nothing consumes them yet. Backfilling identity onto old runs is impossible, so the fields exist from day one.
- `cases[].id` MUST be a manifest id. Runners validate every emitted id against the manifest and **fail fast** (abort the run) on an orphan id, so result files always join cleanly.
- `weaveVersion` for the Node wrapper is read from this repo's `gradle.properties`; correct as long as the staged `dwlib` matches the current checkout (true in a normal build).
- `schemaVersion` lets the schema evolve as Python/engine runners join without breaking old result files.

## 4. Node runner (the deliverable)

Two entry points, because true cold-start cannot be measured in-process:

**a) `warm-bench.ts` — in-process, vitest `bench`.** Handles **first-run**, **warm**, **streaming**. Loads the manifest; for each case runs only the metrics it declares:
- *first-run*: fresh `new DataWeave()` + first `run()` of the script (compile + exec).
- *warm*: `bench()` over an already-run script; vitest yields min/median/p-values directly — we do not hand-roll a timing loop where vitest already provides percentiles.
- *streaming*: drive `runStreaming` / `runTransform` over the generated large input; measure bytes / elapsed → MB/s.

**b) `coldstart.ts` — spawn harness.** Spawns N fresh `node` child processes; each does `initialize()` + one trivial `run()` and reports its own elapsed; the parent aggregates min/median/p90/p99/mean. Only way to capture native-image init against a cold OS/runtime cache.

**c) `emit.ts` — collector.** Merges outputs from both entry points into one schema-conformant JSON; stamps `env` (os / cpu / node version / `weaveVersion` from `gradle.properties` / `commit` / `dwlibBuildId`); validates every emitted `id` against the manifest (**fail-fast**); writes `results/node-<timestamp>.json`. vitest `bench` output is captured via its JSON reporter and normalized into the schema here.

**Correctness guards:**
- A case whose `run` returns a non-success result aborts that case with a clear error rather than recording a bogus fast timing — an errored script cannot be benchmarked.
- Generated large inputs are deterministic (fixed seed/size) for cross-run/cross-machine comparability.

## 5. Report script

`report/report.mjs`:
- Inputs: one or more result JSONs (glob `results/*.json` or explicit paths). `--baseline <runner>` names the comparison anchor (defaults to the engine runner when present, else the first).
- Loads `manifest.json` as authority; left-joins result rows by `id`.
- Output: a markdown/console table grouped by **case × metric**, one column per runner, plus a **Δ vs baseline** column (percent). Direction is per-unit: streaming (MB/s) higher-is-better, latency (ms) lower-is-better, so deltas read correctly.
- **Skew banner:** if runners' `env.weaveVersion` differ, print a prominent warning above the table (e.g. engine `2.13.0-SNAPSHOT` vs wrapper `2.12.0-20260413`) so the comparison is never read as clean.
- Manifest case with no result row → shown as "not run" for that runner. Result row with unknown `id` → flagged defensively (should not occur given fail-fast upstream).
- **Exporter seam:** a documented `--emit <target>` extension point (no target built in this deliverable). The report already reads flat result JSONs; a future sink (CSV / SQL / time-series push) is additive.

## 6. Gradle wiring

- New `native-lib:benchmark` task, **off by default** and opt-in via `-Pbenchmark=true` (consistent with existing `skipNodeTests` / `skipPythonTests` conventions). Runs the Node runner (both entry points) → emits JSON → optionally invokes the report.
- Prerequisites match `nodeTest`: staged `dwlib` + built Node addon.
- The normal `build` / CI path is unaffected — benchmarks run only when explicitly requested.

## 7. Results accumulation (current answer + seam)

For this deliverable, results are **local-only and ephemeral**: `results/` is gitignored, one JSON per run, not committed or collected. This is deliberate — a regression gate and local profiling do not need accumulation, and committing machine-varying timing JSON would be git noise.

The design keeps a durable-history feature a bolt-on rather than a redesign:
- Flat `(runner, id, metric, unit)` rows are natural time-series keys; `stats.median` / MB/s are the values.
- `env` fields (os / cpu / runtimeVersion / weaveVersion / commit / dwlibBuildId) map to series labels and make every point attributable.
- Immutable `id` keeps series stable across runs.
- The report `--emit` seam is the future push point.

**Explicitly deferred (YAGNI):** the datastore, the push step, the dashboard (e.g. Grafana), and CI result upload. A follow-up "benchmark history" spec picks the sink (CI artifact / committed baseline / external time-series store) once the need is concrete.

**Documented caveat, not solved here:** cross-machine noise. Historical series are only meaningful if runs come from a dedicated bench machine, or `env` (cpu/os) is treated as a series dimension so numbers are never averaged across hardware. The `env` labels already permit the latter.

## Out of scope (this spec)

- Python runner (`runners/python/`) — follow-up.
- Engine baseline harness — follow-up, but **built in this repo** (not the `data-weave` repo) as a JVM runner under `runners/engine/`, using the already-present `org.mule.weave:runtime` dependency. This deliverable ships the corpus + schema it will consume; the harness itself is deferred pending a short spec on the JVM-specific decisions (see below).
- Any results datastore, dashboard, or CI benchmark job.

## Non-obvious context

- The engine runner is greenfield (no existing harness to reuse). Because it lives here and uses the Maven `runtime` dependency, it drives the same `DataWeaveScriptingEngine` API that `native-cli`'s `NativeRuntime` already wraps. The remaining JVM-specific design decisions — deferred to the engine runner's own spec — are: (1) warmup iteration counts sufficient for the JIT to reach steady state before `warm` sampling; (2) whether cold-start/first-run spawn fresh JVMs (`java -cp … Child` per sample, the honest JVM cold path incl. classload) mirroring the Node spawn harness; (3) whether "engine" means the bare `DataWeaveScriptingEngine` or the CLI's `NativeRuntime` wrapper. Timing must use the same methodology as the Node runner (`System.nanoTime()` ⇢ ms), which is why that methodology was hand-rolled rather than delegated to a JS bench library.
- Engine and wrapper are on different weave versions; the skew banner is a first-class requirement, not a nicety.
