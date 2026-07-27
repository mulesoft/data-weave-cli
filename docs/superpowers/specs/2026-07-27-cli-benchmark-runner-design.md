# CLI benchmark runner — design

_2026-07-27_

## Goal

Add a fourth runner to the `benchmarks/` harness that measures the **`dw` native
CLI**. Today the harness compares three runners over a shared corpus: the Node and
Python `native-lib` wrappers (native shared library via FFI) and a JVM engine
baseline (`DataWeaveScriptingEngine`). The CLI runner completes the picture with
the shipped native executable, driving `NativeRuntime` — the CLI's own engine
wrapper — through a GraalVM native image.

The runner integrates through the harness's existing extension contract: emit a
result file conforming to `schema/result.schema.json` and tag a Gradle task
`ext.benchmarkRunner = true`. The root `benchmarkCompare` auto-discovers it; no
edit to that aggregator, `report.mjs`, or the schema is required.

## Why a CLI runner is distinct

The three quadrants of the DataWeave runtime are:

- **engine** — JVM library (`DataWeaveScriptingEngine` directly).
- **node / python** — native shared library (`dwlib`) via FFI.
- **cli** — native standalone executable (`dw`), driving `NativeRuntime`.

Because `NativeRuntime` is the CLI's wrapper over the same scripting engine the
`engine` runner drives raw, the **engine-vs-cli delta isolates native-image vs
JVM** for the same logical runtime work — the same reason the README calls the
cross-runner cold-start comparison meaningful (the native image has no JVM to
boot).

## Key decisions

These were settled during brainstorming; recorded here so the rationale is not lost.

1. **Measure the native binary, not a JVM entrypoint.** A JVM entrypoint runner
   would heavily overlap the existing `engine` runner and would not measure the
   shipped artifact. The native binary is what users actually run.

2. **The binary emits a `READY` marker in benchmark mode.** Rather than treat the
   binary as a black box (which would force one whole-process number per
   invocation and lose the init/exec split), we add a benchmark harness *inside*
   `native-cli` that prints `READY` the instant `NativeRuntime` is constructed.
   This reuses the exact parent/child protocol the Node/Python/engine runners
   already use, yielding a true `cold-start` **and** a like-for-like `first-run`
   **and** a `warm` steady-state loop — all from the real native binary.

3. **Build-time gate, stripped from production.** The benchmark harness must not
   ship in the production `dw`. This is a security-sensitive CLI (`--untrusted`,
   `--privileges`); an always-reachable alternate entrypoint is both a footgun (a
   stray `DW_BENCH` env var could hijack a normal invocation) and unwanted
   surface. A generated `BenchmarkMode.ENABLED` constant is `false` in normal
   builds and `true` only under `-Pbenchmark=true`. Native-image reachability
   analysis folds the harness away as dead code when `ENABLED` is a compile-time
   `false`, so the shipped `dw` contains no benchmark code and no consumer can
   reach it. Cost: the benchmark build performs its own `nativeCompile`.

4. **Corpus only.** The runner measures the shared corpus, exactly like the other
   three runners — no ad-hoc user-script mode, no arbitrary-subcommand timing.
   (Arbitrary subcommands like `dw spell`/`dw validate` cannot reuse the
   cold/first split anyway: init is entangled inside each command, so there is no
   single honest "runtime ready" boundary to mark. Timing those would be a
   separate whole-process tool with a non-comparable metric — explicitly out of
   scope.)

5. **No `streaming` metric.** The `dw run` path writes a whole output stream and
   has no chunked-input FFI like the library's `runTransform`. The CLI emits
   `cold-start`, `first-run`, and `warm` only. The report renders absent cells as
   `—`, so this needs zero report or schema changes — it is a documented,
   honest gap, matching how each runner declares only the metrics it can produce.

## Architecture

Parent/child split mirroring the Node runner, but **the child is the `dw` binary
itself** running in a build-gated benchmark mode.

### Child — benchmark harness inside `native-cli`

- `DWCLI.main` checks, before any picocli parsing or banner output:
  `if (BenchmarkMode.ENABLED && <env var set>) → BenchmarkHarness.main(args)`.
  In a production build `BenchmarkMode.ENABLED` is a compile-time `false`, so the
  entire branch (and `BenchmarkHarness`) is unreachable and tree-shaken out of the
  native image.
- `BenchmarkHarness` is **corpus-agnostic**: it knows nothing about
  `manifest.json`. The parent passes it a script file, input files with explicit
  mime/charset, a mode, and iteration counts. This keeps `native-cli` free of
  benchmark-corpus knowledge and keeps the (gated) footprint minimal.
- The harness constructs **one** `NativeRuntime` and drives it through
  `NativeRuntime.run(script, "bench", inputs, out)`. It reuses the Scala building
  blocks already present in the engine runner package where practical
  (`CountingOutputStream`, the "READY then single JSON line" pattern).

Invocation shape (args produced by the parent):

```
DW_BENCH=1 dw --bench-mode=coldfirst \
  --script=<file> \
  --input=payload=<file>:application/json:utf-8 \
  [--warmup=N --iters=M]
```

(The exact env-var name and flag spelling are an implementation detail; a
specific, collision-unlikely name is used. `ENABLED` is what actually gates
reachability — the env var only selects mode within an already-bench-enabled
binary.)

#### Mode `coldfirst` — one fresh process per sample

The parent spawns this N times; each process yields one cold-start + one first-run.

1. Read script + input files into memory.
2. Construct one `NativeRuntime` (the init being measured from outside).
3. Print `READY\n` and **flush immediately**, before any banner/logging can
   interleave. The parent stamps `cold-start` = wall-clock from spawn to this
   marker (process launch + image load + runtime init).
4. `nowNs()`; `runtime.run(script, "bench", inputs, CountingOutputStream)`;
   measure `first-run` in-process.
5. Assert success. On failure: non-zero exit + stderr, so a bad sample never
   records a bogus timing (same contract as `runners/node/coldstart.mjs`).
6. Print one JSON line: `{"firstRunMs": <n>}`.

#### Mode `warm` — single process, in-process loop

Mirrors `runners/node/warm-bench.mjs`.

1. Construct one `NativeRuntime`, print `READY`.
2. `warmup` unmeasured `run()` calls, then `iters` measured `run()` calls.
3. Print `{"warmMs": [<samples>]}`. The parent computes stats via the shared
   `computeStats`, so the stats path is identical across runners.

#### Output discipline

`dw` prints a banner and can log to stdout. The harness runs with logging silenced
and writes transformation output to a `CountingOutputStream` (never real stdout),
so the only stdout is `READY` + the JSON line. The parent reads the **last** JSON
line and tolerates stray lines (the engine parent already does this).

### Parent — `benchmarks/runners/cli/` (Node)

Mirrors `runners/node/` file layout and reuses the shared libs
(`lib/manifest.mjs`, `lib/stats.mjs`, `lib/env.mjs`) verbatim.

- **`locate.mjs`** — resolves the **benchmark-enabled** `dw` binary. Env override
  `DW_BENCH_BIN=/abs/path/to/dw`, else the default native build output
  `native-cli/build/native/nativeCompile/dw{,.exe}`. Fails fast with a
  "build the bench binary (`./gradlew native-cli:nativeCompile -Pbenchmark=true`)"
  message if absent (same shape as `runners/node/wrapper.mjs`).
- **`coldstart.mjs`** — spawns `dw` in `coldfirst` mode per sample; stamps
  cold-start at spawn→`READY`, parses `firstRunMs`. Structurally
  `runners/node/coldstart.mjs` with the child command swapped from
  `node coldstart-child.mjs` to `dw --bench-mode=coldfirst …`. Same
  reject-on-failure contract.
- **`warm.mjs`** — spawns `dw` once per warm case in `warm` mode, reads the
  `warmMs[]` array, runs it through the shared `computeStats`.
- **`emit.mjs`** — entrypoint. `gatherEnv({ runner: "cli", runtimeVersion:
  "dw <version>" })`, run cold+first then warm, `validateResultIds`, write
  `results/cli-<timestamp>.json`. Same structure as `runners/node/emit.mjs`.

### Metrics emitted

Per case, gated by that case's declared `metrics[]` in `corpus/manifest.json`
(no manifest changes needed).

| metric | how measured | comparable to |
|---|---|---|
| `cold-start` | spawn → `READY` (launch + image load + `NativeRuntime` init) | node/python/engine cold-start — **headline: native binary vs JVM** |
| `first-run` | in-process compile+exec, first call | node/python/engine first-run — like-for-like (both exclude launch) |
| `warm` | in-process steady-state loop | node/python/engine warm |
| `streaming` | not emitted | — (documented gap) |

Env fields: `runtimeVersion` from `dw --version`; `dwlibBuildId = "n/a-cli"`
(following the engine runner's `"n/a-engine"` convention — the CLI is a binary,
not the staged `dwlib`).

## Gradle wiring

In `native-cli/build.gradle`:

- **`genBenchmarkMode`** — generates `BenchmarkMode.java` (constant `ENABLED`)
  into `build/genresource/`, following the existing `genVersions` /
  `ComponentVersion` pattern. `ENABLED = true` only when `-Pbenchmark=true`, else
  `false`. Wired onto the compile classpath.
- The benchmark-enabled native image reuses `nativeCompile` invoked with
  `-Pbenchmark=true` (the flag flows into `genBenchmarkMode`). Normal `build`/CI
  never sets it, so the shipped `dw` is built with `ENABLED=false` and the harness
  is tree-shaken out.
- **`benchmarkCli`** — `onlyIf { benchmark==true }`, `ext.benchmarkRunner = true`,
  `dependsOn nativeCompile` and the shared `genBenchInputs`; runs
  `node corpus/gen-inputs.mjs && node runners/cli/emit.mjs`. It does **not**
  render the report. The root `benchmarkCompare` auto-discovers it via the
  `benchmarkRunner` tag — no edit to that task, per the README "Adding a runner"
  contract.

## Error handling

Same fail-fast contract as `coldstart.mjs` / `EngineChild`:

- Child non-zero exit → parent rejects with stderr.
- Missing `READY` marker → reject.
- Missing JSON line → reject.
- `result.success == false` inside the harness → non-zero exit + stderr.

A bad sample never records a bogus timing. `locate.mjs` fails with a clear
build-the-bench-binary message when the artifact is absent.

## Testing

Follows the repo's parity-guard split: dwlib/binary-free tests run in the normal
`test`; binary-dependent tests are benchmark-only.

- **Scala** (`native-cli:test`, scalatest `AnyFreeSpec`/`Matchers`):
  - `BenchmarkHarness` arg parsing (script / input / mode / iters).
  - `coldfirst` and `warm` each emit exactly `READY` + one JSON line.
  - Transformation output goes to the counting stream, not stdout.
  - Failure path → non-zero exit + stderr.
  - Guard: `BenchmarkMode.ENABLED` is `false` in a normal build.
- **JS** (`benchmarkJsUnitTest`, dwlib-free, always-on parity set):
  - `locate.mjs` resolution + `DW_BENCH_BIN` override.
  - `emit.mjs` result-object builder.
  - Parent parsing of child stdout, using fixtures / a fake child — no real
    binary.
- **Smoke** (benchmark-only, needs the bench binary): one real `coldfirst` spawn
  on the `trivial` case asserting a positive cold-start and a `firstRunMs`.

## Out of scope

- Ad-hoc user-supplied script benchmarking.
- Timing arbitrary `dw` subcommands (`spell`, `validate`, `wizard`, `repl`).
- `streaming` metric for the CLI.
- Any change to `report.mjs`, `schema/result.schema.json`, `benchmarkCompare`, or
  the corpus manifest.

## Documentation

Update `benchmarks/README.md`: add `runners/cli/` to the layout, note the
build-gated bench binary and `-Pbenchmark=true` requirement, and record that the
CLI emits `cold-start`/`first-run`/`warm` (no `streaming`). Add the single-runner
invocation (`./gradlew native-cli:benchmarkCli -Pbenchmark=true`).
