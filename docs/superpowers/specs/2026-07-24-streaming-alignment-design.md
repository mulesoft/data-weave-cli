# Streaming methodology alignment — design

**Date:** 2026-07-24
**Branch:** `W-23545283-native-lib-benchmarks`
**Status:** approved (design), pending implementation plan

## Problem

The benchmark's `streaming` metric is not like-for-like across runners, so its
cross-runner delta is meaningless. Review finding #2 shipped a stopgap: the report
marks `streaming` non-comparable and prints `n/a` instead of a delta. This design
removes the asymmetry itself so the delta becomes meaningful, and then removes the
suppression.

### Ground truth (verified against source, not assumed)

Investigated `~/Documents/mule-emu/data-weave` (the DataWeave runtime) and this repo.

| runner | input path | output path |
|---|---|---|
| **engine** (this repo) | materialized `Array[Byte]` bound as `BindingValue` (`EngineShell.run`) | plain `output application/json`, single `shell.run` — **batch** |
| **node** | 64KB-chunked generator via `runTransform` (`warm-bench.mjs`) — genuinely streamed | plain output → **materialized then wrapped** |
| **python** | 8KB chunks fed to a `PipedInputStream` via a feeder thread (`NativeLib.java`) — genuinely streamed | plain output → **materialized then wrapped** |

Two independent asymmetries:

1. **Input:** the engine materializes the whole input as a byte array; native-lib
   runners stream input incrementally (Node 64KB generator; Python/native-lib a real
   `PipedInputStream` fed by a background `InputCallbackFeeder` thread —
   `NativeLib.java:155-258`, `ScriptRuntime.parseJsonInputsToBindings:165`).
2. **Output:** the shared corpus streaming scripts (`map-scale.dwl`, `json-stream.dwl`)
   declare plain `output application/json`. `DeferredWriter` (`DeferredWriter.scala:29-93`)
   only produces output concurrently — writer on a scheduler thread writing a
   `PipedOutputStream`, `result` returning the live `PipedInputStream` — when the script
   declares `deferred=true`. Without it, the document is materialized before the stream
   is handed back (`:82-83, :88-92`). So output is materialized on **both** runner
   families today.

### Probe evidence

Ran both streaming corpus scripts through the Python binding's `run_transform` against
the prebuilt `dwlib.dylib` (unchanged since May; exports
`run_script_input_output_callback`). Both plain and `deferred=true` variants succeed and
produce identical output (5 chunks, 34103 bytes). The uniform ~8192-byte output chunks
are NativeLib's read-loop buffer size (`NativeLib.java:208`), **not** writer-driven, so
they do not by themselves prove lazy vs. materialized output — that conclusion comes from
the `DeferredWriter` source. Caveat: at ~20KB the payload fits inside the 64KB pipe
buffers, so concurrency could not be *observed* by timing; the deferred-vs-materialized
distinction is established from source, not measurement.

### Feasibility (verified)

- Engine runner builds a bare `DataWeaveScriptingEngine` with
  `ServiceManager(customServices)` that does **not** set a security manager. The default
  is `NoSecurityManagerService`, which grants every privilege including `DEFERRED`
  (`EvaluationContext.scala:409`, `SecurityManagerService.scala:78-88`). So `deferred=true`
  works in the engine runner with **no privilege plumbing**.
- native-lib already reads an `InputStream` result incrementally
  (`ScriptRuntime.runStreaming` + `NativeLib.java:208` read loop), so `deferred=true`
  output requires **no** native-lib code change — only the script changes.

## Goal

After this change, `streaming` on all three runners measures the same operation:
**input fed in fixed-size chunks, concurrent (deferred) output pulled incrementally**,
throughput = `inputBytes / 1e6 / (ms / 1000)` (unchanged formula). The report's `n/a`
suppression for `streaming` is removed and the delta is meaningful again.

Non-goals: warm / first-run / cold-start methodology (untouched); review findings
#3–#13; changing the throughput formula or the stats math.

## Decisions

- **Deferred scoping — separate script variants.** The streaming scripts are shared with
  the `warm`/`first-run` metrics. Adding `deferred=true` to the shared `.dwl` would route
  those metrics through the async deferred writer and corrupt their measurements. So
  streaming gets its own script variant, selected only for the streaming metric.
- **Engine input source — in-memory bytes wrapped as a chunked `InputStream`** (not a real
  pipe + feeder thread). The bytes are already resident before the clock starts (same as
  native-lib's pre-resident chunks), so the source of bytes is outside the measured runtime
  path; what matters is that the runtime reads input lazily, which the wrapped stream
  provides. A pipe+feeder would add threading complexity without changing what's measured.
- **Engine chunk size — 64KB**, matching Node's `chunked(buffer, 65536)`. Python uses 8KB;
  chunk size affects only how the input is paced into the runtime, not the throughput
  denominator, and cross-runner chunk-size parity is not a goal (the runtimes differ anyway).

## Design

### Corpus

- New files `scripts/map-scale.stream.dwl` and `scripts/json-stream.stream.dwl`: identical
  transform bodies to their base scripts, but `output application/json deferred=true`.
- Manifest (`corpus/manifest.json`): streaming cases gain an optional `streamingScript`
  field pointing at the variant. Resolution rule: `warm`/`first-run` use `script`;
  `streaming` uses `streamingScript ?? script`.

### Manifest parsers (all three runners)

Add a `resolveStreamingScript(manifest, case)` helper alongside the existing script
resolver in `lib/manifest.mjs`, `runners/python/manifest.py`, and
`runners/engine/.../Manifest.scala`. Streaming code paths call it; warm/first-run paths
keep calling the existing `resolveScript`.

### Engine runner (the substantive change)

`EngineShell` keeps its existing `run(script, name, inputs, out)` **unchanged** (used by
warm/first-run). Add a separate streaming path so the batch path is not disturbed:

```
EngineShell.runStreaming(script, name, inputStream: InputStream, inMime, inCharset): Long
  - bind BindingValue(inputStream, Some(inMime), Map.empty, charset)   // lazy input
  - compile the (deferred) script via the same config builder
  - compiled.write(bindings, serviceManager, target=None) → DataWeaveResult
  - require result content is an InputStream (deferred); drain it in a read loop,
    counting bytes; return the drained byte count
```

`WarmBench.runStreaming` changes to:
- resolve the streaming script variant,
- wrap the primary input bytes in a small `ChunkedInputStream` (64KB reads over the byte
  array) — a new tiny helper class in the engine runner package,
- time the full feed+drain of `EngineShell.runStreaming`,
- compute MB/s over `primaryBytes` (unchanged denominator).

The existing streaming-only warmup guard (`WarmBench.scala:55-60`) is preserved.

### native-lib runners

Script-only change: the streaming loops in `warm-bench.mjs` and `warm_bench.py` resolve
the `streamingScript` variant (via the new manifest helper). No binding or FFI change —
`deferred=true` already runs green through `run_transform`/`runTransform` (probe-confirmed).

### Report

- Remove `"streaming"` from `NON_COMPARABLE_METRICS` in `report.mjs`. `formatDelta` keeps
  its `n/a` branch only for genuinely non-comparable metrics (now empty set) and its `—`
  branch for missing baselines. The streaming footnote is removed.
- `RESULTS.md` streaming rows are regenerated from a fresh benchmark run — not
  hand-edited. Until a fresh run is produced, the committed report may still show `n/a`;
  the plan will regenerate it.

## Testing

- **Engine `WarmBenchTest`:** a streaming-path test that runs a `deferred=true` script
  through `EngineShell.runStreaming` and asserts the drained output is non-empty and the
  byte count is stable (guards against silent truncation, since deferred errors surface via
  logging rather than the return — `DeferredWriter.scala:71-76`).
- **Engine `EngineShellTest`:** assert `runStreaming` binds an `InputStream` (lazy input)
  and returns a positive drained byte count for a known input.
- **Report test:** replace the current "streaming is n/a" assertions with assertions that a
  streaming row now carries a real numeric delta (`comparable === true`, `formatDelta`
  returns a signed percent), and that no streaming footnote is emitted.
- **Manifest tests (all three runners):** `streamingScript` resolves to the variant for the
  streaming metric and to the base script for warm/first-run; the variant declares
  `deferred=true`.

## Risks

- **Async error visibility.** Deferred output runs on a scheduler thread; a mid-stream
  script error is logged, not thrown from `write` (`DeferredWriter.scala:71-76`). Mitigation:
  the engine drain loop and its test assert the full expected byte count so truncation is
  caught.
- **Concurrency not observable at current corpus size.** The 20KB input fits in the pipe
  buffer, so deferred concurrency won't show up as a timing win — but it makes the operation
  *semantically* like-for-like across runners, which is the goal. A larger streaming input
  could be added later if measurable concurrency is wanted (out of scope here).
- **Reversibility.** If aligned numbers prove noisy or misleading, revert is clean: re-add
  `"streaming"` to `NON_COMPARABLE_METRICS` and restore the footnote. The corpus variants
  and engine streaming path can remain.
