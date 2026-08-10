# CLI End-to-End Benchmark — Design

**Date:** 2026-08-10
**Status:** Implemented

## Goal

Make the CLI benchmark measure the customer-visible `dw run` command rather
than an in-process benchmark harness. The CLI runner emits only `first-run`,
defined for this runner as whole-command latency.

## Metric Semantics

For the CLI runner, `first-run` is wall-clock time from just before spawning a
normal `dw run` process until its successful exit. It includes process launch,
native-image load, CLI argument parsing, `NativeRuntime` construction, script
compilation, execution, and output writing.

This differs intentionally from the in-process `first-run` emitted by the
Node, Python, and engine runners. The README and report output must state the
distinction so cross-runner readers do not interpret the values as equivalent
microbenchmarks.

The CLI emits no `cold-start`, `warm`, or `streaming` rows. The shared schema
continues to support those metrics for the other runners.

## Runner Architecture

`benchmarks/runners/cli/` becomes a normal-command parent only:

- Read the shared manifest and select cases declaring `first-run`.
- For every configured sample, spawn the selected `dw` binary using its normal
  `run` command with the corpus script and declared inputs. Input MIME types are
  inferred by the existing CLI from file extensions; the current UTF-16 XML
  corpus input has been verified through this public path and remains included.
- Capture stdout and stderr, fail the sample on nonzero exit, and measure
  spawn-to-exit elapsed time with `process.hrtime.bigint()`.
- Aggregate samples with the shared `computeStats` helper and emit standard
  flat result rows using metric `first-run` and unit `ms`.

The runner must use the same production binary that customers invoke.
`DW_BENCH_BIN` remains an optional path override, but it no longer requires a
benchmark-enabled artifact.

## Removed Components

Remove all benchmark-only behavior from `native-cli`:

- Generated `BenchmarkMode` source and its Gradle generation task/wiring.
- `DWCLI` dispatch based on `BenchmarkMode` and `DW_BENCH`.
- `BenchmarkHarness.scala` and its test suite.
- Build-time `-Pbenchmark=true` requirement for compiling a benchmark harness.

Remove the CLI runner's `coldstart.mjs` and `warm.mjs`, including their
`coldfirst`, `warm`, and `READY` protocol handling. Replace them with one
normal-command sampling module and focused parent-level tests using a fake
child process; tests must not require a native binary.

## Gradle and Documentation

`native-cli:benchmarkCli` remains an opt-in, aggregator-registered task. It
continues to depend on `nativeCompile` when `DW_BENCH_BIN` is absent and skips
that dependency when the override is set. It no longer relies on
`-Pbenchmark=true` to make the selected binary capable of benchmark execution;
the property gates task execution only.

Update `benchmarks/README.md` to document the CLI's end-to-end `first-run`
semantics and its absence of `cold-start`, `warm`, and `streaming`. Update
report text/labels to distinguish CLI end-to-end `first-run` from in-process
first-run results. Mark the prior CLI benchmark design as superseded by this
document.

## Testing

- Unit-test command construction, successful sample parsing, nonzero-exit
  handling, and timing-row aggregation through injected/fake child execution.
- Verify the CLI emitter produces only `first-run` rows from a representative
  manifest fixture.
- Run the dependency-free Node benchmark-harness test task.
- Run Gradle dry runs with and without `DW_BENCH_BIN` to confirm the existing
  dependency behavior remains intact.
