# DataWeave native-lib benchmarks

Language-agnostic benchmark harness for the DataWeave native-lib wrappers.

## Layout

- `corpus/` — shared benchmark cases: `manifest.json` (the contract), `scripts/*.dwl`,
  `inputs/` (committed small inputs), `inputs/generated/` (regenerated large inputs),
  `gen-inputs.mjs` (deterministic generator).
- `schema/result.schema.json` — the JSON schema every runner's output conforms to.
- `lib/` — dependency-free shared modules (stats, manifest, env).
- `runners/node/` — the Node reference runner. `runners/engine/` is the JVM baseline
  (Scala/Gradle subproject `:benchmarks-engine`, depends on `org.mule.weave:runtime` at
  the same `weaveVersion` the native image is built from). `runners/python/` is the
  Python runner (stdlib scripts under `native-lib`, wrapping the same staged `dwlib` as Node).
- `report/report.mjs` — joins result files against the manifest and prints a comparison table.
- `results/` — gitignored per-run output.

## Metrics

`cold-start` and `first-run` (fresh process per sample), `warm` (in-process steady state),
`streaming` (MB/s). Each case declares which apply via `metrics[]`.

**Cold-start is measured by the parent, not the child** — every runner spawns a fresh
child that prints a `READY` marker the instant its runtime is initialized, and the parent
records wall-clock from just-before-spawn to that marker. So cold-start includes process
launch + library/class load + runtime init on all three runners, which is what makes the
native-image-vs-JVM comparison meaningful (the native image has no JVM to boot; the JVM's
cold cost *is* launch + classload). Adding a runner requires the same protocol: print
`READY` (flushed) after init, then a JSON line with the in-process `firstRunMs`. Note only
the first sample sees a truly cold OS page cache; the reported median is warm-cache init.

## Prerequisites

The Node and Python runners benchmark the staged `dwlib` (a GraalVM native image), so
running them — and therefore `benchmarkCompare` — requires the same GraalVM toolchain as the
rest of the repo: **GraalVM (Java 21+, `graalvm-community`)** with `native-image`, `GRAALVM_HOME`
and `JAVA_HOME` set to it (see the root README / `CLAUDE.md`). The pinned build JDK is
`graalvmVersion` in `gradle.properties`. The **engine runner alone** drives the JVM
`DataWeaveScriptingEngine` and runs on any JDK — no native image required.

## Running

The one-shot cross-runner comparison — runs **every** registered runner and prints the table:

    ./gradlew benchmarkCompare -Pbenchmark=true          # all runners + comparison report

Single-runner options:

    ./gradlew native-lib:benchmark -Pbenchmark=true              # Node only: build wrapper, run, report
    ./gradlew benchmarks-engine:benchmarkEngine -Pbenchmark=true # engine (JVM) only: writes results/engine-<ts>.json
    ./gradlew native-lib:benchmarkPython -Pbenchmark=true        # Python only: writes results/python-<ts>.json

Or directly, once the wrapper is built (`./gradlew native-lib:buildNodePackage`):

    node corpus/gen-inputs.mjs                            # generate large inputs first (idempotent)
    node runners/node/emit.mjs                            # writes results/node-<ts>.json
    node report/report.mjs results/*.json                 # renders the table

Results are local-only; no history is accumulated (see the design spec).

## Adding a runner

`benchmarkCompare` (defined in the root `build.gradle`) auto-discovers runners — you do
**not** edit that task to add one. A new runner integrates itself in two steps:

1. In the runner's Gradle module, register a task that runs the runner and writes its
   result to `benchmarks/results/<runner>-<timestamp>.json`, conforming to
   `schema/result.schema.json`. The result's `runner` field is the report's column name
   and the dedupe key. The task should **not** render the report — `benchmarkCompare`
   does that once, over all runners. (Follow `benchmarks-engine:benchmarkEngine` for a
   JVM runner or `native-lib:benchmarkNode` for a scripted one.)
2. Tag that task: `ext.benchmarkRunner = true`.

`benchmarkCompare` then runs it alongside the others and includes its column in the table.
`report.mjs` keeps only the latest result per runner, so re-runs never duplicate a column.
