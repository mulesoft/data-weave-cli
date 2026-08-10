# Engine runner (JVM benchmark baseline) — design

**Date:** 2026-07-23
**Status:** Approved (design)
**Parent spec:** [`2026-07-22-native-lib-benchmarks-design.md`](./2026-07-22-native-lib-benchmarks-design.md) — this resolves the three JVM-specific decisions that spec deferred to the engine runner.

> **Superseded by current implementation:** This document records the original
> JVM runner design. For current task wiring, runner layout, and usage, use
> [`benchmarks/README.md`](../../benchmarks/README.md) and
> [`benchmarks/runners/engine/`](../../benchmarks/runners/engine/). Retain this
> document for its original decisions and rationale.

## Purpose

Build the **engine runner** — the JVM baseline the native-lib wrappers are compared against. It drives the DataWeave engine over the **same shared corpus** the Node runner consumes and emits the **same JSON schema**, so `report/report.mjs` joins the results and produces the headline delta: **native-image wrappers vs. the JVM engine**.

The comparison is clean by construction. This repo already depends on the engine runtime as a Maven artifact (`api org.mule.weave:runtime:${weaveVersion}` in both `native-lib` and `native-cli`), so the harness runs the **same `weaveVersion` the native image is built from** (`2.12.0-20260413`). There is no version skew to caveat — the delta isolates purely the native-image-vs-JVM axis. The report's skew banner remains a safety net, not a standing condition.

## Decisions resolved (the three the parent spec deferred)

The parent spec's "Non-obvious context" listed three open JVM-specific decisions. Resolved here:

1. **What "engine" means** → the **bare `DataWeaveScriptingEngine`** (the low-level engine named in the parent spec and the original request), not `NativeRuntime` and not the `DWScriptingEngine` wrapper. The harness drives it directly, mirroring how `native-cli`'s `NativeRuntime.scala` sets it up.
2. **Cold-start / first-run methodology** → **fresh JVM per sample** (`java -cp … EngineChild <case>`), mirroring the Node `coldstart-child.mjs` spawn harness. This captures the honest JVM cold path — process launch + classload + engine init + first compile+exec — which is exactly the overhead the native image eliminates.
3. **JIT warmup** → **JVM warmup floor**: effective warmup = `max(manifest.warmup, 2000)`, so C2 reaches steady state before `warm` sampling. The manifest stays the source of truth for sample counts; the floor acknowledges JIT reality. The effective warmup used is logged.

Timing is `System.nanoTime()` → ms throughout, matching the Node methodology (the reason that methodology was hand-rolled rather than delegated to a JS bench library).

## Language & placement

- **Language: Scala 2.12.18** (the repo's `scalaVersion`). Mirrors `native-cli/src/main/scala/org/mule/weave/dwnative/NativeRuntime.scala`, which is the direct template for driving the engine. The root `build.gradle` already applies the `scala` plugin and `implementation org.scala-lang:scala-library:${scalaVersion}` to **every** subproject, and `org.mule.weave:runtime` is itself a Scala library — so choosing Scala brings **no new dependency**. Driving the raw Scala engine API (`ScriptingBindings`, `ServiceManager`, `InputType`, `ParserConfiguration`, Scala collection interop) from Scala avoids the interop boilerplate (`Option.apply`, `Map$.MODULE$`, `Seq` conversions) that `native-lib/.../ScriptRuntime.java` has to carry.
- **Placement:** `benchmarks/runners/engine/`, exactly where the parent spec's layout diagram puts it. It is a self-contained Gradle subproject depending on `org.mule.weave:runtime`, reading the shared corpus and emitting the shared schema.

## Architecture & layout

```
benchmarks/runners/engine/
  build.gradle                     # subproject: api org.mule.weave:runtime + core-modules, application, scalatest
  src/main/scala/org/mule/weave/benchmark/engine/
    EngineShell.scala              # minimal DataWeaveScriptingEngine setup + one run() (the "bare engine")
    EngineChild.scala              # main: fresh-JVM worker — times init + first-run for one case, prints one JSON line
    WarmBench.scala                # in-process: warm (JIT floor) + streaming (MB/s)
    Emit.scala                     # main: spawn children, run warm/streaming, stats + env + id-validate, write result JSON
    Stats.scala                    # nearest-rank percentiles — parity with lib/stats.mjs (guarded by a test)
    Manifest.scala                 # parse corpus/manifest.json, resolve script + input files
    Env.scala                      # env stamp: os/cpu/jvm/weaveVersion/commit
  src/test/scala/.../StatsParityTest.scala   # asserts Stats matches lib/stats.mjs on fixed vectors
```

`benchmarks/runners/` is the parent spec's extension point: one subdirectory per surface, each a self-contained consumer of the shared corpus + schema. The engine runner adds one such subdirectory and touches nothing else — corpus, schema, Node runner, and `report.mjs` are all unchanged.

## 1. Gradle & module wiring

- **`settings.gradle`:**
  ```groovy
  include 'benchmarks-engine'
  project(':benchmarks-engine').projectDir = file('benchmarks/runners/engine')
  ```
- **`benchmarks/runners/engine/build.gradle`:**
  - `api group: 'org.mule.weave', name: 'runtime', version: weaveVersion`
  - `api group: 'org.mule.weave', name: 'core-modules', version: weaveVersion` (data formats: JSON/XML/CSV — matches `native-lib`)
  - `application` plugin (for `EngineChild` / `Emit` main classes) + the `scalatest` plugin (matching `native-cli`).
  - Inherits `scala`, `java-library`, `scala-library`, and the mule repositories from the root `subprojects {}` block — no per-module repo/plugin duplication.
- **Task `benchmarkEngine`** on this module, opt-in only (`onlyIf { project.findProperty('benchmark')?.toString()?.toBoolean() == true }`, consistent with `native-lib:benchmark`). Runs `Emit` with the corpus dir as arg → writes `benchmarks/results/engine-<ts>.json`. Never part of `build`/`test`.
- **Report pickup is automatic:** `report.mjs` globs `results/*.json` and already auto-selects `runner === "engine"` as the delta baseline (`report.mjs:92`). No report change.

**SPI note:** data formats and module loaders are registered via `META-INF/services` (`org.mule.weave.v2.module.DataFormat`, `...parser.phase.ModuleLoader`). On a plain JVM classpath (not a fat-jar merge, not native-image) the jar-provided service files resolve as-is — so, unlike the native builds, **no service-file re-materialization is needed**. A missing format would surface as a runtime "unknown mime type"; `core-modules` on the classpath prevents that.

## 2. Engine shell — what "bare engine" means concretely

`EngineShell` is a ~40-line reduction of `NativeRuntime`, keeping only what a benchmark needs:

- **Construction (timed as `initMs`):**
  ```scala
  setupEnv()  // System.setProperty io.netty.processId + io.netty.noUnsafe — from NativeRuntime.setupEnv
  val resolver = ClassLoaderWeaveResourceResolver.apply()
  new DataWeaveScriptingEngine(ModuleComponentsFactory.apply(resolver), ParserConfiguration(), new Properties())
  ```
  No `PathBasedResourceResolver`, no spell/dependency machinery, no composite resolver — just the classloader resolver, which is all the corpus scripts need.
- **Per run:**
  - Build `ScriptingBindings` from the case's manifest inputs — bytes + `mimeType` + `charset`, exactly the shape `ScriptRuntime.parseJsonInputsToBindings` produces, but read from corpus files rather than a JSON envelope. No-input cases (e.g. `trivial`, `compile-heavy`) pass empty bindings.
  - `compileWith(newConfig().withScript(src).withInputs(inputs).withNameIdentifier(ni).withDefaultOutputType("application/json"))`
  - `.write(bindings, serviceManager, out)` into a provided `OutputStream`.
- **`serviceManager`** carries a UTF-8 `CharsetProviderService` (required so the UTF-16 `xml-to-csv` case decodes correctly), mirroring `NativeRuntime.createServiceManager`. No security-manager/privileges wiring (not exercised by the corpus).

## 3. Metric methodology

| Metric | Where | How |
|---|---|---|
| **cold-start** | fresh JVM per sample | `EngineChild` times `EngineShell` construction (`initMs`). Samples = `iterations.samples`. |
| **first-run** | fresh JVM per sample | Same child times the first `compile+write` (`firstRunMs`). Samples = `iterations.samples`. |
| **warm** | in-process | Warmup = `max(manifest.warmup, 2000)` (JIT floor, logged), then `iterations.warm` timed reps of `compile+write` (or an already-compiled `write` — see open note). min/median/p90/p99/mean. |
| **streaming** | in-process | `iterations.streaming` reps: `write` to a byte-counting `OutputStream` over the large generated input; MB/s = `bytes / 1e6 / (ms/1000)` (decimal-MB, identical to `stats.mjs:toMBps`). |

Which metrics run for a case is read from the case's `metrics[]` in the manifest — a case's unlisted metrics are skipped, exactly like the Node runner (`casesForMetric`).

**Correctness guard:** a case whose engine run returns a non-success result or throws **aborts that case with a clear error** rather than recording a bogus fast timing — an errored script cannot be benchmarked (matches the Node runner's guard).

## 4. Fresh-JVM spawn harness

`EngineChild` mirrors `coldstart-child.mjs`:

- Invoked as `java -cp <runtime-classpath> org.mule.weave.benchmark.engine.EngineChild <corpusDir> <caseId>`.
- Loads the manifest, resolves the case's script + inputs, constructs `EngineShell` (timing `initMs`), runs once (timing `firstRunMs`), aborts on non-success, prints a single JSON line `{"initMs":…,"firstRunMs":…}` to stdout.
- `Emit` spawns N children per case (N = `iterations.samples`), parses the last stdout line of each, aggregates into cold-start and/or first-run rows (only the metrics the case declares).

The child classpath is the module's `runtimeClasspath` — resolved by Gradle and passed to `Emit`, which builds the `java` command. `java.home` locates the JVM binary (GraalVM's `java`, since `GRAALVM_HOME`/`JAVA_HOME` point there).

## 5. Self-contained emit: stats, env, id-validation, schema

The engine runner is **fully JVM** — no Node dependency at runtime. `Emit` reimplements what `benchmarks/lib/*.mjs` does, with a **parity test** to prevent drift (the one real risk of a self-contained emit):

- **Stats (`Stats.scala`)** — nearest-rank percentiles identical to `lib/stats.mjs`: sorted copy, `pct(p) = sorted[ min(n-1, max(0, ceil(p/100 * n) - 1)) ]`, `mean = sum/n`. `StatsParityTest` asserts the Scala output matches known `lib/stats.mjs` results on fixed vectors — e.g. `1..100` → `{min:1, median:50, p90:90, p99:99, mean:50.5}` — so any divergence fails the build, never the deltas.
- **Env (`Env.scala`)** — `runner: "engine"`; `os: "<os.name normalized>-<os.arch>"`; `cpu:` best-effort model (`os.name`/`os.arch` fallback if unavailable); `runtimeVersion: "jvm " + System.getProperty("java.version")`; `weaveVersion` parsed from `gradle.properties` (`^weaveVersion=` — same source as `lib/env.mjs`); `commit` from `git rev-parse --short HEAD` (fallback `"unknown"`).
- **`dwlibBuildId`** — the schema requires it, but it is meaningless for a JVM runner (there is no staged `dwlib`). Set to the literal **`"n/a-engine"`**. The field exists in the schema for wrapper attribution; the engine emits a documented sentinel rather than omitting it (which would fail schema validation).
- **Id-validation** — every emitted `id` is checked against the manifest; **fail-fast** (abort the run) on any orphan id, matching `validateResultIds`. Guarantees the result file left-joins cleanly in the report.
- **Schema conformance** — the JSON is hand-written to match `schema/result.schema.json` exactly: top-level `schemaVersion:"1.0"`, `runner`, `env`, `timestamp` (ISO), `cases[]`; each case a flat `{id, metric, unit, stats, iterations}` row; `additionalProperties:false` respected (no stray keys). The existing `schema/schema.test.mjs` validates any result file, so engine output is covered by the existing schema test path.

Output path: `benchmarks/results/engine-<ISO-timestamp>.json` (`results/` is gitignored, per the parent spec).

## 6. Docs fix (in scope)

`benchmarks/README.md` currently states the engine harness "lives in the `data-weave` repo but reads this corpus." That contradicts the parent-spec correction. Update the Layout section to: the engine runner lives here under `runners/engine/` as a Scala/Gradle subproject on `org.mule.weave:runtime`, run via:

```
./gradlew benchmarks-engine:benchmarkEngine -Pbenchmark=true
```

## Out of scope

- **Python runner** (`runners/python/`) — separate follow-up.
- **Any results datastore, dashboard, or CI benchmark upload** — deferred per the parent spec (the `--emit` seam remains the future push point).
- **Changes to corpus, schema, Node runner, or `report.mjs`** — the engine slots into all four unchanged. The only shared-file edit is the `benchmarks/README.md` correction above and the `settings.gradle` include.

## Testing

- **`StatsParityTest`** (scalatest) — the load-bearing test: Scala `Stats` vs. known `lib/stats.mjs` outputs on fixed vectors. Prevents self-contained-emit drift.
- **Schema conformance** — a produced `engine-*.json` validates against `schema/result.schema.json` (reuse the existing `schema.test.mjs` path, or a small scalatest that validates a sample emit).
- **Manifest parsing** — `Manifest.scala` resolves each case's script + input files against the corpus; a test asserts every manifest case's declared files exist and resolve.
- Run the engine harness end-to-end once (`benchmarkEngine -Pbenchmark=true`) and confirm `report.mjs results/*.json` renders the engine as baseline with wrapper deltas and no skew banner (same `weaveVersion`).

## Non-obvious context / open notes

- **`warm` — both sides recompile per run.** Neither the engine nor the wrapper caches compiled scripts. The Node `warm` loop re-runs `api.run(script, inputs)` (compile+exec), and the engine `warm` runs `compile+write` per iteration — both measure compile+exec steady-state after JIT warmup.
- **GraalVM `java` as the child JVM.** `EngineChild` runs under whatever `java.home` resolves to — with `GRAALVM_HOME`/`JAVA_HOME` set (the repo's build prerequisite), that is GraalVM's HotSpot `java`. The cold-start number is therefore "GraalVM JVM cold start," the correct competitor to the GraalVM-built native image.
- **No skew, but the banner stays.** Because the engine runs the same `weaveVersion` as the staged `dwlib`, `report.mjs` prints no skew banner in the normal path. The banner remains a safety net for an accidental mismatch (e.g. a stale staged lib), not a standing caveat.
