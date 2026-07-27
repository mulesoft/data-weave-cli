# Engine Runner (JVM Benchmark Baseline) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the engine runner — a Scala/Gradle subproject at `benchmarks/runners/engine/` that drives the bare `DataWeaveScriptingEngine` over the shared benchmark corpus and emits a schema-conformant `results/engine-<ts>.json`, so `report/report.mjs` produces the headline native-wrapper-vs-JVM-engine comparison at the same weave version.

**Architecture:** A new Gradle subproject depends on `org.mule.weave:runtime` (already a build dependency of this repo) so `DataWeaveScriptingEngine` is on the JVM classpath at the *same* `weaveVersion` the native image is built from. The harness mirrors `native-cli`'s `NativeRuntime.scala`: a minimal `EngineShell` compiles a script and writes into an `OutputStream`. Cold-start and first-run are measured by spawning a fresh JVM per sample (`EngineChild`), mirroring the Node `coldstart-child.mjs`; warm and streaming are measured in-process (`WarmBench`) with a JVM JIT warmup floor. `Emit` orchestrates both, computes stats with nearest-rank percentiles identical to `benchmarks/lib/stats.mjs` (guarded by a parity test), stamps env, fail-fast-validates case ids against the manifest, and writes the result JSON.

**Tech Stack:** Scala 2.12.18, `org.mule.weave:runtime` + `core-modules` (weave engine + data formats), `org.json:json` (JSON I/O), scalatest (`com.github.maiflai.scalatest` plugin + `flexmark` runtime, matching `native-cli`), Gradle `JavaExec`, `System.nanoTime()` timing. Reuses the existing `benchmarks/corpus/`, `benchmarks/schema/`, and `report/report.mjs` unchanged; regenerates large inputs via the existing `corpus/gen-inputs.mjs` (Node).

## Global Constraints

- **Same weave version, no skew.** The engine runner uses `api org.mule.weave:runtime:${weaveVersion}` — the *same* `weaveVersion` (`2.12.0-20260413`) the native image is built from. Do not pin a different version. `report.mjs` prints no skew banner in the normal path.
- **Timing methodology (every metric):** measure with `System.nanoTime()`; convert to ms as `(endNs - startNs) / 1e6` (Double). Identical in spirit to the Node runner's `process.hrtime.bigint()` → ms.
- **`id` is the immutable join key.** Emit each case `id` verbatim from the manifest; never invent or rename. Deprecate, never rename.
- **Fail-fast on orphan ids.** `Emit` MUST abort before writing output if any emitted `id` is absent from the manifest.
- **Explicit metrics.** Each manifest case declares `metrics[]` from exactly `["cold-start","first-run","warm","streaming"]`. Run only the metrics a case declares (`casesForMetric`).
- **Correctness guard.** A case whose engine run throws (compile/exec failure) aborts that case with a clear error — never record a bogus fast timing.
- **Schema conformance (mandatory `env` fields).** Every result file validates against `benchmarks/schema/result.schema.json`: top-level `schemaVersion:"1.0"`, `runner`, `env`, `timestamp`, `cases[]`; each case a flat `{id, metric, unit, stats, iterations}` row with no extra keys; `env` includes `os`, `cpu`, `runtimeVersion`, `weaveVersion`, `commit`, `dwlibBuildId` (all required). `dwlibBuildId` has no meaning for a JVM runner → literal `"n/a-engine"`.
- **`runner` is `"engine"`** so `report.mjs:92` auto-selects it as the delta baseline.
- **Units per row:** `ms` for latency metrics (cold-start, first-run, warm), `MB/s` for streaming.
- **JVM warmup floor:** `warm` warmup iterations = `max(manifestWarmup, 2000)` so the JIT reaches steady state; the effective warmup used is logged.
- **Results are local-only.** Output to `benchmarks/results/` (already gitignored via `benchmarks/.gitignore`). Do not commit result files or generated inputs.
- **Deterministic large inputs.** The generated `corpus/inputs/generated/records-large.json` is produced by the existing `corpus/gen-inputs.mjs` (Node) and shared byte-for-byte across runners — the engine consumes that file, never regenerates it differently.

---

### Task 1: Gradle subproject scaffold + settings wiring + smoke test

**Files:**
- Modify: `settings.gradle`
- Create: `benchmarks/runners/engine/build.gradle`
- Create: `benchmarks/runners/engine/src/main/scala/org/mule/weave/benchmark/engine/CountingOutputStream.scala`
- Test: `benchmarks/runners/engine/src/test/scala/org/mule/weave/benchmark/engine/SmokeTest.scala`

**Interfaces:**
- Consumes: nothing.
- Produces: a compilable, testable Gradle subproject at path `:benchmarks-engine`, package `org.mule.weave.benchmark.engine`. `CountingOutputStream` — a `java.io.OutputStream` that discards bytes and exposes `def count(): Long` (used by later tasks as the write sink).

- [ ] **Step 1: Add the subproject to `settings.gradle`**

Append to `settings.gradle` (currently three `include` lines):

```groovy
include 'benchmarks-engine'
project(':benchmarks-engine').projectDir = file('benchmarks/runners/engine')
```

- [ ] **Step 2: Create the module `build.gradle`**

Create `benchmarks/runners/engine/build.gradle`. The root `subprojects {}` block already applies `scala`, `java-library`, `scala-library`, the mule repositories, and the graalvm plugin — this module only adds the scalatest plugin, the weave deps, `org.json`, test deps, and the two tasks:

```groovy
plugins {
    id "com.github.maiflai.scalatest" version "${scalaTestPluginVersion}"
}

dependencies {
    api group: 'org.mule.weave', name: 'runtime', version: weaveVersion
    api group: 'org.mule.weave', name: 'core-modules', version: weaveVersion
    implementation group: 'org.mule.weave', name: 'parser', version: weaveVersion
    implementation group: 'org.mule.weave', name: 'wlang', version: weaveVersion
    implementation 'org.json:json:20240303'

    testImplementation group: 'org.scalatest', name: 'scalatest_2.12', version: scalaTestVersion
    testRuntimeOnly 'com.vladsch.flexmark:flexmark-all:0.62.2'
}

def benchmarksDir = "${rootDir}/benchmarks"

// Regenerate the shared large input via the existing Node generator (idempotent,
// deterministic). Keeps the engine consuming the SAME bytes as other runners.
tasks.register('genBenchInputs', Exec) {
    onlyIf { project.findProperty('benchmark')?.toString()?.toBoolean() == true }
    workingDir(benchmarksDir)
    if (System.getProperty('os.name').toLowerCase().contains('windows')) {
        commandLine('cmd', '/c', 'node corpus/gen-inputs.mjs')
    } else {
        commandLine('bash', '-c', 'node corpus/gen-inputs.mjs')
    }
}

// Opt-in only: skipped unless -Pbenchmark=true. Never part of build/test.
tasks.register('benchmarkEngine', JavaExec) {
    onlyIf { project.findProperty('benchmark')?.toString()?.toBoolean() == true }
    dependsOn tasks.named('genBenchInputs'), tasks.named('classes')
    classpath = sourceSets.main.runtimeClasspath
    mainClass = 'org.mule.weave.benchmark.engine.Emit'
    args("${benchmarksDir}/corpus", "${benchmarksDir}/results", "${rootDir}")
    jvmArgs = ['-Xmx6G']
}
```

- [ ] **Step 3: Write the failing smoke test**

Create `benchmarks/runners/engine/src/test/scala/org/mule/weave/benchmark/engine/SmokeTest.scala`:

```scala
package org.mule.weave.benchmark.engine

import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class SmokeTest extends AnyFreeSpec with Matchers {
  "CountingOutputStream counts written bytes" in {
    val out = new CountingOutputStream()
    out.write("hello".getBytes("UTF-8"))
    out.write(42)
    out.count() shouldBe 6L
  }
}
```

- [ ] **Step 4: Run the test to verify it fails (does not compile — class missing)**

Run: `./gradlew benchmarks-engine:test --tests "org.mule.weave.benchmark.engine.SmokeTest"`
Expected: FAIL — compilation error, `CountingOutputStream` not found.

- [ ] **Step 5: Implement `CountingOutputStream`**

Create `benchmarks/runners/engine/src/main/scala/org/mule/weave/benchmark/engine/CountingOutputStream.scala`:

```scala
package org.mule.weave.benchmark.engine

import java.io.OutputStream

/** An OutputStream that discards all bytes and only counts how many were written.
  * Used as the write sink when benchmarking — we measure the work of producing
  * output without paying for allocation/retention of the result. */
class CountingOutputStream extends OutputStream {
  private var written: Long = 0L

  override def write(b: Int): Unit = { written += 1 }

  override def write(b: Array[Byte]): Unit = { written += b.length }

  override def write(b: Array[Byte], off: Int, len: Int): Unit = { written += len }

  def count(): Long = written
}
```

- [ ] **Step 6: Run the test to verify it passes**

Run: `./gradlew benchmarks-engine:test --tests "org.mule.weave.benchmark.engine.SmokeTest"`
Expected: PASS (1 test). This proves the subproject compiles, resolves the weave deps, and runs scalatest.

- [ ] **Step 7: Commit**

```bash
git add settings.gradle benchmarks/runners/engine/build.gradle \
  benchmarks/runners/engine/src/main/scala/org/mule/weave/benchmark/engine/CountingOutputStream.scala \
  benchmarks/runners/engine/src/test/scala/org/mule/weave/benchmark/engine/SmokeTest.scala
git commit -m "W-23545283: Scaffold engine-runner Gradle subproject"
```

---

### Task 2: Stats — nearest-rank percentiles with parity test vs `lib/stats.mjs`

**Files:**
- Create: `benchmarks/runners/engine/src/main/scala/org/mule/weave/benchmark/engine/Stats.scala`
- Test: `benchmarks/runners/engine/src/test/scala/org/mule/weave/benchmark/engine/StatsParityTest.scala`

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `Stats.Summary(min: Double, median: Double, p90: Double, p99: Double, mean: Double)`
  - `Stats.computeStats(samples: Seq[Double]): Stats.Summary` — nearest-rank percentiles on a sorted copy: `pct(p) = sorted( min(n-1, max(0, ceil(p/100 * n) - 1)) )`; `mean = sum/n`. Throws `IllegalArgumentException` on empty input. Identical math to `benchmarks/lib/stats.mjs:computeStats`.
  - `Stats.toMBps(totalBytes: Long, elapsedMs: Double): Double` — `totalBytes / 1e6 / (elapsedMs / 1000)`; throws if `elapsedMs <= 0`. Identical to `lib/stats.mjs:toMBps`.

- [ ] **Step 1: Write the failing test**

Create `benchmarks/runners/engine/src/test/scala/org/mule/weave/benchmark/engine/StatsParityTest.scala`. These expected values are the exact outputs of `benchmarks/lib/stats.mjs` for the same inputs:

```scala
package org.mule.weave.benchmark.engine

import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class StatsParityTest extends AnyFreeSpec with Matchers {
  "computeStats matches lib/stats.mjs nearest-rank on 1..100" in {
    val s = Stats.computeStats((1 to 100).map(_.toDouble))
    s.min shouldBe 1.0
    s.median shouldBe 50.0 // ceil(0.5*100)-1 = 49 -> sorted(49) = 50
    s.p90 shouldBe 90.0    // ceil(0.9*100)-1 = 89 -> 90
    s.p99 shouldBe 99.0    // ceil(0.99*100)-1 = 98 -> 99
    s.mean shouldBe 50.5
  }

  "computeStats sorts before ranking" in {
    val s = Stats.computeStats(Seq(5.0, 1.0, 3.0, 2.0, 4.0))
    s.min shouldBe 1.0
    s.median shouldBe 3.0 // ceil(0.5*5)-1 = 2 -> sorted(2) = 3
    s.p90 shouldBe 5.0    // ceil(0.9*5)-1 = 4 -> 5
    s.mean shouldBe 3.0
  }

  "computeStats rejects empty input" in {
    an [IllegalArgumentException] should be thrownBy Stats.computeStats(Seq.empty)
  }

  "toMBps matches decimal-MB convention" in {
    Stats.toMBps(1000000L, 1000.0) shouldBe 1.0
    Stats.toMBps(500000L, 250.0) shouldBe 2.0
    an [IllegalArgumentException] should be thrownBy Stats.toMBps(10L, 0.0)
  }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `./gradlew benchmarks-engine:test --tests "org.mule.weave.benchmark.engine.StatsParityTest"`
Expected: FAIL — `Stats` not found (compilation error).

- [ ] **Step 3: Implement `Stats`**

Create `benchmarks/runners/engine/src/main/scala/org/mule/weave/benchmark/engine/Stats.scala`:

```scala
package org.mule.weave.benchmark.engine

/** Percentile + throughput math kept byte-for-byte compatible with
  * benchmarks/lib/stats.mjs so engine deltas compare cleanly against Node.
  * Any divergence is caught by StatsParityTest. */
object Stats {

  final case class Summary(min: Double, median: Double, p90: Double, p99: Double, mean: Double)

  /** Nearest-rank percentiles on a sorted copy; mean is the arithmetic mean. */
  def computeStats(samples: Seq[Double]): Summary = {
    if (samples.isEmpty) {
      throw new IllegalArgumentException("computeStats requires a non-empty sequence of numbers")
    }
    val sorted = samples.sorted.toVector
    val n = sorted.length
    def pct(p: Double): Double = {
      val idx = math.min(n - 1, math.max(0, math.ceil(p / 100.0 * n).toInt - 1))
      sorted(idx)
    }
    val sum = sorted.sum
    Summary(min = sorted.head, median = pct(50), p90 = pct(90), p99 = pct(99), mean = sum / n)
  }

  /** Throughput in decimal megabytes per second (1 MB = 1e6 bytes). */
  def toMBps(totalBytes: Long, elapsedMs: Double): Double = {
    if (elapsedMs <= 0) throw new IllegalArgumentException("elapsedMs must be > 0")
    totalBytes / 1e6 / (elapsedMs / 1000.0)
  }
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `./gradlew benchmarks-engine:test --tests "org.mule.weave.benchmark.engine.StatsParityTest"`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add benchmarks/runners/engine/src/main/scala/org/mule/weave/benchmark/engine/Stats.scala \
  benchmarks/runners/engine/src/test/scala/org/mule/weave/benchmark/engine/StatsParityTest.scala
git commit -m "W-23545283: Add engine-runner stats with lib/stats.mjs parity test"
```

---

### Task 3: Manifest parser + input resolution

**Files:**
- Create: `benchmarks/runners/engine/src/main/scala/org/mule/weave/benchmark/engine/Manifest.scala`
- Test: `benchmarks/runners/engine/src/test/scala/org/mule/weave/benchmark/engine/ManifestTest.scala`

**Interfaces:**
- Consumes: `org.json` (parsing), the real corpus at `benchmarks/corpus/`.
- Produces:
  - `case class CaseInput(name: String, file: String, mimeType: String, charset: Option[String], generated: Boolean)`
  - `case class BenchCase(id: String, script: String, inputs: Seq[CaseInput], metrics: Set[String], iterations: Map[String, Int])` with helpers `warm: Int` (default 100), `warmup: Int` (default 10), `streaming: Int` (default 10), `samples: Int` (default 20).
  - `case class ResolvedInput(name: String, bytes: Array[Byte], mimeType: String, charset: Option[String])`
  - `class Manifest(val corpusDir: File, val cases: Seq[BenchCase])` with `def ids: Set[String]`.
  - `object Manifest`:
    - `def load(corpusDir: File): Manifest` — parse + validate `manifest.json` (mirrors `lib/manifest.mjs:loadManifest`: non-empty unique ids, metrics from the allowed set, script file exists, non-generated input files exist). Throws on any violation.
    - `def casesForMetric(m: Manifest, metric: String): Seq[BenchCase]`
    - `def validateResultIds(m: Manifest, resultIds: Seq[String]): Unit` — throws on any id not in `m.ids`.
    - `def resolveScript(m: Manifest, c: BenchCase): String` — read the script file.
    - `def resolveInputs(m: Manifest, c: BenchCase): Seq[ResolvedInput]` — read each input file into bytes.

- [ ] **Step 1: Write the failing test**

Create `benchmarks/runners/engine/src/test/scala/org/mule/weave/benchmark/engine/ManifestTest.scala`. Uses the real corpus (Gradle runs tests with `workingDir = <module projectDir>`, i.e. `benchmarks/runners/engine`, so `../../corpus` is `benchmarks/corpus`):

```scala
package org.mule.weave.benchmark.engine

import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import java.io.File

class ManifestTest extends AnyFreeSpec with Matchers {
  private val corpus = new File("../../corpus").getCanonicalFile

  "loads all corpus cases with stable ids" in {
    val m = Manifest.load(corpus)
    m.ids should contain allOf ("trivial", "object-transform", "map-scale", "xml-to-csv", "json-stream", "compile-heavy")
  }

  "casesForMetric filters by declared metrics" in {
    val m = Manifest.load(corpus)
    Manifest.casesForMetric(m, "streaming").map(_.id) should contain allOf ("map-scale", "json-stream")
    Manifest.casesForMetric(m, "cold-start").map(_.id) should contain ("trivial")
  }

  "resolveScript reads the .dwl source" in {
    val m = Manifest.load(corpus)
    val trivial = m.cases.find(_.id == "trivial").get
    Manifest.resolveScript(m, trivial).trim should include ("2 + 2")
  }

  "resolveInputs reads committed input bytes with mime + charset" in {
    val m = Manifest.load(corpus)
    val xml = m.cases.find(_.id == "xml-to-csv").get
    val inputs = Manifest.resolveInputs(m, xml)
    inputs should have size 1
    inputs.head.name shouldBe "payload"
    inputs.head.mimeType shouldBe "application/xml"
    inputs.head.charset shouldBe Some("UTF-16")
    inputs.head.bytes.length should be > 0
  }

  "validateResultIds throws on an orphan id" in {
    val m = Manifest.load(corpus)
    an [RuntimeException] should be thrownBy Manifest.validateResultIds(m, Seq("trivial", "not-a-case"))
    noException should be thrownBy Manifest.validateResultIds(m, Seq("trivial"))
  }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `./gradlew benchmarks-engine:test --tests "org.mule.weave.benchmark.engine.ManifestTest"`
Expected: FAIL — `Manifest` not found.

- [ ] **Step 3: Implement `Manifest`**

Create `benchmarks/runners/engine/src/main/scala/org/mule/weave/benchmark/engine/Manifest.scala`:

```scala
package org.mule.weave.benchmark.engine

import org.json.{ JSONArray, JSONObject }

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import scala.collection.mutable

final case class CaseInput(name: String, file: String, mimeType: String, charset: Option[String], generated: Boolean)

final case class BenchCase(
  id: String,
  script: String,
  inputs: Seq[CaseInput],
  metrics: Set[String],
  iterations: Map[String, Int]) {

  def warm: Int = iterations.getOrElse("warm", 100)
  def warmup: Int = iterations.getOrElse("warmup", 10)
  def streaming: Int = iterations.getOrElse("streaming", 10)
  def samples: Int = iterations.getOrElse("samples", 20)
}

final case class ResolvedInput(name: String, bytes: Array[Byte], mimeType: String, charset: Option[String])

class Manifest(val corpusDir: File, val cases: Seq[BenchCase]) {
  def ids: Set[String] = cases.map(_.id).toSet
}

object Manifest {

  private val AllowedMetrics = Set("cold-start", "first-run", "warm", "streaming")

  def load(corpusDir: File): Manifest = {
    val manifestFile = new File(corpusDir, "manifest.json")
    val raw = new String(Files.readAllBytes(manifestFile.toPath), StandardCharsets.UTF_8)
    val root = new JSONObject(raw)
    val casesArr: JSONArray = root.getJSONArray("cases")

    val seen = mutable.Set[String]()
    val cases = (0 until casesArr.length()).map { i =>
      val obj = casesArr.getJSONObject(i)
      val id = obj.getString("id")
      if (id.isEmpty) throw new RuntimeException("manifest case is missing an id")
      if (seen.contains(id)) throw new RuntimeException(s"duplicate case id: $id")
      seen += id

      val metricsArr = obj.getJSONArray("metrics")
      if (metricsArr.length() == 0) throw new RuntimeException(s"case $id must declare a non-empty metrics[]")
      val metrics = (0 until metricsArr.length()).map(metricsArr.getString).toSet
      metrics.foreach(m => if (!AllowedMetrics.contains(m)) throw new RuntimeException(s"case $id has unknown metric: $m"))

      val script = obj.getString("script")
      if (!new File(corpusDir, script).exists()) throw new RuntimeException(s"case $id script not found: $script")

      val iterations: Map[String, Int] =
        if (obj.has("iterations")) {
          val it = obj.getJSONObject("iterations")
          it.keySet().toArray.map(_.asInstanceOf[String]).map(k => k -> it.getInt(k)).toMap
        } else Map.empty

      val inputs: Seq[CaseInput] =
        if (obj.has("inputs")) {
          val ins = obj.getJSONObject("inputs")
          ins.keySet().toArray.map(_.asInstanceOf[String]).toSeq.map { name =>
            val io = ins.getJSONObject(name)
            val file = io.getString("file")
            val generated = io.optBoolean("generated", false)
            if (!generated && !new File(corpusDir, file).exists()) {
              throw new RuntimeException(s"case $id input '$name' file not found: $file")
            }
            CaseInput(
              name = name,
              file = file,
              mimeType = io.getString("mimeType"),
              charset = if (io.has("charset")) Some(io.getString("charset")) else None,
              generated = generated)
          }
        } else Seq.empty

      BenchCase(id, script, inputs, metrics, iterations)
    }
    new Manifest(corpusDir, cases)
  }

  def casesForMetric(m: Manifest, metric: String): Seq[BenchCase] =
    m.cases.filter(_.metrics.contains(metric))

  def validateResultIds(m: Manifest, resultIds: Seq[String]): Unit = {
    resultIds.foreach { id =>
      if (!m.ids.contains(id)) throw new RuntimeException(s"result contains orphan id not in manifest: $id")
    }
  }

  def resolveScript(m: Manifest, c: BenchCase): String =
    new String(Files.readAllBytes(new File(m.corpusDir, c.script).toPath), StandardCharsets.UTF_8)

  def resolveInputs(m: Manifest, c: BenchCase): Seq[ResolvedInput] =
    c.inputs.map { in =>
      val bytes = Files.readAllBytes(new File(m.corpusDir, in.file).toPath)
      ResolvedInput(in.name, bytes, in.mimeType, in.charset)
    }
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `./gradlew benchmarks-engine:test --tests "org.mule.weave.benchmark.engine.ManifestTest"`
Expected: PASS (5 tests).

- [ ] **Step 5: Commit**

```bash
git add benchmarks/runners/engine/src/main/scala/org/mule/weave/benchmark/engine/Manifest.scala \
  benchmarks/runners/engine/src/test/scala/org/mule/weave/benchmark/engine/ManifestTest.scala
git commit -m "W-23545283: Add engine-runner manifest parser + input resolution"
```

---

### Task 4: EngineShell — the bare `DataWeaveScriptingEngine`

**Files:**
- Create: `benchmarks/runners/engine/src/main/scala/org/mule/weave/benchmark/engine/EngineShell.scala`
- Test: `benchmarks/runners/engine/src/test/scala/org/mule/weave/benchmark/engine/EngineShellTest.scala`

**Interfaces:**
- Consumes: `Manifest.ResolvedInput`, `CountingOutputStream`, the weave runtime API (`DataWeaveScriptingEngine`, `ModuleComponentsFactory`, `ClassLoaderWeaveResourceResolver`, `ParserConfiguration`, `ScriptingBindings`, `BindingValue`, `InputType`, `ServiceManager`, `CharsetProviderService`, `NameIdentifier`).
- Produces:
  - `class EngineShell` — constructing it builds a fresh engine (this construction is what `EngineChild` times as `initMs`).
  - `EngineShell.run(script: String, name: String, inputs: Seq[ResolvedInput], out: OutputStream): Unit` — compile the script and write output into `out`. Throws on compile/exec failure (the correctness guard). Recompiles on every call (matches the Node wrapper, which recompiles each `run()`), so `warm` measures compile+exec on both sides.
  - `EngineShell.safeName(id: String): String` — a NameIdentifier-safe logical name derived from a case id (`"bench_" + id.replaceAll("[^A-Za-z0-9_]", "_")`), so different scripts compiled on one engine never collide on name.

- [ ] **Step 1: Write the failing test**

Create `benchmarks/runners/engine/src/test/scala/org/mule/weave/benchmark/engine/EngineShellTest.scala`:

```scala
package org.mule.weave.benchmark.engine

import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import java.io.File

class EngineShellTest extends AnyFreeSpec with Matchers {
  private val corpus = new File("../../corpus").getCanonicalFile
  private val manifest = Manifest.load(corpus)

  private def runCase(id: String): Long = {
    val c = manifest.cases.find(_.id == id).get
    val shell = new EngineShell()
    val out = new CountingOutputStream()
    shell.run(Manifest.resolveScript(manifest, c), EngineShell.safeName(id), Manifest.resolveInputs(manifest, c), out)
    out.count()
  }

  "runs a no-input script (trivial) and writes output" in {
    runCase("trivial") should be > 0L
  }

  "runs an object transform with a JSON input binding" in {
    runCase("object-transform") should be > 0L
  }

  "runs the UTF-16 xml-to-csv case (charset path)" in {
    runCase("xml-to-csv") should be > 0L
  }

  "safeName sanitizes hyphens" in {
    EngineShell.safeName("xml-to-csv") shouldBe "bench_xml_to_csv"
  }

  "a failing script aborts with an exception" in {
    val shell = new EngineShell()
    an [Exception] should be thrownBy shell.run("output application/json --- payload.nope + 1", "bench_bad", Seq.empty, new CountingOutputStream())
  }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `./gradlew benchmarks-engine:test --tests "org.mule.weave.benchmark.engine.EngineShellTest"`
Expected: FAIL — `EngineShell` not found.

- [ ] **Step 3: Implement `EngineShell`**

Create `benchmarks/runners/engine/src/main/scala/org/mule/weave/benchmark/engine/EngineShell.scala`. This is a minimal reduction of `native-cli`'s `NativeRuntime.scala` — classloader resolver only, a UTF-8 `CharsetProviderService`, and `compileWith` + `write(bindings, sm, Option(out))` exactly as `NativeRuntime.run` drives it:

```scala
package org.mule.weave.benchmark.engine

import io.netty.util.internal.PlatformDependent
import org.mule.weave.v2.model.ServiceManager
import org.mule.weave.v2.model.service.CharsetProviderService
import org.mule.weave.v2.parser.ast.variables.NameIdentifier
import org.mule.weave.v2.runtime.{
  BindingValue,
  DataWeaveScript,
  DataWeaveScriptingEngine,
  InputType,
  ModuleComponentsFactory,
  ParserConfiguration,
  ScriptingBindings
}
import org.mule.weave.v2.sdk.ClassLoaderWeaveResourceResolver

import java.io.OutputStream
import java.nio.charset.{ Charset, StandardCharsets }
import java.util.Properties

/** A minimal engine harness: builds a bare DataWeaveScriptingEngine (classloader
  * resolver only) and compiles+writes a script per run(), mirroring how
  * native-cli's NativeRuntime drives the engine. Constructing this class is the
  * work EngineChild times as `initMs`. */
class EngineShell {

  EngineShell.setupEnv()

  private val engine: DataWeaveScriptingEngine = {
    val resolver = ClassLoaderWeaveResourceResolver.apply()
    new DataWeaveScriptingEngine(ModuleComponentsFactory.apply(resolver), ParserConfiguration(), new Properties())
  }

  // UTF-8 default charset service, matching NativeRuntime.createServiceManager.
  // Required so cases that don't pin a charset decode as UTF-8; per-input charsets
  // (e.g. the UTF-16 xml-to-csv case) come from the binding itself.
  private val serviceManager: ServiceManager = {
    val charsetService = new CharsetProviderService {
      override def defaultCharset(): Charset = StandardCharsets.UTF_8
    }
    val customServices: Map[Class[_], _] = Map(classOf[CharsetProviderService] -> charsetService)
    ServiceManager(customServices)
  }

  /** Compile `script` and write its output into `out`. Throws on failure. */
  def run(script: String, name: String, inputs: Seq[ResolvedInput], out: OutputStream): Unit = {
    val bindings = new ScriptingBindings()
    inputs.foreach { in =>
      val charset = Charset.forName(in.charset.getOrElse("UTF-8"))
      val bv = new BindingValue(in.bytes, Some(in.mimeType), Map.empty[String, Any], charset)
      bindings.addBinding(in.name, bv)
    }

    val config = engine.newConfig()
      .withScript(script)
      .withNameIdentifier(NameIdentifier(name))
      .withInputs(inputs.map(in => new InputType(in.name, None)).toArray)
      .withDefaultOutputType("application/json")

    val compiled: DataWeaveScript = engine.compileWith(config)
    // 3-arg write(bindings, serviceManager, target: Option[Any]) writes into `out`,
    // exactly as NativeRuntime.run does. A compile/exec failure throws here.
    compiled.write(bindings, serviceManager, Option(out))
  }
}

object EngineShell {

  /** Netty init properties, copied from NativeRuntime.setupEnv. */
  def setupEnv(): Unit = {
    System.setProperty("io.netty.processId", Math.abs(PlatformDependent.threadLocalRandom.nextInt).toString)
    System.setProperty("io.netty.noUnsafe", true.toString)
  }

  /** A NameIdentifier-safe logical name derived from a case id. */
  def safeName(id: String): String = "bench_" + id.replaceAll("[^A-Za-z0-9_]", "_")
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `./gradlew benchmarks-engine:test --tests "org.mule.weave.benchmark.engine.EngineShellTest"`
Expected: PASS (5 tests). If a data format is unexpectedly missing (runtime "unknown mime type"), confirm `core-modules` is on the classpath (Task 1 dep) — it ships the `META-INF/services` DataFormat/ModuleLoader files.

- [ ] **Step 5: Commit**

```bash
git add benchmarks/runners/engine/src/main/scala/org/mule/weave/benchmark/engine/EngineShell.scala \
  benchmarks/runners/engine/src/test/scala/org/mule/weave/benchmark/engine/EngineShellTest.scala
git commit -m "W-23545283: Add EngineShell driving bare DataWeaveScriptingEngine"
```

---

### Task 5: EngineChild — fresh-JVM cold-start / first-run worker

**Files:**
- Create: `benchmarks/runners/engine/src/main/scala/org/mule/weave/benchmark/engine/EngineChild.scala`
- Test: `benchmarks/runners/engine/src/test/scala/org/mule/weave/benchmark/engine/EngineChildTest.scala`

**Interfaces:**
- Consumes: `Manifest`, `EngineShell`, `CountingOutputStream`.
- Produces: `object EngineChild` with `def main(args: Array[String]): Unit`. Args: `<corpusDir> <caseId>`. Times `initMs` (EngineShell construction) and `firstRunMs` (first compile+write), then prints exactly one JSON line to stdout: `{"initMs":<double>,"firstRunMs":<double>}`. Exits non-zero (uncaught exception) if the case run fails. This is the fresh-JVM analog of `runners/node/coldstart-child.mjs`.

- [ ] **Step 1: Write the failing test**

Create `benchmarks/runners/engine/src/test/scala/org/mule/weave/benchmark/engine/EngineChildTest.scala`. It spawns a real child JVM using the test's own classpath — proving the spawn path `Emit` will use works end-to-end:

```scala
package org.mule.weave.benchmark.engine

import org.json.JSONObject
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import java.io.File

class EngineChildTest extends AnyFreeSpec with Matchers {
  private val corpus = new File("../../corpus").getCanonicalFile

  private def spawn(caseId: String): (Int, String) = {
    val javaBin = new File(System.getProperty("java.home"), "bin/java").getAbsolutePath
    val cp = System.getProperty("java.class.path")
    val pb = new ProcessBuilder(
      javaBin, "-cp", cp,
      "org.mule.weave.benchmark.engine.EngineChild",
      corpus.getAbsolutePath, caseId)
    val p = pb.start()
    val out = scala.io.Source.fromInputStream(p.getInputStream).getLines().toList
    val code = p.waitFor()
    (code, out.lastOption.getOrElse(""))
  }

  "child prints init + first-run timings for a case" in {
    val (code, line) = spawn("trivial")
    code shouldBe 0
    val obj = new JSONObject(line)
    obj.getDouble("initMs") should be > 0.0
    obj.getDouble("firstRunMs") should be > 0.0
  }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `./gradlew benchmarks-engine:test --tests "org.mule.weave.benchmark.engine.EngineChildTest"`
Expected: FAIL — child exits non-zero / no output because `EngineChild` main class does not exist.

- [ ] **Step 3: Implement `EngineChild`**

Create `benchmarks/runners/engine/src/main/scala/org/mule/weave/benchmark/engine/EngineChild.scala`:

```scala
package org.mule.weave.benchmark.engine

import java.io.File

/** Fresh-process worker. Measures a cold engine init + a cold (first) compile+exec
  * for one case, then prints a single JSON line. Spawned by Emit — the honest JVM
  * cold path (process launch + classload + engine init + first compile). */
object EngineChild {

  private def nowNs(): Long = System.nanoTime()
  private def msSince(startNs: Long): Double = (System.nanoTime() - startNs) / 1e6

  def main(args: Array[String]): Unit = {
    val corpusDir = new File(args(0))
    val caseId = args(1)

    val manifest = Manifest.load(corpusDir)
    val c = manifest.cases.find(_.id == caseId).getOrElse(sys.error(s"unknown case: $caseId"))
    val script = Manifest.resolveScript(manifest, c)
    val inputs = Manifest.resolveInputs(manifest, c)

    val initStart = nowNs()
    val shell = new EngineShell()
    val initMs = msSince(initStart)

    val runStart = nowNs()
    shell.run(script, EngineShell.safeName(caseId), inputs, new CountingOutputStream())
    val firstRunMs = msSince(runStart)

    // Single JSON line on stdout; Emit reads the last line.
    println(s"""{"initMs":$initMs,"firstRunMs":$firstRunMs}""")
  }
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `./gradlew benchmarks-engine:test --tests "org.mule.weave.benchmark.engine.EngineChildTest"`
Expected: PASS (1 test).

- [ ] **Step 5: Commit**

```bash
git add benchmarks/runners/engine/src/main/scala/org/mule/weave/benchmark/engine/EngineChild.scala \
  benchmarks/runners/engine/src/test/scala/org/mule/weave/benchmark/engine/EngineChildTest.scala
git commit -m "W-23545283: Add EngineChild fresh-JVM cold-start/first-run worker"
```

---

### Task 6: WarmBench — in-process warm (JIT floor) + streaming

**Files:**
- Create: `benchmarks/runners/engine/src/main/scala/org/mule/weave/benchmark/engine/Row.scala`
- Create: `benchmarks/runners/engine/src/main/scala/org/mule/weave/benchmark/engine/WarmBench.scala`
- Create: `benchmarks/runners/engine/src/test/scala/org/mule/weave/benchmark/engine/TestSupport.scala`
- Test: `benchmarks/runners/engine/src/test/scala/org/mule/weave/benchmark/engine/WarmBenchTest.scala`

**Interfaces:**
- Consumes: `EngineShell`, `Manifest`, `Stats`, `CountingOutputStream`.
- Produces:
  - `case class Row(id: String, metric: String, unit: String, stats: Stats.Summary, iterations: Int)` (shared result row; also used by Task 7).
  - `object WarmBench`:
    - `WARMUP_FLOOR: Int = 2000`
    - `runWarm(shell: EngineShell, m: Manifest, warmupCap: Option[Int] = None, iterCap: Option[Int] = None): Seq[Row]` — for each `casesForMetric(m,"warm")`: warmup `warmupCap.getOrElse(max(case.warmup, WARMUP_FLOOR))` iterations, then time `iterCap.getOrElse(case.warm)` iterations of `compile+write`; emit a `warm`/`ms` row. Logs the effective warmup per case.
    - `runStreaming(shell: EngineShell, m: Manifest, iterCap: Option[Int] = None): Seq[Row]` — for each `casesForMetric(m,"streaming")`: time `iterCap.getOrElse(case.streaming)` iterations; MB/s = `Stats.toMBps(primaryInput.bytes.length, elapsedMs)` over the first declared input; emit a `streaming`/`MB/s` row.
  - `object TestSupport.ensureGeneratedInputs(corpus: File): Unit` — runs `node corpus/gen-inputs.mjs` with `BENCH_LARGE_N=500` if the generated file is missing; used by tests that touch generated-input cases.

- [ ] **Step 1: Write the failing test**

Create `benchmarks/runners/engine/src/test/scala/org/mule/weave/benchmark/engine/TestSupport.scala`:

```scala
package org.mule.weave.benchmark.engine

import java.io.File

object TestSupport {
  /** Ensure the shared large input exists (small N for tests). Best-effort:
    * requires Node, which the benchmarks already depend on. */
  def ensureGeneratedInputs(corpus: File): Boolean = {
    val gen = new File(corpus, "inputs/generated/records-large.json")
    if (gen.exists()) return true
    try {
      val pb = new ProcessBuilder("node", "corpus/gen-inputs.mjs").directory(corpus.getParentFile)
      pb.environment().put("BENCH_LARGE_N", "500")
      pb.inheritIO()
      pb.start().waitFor()
      gen.exists()
    } catch {
      case _: Throwable => false
    }
  }
}
```

Create `benchmarks/runners/engine/src/test/scala/org/mule/weave/benchmark/engine/WarmBenchTest.scala`:

```scala
package org.mule.weave.benchmark.engine

import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import java.io.File

class WarmBenchTest extends AnyFreeSpec with Matchers {
  private val corpus = new File("../../corpus").getCanonicalFile
  private val manifest = Manifest.load(corpus)

  "warm rows are produced with ms unit and positive median" in {
    val shell = new EngineShell()
    val rows = WarmBench.runWarm(shell, manifest, warmupCap = Some(2), iterCap = Some(3))
    rows.map(_.id) should contain ("trivial")
    all (rows.map(_.metric)) shouldBe "warm"
    all (rows.map(_.unit)) shouldBe "ms"
    all (rows.map(_.stats.median)) should be > 0.0
    all (rows.map(_.iterations)) shouldBe 3
  }

  "streaming rows are produced with MB/s unit" in {
    if (!TestSupport.ensureGeneratedInputs(corpus)) cancel("generated input unavailable (node missing?)")
    val shell = new EngineShell()
    val rows = WarmBench.runStreaming(shell, manifest, iterCap = Some(2))
    rows.map(_.id) should contain allOf ("map-scale", "json-stream")
    all (rows.map(_.metric)) shouldBe "streaming"
    all (rows.map(_.unit)) shouldBe "MB/s"
    all (rows.map(_.stats.median)) should be > 0.0
  }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `./gradlew benchmarks-engine:test --tests "org.mule.weave.benchmark.engine.WarmBenchTest"`
Expected: FAIL — `Row` / `WarmBench` not found.

- [ ] **Step 3: Implement `Row` and `WarmBench`**

Create `benchmarks/runners/engine/src/main/scala/org/mule/weave/benchmark/engine/Row.scala`:

```scala
package org.mule.weave.benchmark.engine

/** One flat (case, metric) result row — the schema's unit of output. */
final case class Row(id: String, metric: String, unit: String, stats: Stats.Summary, iterations: Int)
```

Create `benchmarks/runners/engine/src/main/scala/org/mule/weave/benchmark/engine/WarmBench.scala`:

```scala
package org.mule.weave.benchmark.engine

/** In-process metrics: warm steady-state (with a JVM JIT warmup floor) and
  * streaming throughput. Timing mirrors the Node runner: System.nanoTime -> ms. */
object WarmBench {

  val WARMUP_FLOOR: Int = 2000

  private def nowNs(): Long = System.nanoTime()
  private def msSince(startNs: Long): Double = (System.nanoTime() - startNs) / 1e6

  def runWarm(shell: EngineShell, m: Manifest, warmupCap: Option[Int] = None, iterCap: Option[Int] = None): Seq[Row] = {
    Manifest.casesForMetric(m, "warm").map { c =>
      val script = Manifest.resolveScript(m, c)
      val inputs = Manifest.resolveInputs(m, c)
      val name = EngineShell.safeName(c.id)
      val warmup = warmupCap.getOrElse(math.max(c.warmup, WARMUP_FLOOR))
      val iters = iterCap.getOrElse(c.warm)

      println(s"[warm] ${c.id}: warmup=$warmup iters=$iters")
      var i = 0
      while (i < warmup) { shell.run(script, name, inputs, new CountingOutputStream()); i += 1 }

      val samples = new Array[Double](iters)
      i = 0
      while (i < iters) {
        val start = nowNs()
        shell.run(script, name, inputs, new CountingOutputStream())
        samples(i) = msSince(start)
        i += 1
      }
      Row(c.id, "warm", "ms", Stats.computeStats(samples.toSeq), iters)
    }
  }

  def runStreaming(shell: EngineShell, m: Manifest, iterCap: Option[Int] = None): Seq[Row] = {
    Manifest.casesForMetric(m, "streaming").map { c =>
      val script = Manifest.resolveScript(m, c)
      val inputs = Manifest.resolveInputs(m, c)
      val name = EngineShell.safeName(c.id)
      val primaryBytes = inputs.head.bytes.length.toLong
      val iters = iterCap.getOrElse(c.streaming)

      val mbps = new Array[Double](iters)
      var i = 0
      while (i < iters) {
        val start = nowNs()
        shell.run(script, name, inputs, new CountingOutputStream())
        mbps(i) = Stats.toMBps(primaryBytes, msSince(start))
        i += 1
      }
      Row(c.id, "streaming", "MB/s", Stats.computeStats(mbps.toSeq), iters)
    }
  }
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `./gradlew benchmarks-engine:test --tests "org.mule.weave.benchmark.engine.WarmBenchTest"`
Expected: PASS (2 tests; streaming test cancels rather than fails if Node is unavailable).

- [ ] **Step 5: Commit**

```bash
git add benchmarks/runners/engine/src/main/scala/org/mule/weave/benchmark/engine/Row.scala \
  benchmarks/runners/engine/src/main/scala/org/mule/weave/benchmark/engine/WarmBench.scala \
  benchmarks/runners/engine/src/test/scala/org/mule/weave/benchmark/engine/TestSupport.scala \
  benchmarks/runners/engine/src/test/scala/org/mule/weave/benchmark/engine/WarmBenchTest.scala
git commit -m "W-23545283: Add WarmBench (warm + streaming) with JIT warmup floor"
```

---

### Task 7: Env stamp + Result JSON + Emit orchestrator

**Files:**
- Create: `benchmarks/runners/engine/src/main/scala/org/mule/weave/benchmark/engine/EnvStamp.scala`
- Create: `benchmarks/runners/engine/src/main/scala/org/mule/weave/benchmark/engine/Result.scala`
- Create: `benchmarks/runners/engine/src/main/scala/org/mule/weave/benchmark/engine/Emit.scala`
- Test: `benchmarks/runners/engine/src/test/scala/org/mule/weave/benchmark/engine/EmitTest.scala`

**Interfaces:**
- Consumes: `Manifest`, `EngineShell`, `WarmBench`, `Row`, `Stats`, `org.json`.
- Produces:
  - `case class Env(runner: String, os: String, cpu: String, runtimeVersion: String, weaveVersion: String, commit: String, dwlibBuildId: String)`
  - `object EnvStamp.gather(repoRoot: File): Env` — `runner="engine"`; `os = "<os.name>-<os.arch>"`; `cpu` best-effort (mac `sysctl -n machdep.cpu.brand_string`, linux `/proc/cpuinfo model name`, else `os.arch`); `runtimeVersion = "jvm " + java.version`; `weaveVersion` from `repoRoot/gradle.properties` (`^weaveVersion=`); `commit` from `git rev-parse --short HEAD` (fallback `"unknown"`); `dwlibBuildId = "n/a-engine"`.
  - `object Result.toJson(env: Env, rows: Seq[Row], timestamp: String): String` — schema-conformant JSON (2-space indent).
  - `object Emit`:
    - `case class Caps(samples: Option[Int] = None, warmup: Option[Int] = None, warm: Option[Int] = None, streaming: Option[Int] = None)`
    - `run(corpus: File, resultsDir: File, repoRoot: File, caps: Caps = Caps()): File` — spawn `EngineChild` for cold-start/first-run rows, run `WarmBench` in-process, validate ids (fail-fast), stamp env, write `resultsDir/engine-<ts>.json`, return the file.
    - `main(args: Array[String]): Unit` — args `<corpus> <resultsDir> <repoRoot>`, no caps.

- [ ] **Step 1: Write the failing test**

Create `benchmarks/runners/engine/src/test/scala/org/mule/weave/benchmark/engine/EmitTest.scala`:

```scala
package org.mule.weave.benchmark.engine

import org.json.JSONObject
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files

class EmitTest extends AnyFreeSpec with Matchers {
  private val corpus = new File("../../corpus").getCanonicalFile
  private val repoRoot = new File("../../..").getCanonicalFile

  "emit writes a schema-shaped result file with engine runner and valid ids" in {
    if (!TestSupport.ensureGeneratedInputs(corpus)) cancel("generated input unavailable (node missing?)")
    val resultsDir = Files.createTempDirectory("engine-emit-test").toFile
    val out = Emit.run(corpus, resultsDir, repoRoot,
      Emit.Caps(samples = Some(2), warmup = Some(2), warm = Some(2), streaming = Some(2)))

    out.exists() shouldBe true
    out.getName should (startWith ("engine-") and endWith (".json"))

    val root = new JSONObject(new String(Files.readAllBytes(out.toPath), StandardCharsets.UTF_8))
    root.getString("schemaVersion") shouldBe "1.0"
    root.getString("runner") shouldBe "engine"

    val env = root.getJSONObject("env")
    Seq("os", "cpu", "runtimeVersion", "weaveVersion", "commit", "dwlibBuildId")
      .foreach(k => env.has(k) shouldBe true)
    env.getString("dwlibBuildId") shouldBe "n/a-engine"
    env.getString("weaveVersion") should not be empty

    val cases = root.getJSONArray("cases")
    cases.length() should be > 0
    val manifest = Manifest.load(corpus)
    for (i <- 0 until cases.length()) {
      val c = cases.getJSONObject(i)
      manifest.ids should contain (c.getString("id"))
      Seq("cold-start", "first-run", "warm", "streaming") should contain (c.getString("metric"))
      Seq("ms", "MB/s") should contain (c.getString("unit"))
      c.getJSONObject("stats").getDouble("median") should be >= 0.0
    }
  }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `./gradlew benchmarks-engine:test --tests "org.mule.weave.benchmark.engine.EmitTest"`
Expected: FAIL — `Emit` / `EnvStamp` / `Result` not found.

- [ ] **Step 3: Implement `EnvStamp`**

Create `benchmarks/runners/engine/src/main/scala/org/mule/weave/benchmark/engine/EnvStamp.scala`:

```scala
package org.mule.weave.benchmark.engine

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import scala.util.control.NonFatal

final case class Env(
  runner: String,
  os: String,
  cpu: String,
  runtimeVersion: String,
  weaveVersion: String,
  commit: String,
  dwlibBuildId: String)

object EnvStamp {

  def gather(repoRoot: File): Env = Env(
    runner = "engine",
    os = s"${System.getProperty("os.name")}-${System.getProperty("os.arch")}",
    cpu = cpuModel(),
    runtimeVersion = "jvm " + System.getProperty("java.version"),
    weaveVersion = readWeaveVersion(repoRoot),
    commit = gitCommit(repoRoot),
    dwlibBuildId = "n/a-engine")

  private def readWeaveVersion(repoRoot: File): String = {
    val txt = new String(Files.readAllBytes(new File(repoRoot, "gradle.properties").toPath), StandardCharsets.UTF_8)
    """(?m)^weaveVersion=(.+)$""".r.findFirstMatchIn(txt).map(_.group(1).trim)
      .getOrElse(throw new RuntimeException("weaveVersion not found in gradle.properties"))
  }

  private def gitCommit(repoRoot: File): String =
    exec(Seq("git", "rev-parse", "--short", "HEAD"), repoRoot).getOrElse("unknown")

  private def cpuModel(): String = {
    val os = System.getProperty("os.name").toLowerCase
    val fromShell =
      if (os.contains("mac")) exec(Seq("sysctl", "-n", "machdep.cpu.brand_string"), new File("."))
      else if (os.contains("linux"))
        exec(Seq("bash", "-c", "grep -m1 'model name' /proc/cpuinfo | cut -d: -f2"), new File("."))
      else None
    fromShell.map(_.trim).filter(_.nonEmpty).getOrElse(System.getProperty("os.arch"))
  }

  private def exec(cmd: Seq[String], dir: File): Option[String] =
    try {
      val p = new ProcessBuilder(cmd: _*).directory(dir).start()
      val out = scala.io.Source.fromInputStream(p.getInputStream).mkString.trim
      if (p.waitFor() == 0 && out.nonEmpty) Some(out) else None
    } catch { case NonFatal(_) => None }
}
```

- [ ] **Step 4: Implement `Result`**

Create `benchmarks/runners/engine/src/main/scala/org/mule/weave/benchmark/engine/Result.scala`:

```scala
package org.mule.weave.benchmark.engine

import org.json.{ JSONArray, JSONObject }

/** Serializes rows + env into the shared benchmark result schema
  * (benchmarks/schema/result.schema.json). */
object Result {

  def toJson(env: Env, rows: Seq[Row], timestamp: String): String = {
    val root = new JSONObject()
    root.put("schemaVersion", "1.0")
    root.put("runner", env.runner)

    val envObj = new JSONObject()
    envObj.put("os", env.os)
    envObj.put("cpu", env.cpu)
    envObj.put("runtimeVersion", env.runtimeVersion)
    envObj.put("weaveVersion", env.weaveVersion)
    envObj.put("commit", env.commit)
    envObj.put("dwlibBuildId", env.dwlibBuildId)
    root.put("env", envObj)

    root.put("timestamp", timestamp)

    val casesArr = new JSONArray()
    rows.foreach { r =>
      val c = new JSONObject()
      c.put("id", r.id)
      c.put("metric", r.metric)
      c.put("unit", r.unit)
      c.put("iterations", r.iterations)
      val s = new JSONObject()
      s.put("min", r.stats.min)
      s.put("median", r.stats.median)
      s.put("p90", r.stats.p90)
      s.put("p99", r.stats.p99)
      s.put("mean", r.stats.mean)
      c.put("stats", s)
      casesArr.put(c)
    }
    root.put("cases", casesArr)

    root.toString(2)
  }
}
```

- [ ] **Step 5: Implement `Emit`**

Create `benchmarks/runners/engine/src/main/scala/org/mule/weave/benchmark/engine/Emit.scala`:

```scala
package org.mule.weave.benchmark.engine

import org.json.JSONObject

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.time.Instant

/** Orchestrator: spawns EngineChild for cold-start/first-run, runs WarmBench
  * in-process, validates ids, stamps env, and writes the result JSON. */
object Emit {

  final case class Caps(
    samples: Option[Int] = None,
    warmup: Option[Int] = None,
    warm: Option[Int] = None,
    streaming: Option[Int] = None)

  def run(corpus: File, resultsDir: File, repoRoot: File, caps: Caps = Caps()): File = {
    val manifest = Manifest.load(corpus)

    val coldRows = spawnColdAndFirstRun(manifest, corpus, caps.samples)

    val shell = new EngineShell()
    val warmRows =
      WarmBench.runWarm(shell, manifest, caps.warmup, caps.warm) ++
        WarmBench.runStreaming(shell, manifest, caps.streaming)

    val rows = coldRows ++ warmRows
    Manifest.validateResultIds(manifest, rows.map(_.id)) // fail-fast on orphan ids

    val env = EnvStamp.gather(repoRoot)
    val now = Instant.now().toString
    val json = Result.toJson(env, rows, now)

    resultsDir.mkdirs()
    val out = new File(resultsDir, s"engine-${now.replaceAll("[:.]", "-")}.json")
    Files.write(out.toPath, json.getBytes(StandardCharsets.UTF_8))
    println(s"wrote ${out.getAbsolutePath} (${rows.length} rows)")
    out
  }

  /** Spawn a fresh JVM per sample; aggregate init/first-run per case. */
  private def spawnColdAndFirstRun(manifest: Manifest, corpus: File, samplesCap: Option[Int]): Seq[Row] = {
    val ids = (Manifest.casesForMetric(manifest, "cold-start").map(_.id) ++
      Manifest.casesForMetric(manifest, "first-run").map(_.id)).distinct

    ids.flatMap { id =>
      val c = manifest.cases.find(_.id == id).get
      val n = samplesCap.getOrElse(c.samples)
      val inits = new Array[Double](n)
      val firsts = new Array[Double](n)
      var i = 0
      while (i < n) {
        val (initMs, firstMs) = sampleOnce(corpus, id)
        inits(i) = initMs
        firsts(i) = firstMs
        i += 1
      }
      val rows = scala.collection.mutable.ArrayBuffer[Row]()
      if (c.metrics.contains("cold-start"))
        rows += Row(id, "cold-start", "ms", Stats.computeStats(inits.toSeq), n)
      if (c.metrics.contains("first-run"))
        rows += Row(id, "first-run", "ms", Stats.computeStats(firsts.toSeq), n)
      rows.toSeq
    }
  }

  private def sampleOnce(corpus: File, caseId: String): (Double, Double) = {
    val javaBin = new File(System.getProperty("java.home"), "bin/java").getAbsolutePath
    val cp = System.getProperty("java.class.path")
    val pb = new ProcessBuilder(
      javaBin, "-cp", cp,
      "org.mule.weave.benchmark.engine.EngineChild",
      corpus.getAbsolutePath, caseId)
    val p = pb.start()
    val lines = scala.io.Source.fromInputStream(p.getInputStream).getLines().toList
    val code = p.waitFor()
    if (code != 0) throw new RuntimeException(s"EngineChild failed for case '$caseId' (exit $code)")
    val obj = new JSONObject(lines.last)
    (obj.getDouble("initMs"), obj.getDouble("firstRunMs"))
  }

  def main(args: Array[String]): Unit = {
    require(args.length >= 3, "usage: Emit <corpusDir> <resultsDir> <repoRoot>")
    run(new File(args(0)), new File(args(1)), new File(args(2)))
  }
}
```

- [ ] **Step 6: Run the test to verify it passes**

Run: `./gradlew benchmarks-engine:test --tests "org.mule.weave.benchmark.engine.EmitTest"`
Expected: PASS (1 test; cancels if Node unavailable for input generation).

- [ ] **Step 7: Commit**

```bash
git add benchmarks/runners/engine/src/main/scala/org/mule/weave/benchmark/engine/EnvStamp.scala \
  benchmarks/runners/engine/src/main/scala/org/mule/weave/benchmark/engine/Result.scala \
  benchmarks/runners/engine/src/main/scala/org/mule/weave/benchmark/engine/Emit.scala \
  benchmarks/runners/engine/src/test/scala/org/mule/weave/benchmark/engine/EmitTest.scala
git commit -m "W-23545283: Add Emit orchestrator, env stamp, and result serializer"
```

---

### Task 8: Wire the Gradle task end-to-end + README fix + report validation

**Files:**
- Modify: `benchmarks/README.md`
- (Verification only, no source change) `benchmarks/runners/engine/build.gradle` (`benchmarkEngine` task from Task 1), `benchmarks/report/report.mjs`, `benchmarks/schema/schema.test.mjs`.

**Interfaces:**
- Consumes: `Emit` (Task 7), the existing `report/report.mjs` and `schema/schema.test.mjs`.
- Produces: a runnable `./gradlew benchmarks-engine:benchmarkEngine -Pbenchmark=true` that writes `benchmarks/results/engine-<ts>.json` which `report.mjs` renders as the baseline; corrected README.

- [ ] **Step 1: Fix the stale README claim**

In `benchmarks/README.md` the Layout bullet currently spans two lines (12–13):

```
- `runners/node/` — the Node reference runner. `runners/python/` and `runners/engine/`
  are follow-ups; the engine harness lives in the `data-weave` repo but reads this corpus.
```

Replace those two lines (Edit `old_string` must match both verbatim, including the leading `- ` and the two-space continuation indent) with:

```markdown
- `runners/node/` — the Node reference runner. `runners/engine/` is the JVM baseline
  (Scala/Gradle subproject `:benchmarks-engine`, depends on `org.mule.weave:runtime` at
  the same `weaveVersion` the native image is built from). `runners/python/` is a follow-up.
```

Then add an engine-runner invocation under the "Running" section:

```markdown
Run the engine (JVM) baseline and let the report pick it up as the comparison anchor:

    ./gradlew benchmarks-engine:benchmarkEngine -Pbenchmark=true   # writes results/engine-<ts>.json
    node report/report.mjs results/*.json                          # engine is auto-selected as baseline
```

- [ ] **Step 2: Commit the docs fix**

```bash
git add benchmarks/README.md
git commit -m "W-23545283: Correct README — engine runner lives in this repo"
```

- [ ] **Step 3: Run the whole engine test suite**

Run: `./gradlew benchmarks-engine:test`
Expected: PASS — SmokeTest, StatsParityTest, ManifestTest, EngineShellTest, EngineChildTest, WarmBenchTest, EmitTest all green (streaming/emit tests cancel only if Node is missing).

- [ ] **Step 4: Run the opt-in benchmark task end-to-end**

Run: `./gradlew benchmarks-engine:benchmarkEngine -Pbenchmark=true`
Expected: `genBenchInputs` regenerates `corpus/inputs/generated/records-large.json`, then `Emit` prints `wrote .../benchmarks/results/engine-<ts>.json (<N> rows)`. This is real timing — it takes a few minutes (fresh JVM per cold-start/first-run sample + 2000-iter warmup per warm case).

- [ ] **Step 5: Verify the emitted result conforms to the schema shape**

`benchmarks/schema/schema.test.mjs` only asserts the *schema document's* shape — it does not validate result files. So confirm the engine output structurally with a one-off (no new deps; pure Node built-ins), from the repo root:

Run:
```bash
node -e '
const {readFileSync,readdirSync}=require("node:fs");
const dir="benchmarks/results";
const f=readdirSync(dir).filter(n=>n.startsWith("engine-")&&n.endsWith(".json")).sort().pop();
const r=JSON.parse(readFileSync(dir+"/"+f,"utf8"));
const need=(o,k)=>{if(!(k in o))throw new Error("missing "+k);};
["schemaVersion","runner","env","timestamp","cases"].forEach(k=>need(r,k));
if(r.schemaVersion!=="1.0")throw new Error("schemaVersion");
if(r.runner!=="engine")throw new Error("runner");
["os","cpu","runtimeVersion","weaveVersion","commit","dwlibBuildId"].forEach(k=>need(r.env,k));
for(const c of r.cases){["id","metric","unit","stats","iterations"].forEach(k=>need(c,k));
  if(!["cold-start","first-run","warm","streaming"].includes(c.metric))throw new Error("metric "+c.metric);
  if(!["ms","MB/s"].includes(c.unit))throw new Error("unit "+c.unit);
  need(c.stats,"median");}
console.log("OK "+f+" ("+r.cases.length+" rows)");
'
```
Expected: `OK engine-<ts>.json (<N> rows)`. Any missing key or bad enum throws.

- [ ] **Step 6: Verify the report renders the engine as baseline with no skew banner**

Run: `cd benchmarks && node report/report.mjs results/*.json`
Expected: a markdown table grouped by case × metric with an `engine` column and a `Δ vs engine` column; **no** `⚠️ WEAVE VERSION SKEW` banner (engine and any wrapper result share the same `weaveVersion` from `gradle.properties`). If only the engine result exists, it appears as the single baseline column.

- [ ] **Step 7: Final commit (if any verification-driven tweaks were needed)**

```bash
git add -A
git commit -m "W-23545283: Verify engine-runner end-to-end (benchmarkEngine + report)"
```

---

## Self-Review

**1. Spec coverage** (against `docs/superpowers/specs/2026-07-23-engine-runner-design.md`):
- Bare `DataWeaveScriptingEngine` → Task 4 (`EngineShell`). ✓
- Scala language, `benchmarks/runners/engine/` placement → Tasks 1–7. ✓
- Gradle subproject + settings + opt-in task + auto report pickup → Tasks 1, 8. ✓
- SPI note (core-modules on classpath, no re-materialization) → Task 1 dep + Task 4 Step 4 note. ✓
- Engine shell specifics (classloader resolver, netty setupEnv, UTF-8 CharsetProviderService, compileWith + write-to-OutputStream) → Task 4. ✓
- Fresh-JVM cold-start + first-run → Task 5 (`EngineChild`) + Task 7 spawn aggregation. ✓
- JIT warmup floor `max(manifest, 2000)`, streaming MB/s → Task 6. ✓
- Self-contained emit: Stats parity test, env, id fail-fast, schema conformance, `dwlibBuildId:"n/a-engine"` → Tasks 2, 7. ✓
- Docs fix → Task 8. ✓
- Testing (StatsParityTest, schema conformance, manifest parsing, e2e report) → Tasks 2, 3, 8. ✓
- Open note resolved: Node wrapper recompiles per `run()`, so engine `warm` also recompiles per iteration (compile+exec both sides) → Task 4 interface + Task 6. ✓

**2. Placeholder scan:** No "TBD"/"TODO"/"handle edge cases" — every code step has complete, compilable code. ✓

**3. Type consistency:** `Row`, `Stats.Summary`, `Env`, `ResolvedInput`, `BenchCase`, `Emit.Caps`, `EngineShell.run(script, name, inputs, out)`, `EngineShell.safeName`, `Manifest.casesForMetric/validateResultIds/resolveScript/resolveInputs`, `WarmBench.runWarm/runStreaming`, `Result.toJson`, `EnvStamp.gather` are named identically everywhere they appear across tasks. ✓
