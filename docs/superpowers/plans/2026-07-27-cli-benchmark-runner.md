# CLI Benchmark Runner Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a fourth benchmark runner that measures the shipped `dw` native CLI over the shared corpus, emitting `cold-start`, `first-run`, and `warm` metrics into the existing comparison harness.

**Architecture:** A build-gated benchmark harness inside `native-cli` (compiled into `dw` only under `-Pbenchmark=true`, tree-shaken out of production) prints a `READY` marker after constructing one `NativeRuntime`, then runs timed work — mirroring the Node/Python/engine child protocol. A Node parent under `benchmarks/runners/cli/` spawns that binary per case, stamps cold-start at spawn→READY, and reads back in-process timings, reusing the shared `lib/` modules.

**Tech Stack:** Java (picocli entrypoint + generated constant), Scala 2.12 (benchmark harness on `NativeRuntime`), GraalVM native-image, Node.js (parent orchestrator, ESM), Gradle, scalatest, `node --test`.

## Global Constraints

- **Weave runtime version:** pinned by `weaveVersion` in `gradle.properties` — never hardcode; read it (parent already does via `lib/env.mjs`).
- **Benchmark tasks are opt-in only:** every Gradle benchmark task guards with `onlyIf { project.findProperty('benchmark')?.toString()?.toBoolean() == true }`. Never part of normal `build`/`test`/CI.
- **Production `dw` must not contain benchmark code:** gated by a generated `BenchmarkMode.ENABLED` constant that is `false` unless `-Pbenchmark=true`; native-image folds the unreachable branch away.
- **Result schema is frozen:** output must conform to `benchmarks/schema/result.schema.json` (`schemaVersion: "1.0"`; metrics ∈ `cold-start|first-run|warm|streaming`; units ∈ `ms|MB/s`). Do NOT edit the schema, `report.mjs`, `benchmarkCompare`, or `corpus/manifest.json`.
- **Runner column name:** the result's `runner` field is `"cli"` (the report's column + dedupe key).
- **Runner registration contract:** a runner integrates by (1) writing `benchmarks/results/<runner>-<timestamp>.json` and (2) tagging its Gradle task `ext.benchmarkRunner = true`. `benchmarkCompare` auto-discovers it — do NOT edit `benchmarkCompare`.
- **Child stdout discipline:** the only stdout the harness emits is the line `READY` (flushed) followed by exactly one JSON line. Transformation output goes to a discarding stream, never stdout.
- **`dwlibBuildId` env field:** `"n/a-cli"` (the CLI is a binary, not the staged `dwlib`), following the engine runner's `"n/a-engine"` convention.
- **Cross-platform:** Gradle `Exec` tasks branch on `os.name` containing `windows` (`cmd /c` vs `bash -c`), matching existing tasks. Binary name is `dw` (`dw.exe` on Windows).

---

## File Structure

**Created:**
- `native-cli/src/main/scala/org/mule/weave/dwnative/benchmark/BenchmarkHarness.scala` — the corpus-agnostic in-binary harness (arg parse, `coldfirst`/`warm` modes, READY + JSON output).
- `native-cli/src/test/scala/org/mule/weave/dwnative/benchmark/BenchmarkHarnessTest.scala` — scalatest for the harness + a guard that `BenchmarkMode.ENABLED` is `false` in a normal build.
- `benchmarks/runners/cli/locate.mjs` — resolves the bench-enabled `dw` binary.
- `benchmarks/runners/cli/coldstart.mjs` — spawns `coldfirst` per sample; cold-start + first-run rows.
- `benchmarks/runners/cli/warm.mjs` — spawns `warm` once per warm case; warm rows.
- `benchmarks/runners/cli/emit.mjs` — assembles env + rows, writes `results/cli-<ts>.json`.
- `benchmarks/runners/cli/locate.test.mjs` — dwlib/binary-free unit test for `locate.mjs`.
- `benchmarks/runners/cli/emit.test.mjs` — dwlib/binary-free unit test for the result builder.

**Modified:**
- `native-cli/src/main/java/org/mule/weave/cli/DWCLI.java` — dispatch to the harness before picocli when gated + env set.
- `native-cli/build.gradle` — `genBenchmarkMode` task (generates `BenchmarkMode.java`), wire onto compile, `benchmarkCli` runner task.
- `native-lib/build.gradle` — add `runners/cli/*.test.mjs` to the always-on `benchmarkJsUnitTest` file list.
- `benchmarks/README.md` — document the CLI runner.

---

## Task 1: Generate the `BenchmarkMode.ENABLED` build gate

**Files:**
- Modify: `native-cli/build.gradle` (add `genBenchmarkMode` task near `genVersions` at line ~51; wire into `compileScala`/`compileJava` deps like `genVersions`)
- Verify against: `native-cli/build.gradle:48-74` (the `genVersions` pattern generates into `build/genresource`, which is already a source dir per `native-cli/build.gradle:7-13`)

**Interfaces:**
- Produces: a generated Java class `org.mule.weave.cli.BenchmarkMode` with `public static final boolean ENABLED` — `true` only when the Gradle property `benchmark` is truthy, else `false`. Consumed by Task 2 (`DWCLI`) and Task 3's guard test.

Rationale: a **Java** constant (not Scala) so `DWCLI.java` reads it with no cross-language friction; `build/genresource` is already on the Scala srcDir but `javac` also compiles generated Java there via the existing `compileJava` classpath wiring (`native-cli/build.gradle:153-154`). Generate into a Java-compiled location: use a dedicated `build/genjava` dir added to the java sourceSet to keep it unambiguous.

- [ ] **Step 1: Add a generated-Java source dir to the java sourceSet**

In `native-cli/build.gradle`, extend the `sourceSets` block (currently lines 7-13) to add a java srcDir:

```groovy
sourceSets {
    main {
        scala {
            srcDirs = ['src/main/scala', 'build/genresource']
        }
        java {
            srcDirs += 'build/genjava'
        }
    }
}
```

- [ ] **Step 2: Add the `genBenchmarkMode` task**

Immediately after the `genVersions` task (after line 67 in `native-cli/build.gradle`), add:

```groovy
def genJavaDirectory = new File("$project.buildDir/genjava")

task genBenchmarkMode() {
    def enabled = project.findProperty('benchmark')?.toString()?.toBoolean() == true
    def benchmarkMode = new File(genJavaDirectory, "org/mule/weave/cli/BenchmarkMode.java")
    def parentFile = benchmarkMode.getParentFile()
    if (!parentFile.exists()) {
        parentFile.mkdirs()
    }
    final PrintWriter outputPrinter = new PrintWriter(new FileWriter(benchmarkMode))
    outputPrinter.println("package org.mule.weave.cli;")
    outputPrinter.println()
    outputPrinter.println("// GENERATED by genBenchmarkMode — do not edit.")
    outputPrinter.println("// ENABLED is true only when built with -Pbenchmark=true; native-image")
    outputPrinter.println("// folds the benchmark branch away as dead code when this is false.")
    outputPrinter.println("public final class BenchmarkMode {")
    outputPrinter.println("    private BenchmarkMode() {}")
    outputPrinter.println("    public static final boolean ENABLED = " + enabled + ";")
    outputPrinter.println("}")
    outputPrinter.close()
}
```

- [ ] **Step 3: Wire it into compilation**

Update the existing `compileScala` block (lines 72-74) and add a `compileJava` dependency so the constant exists before either compiles:

```groovy
defaultTasks += genVersions

compileScala {
    dependsOn genVersions
    dependsOn genBenchmarkMode
}

compileJava {
    dependsOn genBenchmarkMode
}
```

- [ ] **Step 4: Verify normal build generates `ENABLED = false`**

Run: `./gradlew native-cli:genBenchmarkMode && cat native-cli/build/genjava/org/mule/weave/cli/BenchmarkMode.java`
Expected: file contains `public static final boolean ENABLED = false;`

- [ ] **Step 5: Verify benchmark build generates `ENABLED = true`**

Run: `./gradlew native-cli:genBenchmarkMode -Pbenchmark=true && cat native-cli/build/genjava/org/mule/weave/cli/BenchmarkMode.java`
Expected: file contains `public static final boolean ENABLED = true;`

- [ ] **Step 6: Commit**

```bash
git add native-cli/build.gradle
git commit -m "build: generate BenchmarkMode.ENABLED gate for native-cli"
```

---

## Task 2: `BenchmarkHarness` — the in-binary corpus-agnostic harness

**Files:**
- Create: `native-cli/src/main/scala/org/mule/weave/dwnative/benchmark/BenchmarkHarness.scala`
- Create: `native-cli/src/test/scala/org/mule/weave/dwnative/benchmark/BenchmarkHarnessTest.scala`

**Interfaces:**
- Consumes: `org.mule.weave.dwnative.NativeRuntime` (constructor `new NativeRuntime(libDir: File, path: Array[File], console: Console, maybeLanguageLevel: Option[DataWeaveVersion])`; method `run(script: String, nameIdentifier: String, inputs: ScriptingBindings, out: OutputStream, defaultOutputMimeType: String, maybePrivileges: Option[Seq[String]]): WeaveExecutionResult` where `WeaveExecutionResult.success(): Boolean` and `.result(): String`); `org.mule.weave.dwnative.utils.DataWeaveUtils#getLibPathHome(): File`; `org.mule.weave.dwnative.cli.DefaultConsole`; `org.mule.weave.v2.runtime.ScriptingBindings#addBinding(name, value: BindingValue)`; `org.mule.weave.v2.runtime.BindingValue(bytes: Array[Byte], mimeType: Option[String], props: Map[String,Any], charset: Charset)`.
- Produces: `object BenchmarkHarness { def main(args: Array[String]): Unit }` and (for tests) `def parseArgs(args: Array[String]): BenchArgs`, `case class BenchArgs(mode: String, scriptFile: String, inputs: Seq[BenchInput], warmup: Int, iters: Int)`, `case class BenchInput(name: String, file: String, mimeType: String, charset: String)`, and `def runColdFirst(args, out: java.io.PrintStream, sink: OutputStream): Unit` / `def runWarm(args, out: java.io.PrintStream, sink: OutputStream): Unit` (out = where READY/JSON go; sink = discard stream for transform output). `main` calls these with `System.out` and a fresh `CountingOutputStream`.

Reuse a discarding stream identical in behavior to the engine runner's `CountingOutputStream`; define a small private one here rather than depend on the `benchmarks-engine` module (no such dependency exists from `native-cli`).

Arg format from the parent (one `--input` per binding):
```
--bench-mode=coldfirst|warm
--script=<absolute file path>
--input=<name>=<file>:<mimeType>:<charset>
--warmup=<int>   (warm mode only; default 0)
--iters=<int>    (warm mode only; default 100)
```

- [ ] **Step 1: Write the failing test for arg parsing**

Create `native-cli/src/test/scala/org/mule/weave/dwnative/benchmark/BenchmarkHarnessTest.scala`:

```scala
package org.mule.weave.dwnative.benchmark

import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class BenchmarkHarnessTest extends AnyFreeSpec with Matchers {

  "parseArgs" - {
    "parses coldfirst mode with one input" in {
      val a = BenchmarkHarness.parseArgs(Array(
        "--bench-mode=coldfirst",
        "--script=/tmp/x.dwl",
        "--input=payload=/tmp/p.json:application/json:utf-8"))
      a.mode shouldBe "coldfirst"
      a.scriptFile shouldBe "/tmp/x.dwl"
      a.inputs should have size 1
      a.inputs.head shouldBe BenchInput("payload", "/tmp/p.json", "application/json", "utf-8")
    }

    "parses warm mode with warmup and iters" in {
      val a = BenchmarkHarness.parseArgs(Array(
        "--bench-mode=warm", "--script=/tmp/x.dwl", "--warmup=5", "--iters=30"))
      a.mode shouldBe "warm"
      a.warmup shouldBe 5
      a.iters shouldBe 30
      a.inputs shouldBe empty
    }

    "handles a mimeType-only input (charset defaults to utf-8)" in {
      val a = BenchmarkHarness.parseArgs(Array(
        "--bench-mode=coldfirst", "--script=/tmp/x.dwl",
        "--input=payload=/tmp/p.json:application/json"))
      a.inputs.head.charset shouldBe "utf-8"
    }
  }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `./gradlew native-cli:test --tests "org.mule.weave.dwnative.benchmark.BenchmarkHarnessTest"`
Expected: FAIL — `BenchmarkHarness` / `BenchInput` not found (compilation error).

- [ ] **Step 3: Implement `BenchmarkHarness` with parsing + modes**

Create `native-cli/src/main/scala/org/mule/weave/dwnative/benchmark/BenchmarkHarness.scala`:

```scala
package org.mule.weave.dwnative.benchmark

import org.mule.weave.dwnative.NativeRuntime
import org.mule.weave.dwnative.WeaveExecutionResult
import org.mule.weave.dwnative.cli.DefaultConsole
import org.mule.weave.dwnative.utils.DataWeaveUtils
import org.mule.weave.v2.runtime.BindingValue
import org.mule.weave.v2.runtime.ScriptingBindings

import java.io.{ File, OutputStream, PrintStream }
import java.nio.charset.Charset
import java.nio.file.Files

final case class BenchInput(name: String, file: String, mimeType: String, charset: String)
final case class BenchArgs(mode: String, scriptFile: String, inputs: Seq[BenchInput], warmup: Int, iters: Int)

/** Corpus-agnostic in-binary benchmark harness. Reachable only in a build made with
  * -Pbenchmark=true (guarded by BenchmarkMode.ENABLED in DWCLI); native-image folds it
  * out of a production dw. Prints "READY" the instant one NativeRuntime is constructed,
  * then a single JSON line of timings. Parent (benchmarks/runners/cli) measures cold-start
  * as spawn->READY wall-clock. */
object BenchmarkHarness {

  /** Discards bytes; used as the transform write sink so we never touch real stdout. */
  private final class DiscardStream extends OutputStream {
    override def write(b: Int): Unit = ()
    override def write(b: Array[Byte]): Unit = ()
    override def write(b: Array[Byte], off: Int, len: Int): Unit = ()
  }

  private def nowNs(): Long = System.nanoTime()
  private def msSince(startNs: Long): Double = (System.nanoTime() - startNs) / 1e6

  def parseArgs(args: Array[String]): BenchArgs = {
    var mode = ""
    var script = ""
    val inputs = scala.collection.mutable.ArrayBuffer[BenchInput]()
    var warmup = 0
    var iters = 100
    args.foreach { arg =>
      val eq = arg.indexOf('=')
      val key = if (eq >= 0) arg.substring(0, eq) else arg
      val value = if (eq >= 0) arg.substring(eq + 1) else ""
      key match {
        case "--bench-mode" => mode = value
        case "--script"     => script = value
        case "--warmup"     => warmup = value.toInt
        case "--iters"      => iters = value.toInt
        case "--input"      =>
          // value = <name>=<file>:<mimeType>[:<charset>]
          val nameSep = value.indexOf('=')
          val name = value.substring(0, nameSep)
          val rest = value.substring(nameSep + 1)
          val parts = rest.split(":", 3)
          val file = parts(0)
          val mimeType = parts(1)
          val charset = if (parts.length > 2 && parts(2).nonEmpty) parts(2) else "utf-8"
          inputs += BenchInput(name, file, mimeType, charset)
        case _ => throw new RuntimeException(s"unknown bench arg: $arg")
      }
    }
    if (mode.isEmpty) throw new RuntimeException("--bench-mode is required")
    if (script.isEmpty) throw new RuntimeException("--script is required")
    BenchArgs(mode, script, inputs.toSeq, warmup, iters)
  }

  private def newRuntime(): NativeRuntime = {
    val console = DefaultConsole.enableSilent()
    val utils = new DataWeaveUtils(console)
    new NativeRuntime(utils.getLibPathHome(), Array.empty[File], console, None)
  }

  private def readScript(a: BenchArgs): String =
    new String(Files.readAllBytes(new File(a.scriptFile).toPath), java.nio.charset.StandardCharsets.UTF_8)

  private def bindings(a: BenchArgs): ScriptingBindings = {
    val b = new ScriptingBindings()
    a.inputs.foreach { in =>
      val bytes = Files.readAllBytes(new File(in.file).toPath)
      val bv = new BindingValue(bytes, Some(in.mimeType), Map.empty[String, Any], Charset.forName(in.charset))
      b.addBinding(in.name, bv)
    }
    b
  }

  private def assertOk(r: WeaveExecutionResult): Unit =
    if (!r.success()) throw new RuntimeException("run failed: " + r.result())

  def runColdFirst(a: BenchArgs, out: PrintStream, sink: OutputStream): Unit = {
    val script = readScript(a)
    val b = bindings(a)
    val rt = newRuntime()          // engine init — measured externally as cold-start
    out.println("READY"); out.flush()
    val start = nowNs()
    assertOk(rt.run(script, "bench", b, sink, "application/json", None))
    val firstRunMs = msSince(start)
    out.println("{\"firstRunMs\":" + firstRunMs + "}")
  }

  def runWarm(a: BenchArgs, out: PrintStream, sink: OutputStream): Unit = {
    val script = readScript(a)
    val b = bindings(a)
    val rt = newRuntime()
    out.println("READY"); out.flush()
    var i = 0
    while (i < a.warmup) { assertOk(rt.run(script, "bench", b, sink, "application/json", None)); i += 1 }
    val samples = new Array[Double](a.iters)
    i = 0
    while (i < a.iters) {
      val start = nowNs()
      assertOk(rt.run(script, "bench", b, sink, "application/json", None))
      samples(i) = msSince(start)
      i += 1
    }
    out.println("{\"warmMs\":[" + samples.mkString(",") + "]}")
  }

  def main(args: Array[String]): Unit = {
    val a = parseArgs(args)
    val sink = new DiscardStream()
    a.mode match {
      case "coldfirst" => runColdFirst(a, System.out, sink)
      case "warm"      => runWarm(a, System.out, sink)
      case other       => throw new RuntimeException(s"unknown --bench-mode: $other")
    }
  }
}
```

- [ ] **Step 4: Run the parsing test to verify it passes**

Run: `./gradlew native-cli:test --tests "org.mule.weave.dwnative.benchmark.BenchmarkHarnessTest"`
Expected: PASS (3 parsing tests).

- [ ] **Step 5: Add behavioral tests (READY + JSON discipline, warm array, failure)**

Append to `BenchmarkHarnessTest.scala` inside the class, using a temp script/input and capturing an in-memory `PrintStream`:

```scala
  import java.io.{ ByteArrayOutputStream, File, PrintStream }
  import java.nio.charset.StandardCharsets
  import java.nio.file.Files

  private def tmp(suffix: String, content: String): File = {
    val f = File.createTempFile("bench", suffix)
    f.deleteOnExit()
    Files.write(f.toPath, content.getBytes(StandardCharsets.UTF_8))
    f
  }

  private def capture(fn: PrintStream => Unit): String = {
    val buf = new ByteArrayOutputStream()
    val ps = new PrintStream(buf, true, "UTF-8")
    fn(ps)
    new String(buf.toByteArray, StandardCharsets.UTF_8)
  }

  "runColdFirst" - {
    "emits READY then a single firstRunMs JSON line, output not on the stream" in {
      val script = tmp(".dwl", "output application/json --- payload.a + 1")
      val input = tmp(".json", "{\"a\": 41}")
      val a = BenchArgs("coldfirst", script.getAbsolutePath,
        Seq(BenchInput("payload", input.getAbsolutePath, "application/json", "utf-8")), 0, 0)
      val sink = new ByteArrayOutputStream()
      val stdout = capture(ps => BenchmarkHarness.runColdFirst(a, ps, sink))
      val lines = stdout.split("\n").filter(_.nonEmpty)
      lines.head shouldBe "READY"
      lines.last should include ("firstRunMs")
      lines.count(_.contains("firstRunMs")) shouldBe 1
      // The transformed "42" went to the sink, NOT to stdout.
      new String(sink.toByteArray, StandardCharsets.UTF_8).trim shouldBe "42"
    }
  }

  "runWarm" - {
    "emits READY then a warmMs array of length iters" in {
      val script = tmp(".dwl", "output application/json --- payload.a + 1")
      val input = tmp(".json", "{\"a\": 41}")
      val a = BenchArgs("warm", script.getAbsolutePath,
        Seq(BenchInput("payload", input.getAbsolutePath, "application/json", "utf-8")), 1, 3)
      val stdout = capture(ps => BenchmarkHarness.runWarm(a, ps, new ByteArrayOutputStream()))
      val json = stdout.split("\n").filter(_.contains("warmMs")).head
      json should include ("warmMs")
      // 3 comma-separated samples -> 2 commas inside the array
      json.count(_ == ',') shouldBe 2
    }
  }

  "a failing script throws (non-zero exit path)" in {
    val script = tmp(".dwl", "output application/json --- payload.missing.deep.path()")
    val input = tmp(".json", "{}")
    val a = BenchArgs("coldfirst", script.getAbsolutePath,
      Seq(BenchInput("payload", input.getAbsolutePath, "application/json", "utf-8")), 0, 0)
    an [RuntimeException] should be thrownBy
      BenchmarkHarness.runColdFirst(a, capturePs(), new ByteArrayOutputStream())
  }

  private def capturePs(): PrintStream = new PrintStream(new ByteArrayOutputStream(), true, "UTF-8")
```

- [ ] **Step 6: Run all harness tests to verify they pass**

Run: `./gradlew native-cli:test --tests "org.mule.weave.dwnative.benchmark.BenchmarkHarnessTest"`
Expected: PASS (parsing + coldfirst + warm + failure).

Note: if the `.dwl` script for the failure case does not actually throw, replace its body with one that reliably fails, e.g. `output application/json --- 1 / 0` — the intent is only that a failed run raises.

- [ ] **Step 7: Commit**

```bash
git add native-cli/src/main/scala/org/mule/weave/dwnative/benchmark/BenchmarkHarness.scala \
        native-cli/src/test/scala/org/mule/weave/dwnative/benchmark/BenchmarkHarnessTest.scala
git commit -m "feat: add in-binary BenchmarkHarness for native-cli"
```

---

## Task 3: Gate the harness behind `BenchmarkMode.ENABLED` in `DWCLI`

**Files:**
- Modify: `native-cli/src/main/java/org/mule/weave/cli/DWCLI.java:32-34` (the `main` method)
- Modify: `native-cli/src/test/scala/org/mule/weave/dwnative/benchmark/BenchmarkHarnessTest.scala` (add the `ENABLED == false` guard)

**Interfaces:**
- Consumes: `org.mule.weave.cli.BenchmarkMode.ENABLED` (Task 1), `org.mule.weave.dwnative.benchmark.BenchmarkHarness.main` (Task 2).
- Produces: no new public API; behavior — when `BenchmarkMode.ENABLED && System.getenv("DW_BENCH") != null`, `dw` dispatches to `BenchmarkHarness.main(args)` before picocli. Otherwise unchanged.

The env-var name is `DW_BENCH` (specific, collision-unlikely). `ENABLED` is the real gate: in production it is a compile-time `false`, so `BenchmarkHarness` is unreachable and native-image drops it.

- [ ] **Step 1: Write the guard test that production has ENABLED=false**

Append to `BenchmarkHarnessTest.scala`:

```scala
  "BenchmarkMode.ENABLED" - {
    "is false in a normal (non -Pbenchmark) build" in {
      // Tests run without -Pbenchmark, so the generated constant must be false —
      // proving the harness is dead code / stripped from a production image.
      org.mule.weave.cli.BenchmarkMode.ENABLED shouldBe false
    }
  }
```

- [ ] **Step 2: Run to verify it fails to compile (constant not generated for test yet)**

Run: `./gradlew native-cli:test --tests "org.mule.weave.dwnative.benchmark.BenchmarkHarnessTest"`
Expected: FAIL — `BenchmarkMode` symbol not found *unless* `genBenchmarkMode` ran. If it fails on the symbol, run `./gradlew native-cli:genBenchmarkMode` once, then re-run. Expected after generation: PASS for this guard (normal build → `false`).

Note: `compileScala`/`compileJava` already `dependsOn genBenchmarkMode` (Task 1 Step 3), so the test compile generates it. If the IDE/test invocation skips it, the explicit `genBenchmarkMode` run resolves it.

- [ ] **Step 3: Modify `DWCLI.main` to dispatch when gated**

In `native-cli/src/main/java/org/mule/weave/cli/DWCLI.java`, replace the `main` method (lines 32-34):

```java
    public static void main(String[] args) {
        // Benchmark dispatch: only reachable in a build made with -Pbenchmark=true
        // (BenchmarkMode.ENABLED is a compile-time false in production, so native-image
        // folds this branch and BenchmarkHarness away). DW_BENCH selects the mode.
        if (BenchmarkMode.ENABLED && System.getenv("DW_BENCH") != null) {
            org.mule.weave.dwnative.benchmark.BenchmarkHarness.main(args);
            return;
        }
        new DWCLI().run(args, DefaultConsole$.MODULE$);
    }
```

- [ ] **Step 4: Run the full native-cli test suite to verify nothing regressed**

Run: `./gradlew native-cli:test`
Expected: PASS, including the `ENABLED shouldBe false` guard.

- [ ] **Step 5: Commit**

```bash
git add native-cli/src/main/java/org/mule/weave/cli/DWCLI.java \
        native-cli/src/test/scala/org/mule/weave/dwnative/benchmark/BenchmarkHarnessTest.scala
git commit -m "feat: gate BenchmarkHarness dispatch behind BenchmarkMode.ENABLED + DW_BENCH"
```

---

## Task 4: Parent `locate.mjs` — resolve the bench-enabled `dw` binary

**Files:**
- Create: `benchmarks/runners/cli/locate.mjs`
- Create: `benchmarks/runners/cli/locate.test.mjs`

**Interfaces:**
- Produces: `export function locateBinary(): string` — returns an absolute path to the `dw` binary. Resolution order: `process.env.DW_BENCH_BIN` if set (used as-is), else `<repoRoot>/native-cli/build/native/nativeCompile/dw` (`dw.exe` on Windows). Throws with a build hint if the resolved path does not exist. Consumed by Tasks 5 & 6.

Mirror `benchmarks/runners/node/wrapper.mjs` (repo-root computation via `import.meta.url`, `existsSync` check, actionable error).

- [ ] **Step 1: Write the failing test**

Create `benchmarks/runners/cli/locate.test.mjs`:

```javascript
import { test } from "node:test";
import assert from "node:assert/strict";
import { locateBinary } from "./locate.mjs";

test("DW_BENCH_BIN override is returned as-is when it exists", () => {
  // Point at a file guaranteed to exist: this test file itself.
  const self = new URL(import.meta.url).pathname;
  process.env.DW_BENCH_BIN = self;
  try {
    assert.equal(locateBinary(), self);
  } finally {
    delete process.env.DW_BENCH_BIN;
  }
});

test("throws an actionable error when the binary is absent", () => {
  process.env.DW_BENCH_BIN = "/nonexistent/dw-binary-xyz";
  try {
    assert.throws(() => locateBinary(), /nativeCompile|not found|build/i);
  } finally {
    delete process.env.DW_BENCH_BIN;
  }
});
```

- [ ] **Step 2: Run to verify it fails**

Run: `node --test benchmarks/runners/cli/locate.test.mjs`
Expected: FAIL — cannot find module `./locate.mjs`.

- [ ] **Step 3: Implement `locate.mjs`**

Create `benchmarks/runners/cli/locate.mjs`:

```javascript
import { existsSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));
// benchmarks/runners/cli -> benchmarks/runners -> benchmarks -> repo root
const REPO_ROOT = join(__dirname, "..", "..", "..");
const BIN_NAME = process.platform === "win32" ? "dw.exe" : "dw";
const DEFAULT_BIN = join(REPO_ROOT, "native-cli", "build", "native", "nativeCompile", BIN_NAME);

/**
 * Resolve the benchmark-enabled `dw` native binary. Honors DW_BENCH_BIN (absolute
 * path to a bench-built dw); otherwise the default nativeCompile output. The binary
 * must be built with -Pbenchmark=true so BenchmarkHarness is reachable.
 */
export function locateBinary() {
  const candidate = process.env.DW_BENCH_BIN || DEFAULT_BIN;
  if (!existsSync(candidate)) {
    throw new Error(
      `dw benchmark binary not found at ${candidate}. ` +
        `Build it with: ./gradlew native-cli:nativeCompile -Pbenchmark=true ` +
        `(or set DW_BENCH_BIN to a bench-enabled dw).`
    );
  }
  return candidate;
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `node --test benchmarks/runners/cli/locate.test.mjs`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add benchmarks/runners/cli/locate.mjs benchmarks/runners/cli/locate.test.mjs
git commit -m "feat: add cli runner binary locator"
```

---

## Task 5: Parent `coldstart.mjs` — cold-start + first-run rows

**Files:**
- Create: `benchmarks/runners/cli/coldstart.mjs`

**Interfaces:**
- Consumes: `locateBinary` (Task 4); shared libs `casesForMetric`, `resolveInputs` from `../../lib/manifest.mjs`; `computeStats` from `../../lib/stats.mjs`.
- Produces: `export async function runColdStartAndFirstRun(manifest, { samplesOverride } = {}): Promise<Array<{id,metric,unit,stats,iterations}>>` — for each case declaring `cold-start` or `first-run`, spawns `dw` in `coldfirst` mode `n` times, stamps cold-start at spawn→`READY`, parses `firstRunMs`. Emits a `cold-start` row (unit `ms`) only for cases that declare it, and a `first-run` row only for cases that declare it. Consumed by Task 7.

This is `benchmarks/runners/node/coldstart.mjs` with the child command swapped to the `dw` binary. Build the per-input arg `--input=<name>=<file>:<mime>:<charset>` using absolute corpus file paths.

- [ ] **Step 1: Implement `coldstart.mjs`**

Create `benchmarks/runners/cli/coldstart.mjs`:

```javascript
import { spawn } from "node:child_process";
import { join } from "node:path";
import { casesForMetric } from "../../lib/manifest.mjs";
import { computeStats } from "../../lib/stats.mjs";
import { locateBinary } from "./locate.mjs";

/** Build `--input=name=file:mime:charset` args for a case (absolute paths). */
function inputArgs(manifest, c) {
  const args = [];
  for (const [name, inp] of Object.entries(c.inputs ?? {})) {
    const file = join(manifest.corpusDir, inp.file);
    const charset = inp.charset ?? "utf-8";
    args.push(`--input=${name}=${file}:${inp.mimeType}:${charset}`);
  }
  return args;
}

/**
 * Spawn one fresh dw process in coldfirst mode. Cold-start = wall-clock from just
 * before spawn to the child's "READY" marker (process launch + native image load +
 * NativeRuntime init). first-run is timed in-process by the child. Rejects on a
 * non-zero exit or a missing READY/JSON line so a failed sample never records a
 * bogus timing.
 */
function sampleOnce(bin, manifest, c) {
  const scriptPath = join(manifest.corpusDir, c.script);
  const args = ["--bench-mode=coldfirst", `--script=${scriptPath}`, ...inputArgs(manifest, c)];
  return new Promise((resolve, reject) => {
    const t0 = process.hrtime.bigint();
    const child = spawn(bin, args, {
      stdio: ["ignore", "pipe", "pipe"],
      env: { ...process.env, DW_BENCH: "1" },
    });
    let coldStartMs;
    let stdout = "";
    let stderr = "";
    child.stdout.setEncoding("utf-8");
    child.stdout.on("data", (chunk) => {
      stdout += chunk;
      if (coldStartMs === undefined && stdout.includes("READY\n")) {
        coldStartMs = Number(process.hrtime.bigint() - t0) / 1e6;
      }
    });
    child.stderr.setEncoding("utf-8");
    child.stderr.on("data", (chunk) => (stderr += chunk));
    child.on("error", reject);
    child.on("close", (code) => {
      if (code !== 0) {
        reject(new Error(`cli coldfirst failed for '${c.id}' (exit ${code})\n${stderr}`));
        return;
      }
      if (coldStartMs === undefined) {
        reject(new Error(`cli coldfirst for '${c.id}' never printed READY\n${stderr}`));
        return;
      }
      const jsonLine = stdout.split("\n").filter((l) => l && l !== "READY").pop();
      if (!jsonLine) {
        reject(new Error(`cli coldfirst for '${c.id}' printed no result line\n${stderr}`));
        return;
      }
      const { firstRunMs } = JSON.parse(jsonLine);
      resolve({ coldStartMs, firstRunMs });
    });
  });
}

/** @returns {Promise<Array<{id,metric,unit,stats,iterations}>>} */
export async function runColdStartAndFirstRun(manifest, { samplesOverride } = {}) {
  const bin = locateBinary();
  const rows = [];
  const ids = new Set([
    ...casesForMetric(manifest, "cold-start").map((c) => c.id),
    ...casesForMetric(manifest, "first-run").map((c) => c.id),
  ]);

  for (const id of ids) {
    const c = manifest.cases.find((x) => x.id === id);
    const n = samplesOverride ?? c.iterations?.samples ?? 20;
    const colds = [];
    const firsts = [];
    for (let i = 0; i < n; i++) {
      const { coldStartMs, firstRunMs } = await sampleOnce(bin, manifest, c);
      colds.push(coldStartMs);
      firsts.push(firstRunMs);
    }
    if (c.metrics.includes("cold-start")) {
      rows.push({ id, metric: "cold-start", unit: "ms", stats: computeStats(colds), iterations: n });
    }
    if (c.metrics.includes("first-run")) {
      rows.push({ id, metric: "first-run", unit: "ms", stats: computeStats(firsts), iterations: n });
    }
  }
  return rows;
}
```

- [ ] **Step 2: Sanity-check syntax (no binary needed)**

Run: `node --check benchmarks/runners/cli/coldstart.mjs`
Expected: no output, exit 0.

Note: an end-to-end run of this file requires a bench-built `dw` and is exercised by the smoke test in Task 8; there is no dwlib-free unit test for it (it spawns the binary), matching how `runners/node/coldstart.test.mjs` is excluded from the always-on JS parity set.

- [ ] **Step 3: Commit**

```bash
git add benchmarks/runners/cli/coldstart.mjs
git commit -m "feat: add cli runner cold-start + first-run sampler"
```

---

## Task 6: Parent `warm.mjs` — warm rows

**Files:**
- Create: `benchmarks/runners/cli/warm.mjs`

**Interfaces:**
- Consumes: `locateBinary` (Task 4); `casesForMetric` from `../../lib/manifest.mjs`; `computeStats` from `../../lib/stats.mjs`.
- Produces: `export async function runWarm(manifest): Promise<Array<{id,metric,unit,stats,iterations}>>` — for each case declaring `warm`, spawns `dw` once in `warm` mode with `--warmup`/`--iters` from the case's `iterations`, reads back the `warmMs[]` array, and produces a `warm` row (unit `ms`). Consumed by Task 7.

Reuse the same `inputArgs` shape as Task 5 (duplicated as a small local helper — the two samplers are independent and each is small; a shared module is not warranted by YAGNI, matching how the Node runner keeps `coldstart.mjs` and `warm-bench.mjs` separate).

- [ ] **Step 1: Implement `warm.mjs`**

Create `benchmarks/runners/cli/warm.mjs`:

```javascript
import { spawn } from "node:child_process";
import { join } from "node:path";
import { casesForMetric } from "../../lib/manifest.mjs";
import { computeStats } from "../../lib/stats.mjs";
import { locateBinary } from "./locate.mjs";

function inputArgs(manifest, c) {
  const args = [];
  for (const [name, inp] of Object.entries(c.inputs ?? {})) {
    const file = join(manifest.corpusDir, inp.file);
    const charset = inp.charset ?? "utf-8";
    args.push(`--input=${name}=${file}:${inp.mimeType}:${charset}`);
  }
  return args;
}

/** Spawn dw once in warm mode; resolve the parsed warmMs[] sample array. */
function warmSamples(bin, manifest, c) {
  const scriptPath = join(manifest.corpusDir, c.script);
  const warmup = c.iterations?.warmup ?? 10;
  const iters = c.iterations?.warm ?? 100;
  const args = [
    "--bench-mode=warm",
    `--script=${scriptPath}`,
    `--warmup=${warmup}`,
    `--iters=${iters}`,
    ...inputArgs(manifest, c),
  ];
  return new Promise((resolve, reject) => {
    const child = spawn(bin, args, {
      stdio: ["ignore", "pipe", "pipe"],
      env: { ...process.env, DW_BENCH: "1" },
    });
    let stdout = "";
    let stderr = "";
    child.stdout.setEncoding("utf-8");
    child.stdout.on("data", (chunk) => (stdout += chunk));
    child.stderr.setEncoding("utf-8");
    child.stderr.on("data", (chunk) => (stderr += chunk));
    child.on("error", reject);
    child.on("close", (code) => {
      if (code !== 0) {
        reject(new Error(`cli warm failed for '${c.id}' (exit ${code})\n${stderr}`));
        return;
      }
      const jsonLine = stdout.split("\n").filter((l) => l && l !== "READY").pop();
      if (!jsonLine) {
        reject(new Error(`cli warm for '${c.id}' printed no result line\n${stderr}`));
        return;
      }
      const { warmMs } = JSON.parse(jsonLine);
      if (!Array.isArray(warmMs) || warmMs.length === 0) {
        reject(new Error(`cli warm for '${c.id}' returned no samples\n${stderr}`));
        return;
      }
      resolve({ warmMs, iters });
    });
  });
}

/** @returns {Promise<Array<{id,metric,unit,stats,iterations}>>} */
export async function runWarm(manifest) {
  const bin = locateBinary();
  const rows = [];
  for (const c of casesForMetric(manifest, "warm")) {
    const { warmMs, iters } = await warmSamples(bin, manifest, c);
    rows.push({ id: c.id, metric: "warm", unit: "ms", stats: computeStats(warmMs), iterations: iters });
  }
  return rows;
}
```

- [ ] **Step 2: Sanity-check syntax**

Run: `node --check benchmarks/runners/cli/warm.mjs`
Expected: no output, exit 0.

- [ ] **Step 3: Commit**

```bash
git add benchmarks/runners/cli/warm.mjs
git commit -m "feat: add cli runner warm sampler"
```

---

## Task 7: Parent `emit.mjs` — assemble and write the result file

**Files:**
- Create: `benchmarks/runners/cli/emit.mjs`
- Create: `benchmarks/runners/cli/emit.test.mjs`

**Interfaces:**
- Consumes: `loadManifest`, `validateResultIds` from `../../lib/manifest.mjs`; `gatherEnv` from `../../lib/env.mjs`; `runColdStartAndFirstRun` (Task 5); `runWarm` (Task 6); `locateBinary` (Task 4, for the version probe).
- Produces: `export function buildResult(env, cases)` (schema-shaped object, identical contract to the Node runner's) and `export async function main(): Promise<string>` (writes `results/cli-<timestamp>.json`, returns its path). Runner name `"cli"`.

`runtimeVersion`: probe `dw --version` synchronously; take the first line, or fall back to `"dw"` if the probe fails. `dwlibBuildId` comes from `gatherEnv` but the CLI is not the staged dwlib — override it to `"n/a-cli"` after gathering.

- [ ] **Step 1: Write the failing test for `buildResult`**

Create `benchmarks/runners/cli/emit.test.mjs`:

```javascript
import { test } from "node:test";
import assert from "node:assert/strict";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { loadManifest, validateResultIds } from "../../lib/manifest.mjs";
import { buildResult } from "./emit.mjs";

const __dirname = dirname(fileURLToPath(import.meta.url));
const CORPUS = join(__dirname, "..", "..", "corpus");

test("buildResult produces a schema-shaped object with runner 'cli'", () => {
  const env = {
    runner: "cli", os: "x", cpu: "y", runtimeVersion: "dw vX",
    weaveVersion: "2.12.0-x", commit: "abc", dwlibBuildId: "n/a-cli",
  };
  const cases = [{ id: "trivial", metric: "cold-start", unit: "ms", stats: { median: 1 }, iterations: 10 }];
  const r = buildResult(env, cases);
  assert.equal(r.schemaVersion, "1.0");
  assert.equal(r.runner, "cli");
  assert.ok(typeof r.timestamp === "string");
  assert.deepEqual(r.cases, cases);
});

test("orphan ids are rejected before writing", () => {
  const manifest = loadManifest(CORPUS);
  assert.throws(() => validateResultIds(manifest, [{ id: "totally-made-up" }]), /orphan id/);
});
```

- [ ] **Step 2: Run to verify it fails**

Run: `node --test benchmarks/runners/cli/emit.test.mjs`
Expected: FAIL — cannot find module `./emit.mjs`.

- [ ] **Step 3: Implement `emit.mjs`**

Create `benchmarks/runners/cli/emit.mjs`:

```javascript
import { writeFileSync, mkdirSync } from "node:fs";
import { execFileSync } from "node:child_process";
import { join, dirname } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { loadManifest, validateResultIds } from "../../lib/manifest.mjs";
import { gatherEnv } from "../../lib/env.mjs";
import { locateBinary } from "./locate.mjs";
import { runColdStartAndFirstRun } from "./coldstart.mjs";
import { runWarm } from "./warm.mjs";

const __dirname = dirname(fileURLToPath(import.meta.url));
const CORPUS = join(__dirname, "..", "..", "corpus");
const RESULTS_DIR = join(__dirname, "..", "..", "results");

/** Assemble the full schema object (identical contract to the Node runner). */
export function buildResult(env, cases) {
  return {
    schemaVersion: "1.0",
    runner: env.runner,
    env,
    timestamp: new Date().toISOString(),
    cases,
  };
}

/** Best-effort `dw --version` first line; falls back to "dw". */
function probeVersion(bin) {
  try {
    const out = execFileSync(bin, ["--version"], { encoding: "utf-8" });
    const line = out.split("\n").map((l) => l.trim()).filter(Boolean)[0];
    return line ? `dw ${line}` : "dw";
  } catch {
    return "dw";
  }
}

export async function main() {
  const manifest = loadManifest(CORPUS);
  const bin = locateBinary();
  const env = gatherEnv({ runner: "cli", runtimeVersion: probeVersion(bin) });
  // The CLI is a native binary, not the staged dwlib — override the lib fingerprint.
  env.dwlibBuildId = "n/a-cli";

  const coldRows = await runColdStartAndFirstRun(manifest);
  const warmRows = await runWarm(manifest);

  const cases = [...coldRows, ...warmRows];
  validateResultIds(manifest, cases);

  mkdirSync(RESULTS_DIR, { recursive: true });
  const stamp = new Date().toISOString().replace(/[:.]/g, "-");
  const outPath = join(RESULTS_DIR, `cli-${stamp}.json`);
  writeFileSync(outPath, JSON.stringify(buildResult(env, cases), null, 2));
  console.log(`wrote ${outPath} (${cases.length} rows)`);
  return outPath;
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch((e) => {
    console.error(e.message);
    process.exit(1);
  });
}
```

- [ ] **Step 4: Run to verify the test passes**

Run: `node --test benchmarks/runners/cli/emit.test.mjs`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add benchmarks/runners/cli/emit.mjs benchmarks/runners/cli/emit.test.mjs
git commit -m "feat: add cli runner emit entrypoint"
```

---

## Task 8: Gradle `benchmarkCli` runner task + JS test wiring

**Files:**
- Modify: `native-cli/build.gradle` (add `benchmarkCli` task)
- Modify: `native-lib/build.gradle:307-320` (add cli test files to `benchmarkJsUnitTest`)

**Interfaces:**
- Consumes: `native-cli:nativeCompile` (must be invoked with `-Pbenchmark=true` so the harness is present); the shared `node corpus/gen-inputs.mjs`; `benchmarks/runners/cli/emit.mjs` (Task 7).
- Produces: a Gradle task `benchmarkCli` tagged `ext.benchmarkRunner = true`, discovered by the root `benchmarkCompare`. Writes `benchmarks/results/cli-<ts>.json`; does NOT render the report.

- [ ] **Step 1: Add the `benchmarkCli` task to `native-cli/build.gradle`**

Append to `native-cli/build.gradle`:

```groovy
// The CLI runner as an aggregator-registered runner: emits its result file but
// does NOT render the report (the root :benchmarkCompare renders once over all
// runners). Tagged `benchmarkRunner` so :benchmarkCompare discovers it automatically.
// Requires the bench-enabled binary — nativeCompile must run with -Pbenchmark=true so
// BenchmarkMode.ENABLED is true and BenchmarkHarness is reachable in dw.
tasks.register('benchmarkCli', Exec) {
    onlyIf { project.findProperty('benchmark')?.toString()?.toBoolean() == true }
    ext.benchmarkRunner = true

    dependsOn tasks.named('nativeCompile')
    workingDir("${rootDir}/benchmarks")

    def script = 'node corpus/gen-inputs.mjs && node runners/cli/emit.mjs'
    if (System.getProperty('os.name').toLowerCase().contains('windows')) {
        commandLine('cmd', '/c', script)
    } else {
        commandLine('bash', '-c', script)
    }
}
```

- [ ] **Step 2: Add the cli JS tests to the always-on parity set**

In `native-lib/build.gradle`, in `benchmarkJsUnitTest` (lines 307-320), extend the `files` string to include the cli runner's dwlib-free tests:

```groovy
  def files = 'lib/stats.test.mjs lib/manifest.test.mjs lib/env.test.mjs ' +
    'report/report.test.mjs runners/node/emit.test.mjs ' +
    'runners/cli/locate.test.mjs runners/cli/emit.test.mjs'
```

- [ ] **Step 3: Verify the JS parity tests pass (no binary needed)**

Run: `./gradlew native-lib:benchmarkJsUnitTest`
Expected: PASS — includes `runners/cli/locate.test.mjs` and `runners/cli/emit.test.mjs`.

- [ ] **Step 4: Verify `benchmarkCli` is discovered but skipped without the opt-in**

Run: `./gradlew native-cli:benchmarkCli`
Expected: task is SKIPPED (the `onlyIf` is false without `-Pbenchmark=true`), build succeeds.

- [ ] **Step 5: Commit**

```bash
git add native-cli/build.gradle native-lib/build.gradle
git commit -m "build: register benchmarkCli runner + wire cli JS parity tests"
```

---

## Task 9: End-to-end smoke test + README

**Files:**
- Modify: `benchmarks/README.md`

**Interfaces:**
- Consumes: everything above. This task validates the full path once against a real bench-built binary, then documents the runner.

The end-to-end run needs a GraalVM toolchain (per the repo README/CLAUDE.md). It is a manual verification gate, not an automated test in `build`.

- [ ] **Step 1: Build the bench-enabled binary**

Run: `./gradlew native-cli:nativeCompile -Pbenchmark=true`
Expected: produces `native-cli/build/native/nativeCompile/dw`. (Several minutes; needs `GRAALVM_HOME`/`JAVA_HOME` set to a GraalVM with `native-image`, per CLAUDE.md.)

- [ ] **Step 2: Smoke-run one cold-start sample directly against the binary**

Run:
```bash
node -e '
import("./benchmarks/runners/cli/coldstart.mjs").then(async (m) => {
  const { loadManifest } = await import("./benchmarks/lib/manifest.mjs");
  const manifest = loadManifest("./benchmarks/corpus");
  const rows = await m.runColdStartAndFirstRun(manifest, { samplesOverride: 2 });
  const cold = rows.filter(r => r.metric === "cold-start");
  const first = rows.filter(r => r.metric === "first-run");
  if (cold.length < 1 || first.length < 1) { console.error("missing rows"); process.exit(1); }
  for (const r of [...cold, ...first]) {
    if (!(r.stats.median > 0)) { console.error("non-positive median", r); process.exit(1); }
  }
  console.log("smoke OK:", cold.length, "cold,", first.length, "first rows");
});
'
```
Expected: prints `smoke OK: N cold, M first rows`; a positive `cold-start` (spawn→READY) and `firstRunMs` for each sampled case. (Uses `samplesOverride: 2` to stay fast.)

- [ ] **Step 3: Run the full cross-runner comparison including cli**

Run: `./gradlew benchmarkCompare -Pbenchmark=true`
Expected: the printed table includes a `cli` column with `cold-start`, `first-run`, and `warm` rows populated, `streaming` rows blank (`—`) for the cli column, and a `Δ cli vs <baseline>` column.

- [ ] **Step 4: Document the runner in `benchmarks/README.md`**

In `benchmarks/README.md`:

Under **Layout** (after the `runners/python/` sentence, ~line 15), add:
```
  `runners/cli/` is the CLI runner: a Node parent that spawns the `dw` native
  binary (built with `-Pbenchmark=true`, which compiles in an in-binary
  benchmark harness gated by `BenchmarkMode.ENABLED` and dispatched via the
  `DW_BENCH` env var — the shipped `dw` contains none of it). It emits
  `cold-start`, `first-run`, and `warm`; it does **not** emit `streaming`
  (the `dw run` path has no chunked-input FFI like the library's).
```

Under **Single-runner options** (~line 52), add:
```
    ./gradlew native-cli:benchmarkCli -Pbenchmark=true            # CLI only: writes results/cli-<ts>.json
```
and note its prerequisite:
```
The **CLI runner** requires the bench-enabled binary
(`./gradlew native-cli:nativeCompile -Pbenchmark=true`); set `DW_BENCH_BIN` to
point at a prebuilt one. Like the library runners it needs the GraalVM toolchain.
```

- [ ] **Step 5: Commit**

```bash
git add benchmarks/README.md
git commit -m "docs: document the CLI benchmark runner"
```

---

## Self-Review

**Spec coverage:**
- Native binary measured, not JVM entrypoint → Tasks 2, 8 (spawns `dw`). ✓
- READY-marker protocol, cold-start + first-run + warm → Tasks 2 (harness), 5 (cold/first), 6 (warm). ✓
- Build-time gate, stripped from production → Tasks 1 (`BenchmarkMode`), 3 (`ENABLED &&` dispatch + guard test). ✓
- Corpus only, no streaming → no manifest edit; cli emits only cold/first/warm (Tasks 5–7); README documents the gap (Task 9). ✓
- Corpus-agnostic harness (no manifest knowledge in native-cli) → Task 2 takes file-path args; parent resolves corpus (Tasks 5–7). ✓
- Runner registration contract (result file + `ext.benchmarkRunner`, no `benchmarkCompare` edit) → Task 8. ✓
- `runner: "cli"`, `dwlibBuildId: "n/a-cli"`, `runtimeVersion` from `dw --version` → Task 7. ✓
- Error handling (non-zero exit / missing READY / missing JSON / failed run) → Tasks 2 (harness throws), 5 & 6 (parent rejects). ✓
- Output discipline (only READY + one JSON line; output to discard stream) → Task 2 + its behavioral test. ✓
- Testing: Scala harness + ENABLED guard (Tasks 2, 3); dwlib-free JS in parity set (Tasks 4, 7, 8); smoke (Task 9). ✓
- README update → Task 9. ✓

**Placeholder scan:** No TBD/TODO/"handle edge cases"; every code step shows full code; every command has expected output. ✓

**Type consistency:** `BenchArgs`/`BenchInput`/`parseArgs`/`runColdFirst`/`runWarm`/`main` (Task 2) are used consistently in Task 3's dispatch and Task 2's tests. `runColdStartAndFirstRun(manifest, {samplesOverride})` (Task 5) and `runWarm(manifest)` (Task 6) match their calls in `emit.mjs` (Task 7) and the smoke test (Task 9). `locateBinary()` (Task 4) is imported by Tasks 5, 6, 7. `buildResult(env, cases)` (Task 7) matches its test. `--input=name=file:mime:charset` arg format is identical between the parser (Task 2) and both parent samplers (Tasks 5, 6). `BenchmarkMode.ENABLED` (Task 1) matches its use in Task 3 and the guard test. ✓
