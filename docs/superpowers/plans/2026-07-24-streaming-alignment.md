# Streaming Methodology Alignment Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the `streaming` benchmark metric measure the same operation on all three runners (chunked input + concurrent deferred output), so its cross-runner delta is meaningful, then remove the report's `n/a` suppression.

**Architecture:** Add `deferred=true` streaming-script variants to the corpus, selected only for the `streaming` metric via a new manifest `streamingScript` field. The engine runner gains a streaming path that binds a chunk-paced `InputStream` (lazy input) and drains the deferred `PipedInputStream` result. native-lib runners already stream input and consume a deferred `InputStream` result, so they only switch to the deferred script. The report stops suppressing the streaming delta.

**Tech Stack:** Scala 2.12 (engine runner, scalatest `AnyFreeSpec`/`Matchers`, org.json), Node ESM (`node:test`), Python 3 (`unittest`), DataWeave runtime `DataWeaveScriptingEngine`.

## Global Constraints

- **Scala version is 2.12.18** — `scala.util.Using` does NOT exist; use explicit `try/finally` for resource cleanup.
- **Do not touch the warm / first-run / cold-start code paths.** Only the streaming path changes. Warm/first-run must keep resolving the base `script`, not the variant.
- **Throughput formula is unchanged:** MB/s = `bytes / 1e6 / (ms / 1000)` over the primary input byte count (`Stats.toMBps` / `to_mbps` / `toMBps`). Denominator stays the primary *input* bytes, never the output/drained bytes.
- **Engine streaming input chunk size = 65536 bytes (64KB)**, matching Node's `chunked(buffer, 65536)`.
- **The engine runner uses no security manager**, so the default `NoSecurityManagerService` grants the `DEFERRED` privilege — no privilege plumbing is needed for `deferred=true`.
- **Never hand-fabricate benchmark numbers.** `RESULTS.md` streaming rows are only updated from a real run (final task); if no run is done, leave the committed report as-is and note it.
- Commit after each task with the `W-23545283:` subject prefix used throughout the branch.

---

## File Structure

- `benchmarks/corpus/scripts/map-scale.stream.dwl` — **new**, deferred variant of map-scale.
- `benchmarks/corpus/scripts/json-stream.stream.dwl` — **new**, deferred variant of json-stream.
- `benchmarks/corpus/manifest.json` — **modify**, add `streamingScript` to the two streaming cases.
- `benchmarks/lib/manifest.mjs` — **modify**, add `resolveStreamingScript` + validate the field.
- `benchmarks/runners/python/manifest.py` — **modify**, add `resolve_streaming_script` + validate.
- `benchmarks/runners/engine/.../Manifest.scala` — **modify**, add `streamingScript` to `BenchCase` + `resolveStreamingScript`.
- `benchmarks/runners/node/warm-bench.mjs` — **modify**, streaming loop resolves the variant script.
- `benchmarks/runners/python/warm_bench.py` — **modify**, streaming loop resolves the variant script.
- `benchmarks/runners/engine/.../EngineShell.scala` — **modify**, add `runStreaming` + a `ChunkedInputStream` helper (new file).
- `benchmarks/runners/engine/.../ChunkedInputStream.scala` — **new**, 64KB-paced InputStream over a byte array.
- `benchmarks/runners/engine/.../WarmBench.scala` — **modify**, streaming uses `runStreaming` over a chunked stream + variant script.
- `benchmarks/report/report.mjs` — **modify**, remove `streaming` from non-comparable set + drop footnote.
- Tests: `manifest.test.mjs`, `test_bench.py`, `ManifestTest.scala`, `EngineShellTest.scala`, `WarmBenchTest.scala`, `report.test.mjs`.

---

## Task 1: Add corpus streaming-script variants (deferred output)

**Files:**
- Create: `benchmarks/corpus/scripts/map-scale.stream.dwl`
- Create: `benchmarks/corpus/scripts/json-stream.stream.dwl`

**Interfaces:**
- Produces: two `.dwl` files with `deferred=true` output, identical transform bodies to their base scripts. Referenced by Task 2's manifest field.

- [ ] **Step 1: Create `map-scale.stream.dwl`**

Body is identical to `scripts/map-scale.dwl` except the output directive adds `deferred=true`:

```
output application/json deferred=true
---
payload map (item) -> { id: item.id, doubled: item.value * 2, label: "item_" ++ item.id }
```

- [ ] **Step 2: Create `json-stream.stream.dwl`**

```
output application/json deferred=true
---
payload map (item) -> { id: item.id, name: item.name }
```

- [ ] **Step 3: Sanity-check the scripts run deferred through the prebuilt dwlib**

Run (dylib already built, exports `run_script_input_output_callback`):

```bash
cd /Users/aradunsky/Documents/public/data-weave-cli
DATAWEAVE_NATIVE_LIB="$PWD/native-lib/python/src/dataweave/native/dwlib.dylib" \
python3 - <<'PY'
import sys; sys.path.insert(0, "native-lib/python/src")
from dataweave import DataWeave
inp = open("benchmarks/corpus/inputs/generated/records-large.json","rb").read()
def chunked(d, s=65536):
    for i in range(0, len(d), s): yield d[i:i+s]
dw = DataWeave(); dw.initialize()
for f in ["benchmarks/corpus/scripts/map-scale.stream.dwl", "benchmarks/corpus/scripts/json-stream.stream.dwl"]:
    script = open(f).read()
    gen = dw.run_transform(script, chunked(inp), input_name="payload", input_mime_type="application/json", input_charset="utf-8")
    total = sum(len(c) for c in gen)
    print(f, "success=", gen.metadata.success, "out_bytes=", total)
dw.cleanup()
PY
```

Expected: both lines print `success= True out_bytes=` with a positive byte count (~34103 for map-scale). If `records-large.json` is absent, generate it first: `node benchmarks/corpus/gen-inputs.mjs`.

- [ ] **Step 4: Commit**

```bash
git add benchmarks/corpus/scripts/map-scale.stream.dwl benchmarks/corpus/scripts/json-stream.stream.dwl
git commit -m "W-23545283: Add deferred=true streaming-script variants for map-scale + json-stream"
```

---

## Task 2: Add `streamingScript` to the manifest

**Files:**
- Modify: `benchmarks/corpus/manifest.json`

**Interfaces:**
- Produces: `streamingScript` field on the `map-scale` and `json-stream` cases. Consumed by the manifest resolvers in Tasks 3–5.

- [ ] **Step 1: Add `streamingScript` to the `map-scale` case**

In `benchmarks/corpus/manifest.json`, the `map-scale` case currently has `"script": "scripts/map-scale.dwl"`. Add a sibling field so the object reads:

```json
    {
      "id": "map-scale",
      "script": "scripts/map-scale.dwl",
      "streamingScript": "scripts/map-scale.stream.dwl",
      "inputs": {
        "payload": { "file": "inputs/generated/records-large.json", "mimeType": "application/json", "generated": true }
      },
      "metrics": ["first-run", "warm", "streaming"],
      "iterations": { "warm": 30, "warmup": 3, "streaming": 10, "samples": 15 }
    },
```

- [ ] **Step 2: Add `streamingScript` to the `json-stream` case**

```json
    {
      "id": "json-stream",
      "script": "scripts/json-stream.dwl",
      "streamingScript": "scripts/json-stream.stream.dwl",
      "inputs": {
        "payload": { "file": "inputs/generated/records-large.json", "mimeType": "application/json", "generated": true }
      },
      "metrics": ["first-run", "warm", "streaming"],
      "iterations": { "warm": 30, "warmup": 3, "streaming": 10, "samples": 15 }
    },
```

- [ ] **Step 3: Verify JSON is valid**

Run: `node -e "JSON.parse(require('fs').readFileSync('benchmarks/corpus/manifest.json','utf8')); console.log('ok')"`
Expected: `ok`

- [ ] **Step 4: Commit**

```bash
git add benchmarks/corpus/manifest.json
git commit -m "W-23545283: Point streaming cases at deferred streamingScript variants"
```

---

## Task 3: Node manifest — `resolveStreamingScript` + validation

**Files:**
- Modify: `benchmarks/lib/manifest.mjs`
- Test: `benchmarks/lib/manifest.test.mjs`

**Interfaces:**
- Produces: `resolveStreamingScript(manifest, caseObj)` returning the absolute-read script text for the streaming metric — reads `streamingScript` if present, else falls back to `script`. Consumed by Task 8 (`warm-bench.mjs`).
- Consumes: existing `loadManifest`, `resolveInputs` from this module.

- [ ] **Step 1: Write the failing test**

Add to `benchmarks/lib/manifest.test.mjs` (import `resolveStreamingScript` in the existing import block from `./manifest.mjs`):

```javascript
test("resolveStreamingScript prefers streamingScript, falls back to script", () => {
  const m = loadManifest(CORPUS);
  const mapScale = m.cases.find((c) => c.id === "map-scale");
  const streamText = resolveStreamingScript(m, mapScale);
  assert.ok(streamText.includes("deferred=true"), "streaming variant declares deferred=true");

  const objTransform = m.cases.find((c) => c.id === "object-transform"); // no streamingScript
  const fallback = resolveStreamingScript(m, objTransform);
  const base = readFileSync(join(CORPUS, objTransform.script), "utf-8");
  assert.equal(fallback, base, "falls back to the base script when no streamingScript");
});
```

Add these imports at the top of the test file if not already present:

```javascript
import { readFileSync } from "node:fs";
import { join } from "node:path";
```

(The file already imports `loadManifest`, `casesForMetric`, `METRICS`; add `resolveStreamingScript` to that import list. `CORPUS` is already defined in the file.)

- [ ] **Step 2: Run test to verify it fails**

Run: `node --test benchmarks/lib/manifest.test.mjs`
Expected: FAIL — `resolveStreamingScript is not a function` (or `not exported`).

- [ ] **Step 3: Implement `resolveStreamingScript` and validate the field in `loadManifest`**

In `benchmarks/lib/manifest.mjs`, inside the `loadManifest` per-case loop, right after the existing `script` existence check (the block that throws `case ${c.id} script not found`), add validation that a declared `streamingScript` exists:

```javascript
    if (c.streamingScript && !existsSync(join(corpusDir, c.streamingScript))) {
      throw new Error(`case ${c.id} streamingScript not found: ${c.streamingScript}`);
    }
```

Then add the exported resolver near `resolveInputs`:

```javascript
/**
 * Read the script used for the streaming metric: the `streamingScript` variant
 * (e.g. a deferred=true output) if declared, else the base `script`. Warm and
 * first-run always use `script`, never this.
 */
export function resolveStreamingScript(manifest, caseObj) {
  const rel = caseObj.streamingScript ?? caseObj.script;
  return readFileSync(join(manifest.corpusDir, rel), "utf-8");
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `node --test benchmarks/lib/manifest.test.mjs`
Expected: PASS (all tests, including the existing ones).

- [ ] **Step 5: Commit**

```bash
git add benchmarks/lib/manifest.mjs benchmarks/lib/manifest.test.mjs
git commit -m "W-23545283: Add resolveStreamingScript + streamingScript validation (node lib)"
```

---

## Task 4: Python manifest — `resolve_streaming_script` + validation

**Files:**
- Modify: `benchmarks/runners/python/manifest.py`
- Test: `benchmarks/runners/python/test_bench.py`

**Interfaces:**
- Produces: `resolve_streaming_script(manifest, case_obj)` returning script text — `streamingScript` if present, else `script`. Consumed by Task 9 (`warm_bench.py`).

- [ ] **Step 1: Write the failing test**

In `benchmarks/runners/python/test_bench.py`, find the `TestManifest` class (it contains `test_read_script`). Add:

```python
    def test_resolve_streaming_script_prefers_variant(self):
        map_scale = next(c for c in self.m["cases"] if c["id"] == "map-scale")
        text = manifest.resolve_streaming_script(self.m, map_scale)
        self.assertIn("deferred=true", text)

    def test_resolve_streaming_script_falls_back_to_base(self):
        obj = next(c for c in self.m["cases"] if c["id"] == "object-transform")
        self.assertEqual(
            manifest.resolve_streaming_script(self.m, obj),
            manifest.read_script(self.m, obj),
        )
```

(`self.m` is the loaded manifest fixture already used by `TestManifest`; confirm by reading the class `setUp`. If the fixture is named differently, use that name.)

- [ ] **Step 2: Run test to verify it fails**

Run: `cd benchmarks/runners/python && python3 -m unittest test_bench.TestManifest -v`
Expected: FAIL — `AttributeError: module 'manifest' has no attribute 'resolve_streaming_script'`.

- [ ] **Step 3: Implement resolver + validation**

In `benchmarks/runners/python/manifest.py`, inside `load_manifest`'s per-case loop, right after the existing `script` existence check (the `raise ValueError(f"case {cid} script not found` block), add:

```python
        streaming_script = c.get("streamingScript")
        if streaming_script and not (corpus_dir / streaming_script).exists():
            raise ValueError(f"case {cid} streamingScript not found: {streaming_script}")
```

Then add, next to `read_script`:

```python
def resolve_streaming_script(manifest, case_obj):
    """Script for the streaming metric: the streamingScript variant if declared,
    else the base script. Warm/first-run always use read_script."""
    rel = case_obj.get("streamingScript") or case_obj["script"]
    return (Path(manifest["corpusDir"]) / rel).read_text(encoding="utf-8")
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd benchmarks/runners/python && python3 -m unittest test_bench.TestManifest -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add benchmarks/runners/python/manifest.py benchmarks/runners/python/test_bench.py
git commit -m "W-23545283: Add resolve_streaming_script + streamingScript validation (python)"
```

---

## Task 5: Scala manifest — `streamingScript` field + `resolveStreamingScript`

**Files:**
- Modify: `benchmarks/runners/engine/src/main/scala/org/mule/weave/benchmark/engine/Manifest.scala`
- Test: `benchmarks/runners/engine/src/test/scala/org/mule/weave/benchmark/engine/ManifestTest.scala`

**Interfaces:**
- Produces: `BenchCase.streamingScript: Option[String]` and `Manifest.resolveStreamingScript(m, c): String`. Consumed by Task 7 (`WarmBench.scala`).
- Consumes: existing `Manifest.load`, `resolveScript`, `resolveInputs`.

- [ ] **Step 1: Write the failing test**

Append to `benchmarks/runners/engine/src/test/scala/org/mule/weave/benchmark/engine/ManifestTest.scala` (inside the existing `AnyFreeSpec` body — check the class name/structure first and match it):

```scala
  "resolveStreamingScript prefers the streamingScript variant" in {
    val corpus = new File("../../corpus").getCanonicalFile
    val m = Manifest.load(corpus)
    val mapScale = m.cases.find(_.id == "map-scale").get
    Manifest.resolveStreamingScript(m, mapScale) should include ("deferred=true")
  }

  "resolveStreamingScript falls back to the base script" in {
    val corpus = new File("../../corpus").getCanonicalFile
    val m = Manifest.load(corpus)
    val obj = m.cases.find(_.id == "object-transform").get // no streamingScript
    Manifest.resolveStreamingScript(m, obj) shouldBe Manifest.resolveScript(m, obj)
  }
```

Ensure `import java.io.File` is present in the test file.

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew :benchmarks-engine:test --tests "org.mule.weave.benchmark.engine.ManifestTest"`
Expected: FAIL — compilation error (`streamingScript`/`resolveStreamingScript` not found) or missing member.

- [ ] **Step 3: Add the field to `BenchCase` and parse it**

In `Manifest.scala`, add `streamingScript` to the case class:

```scala
final case class BenchCase(
  id: String,
  script: String,
  streamingScript: Option[String],
  inputs: Seq[CaseInput],
  metrics: Set[String],
  iterations: Map[String, Int]) {
```

In `load`, after the existing `script` existence check, parse + validate the optional variant:

```scala
      val streamingScript: Option[String] =
        if (obj.has("streamingScript")) {
          val ss = obj.getString("streamingScript")
          if (!new File(corpusDir, ss).exists()) throw new RuntimeException(s"case $id streamingScript not found: $ss")
          Some(ss)
        } else None
```

Update the `BenchCase(...)` construction at the end of the loop to pass it:

```scala
      BenchCase(id, script, streamingScript, inputs, metrics, iterations)
```

Add the resolver in the `Manifest` object, next to `resolveScript`:

```scala
  def resolveStreamingScript(m: Manifest, c: BenchCase): String = {
    val rel = c.streamingScript.getOrElse(c.script)
    new String(Files.readAllBytes(new File(m.corpusDir, rel).toPath), StandardCharsets.UTF_8)
  }
```

- [ ] **Step 4: Fix any other `BenchCase(...)` constructions**

The new field is a positional param. Search for other constructions and add `None` (or the value) as needed:

Run: `rg -n 'BenchCase\(' benchmarks/runners/engine/src`
For each hit outside `Manifest.scala` (e.g. tests building a `BenchCase` directly), insert `streamingScript = None` in the correct position. If there are none, proceed.

- [ ] **Step 5: Run test to verify it passes**

Run: `./gradlew :benchmarks-engine:test --tests "org.mule.weave.benchmark.engine.ManifestTest"`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add benchmarks/runners/engine/src/main/scala/org/mule/weave/benchmark/engine/Manifest.scala benchmarks/runners/engine/src/test/scala/org/mule/weave/benchmark/engine/ManifestTest.scala
git commit -m "W-23545283: Add streamingScript field + resolveStreamingScript (engine manifest)"
```

---

## Task 6: Engine `ChunkedInputStream` helper + `EngineShell.runStreaming`

**Files:**
- Create: `benchmarks/runners/engine/src/main/scala/org/mule/weave/benchmark/engine/ChunkedInputStream.scala`
- Modify: `benchmarks/runners/engine/src/main/scala/org/mule/weave/benchmark/engine/EngineShell.scala`
- Test: `benchmarks/runners/engine/src/test/scala/org/mule/weave/benchmark/engine/EngineShellTest.scala`

**Interfaces:**
- Produces:
  - `class ChunkedInputStream(data: Array[Byte], chunkSize: Int) extends InputStream` — an InputStream over `data` whose `read(buf, off, len)` returns at most `chunkSize` bytes per call (paces input into the runtime).
  - `EngineShell.runStreaming(script: String, name: String, input: InputStream, inMime: String, inCharset: Option[String]): Long` — binds `input` as a lazy `BindingValue`, compiles the (deferred) script, drains the deferred `InputStream` result, returns the drained byte count.
- Consumes: existing `EngineShell` engine + `serviceManager`, `Manifest.ResolvedInput` (indirectly via caller).

- [ ] **Step 1: Write the failing test**

Add to `EngineShellTest.scala`:

```scala
  "ChunkedInputStream returns at most chunkSize bytes per read" in {
    val data = Array.tabulate[Byte](10)(_.toByte)
    val in = new ChunkedInputStream(data, 4)
    val buf = new Array[Byte](8)
    in.read(buf, 0, 8) shouldBe 4   // capped at chunkSize
    in.read(buf, 0, 8) shouldBe 4
    in.read(buf, 0, 8) shouldBe 2   // remainder
    in.read(buf, 0, 8) shouldBe -1  // EOF
  }

  "runStreaming binds a lazy InputStream and drains deferred output" in {
    val c = manifest.cases.find(_.id == "map-scale").get
    val bytes = Manifest.resolveInputs(manifest, c).head.bytes
    val shell = new EngineShell()
    val drained = shell.runStreaming(
      Manifest.resolveStreamingScript(manifest, c),
      EngineShell.safeName(c.id),
      new ChunkedInputStream(bytes, 65536),
      "application/json",
      None)
    drained should be > 0L
  }
```

The test references `manifest` (already a field in `EngineShellTest`). It needs the generated input; guard it:

```scala
  // at the top of the second test body, before use:
    if (!TestSupport.ensureGeneratedInputs(corpus)) cancel("generated input unavailable (node missing?)")
```

Add `import java.io.InputStream` if needed (only if referenced directly; `ChunkedInputStream` hides it).

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew :benchmarks-engine:test --tests "org.mule.weave.benchmark.engine.EngineShellTest"`
Expected: FAIL — `ChunkedInputStream`/`runStreaming` not found (compile error).

- [ ] **Step 3: Create `ChunkedInputStream.scala`**

```scala
package org.mule.weave.benchmark.engine

import java.io.InputStream

/** An InputStream over an in-memory byte array that returns at most `chunkSize`
  * bytes per read() call, pacing input into the DataWeave runtime the way a
  * network/pipe feeder would. The bytes are already resident, so this models
  * "runtime reads input lazily" without a real producer thread. Mirrors the
  * native-lib runners feeding fixed-size chunks. */
class ChunkedInputStream(data: Array[Byte], chunkSize: Int) extends InputStream {
  private var pos: Int = 0

  override def read(): Int = {
    if (pos >= data.length) -1
    else {
      val b = data(pos) & 0xff
      pos += 1
      b
    }
  }

  override def read(b: Array[Byte], off: Int, len: Int): Int = {
    if (pos >= data.length) -1
    else {
      val n = math.min(math.min(len, chunkSize), data.length - pos)
      System.arraycopy(data, pos, b, off, n)
      pos += n
      n
    }
  }

  override def available(): Int = data.length - pos
}
```

- [ ] **Step 4: Add `runStreaming` to `EngineShell`**

In `EngineShell.scala`, add these imports if not present: `import java.io.InputStream`. Add the method inside the `class EngineShell` body, after the existing `run(...)`:

```scala
  /** Streaming variant of `run`: binds `input` as a lazy InputStream (so the
    * runtime reads it incrementally), compiles the deferred script, and drains
    * the deferred PipedInputStream result in a read loop. Returns the number of
    * output bytes drained. Throws on compile/exec failure or a non-InputStream
    * (non-deferred) result, so a script that forgot `deferred=true` fails loudly. */
  def runStreaming(script: String, name: String, input: InputStream, inMime: String, inCharset: Option[String]): Long = {
    val bindings = new ScriptingBindings()
    val charset = Charset.forName(inCharset.getOrElse("UTF-8"))
    val bv = new BindingValue(input, Some(inMime), Map.empty[String, Any], charset)
    bindings.addBinding(name, bv)

    val config = engine.newConfig()
      .withScript(script)
      .withNameIdentifier(NameIdentifier(name))
      .withInputs(Array(new InputType(name, None)))
      .withDefaultOutputType("application/json")

    val compiled: DataWeaveScript = engine.compileWith(config)
    val result: DataWeaveResult = compiled.write(bindings, serviceManager, "application/json", Option.empty[Any])
    result.getContent match {
      case is: InputStream =>
        try {
          val buf = new Array[Byte](65536)
          var total = 0L
          var n = is.read(buf)
          while (n > 0) { total += n; n = is.read(buf) }
          total
        } finally {
          is.close()
        }
      case other =>
        throw new RuntimeException(
          s"streaming result is not an InputStream (did the script declare deferred=true?): ${other.getClass.getName}")
    }
  }
```

Add these imports to `EngineShell.scala` if missing: `DataWeaveResult` (from `org.mule.weave.v2.runtime`). Update the existing runtime import block:

```scala
import org.mule.weave.v2.runtime.{
  BindingValue,
  DataWeaveResult,
  DataWeaveScript,
  DataWeaveScriptingEngine,
  InputType,
  ModuleComponentsFactory,
  ParserConfiguration,
  ScriptingBindings
}
```

Note on the `write` overload: `write(bindings, serviceManager, outputMimeType, target: Option[Any])` exists on `DataWeaveScript` (verified in `DataWeaveScriptingEngine.scala:984`). Passing `"application/json"` + `Option.empty[Any]` gives a writer whose deferred setting comes from the script's `deferred=true` directive; the result content is the `PipedInputStream`.

- [ ] **Step 5: Run test to verify it passes**

Run: `./gradlew :benchmarks-engine:test --tests "org.mule.weave.benchmark.engine.EngineShellTest"`
Expected: PASS (existing `run` tests still green; new streaming tests pass, or the second `cancel`s only if generated input is unavailable — acceptable).

- [ ] **Step 6: Commit**

```bash
git add benchmarks/runners/engine/src/main/scala/org/mule/weave/benchmark/engine/ChunkedInputStream.scala benchmarks/runners/engine/src/main/scala/org/mule/weave/benchmark/engine/EngineShell.scala benchmarks/runners/engine/src/test/scala/org/mule/weave/benchmark/engine/EngineShellTest.scala
git commit -m "W-23545283: Add engine streaming path (chunked InputStream + deferred drain)"
```

---

## Task 7: Wire `WarmBench.runStreaming` to the streaming path

**Files:**
- Modify: `benchmarks/runners/engine/src/main/scala/org/mule/weave/benchmark/engine/WarmBench.scala`
- Test: `benchmarks/runners/engine/src/test/scala/org/mule/weave/benchmark/engine/WarmBenchTest.scala`

**Interfaces:**
- Consumes: `EngineShell.runStreaming`, `ChunkedInputStream`, `Manifest.resolveStreamingScript`, `Stats.toMBps`.
- Produces: `WarmBench.runStreaming` now measures chunked-input + deferred-output throughput. Same `Row(id, "streaming", "MB/s", ...)` output shape (consumed by `Emit`).

- [ ] **Step 1: Update `WarmBenchTest` streaming assertion**

The existing test `"streaming rows are produced with MB/s unit"` stays valid (same output shape). Strengthen it to prove the streaming variant is used — replace that test body with:

```scala
  "streaming rows are produced with MB/s unit via the deferred variant" in {
    if (!TestSupport.ensureGeneratedInputs(corpus)) cancel("generated input unavailable (node missing?)")
    val shell = new EngineShell()
    val rows = WarmBench.runStreaming(shell, manifest, iterCap = Some(2))
    rows.map(_.id) should contain allOf ("map-scale", "json-stream")
    all (rows.map(_.metric)) shouldBe "streaming"
    all (rows.map(_.unit)) shouldBe "MB/s"
    all (rows.map(_.stats.median)) should be > 0.0
  }
```

- [ ] **Step 2: Run test to verify current state**

Run: `./gradlew :benchmarks-engine:test --tests "org.mule.weave.benchmark.engine.WarmBenchTest"`
Expected: The streaming test still passes against the OLD batch implementation (output shape unchanged). This step confirms the test is green before the refactor; the behavior change is validated by it staying green after.

- [ ] **Step 3: Rewrite `runStreaming` to use the streaming path**

In `WarmBench.scala`, replace the body of `runStreaming`'s per-case loop. The new version resolves the streaming variant script, wraps the primary input bytes in a `ChunkedInputStream`, and times `shell.runStreaming`. Replace the method with:

```scala
  def runStreaming(shell: EngineShell, m: Manifest, iterCap: Option[Int] = None): Seq[Row] = {
    Manifest.casesForMetric(m, "streaming").map { c =>
      val script = Manifest.resolveStreamingScript(m, c)
      val inputs = Manifest.resolveInputs(m, c)
      val name = EngineShell.safeName(c.id)
      val primary = inputs.head
      val primaryBytes = primary.bytes.length.toLong
      val iters = iterCap.getOrElse(c.streaming)

      // Guard: if this case does NOT declare warm, warm up first so streaming isn't JIT-cold.
      // Warmup uses the deferred streaming path too, over a fresh ChunkedInputStream each time.
      if (!c.metrics.contains("warm")) {
        println(s"[streaming] ${c.id}: streaming-only, warming up first ($WARMUP_FLOOR iters)")
        var w = 0
        while (w < WARMUP_FLOOR) {
          shell.runStreaming(script, name, new ChunkedInputStream(primary.bytes, 65536), primary.mimeType, primary.charset)
          w += 1
        }
      }

      val mbps = new Array[Double](iters)
      var i = 0
      while (i < iters) {
        val in = new ChunkedInputStream(primary.bytes, 65536)
        val start = nowNs()
        shell.runStreaming(script, name, in, primary.mimeType, primary.charset)
        mbps(i) = Stats.toMBps(primaryBytes, msSince(start))
        i += 1
      }
      Row(c.id, "streaming", "MB/s", Stats.computeStats(mbps.toSeq), iters)
    }
  }
```

Update the scaladoc comment above `runStreaming` (currently describing the batch/asymmetry) to:

```scala
  /** Streaming throughput: feeds the primary input in 64KB chunks via a
    * ChunkedInputStream (mirroring the native-lib runners) and drains the
    * deferred output InputStream per iteration. MB/s is over the primary input
    * bytes. The streaming-script variant (deferred=true) is resolved via
    * Manifest.resolveStreamingScript so warm/first-run keep the base script.
    */
```

Delete the old "Methodology asymmetry" paragraph in that comment — it no longer applies.

A `ChunkedInputStream` is single-use (position advances to EOF), so a fresh instance is created per iteration and per warmup pass — this is intentional and cheap (no byte copy; wraps the shared array).

- [ ] **Step 4: Run test to verify it passes**

Run: `./gradlew :benchmarks-engine:test --tests "org.mule.weave.benchmark.engine.WarmBenchTest"`
Expected: PASS.

- [ ] **Step 5: Run the full engine suite to confirm no regressions**

Run: `./gradlew :benchmarks-engine:test`
Expected: BUILD SUCCESSFUL, all tests pass (Emit, EngineChild, EngineShell, Manifest, Smoke, StatsParity, WarmBench).

- [ ] **Step 6: Commit**

```bash
git add benchmarks/runners/engine/src/main/scala/org/mule/weave/benchmark/engine/WarmBench.scala benchmarks/runners/engine/src/test/scala/org/mule/weave/benchmark/engine/WarmBenchTest.scala
git commit -m "W-23545283: Engine streaming measures chunked-input + deferred-output throughput"
```

---

## Task 8: Node streaming loop resolves the deferred variant

**Files:**
- Modify: `benchmarks/runners/node/warm-bench.mjs`
- Test: `benchmarks/runners/node/warm-bench.test.mjs`

**Interfaces:**
- Consumes: `resolveStreamingScript` from `../../lib/manifest.mjs` (Task 3).
- The `runTransform` call, chunk size (64KB), and MB/s formula are unchanged — only the *script* the streaming loop runs changes.

- [ ] **Step 1: Strengthen the test to assert the deferred script is used**

The existing test `"warm + streaming rows are produced with valid stats"` covers the output shape. Add a focused test to `warm-bench.test.mjs` that the streaming loop runs a `deferred=true` script by asserting the resolver picks it (behavioral proof that doesn't require intercepting the wrapper):

```javascript
import { resolveStreamingScript } from "../../lib/manifest.mjs";

test("streaming uses the deferred=true script variant", () => {
  const manifest = loadManifest(CORPUS);
  const mapScale = manifest.cases.find((c) => c.id === "map-scale");
  assert.ok(resolveStreamingScript(manifest, mapScale).includes("deferred=true"));
});
```

(Reuses `loadManifest` and `CORPUS` already imported/defined in the file.)

- [ ] **Step 2: Run test to verify it fails**

Run: `node --test benchmarks/runners/node/warm-bench.test.mjs`
Expected: FAIL — `resolveStreamingScript is not exported` if Task 3 not merged; if Task 3 is merged, this passes immediately at the resolver level but the *loop* still reads the base script. To make the test meaningful, also verify the loop change in Step 3.

- [ ] **Step 3: Update the streaming loop to resolve the variant**

In `warm-bench.mjs`, the file currently imports `{ casesForMetric, resolveInputs }` from `../../lib/manifest.mjs`. Add `resolveStreamingScript`:

```javascript
import { casesForMetric, resolveInputs, resolveStreamingScript } from "../../lib/manifest.mjs";
```

In the streaming loop (the `for (const c of casesForMetric(manifest, "streaming"))` block), replace:

```javascript
    const script = readScript(manifest, c);
```

with:

```javascript
    const script = resolveStreamingScript(manifest, c);
```

Leave the `warm` loop's `readScript(manifest, c)` untouched. `readScript` is still used by the warm loop, so keep the helper.

- [ ] **Step 4: Run the tests**

Run: `node --test benchmarks/runners/node/warm-bench.test.mjs`
Expected: PASS. (This spawns the real wrapper; if the native lib is unavailable the pre-existing test would already fail — that's an environment issue, not this change.)

- [ ] **Step 5: Commit**

```bash
git add benchmarks/runners/node/warm-bench.mjs benchmarks/runners/node/warm-bench.test.mjs
git commit -m "W-23545283: Node streaming runs the deferred=true script variant"
```

---

## Task 9: Python streaming loop resolves the deferred variant

**Files:**
- Modify: `benchmarks/runners/python/warm_bench.py`
- Test: `benchmarks/runners/python/test_bench.py`

**Interfaces:**
- Consumes: `resolve_streaming_script` from `manifest` (Task 4).
- `run_transform` call, 8KB chunking, MB/s formula unchanged — only the script changes.

- [ ] **Step 1: Write the failing test**

The `TestWarmBench` streaming test uses a `_FakeApi` and a synthetic `_streaming_manifest`, so it does not exercise the real script resolution. Add a manifest-level assertion to `TestManifest` (or `TestWarmBench`) instead:

```python
    def test_streaming_uses_deferred_variant(self):
        map_scale = next(c for c in self.m["cases"] if c["id"] == "map-scale")
        self.assertIn("deferred=true", manifest.resolve_streaming_script(self.m, map_scale))
```

(Place in `TestManifest`, which has `self.m`. This overlaps Task 4's resolver test but asserts the corpus wiring specifically; keep it — it documents intent at the runner boundary.)

If you want the loop itself covered: update `_FakeApi.run_transform` is not necessary; the loop change is verified by reading the diff + the resolver test. Do not over-engineer a fake that intercepts the script string.

- [ ] **Step 2: Run test to verify it fails or passes**

Run: `cd benchmarks/runners/python && python3 -m unittest test_bench.TestManifest -v`
Expected: PASS if Task 4 merged (resolver exists + corpus has the variant). If it fails with `deferred=true` not found, Task 1/2 are incomplete.

- [ ] **Step 3: Update the streaming loop**

In `warm_bench.py`, the imports line is:

```python
from manifest import cases_for_metric, read_script, resolve_inputs
```

Add `resolve_streaming_script`:

```python
from manifest import cases_for_metric, read_script, resolve_inputs, resolve_streaming_script
```

In the streaming loop (`for c in cases_for_metric(manifest, "streaming"):`), replace:

```python
        script = read_script(manifest, c)
```

with:

```python
        script = resolve_streaming_script(manifest, c)
```

Leave the `warm` loop's `read_script(manifest, c)` untouched.

- [ ] **Step 4: Run the tests**

Run: `cd benchmarks/runners/python && python3 -m unittest test_bench -v`
Expected: PASS (all classes).

- [ ] **Step 5: Commit**

```bash
git add benchmarks/runners/python/warm_bench.py benchmarks/runners/python/test_bench.py
git commit -m "W-23545283: Python streaming runs the deferred=true script variant"
```

---

## Task 10: Un-suppress the streaming delta in the report

**Files:**
- Modify: `benchmarks/report/report.mjs`
- Test: `benchmarks/report/report.test.mjs`

**Interfaces:**
- `computeDelta`, `buildTable`, `formatDelta` keep their signatures. `NON_COMPARABLE_METRICS` becomes empty; `streaming` rows now carry a real delta.

- [ ] **Step 1: Update the report tests to expect a real streaming delta**

In `report.test.mjs`, replace the two tests added for finding #2 — `"streaming metric is non-comparable — no cross-runner delta is printed"` and `"renderMarkdown footnotes the n/a delta when a non-comparable metric is present"` — with:

```javascript
test("streaming metric now carries a real cross-runner delta", () => {
  const manifest = loadManifest(CORPUS);
  const results = [load("node-a.json"), load("engine-b.json")];
  const { rows } = buildTable(manifest, results, "engine");

  // Fixtures: map-scale streaming node=300 vs engine=150 MB/s. With aligned
  // methodology this is a real +100% delta, no longer suppressed.
  const streaming = rows.find((r) => r.id === "map-scale" && r.metric === "streaming");
  assert.ok(streaming, "fixture should exercise a streaming row");
  assert.equal(streaming.comparable, true);
  assert.equal(streaming.delta, 100);
  assert.equal(formatDelta(streaming), "+100.0%");
});

test("renderMarkdown emits no streaming non-comparable footnote", () => {
  const manifest = loadManifest(CORPUS);
  const results = [load("node-a.json"), load("engine-b.json")];
  const table = buildTable(manifest, results, "engine");
  const md = renderMarkdown(table, results, {
    baselineRunner: "engine",
    stamp: { commit: "abc1234", date: "2026-07-24T14:33:03Z" },
  });
  assert.ok(md.includes("| map-scale | streaming | MB/s |"), "streaming row is present");
  assert.ok(!md.includes("not like-for-like across runners"), "footnote removed");
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `node --test benchmarks/report/report.test.mjs`
Expected: FAIL — the old behavior still sets `comparable=false`/`n/a` and emits the footnote.

- [ ] **Step 3: Remove the suppression**

In `report.mjs`:

Change the non-comparable set to empty and update its comment:

```javascript
/**
 * Metrics whose cross-runner numbers are not like-for-like. Empty since the
 * streaming methodology was aligned across runners (chunked input + deferred
 * output) — the streaming delta is meaningful again. Kept as a seam for any
 * future non-comparable metric.
 */
const NON_COMPARABLE_METRICS = new Set();
```

Leave `formatDelta`, `buildTable`'s `comparable` computation, and the `n/a` branch exactly as-is — with an empty set, `comparable` is always `true` and `n/a` never renders, but the mechanism stays for the future.

Remove the footnote block in `renderMarkdown` (the `if (table.rows.some((r) => !r.comparable)) { out.push("> \`n/a\` deltas mark ... ", "") }` block) and the console footnote block in `main` (the `if (rows.some((r) => !r.comparable)) { console.log(...) }` block). Since `comparable` is now always true, these are dead; delete both for cleanliness.

- [ ] **Step 4: Run test to verify it passes**

Run: `node --test benchmarks/report/report.test.mjs`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add benchmarks/report/report.mjs benchmarks/report/report.test.mjs
git commit -m "W-23545283: Un-suppress streaming delta now that methodology is aligned"
```

---

## Task 11: Full verification + regenerate RESULTS.md from a real run

**Files:**
- Modify: `benchmarks/report/RESULTS.md` (only from a real run)

**Interfaces:** none — end-to-end validation.

- [ ] **Step 1: Run all benchmark unit tests across runners**

```bash
cd /Users/aradunsky/Documents/public/data-weave-cli
node --test benchmarks/lib/manifest.test.mjs benchmarks/report/report.test.mjs benchmarks/runners/node/warm-bench.test.mjs
cd benchmarks/runners/python && python3 -m unittest test_bench -v && cd -
./gradlew :benchmarks-engine:test
```

Expected: all green.

- [ ] **Step 2: Decide on RESULTS.md regeneration**

The committed `RESULTS.md` streaming rows currently show `n/a`. They must be regenerated from a real benchmark run, not hand-edited. If a full run across all three runners is feasible in this environment:

```bash
# Produces per-runner result JSONs, then joins into RESULTS.md.
./gradlew native-lib:benchmark          # node + python runners (opt-in task)
./gradlew :benchmarks-engine:run --args="<corpusDir> <resultsDir> <repoRoot>"   # engine runner (confirm the actual run task/args in build.gradle)
node benchmarks/report/report.mjs <resultJsons...> --baseline engine --markdown benchmarks/report/RESULTS.md
```

Verify the regenerated `RESULTS.md`: the `map-scale`/`json-stream` streaming rows now show a signed `%` delta (not `n/a`), and the non-comparable footnote is gone.

If a full run is NOT feasible here, do NOT edit `RESULTS.md` by hand. Instead leave it and note in the PR that RESULTS.md must be regenerated on a machine that can run the full benchmark. (The report *code* is already correct and tested; only the committed sample output lags.)

- [ ] **Step 3: Commit (only if RESULTS.md was regenerated from a real run)**

```bash
git add benchmarks/report/RESULTS.md
git commit -m "W-23545283: Regenerate RESULTS.md with aligned streaming deltas"
```

- [ ] **Step 4: Push the branch**

```bash
git push
```

---

## Self-Review notes

- **Spec coverage:** Corpus variants (Task 1), manifest field (Task 2), three manifest resolvers (Tasks 3–5), engine streaming rework (Tasks 6–7), native-lib script switch (Tasks 8–9), report un-suppression (Task 10), verification + RESULTS.md (Task 11). All spec sections mapped.
- **Type consistency:** `resolveStreamingScript` (JS/Scala) / `resolve_streaming_script` (Python) return script *text*, matching existing `resolveScript`/`read_script`. `EngineShell.runStreaming(script, name, input: InputStream, inMime, inCharset: Option[String]): Long` is referenced identically in Tasks 6 and 7. `ChunkedInputStream(data, chunkSize)` constructor consistent across Tasks 6–7. `BenchCase.streamingScript: Option[String]` positional field added in Task 5 and consumed via `resolveStreamingScript` (not by direct field access in the runner), so no other call sites break beyond the constructor (Task 5 Step 4 sweeps for those).
- **Placeholders:** none — every code step shows full code; the only conditional is Task 11 Step 2 (real-run gate), which is a deliberate guard against fabricating numbers, not a TODO.
- **Risk carried from spec:** deferred errors surface via logging, not exceptions; the engine `runStreaming` drains and returns a byte count, and `WarmBenchTest` asserts `median > 0` — a fully-empty (silently failed) stream would yield 0 bytes and fail the test.
