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
