package org.mule.weave.benchmark.engine

import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import java.io.File
import java.io.InputStream

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
    if (!TestSupport.ensureGeneratedInputs(corpus)) cancel("generated input unavailable (node missing?)")
    val c = manifest.cases.find(_.id == "map-scale").get
    val resolved = Manifest.resolveInputs(manifest, c).head
    val shell = new EngineShell()
    val drained = shell.runStreaming(
      Manifest.resolveStreamingScript(manifest, c),
      EngineShell.safeName(c.id),   // scriptName — unique per case
      resolved.name,                // inputName — matches what the script reads (payload)
      new ChunkedInputStream(resolved.bytes, 65536),
      "application/json",
      None)
    drained should be > 0L
  }
}
