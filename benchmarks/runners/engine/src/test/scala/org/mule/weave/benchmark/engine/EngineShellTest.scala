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
