package org.mule.weave.benchmark.engine

import org.json.JSONObject
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import java.io.File

class EngineChildTest extends AnyFreeSpec with Matchers {
  private val corpus = new File("../../corpus").getCanonicalFile

  private def spawn(caseId: String): (Int, List[String]) = {
    val javaBin = new File(System.getProperty("java.home"), "bin/java").getAbsolutePath
    val cp = System.getProperty("java.class.path")
    val pb = new ProcessBuilder(
      javaBin, "-cp", cp,
      "org.mule.weave.benchmark.engine.EngineChild",
      corpus.getAbsolutePath, caseId)
    val p = pb.start()
    val out = scala.io.Source.fromInputStream(p.getInputStream).getLines().toList
    val code = p.waitFor()
    (code, out)
  }

  "child prints a READY marker then the first-run timing for a case" in {
    val (code, lines) = spawn("trivial")
    code shouldBe 0
    // READY is the cold-start boundary the parent stamps on; the JSON line carries
    // the in-process first-run timing.
    lines should contain("READY")
    val jsonLine = lines.filter(_.startsWith("{")).lastOption.getOrElse("")
    val obj = new JSONObject(jsonLine)
    obj.getDouble("firstRunMs") should be > 0.0
  }
}
