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
