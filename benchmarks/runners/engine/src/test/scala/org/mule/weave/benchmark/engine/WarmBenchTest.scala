package org.mule.weave.benchmark.engine

import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import java.io.File

class WarmBenchTest extends AnyFreeSpec with Matchers {
  private val corpus = new File("../../corpus").getCanonicalFile
  private val manifest = Manifest.load(corpus)

  "warm rows are produced with ms unit and positive median" in {
    if (!TestSupport.ensureGeneratedInputs(corpus)) cancel("generated input unavailable (node missing?)")
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
