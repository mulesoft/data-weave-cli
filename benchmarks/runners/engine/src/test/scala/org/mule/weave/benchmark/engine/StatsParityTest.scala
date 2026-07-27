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
