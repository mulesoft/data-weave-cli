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
