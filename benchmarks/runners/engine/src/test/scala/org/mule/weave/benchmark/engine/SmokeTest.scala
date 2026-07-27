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

  "CountingOutputStream.write(Array[Byte], Int, Int) counts only len bytes" in {
    val out = new CountingOutputStream()
    val buf = Array[Byte](1, 2, 3, 4, 5)
    out.write(buf, 1, 3)  // write 3 bytes starting at offset 1
    out.count() shouldBe 3L
  }
}
