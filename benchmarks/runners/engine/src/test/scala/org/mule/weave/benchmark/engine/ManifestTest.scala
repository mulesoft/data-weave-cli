package org.mule.weave.benchmark.engine

import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import java.io.File

class ManifestTest extends AnyFreeSpec with Matchers {
  private val corpus = new File("../../corpus").getCanonicalFile

  "loads all corpus cases with stable ids" in {
    val m = Manifest.load(corpus)
    m.ids should contain allOf ("trivial", "object-transform", "map-scale", "xml-to-csv", "json-stream", "compile-heavy")
  }

  "casesForMetric filters by declared metrics" in {
    val m = Manifest.load(corpus)
    Manifest.casesForMetric(m, "streaming").map(_.id) should contain allOf ("map-scale", "json-stream")
    Manifest.casesForMetric(m, "cold-start").map(_.id) should contain ("trivial")
  }

  "resolveScript reads the .dwl source" in {
    val m = Manifest.load(corpus)
    val trivial = m.cases.find(_.id == "trivial").get
    Manifest.resolveScript(m, trivial).trim should include ("2 + 2")
  }

  "resolveInputs reads committed input bytes with mime + charset" in {
    val m = Manifest.load(corpus)
    val xml = m.cases.find(_.id == "xml-to-csv").get
    val inputs = Manifest.resolveInputs(m, xml)
    inputs should have size 1
    inputs.head.name shouldBe "payload"
    inputs.head.mimeType shouldBe "application/xml"
    inputs.head.charset shouldBe Some("UTF-16")
    inputs.head.bytes.length should be > 0
  }

  "validateResultIds throws on an orphan id" in {
    val m = Manifest.load(corpus)
    an [RuntimeException] should be thrownBy Manifest.validateResultIds(m, Seq("trivial", "not-a-case"))
    noException should be thrownBy Manifest.validateResultIds(m, Seq("trivial"))
  }

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
}
