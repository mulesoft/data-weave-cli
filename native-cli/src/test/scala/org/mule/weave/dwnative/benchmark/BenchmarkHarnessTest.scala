package org.mule.weave.dwnative.benchmark

import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

import java.io.{ ByteArrayOutputStream, File, PrintStream }
import java.nio.charset.StandardCharsets
import java.nio.file.Files

class BenchmarkHarnessTest extends AnyFreeSpec with Matchers {

  private def tmp(suffix: String, content: String): File = {
    val f = File.createTempFile("bench", suffix)
    f.deleteOnExit()
    Files.write(f.toPath, content.getBytes(StandardCharsets.UTF_8))
    f
  }

  private def capture(fn: PrintStream => Unit): String = {
    val buf = new ByteArrayOutputStream()
    val ps = new PrintStream(buf, true, "UTF-8")
    fn(ps)
    new String(buf.toByteArray, StandardCharsets.UTF_8)
  }

  "parseArgs" - {
    "parses coldfirst mode with one input" in {
      val a = BenchmarkHarness.parseArgs(Array(
        "--bench-mode=coldfirst",
        "--script=/tmp/x.dwl",
        "--input=payload=/tmp/p.json:application/json:utf-8"))
      a.mode shouldBe "coldfirst"
      a.scriptFile shouldBe "/tmp/x.dwl"
      a.inputs should have size 1
      a.inputs.head shouldBe BenchInput("payload", "/tmp/p.json", "application/json", "utf-8")
    }

    "parses warm mode with warmup and iters" in {
      val a = BenchmarkHarness.parseArgs(Array(
        "--bench-mode=warm", "--script=/tmp/x.dwl", "--warmup=5", "--iters=30"))
      a.mode shouldBe "warm"
      a.warmup shouldBe 5
      a.iters shouldBe 30
      a.inputs shouldBe empty
    }

    "handles a mimeType-only input (charset defaults to utf-8)" in {
      val a = BenchmarkHarness.parseArgs(Array(
        "--bench-mode=coldfirst", "--script=/tmp/x.dwl",
        "--input=payload=/tmp/p.json:application/json"))
      a.inputs.head.charset shouldBe "utf-8"
    }
  }

  "runColdFirst" - {
    "emits READY then a single firstRunMs JSON line, output not on the stream" in {
      val script = tmp(".dwl", "output application/json --- payload.a + 1")
      val input = tmp(".json", "{\"a\": 41}")
      val a = BenchArgs("coldfirst", script.getAbsolutePath,
        Seq(BenchInput("payload", input.getAbsolutePath, "application/json", "utf-8")), 0, 0)
      val sink = new ByteArrayOutputStream()
      val stdout = capture(ps => BenchmarkHarness.runColdFirst(a, ps, sink))
      val lines = stdout.split("\n").filter(_.nonEmpty)
      lines.head shouldBe "READY"
      lines.last should include ("firstRunMs")
      lines.count(_.contains("firstRunMs")) shouldBe 1
      // The transformed "42" went to the sink, NOT to stdout.
      new String(sink.toByteArray, StandardCharsets.UTF_8).trim shouldBe "42"
    }
  }

  "runWarm" - {
    "emits READY then a warmMs array of length iters" in {
      val script = tmp(".dwl", "output application/json --- payload.a + 1")
      val input = tmp(".json", "{\"a\": 41}")
      val a = BenchArgs("warm", script.getAbsolutePath,
        Seq(BenchInput("payload", input.getAbsolutePath, "application/json", "utf-8")), 1, 3)
      val stdout = capture(ps => BenchmarkHarness.runWarm(a, ps, new ByteArrayOutputStream()))
      val json = stdout.split("\n").filter(_.contains("warmMs")).head
      json should include ("warmMs")
      // 3 comma-separated samples -> 2 commas inside the array
      json.count(_ == ',') shouldBe 2
    }
  }

  "a failing script throws (non-zero exit path)" in {
    val script = tmp(".dwl", "output application/json --- 1 / 0")
    val input = tmp(".json", "{}")
    val a = BenchArgs("coldfirst", script.getAbsolutePath,
      Seq(BenchInput("payload", input.getAbsolutePath, "application/json", "utf-8")), 0, 0)
    an [RuntimeException] should be thrownBy
      BenchmarkHarness.runColdFirst(a, capturePs(), new ByteArrayOutputStream())
  }

  private def capturePs(): PrintStream = new PrintStream(new ByteArrayOutputStream(), true, "UTF-8")
}
