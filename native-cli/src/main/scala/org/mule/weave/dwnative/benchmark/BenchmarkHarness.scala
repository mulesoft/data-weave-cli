package org.mule.weave.dwnative.benchmark

import org.mule.weave.dwnative.NativeRuntime
import org.mule.weave.dwnative.WeaveExecutionResult
import org.mule.weave.dwnative.cli.DefaultConsole
import org.mule.weave.dwnative.utils.DataWeaveUtils
import org.mule.weave.v2.runtime.BindingValue
import org.mule.weave.v2.runtime.ScriptingBindings

import java.io.{ File, OutputStream, PrintStream }
import java.nio.charset.Charset
import java.nio.file.Files

final case class BenchInput(name: String, file: String, mimeType: String, charset: String)
final case class BenchArgs(mode: String, scriptFile: String, inputs: Seq[BenchInput], warmup: Int, iters: Int)

/** Corpus-agnostic in-binary benchmark harness. Reachable only in a build made with
  * -Pbenchmark=true (guarded by BenchmarkMode.ENABLED in DWCLI); native-image folds it
  * out of a production dw. Prints "READY" the instant one NativeRuntime is constructed,
  * then a single JSON line of timings. Parent (benchmarks/runners/cli) measures cold-start
  * as spawn->READY wall-clock. */
object BenchmarkHarness {

  /** Discards bytes; used as the transform write sink so we never touch real stdout. */
  private final class DiscardStream extends OutputStream {
    override def write(b: Int): Unit = ()
    override def write(b: Array[Byte]): Unit = ()
    override def write(b: Array[Byte], off: Int, len: Int): Unit = ()
  }

  private def nowNs(): Long = System.nanoTime()
  private def msSince(startNs: Long): Double = (System.nanoTime() - startNs) / 1e6

  def parseArgs(args: Array[String]): BenchArgs = {
    var mode = ""
    var script = ""
    val inputs = scala.collection.mutable.ArrayBuffer[BenchInput]()
    var warmup = 0
    var iters = 100
    args.foreach { arg =>
      val eq = arg.indexOf('=')
      val key = if (eq >= 0) arg.substring(0, eq) else arg
      val value = if (eq >= 0) arg.substring(eq + 1) else ""
      key match {
        case "--bench-mode" => mode = value
        case "--script"     => script = value
        case "--warmup"     => warmup = value.toInt
        case "--iters"      => iters = value.toInt
        case "--input"      =>
          // value = <name>=<file>\t<mimeType>[\t<charset>]
          val nameSep = value.indexOf('=')
          val name = value.substring(0, nameSep)
          val rest = value.substring(nameSep + 1)
          val parts = rest.split("\t", 3)
          val file = parts(0)
          val mimeType = parts(1)
          val charset = if (parts.length > 2 && parts(2).nonEmpty) parts(2) else "utf-8"
          inputs += BenchInput(name, file, mimeType, charset)
        case _ => throw new RuntimeException(s"unknown bench arg: $arg")
      }
    }
    if (mode.isEmpty) throw new RuntimeException("--bench-mode is required")
    if (script.isEmpty) throw new RuntimeException("--script is required")
    BenchArgs(mode, script, inputs.toSeq, warmup, iters)
  }

  private def newRuntime(): NativeRuntime = {
    val console = DefaultConsole.enableSilent()
    val utils = new DataWeaveUtils(console)
    new NativeRuntime(utils.getLibPathHome(), Array.empty[File], console, None)
  }

  private def readScript(a: BenchArgs): String =
    new String(Files.readAllBytes(new File(a.scriptFile).toPath), java.nio.charset.StandardCharsets.UTF_8)

  private def bindings(a: BenchArgs): ScriptingBindings = {
    val b = new ScriptingBindings()
    a.inputs.foreach { in =>
      val bytes = Files.readAllBytes(new File(in.file).toPath)
      val bv = new BindingValue(bytes, Some(in.mimeType), Map.empty[String, Any], Charset.forName(in.charset))
      b.addBinding(in.name, bv)
    }
    b
  }

  private def assertOk(r: WeaveExecutionResult): Unit =
    if (!r.success()) throw new RuntimeException("run failed: " + r.result())

  def runColdFirst(a: BenchArgs, out: PrintStream, sink: OutputStream): Unit = {
    val script = readScript(a)
    val b = bindings(a)
    val rt = newRuntime()          // engine init — measured externally as cold-start
    out.print("READY\n"); out.flush()
    val start = nowNs()
    assertOk(rt.run(script, "bench", b, sink, "application/json", None))
    val firstRunMs = msSince(start)
    out.print("{\"firstRunMs\":" + firstRunMs + "}\n")
  }

  def runWarm(a: BenchArgs, out: PrintStream, sink: OutputStream): Unit = {
    val script = readScript(a)
    val b = bindings(a)
    val rt = newRuntime()
    out.print("READY\n"); out.flush()
    var i = 0
    while (i < a.warmup) { assertOk(rt.run(script, "bench", b, sink, "application/json", None)); i += 1 }
    val samples = new Array[Double](a.iters)
    i = 0
    while (i < a.iters) {
      val start = nowNs()
      assertOk(rt.run(script, "bench", b, sink, "application/json", None))
      samples(i) = msSince(start)
      i += 1
    }
    out.print("{\"warmMs\":[" + samples.mkString(",") + "]}\n")
  }

  def main(args: Array[String]): Unit = {
    val a = parseArgs(args)
    val sink = new DiscardStream()
    a.mode match {
      case "coldfirst" => runColdFirst(a, System.out, sink)
      case "warm"      => runWarm(a, System.out, sink)
      case other       => throw new RuntimeException(s"unknown --bench-mode: $other")
    }
  }
}
