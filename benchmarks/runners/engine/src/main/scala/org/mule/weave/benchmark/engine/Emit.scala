package org.mule.weave.benchmark.engine

import org.json.JSONObject

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.time.Instant

/** Orchestrator: spawns EngineChild for cold-start/first-run, runs WarmBench
  * in-process, validates ids, stamps env, and writes the result JSON. */
object Emit {

  final case class Caps(
    samples: Option[Int] = None,
    warmup: Option[Int] = None,
    warm: Option[Int] = None,
    streaming: Option[Int] = None)

  def run(corpus: File, resultsDir: File, repoRoot: File, caps: Caps = Caps()): File = {
    val manifest = Manifest.load(corpus)

    val coldRows = spawnColdAndFirstRun(manifest, corpus, caps.samples)

    val shell = new EngineShell()
    val warmRows =
      WarmBench.runWarm(shell, manifest, caps.warmup, caps.warm) ++
        WarmBench.runStreaming(shell, manifest, caps.streaming)

    val rows = coldRows ++ warmRows
    Manifest.validateResultIds(manifest, rows.map(_.id)) // fail-fast on orphan ids

    val env = EnvStamp.gather(repoRoot)
    val now = Instant.now().toString
    val json = Result.toJson(env, rows, now)

    resultsDir.mkdirs()
    val out = new File(resultsDir, s"engine-${now.replaceAll("[:.]", "-")}.json")
    Files.write(out.toPath, json.getBytes(StandardCharsets.UTF_8))
    println(s"wrote ${out.getAbsolutePath} (${rows.length} rows)")
    out
  }

  /** Spawn a fresh JVM per sample; aggregate init/first-run per case. */
  private def spawnColdAndFirstRun(manifest: Manifest, corpus: File, samplesCap: Option[Int]): Seq[Row] = {
    val ids = (Manifest.casesForMetric(manifest, "cold-start").map(_.id) ++
      Manifest.casesForMetric(manifest, "first-run").map(_.id)).distinct

    ids.flatMap { id =>
      val c = manifest.cases.find(_.id == id).get
      val n = samplesCap.getOrElse(c.samples)
      val inits = new Array[Double](n)
      val firsts = new Array[Double](n)
      var i = 0
      while (i < n) {
        val (initMs, firstMs) = sampleOnce(corpus, id)
        inits(i) = initMs
        firsts(i) = firstMs
        i += 1
      }
      val rows = scala.collection.mutable.ArrayBuffer[Row]()
      if (c.metrics.contains("cold-start"))
        rows += Row(id, "cold-start", "ms", Stats.computeStats(inits.toSeq), n)
      if (c.metrics.contains("first-run"))
        rows += Row(id, "first-run", "ms", Stats.computeStats(firsts.toSeq), n)
      rows.toSeq
    }
  }

  private def sampleOnce(corpus: File, caseId: String): (Double, Double) = {
    val javaBin = new File(System.getProperty("java.home"), "bin/java").getAbsolutePath
    val cp = System.getProperty("java.class.path")
    val pb = new ProcessBuilder(
      javaBin, "-cp", cp,
      "org.mule.weave.benchmark.engine.EngineChild",
      corpus.getAbsolutePath, caseId)
    val p = pb.start()
    val lines = scala.io.Source.fromInputStream(p.getInputStream).getLines().toList
    val code = p.waitFor()
    if (code != 0) throw new RuntimeException(s"EngineChild failed for case '$caseId' (exit $code)")
    val obj = new JSONObject(lines.last)
    (obj.getDouble("initMs"), obj.getDouble("firstRunMs"))
  }

  def main(args: Array[String]): Unit = {
    require(args.length >= 3, "usage: Emit <corpusDir> <resultsDir> <repoRoot>")
    run(new File(args(0)), new File(args(1)), new File(args(2)))
  }
}
