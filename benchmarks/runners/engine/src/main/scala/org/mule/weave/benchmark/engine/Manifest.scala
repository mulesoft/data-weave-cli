package org.mule.weave.benchmark.engine

import org.json.{ JSONArray, JSONObject }

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import scala.collection.mutable

final case class CaseInput(name: String, file: String, mimeType: String, charset: Option[String], generated: Boolean)

final case class BenchCase(
  id: String,
  script: String,
  streamingScript: Option[String],
  inputs: Seq[CaseInput],
  metrics: Set[String],
  iterations: Map[String, Int]) {

  def warm: Int = iterations.getOrElse("warm", 100)
  def warmup: Int = iterations.getOrElse("warmup", 10)
  def streaming: Int = iterations.getOrElse("streaming", 10)
  def samples: Int = iterations.getOrElse("samples", 20)
}

final case class ResolvedInput(name: String, bytes: Array[Byte], mimeType: String, charset: Option[String])

class Manifest(val corpusDir: File, val cases: Seq[BenchCase]) {
  def ids: Set[String] = cases.map(_.id).toSet
}

object Manifest {

  private val AllowedMetrics = Set("cold-start", "first-run", "warm", "streaming")

  def load(corpusDir: File): Manifest = {
    val manifestFile = new File(corpusDir, "manifest.json")
    val raw = new String(Files.readAllBytes(manifestFile.toPath), StandardCharsets.UTF_8)
    val root = new JSONObject(raw)
    val casesArr: JSONArray = root.getJSONArray("cases")

    val seen = mutable.Set[String]()
    val cases = (0 until casesArr.length()).map { i =>
      val obj = casesArr.getJSONObject(i)
      val id = obj.getString("id")
      if (id.isEmpty) throw new RuntimeException("manifest case is missing an id")
      if (seen.contains(id)) throw new RuntimeException(s"duplicate case id: $id")
      seen += id

      val metricsArr = obj.getJSONArray("metrics")
      if (metricsArr.length() == 0) throw new RuntimeException(s"case $id must declare a non-empty metrics[]")
      val metrics = (0 until metricsArr.length()).map(metricsArr.getString).toSet
      metrics.foreach(m => if (!AllowedMetrics.contains(m)) throw new RuntimeException(s"case $id has unknown metric: $m"))

      val script = obj.getString("script")
      if (!new File(corpusDir, script).exists()) throw new RuntimeException(s"case $id script not found: $script")

      val streamingScript: Option[String] =
        if (obj.has("streamingScript")) {
          val ss = obj.getString("streamingScript")
          if (!new File(corpusDir, ss).exists()) throw new RuntimeException(s"case $id streamingScript not found: $ss")
          Some(ss)
        } else None

      val iterations: Map[String, Int] =
        if (obj.has("iterations")) {
          val it = obj.getJSONObject("iterations")
          it.keySet().toArray.map(_.asInstanceOf[String]).map(k => k -> it.getInt(k)).toMap
        } else Map.empty

      val inputs: Seq[CaseInput] =
        if (obj.has("inputs")) {
          val ins = obj.getJSONObject("inputs")
          ins.keySet().toArray.map(_.asInstanceOf[String]).toSeq.map { name =>
            val io = ins.getJSONObject(name)
            val file = io.getString("file")
            val generated = io.optBoolean("generated", false)
            if (!generated && !new File(corpusDir, file).exists()) {
              throw new RuntimeException(s"case $id input '$name' file not found: $file")
            }
            CaseInput(
              name = name,
              file = file,
              mimeType = io.getString("mimeType"),
              charset = if (io.has("charset")) Some(io.getString("charset")) else None,
              generated = generated)
          }
        } else Seq.empty

      BenchCase(id, script, streamingScript, inputs, metrics, iterations)
    }
    new Manifest(corpusDir, cases)
  }

  def casesForMetric(m: Manifest, metric: String): Seq[BenchCase] =
    m.cases.filter(_.metrics.contains(metric))

  def validateResultIds(m: Manifest, resultIds: Seq[String]): Unit = {
    resultIds.foreach { id =>
      if (!m.ids.contains(id)) throw new RuntimeException(s"result contains orphan id not in manifest: $id")
    }
  }

  def resolveScript(m: Manifest, c: BenchCase): String =
    new String(Files.readAllBytes(new File(m.corpusDir, c.script).toPath), StandardCharsets.UTF_8)

  def resolveStreamingScript(m: Manifest, c: BenchCase): String = {
    val rel = c.streamingScript.getOrElse(c.script)
    new String(Files.readAllBytes(new File(m.corpusDir, rel).toPath), StandardCharsets.UTF_8)
  }

  def resolveInputs(m: Manifest, c: BenchCase): Seq[ResolvedInput] =
    c.inputs.map { in =>
      val bytes = Files.readAllBytes(new File(m.corpusDir, in.file).toPath)
      ResolvedInput(in.name, bytes, in.mimeType, in.charset)
    }
}
