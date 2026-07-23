package org.mule.weave.benchmark.engine

import org.json.{ JSONArray, JSONObject }

/** Serializes rows + env into the shared benchmark result schema
  * (benchmarks/schema/result.schema.json). */
object Result {

  def toJson(env: Env, rows: Seq[Row], timestamp: String): String = {
    val root = new JSONObject()
    root.put("schemaVersion", "1.0")
    root.put("runner", env.runner)

    val envObj = new JSONObject()
    envObj.put("os", env.os)
    envObj.put("cpu", env.cpu)
    envObj.put("runtimeVersion", env.runtimeVersion)
    envObj.put("weaveVersion", env.weaveVersion)
    envObj.put("commit", env.commit)
    envObj.put("dwlibBuildId", env.dwlibBuildId)
    root.put("env", envObj)

    root.put("timestamp", timestamp)

    val casesArr = new JSONArray()
    rows.foreach { r =>
      val c = new JSONObject()
      c.put("id", r.id)
      c.put("metric", r.metric)
      c.put("unit", r.unit)
      c.put("iterations", r.iterations)
      val s = new JSONObject()
      s.put("min", r.stats.min)
      s.put("median", r.stats.median)
      s.put("p90", r.stats.p90)
      s.put("p99", r.stats.p99)
      s.put("mean", r.stats.mean)
      c.put("stats", s)
      casesArr.put(c)
    }
    root.put("cases", casesArr)

    root.toString(2)
  }
}
