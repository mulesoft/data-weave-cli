package org.mule.weave.benchmark.engine

/** One flat (case, metric) result row — the schema's unit of output. */
final case class Row(id: String, metric: String, unit: String, stats: Stats.Summary, iterations: Int)
