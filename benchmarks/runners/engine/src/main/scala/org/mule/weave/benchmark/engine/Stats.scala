package org.mule.weave.benchmark.engine

/** Percentile + throughput math kept byte-for-byte compatible with
  * benchmarks/lib/stats.mjs so engine deltas compare cleanly against Node.
  * Any divergence is caught by StatsParityTest. */
object Stats {

  final case class Summary(min: Double, median: Double, p90: Double, p99: Double, mean: Double)

  /** Nearest-rank percentiles on a sorted copy; mean is the arithmetic mean. */
  def computeStats(samples: Seq[Double]): Summary = {
    if (samples.isEmpty) {
      throw new IllegalArgumentException("computeStats requires a non-empty sequence of numbers")
    }
    val sorted = samples.sorted.toVector
    val n = sorted.length
    def pct(p: Double): Double = {
      val idx = math.min(n - 1, math.max(0, math.ceil(p / 100.0 * n).toInt - 1))
      sorted(idx)
    }
    val sum = sorted.sum
    Summary(min = sorted.head, median = pct(50), p90 = pct(90), p99 = pct(99), mean = sum / n)
  }

  /** Throughput in decimal megabytes per second (1 MB = 1e6 bytes). */
  def toMBps(totalBytes: Long, elapsedMs: Double): Double = {
    if (elapsedMs <= 0) throw new IllegalArgumentException("elapsedMs must be > 0")
    totalBytes / 1e6 / (elapsedMs / 1000.0)
  }
}
