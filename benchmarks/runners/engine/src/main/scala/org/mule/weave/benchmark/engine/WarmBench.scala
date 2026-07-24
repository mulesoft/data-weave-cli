package org.mule.weave.benchmark.engine

/** In-process metrics: warm steady-state (with a JVM JIT warmup floor) and
  * streaming throughput. Timing mirrors the Node runner: System.nanoTime -> ms. */
object WarmBench {

  val WARMUP_FLOOR: Int = 2000

  private def nowNs(): Long = System.nanoTime()
  private def msSince(startNs: Long): Double = (System.nanoTime() - startNs) / 1e6

  def runWarm(shell: EngineShell, m: Manifest, warmupCap: Option[Int] = None, iterCap: Option[Int] = None): Seq[Row] = {
    Manifest.casesForMetric(m, "warm").map { c =>
      val script = Manifest.resolveScript(m, c)
      val inputs = Manifest.resolveInputs(m, c)
      val name = EngineShell.safeName(c.id)
      val warmup = warmupCap.getOrElse(math.max(c.warmup, WARMUP_FLOOR))
      val iters = iterCap.getOrElse(c.warm)

      println(s"[warm] ${c.id}: warmup=$warmup iters=$iters")
      var i = 0
      while (i < warmup) { shell.run(script, name, inputs, new CountingOutputStream()); i += 1 }

      val samples = new Array[Double](iters)
      i = 0
      while (i < iters) {
        val start = nowNs()
        shell.run(script, name, inputs, new CountingOutputStream())
        samples(i) = msSince(start)
        i += 1
      }
      Row(c.id, "warm", "ms", Stats.computeStats(samples.toSeq), iters)
    }
  }

  /** Streaming throughput: feeds the primary input in 64KB chunks via a
    * ChunkedInputStream (mirroring the native-lib runners) and drains the
    * deferred output InputStream per iteration. MB/s is over the primary input
    * bytes. The streaming-script variant (deferred=true) is resolved via
    * Manifest.resolveStreamingScript so warm/first-run keep the base script.
    */
  def runStreaming(shell: EngineShell, m: Manifest, iterCap: Option[Int] = None): Seq[Row] = {
    Manifest.casesForMetric(m, "streaming").map { c =>
      val script = Manifest.resolveStreamingScript(m, c)
      val inputs = Manifest.resolveInputs(m, c)
      val scriptName = EngineShell.safeName(c.id)
      val primary = inputs.head
      val primaryBytes = primary.bytes.length.toLong
      val iters = iterCap.getOrElse(c.streaming)

      // Guard: if this case does NOT declare warm, warm up first so streaming isn't JIT-cold.
      // Warmup uses the deferred streaming path too, over a fresh ChunkedInputStream each time.
      if (!c.metrics.contains("warm")) {
        println(s"[streaming] ${c.id}: streaming-only, warming up first ($WARMUP_FLOOR iters)")
        var w = 0
        while (w < WARMUP_FLOOR) {
          shell.runStreaming(script, scriptName, primary.name, new ChunkedInputStream(primary.bytes, 65536), primary.mimeType, primary.charset)
          w += 1
        }
      }

      val mbps = new Array[Double](iters)
      var i = 0
      while (i < iters) {
        val in = new ChunkedInputStream(primary.bytes, 65536)
        val start = nowNs()
        shell.runStreaming(script, scriptName, primary.name, in, primary.mimeType, primary.charset)
        mbps(i) = Stats.toMBps(primaryBytes, msSince(start))
        i += 1
      }
      Row(c.id, "streaming", "MB/s", Stats.computeStats(mbps.toSeq), iters)
    }
  }
}
