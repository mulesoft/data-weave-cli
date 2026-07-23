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

  /** Streaming throughput: measures MB/s over full compile+write per iteration.
    *
    * Methodology asymmetry: the engine measures throughput over a full shell.run
    * (compile+write) of the whole input, whereas the Node runner's streaming uses an
    * incrementally-chunked runTransform. The streaming delta across runners is NOT
    * strictly like-for-like.
    *
    * Engine streaming relies on JIT warmth from runWarm having run first on the same
    * shell (every current streaming case also declares warm). For future streaming-only
    * cases that do NOT also declare warm, a lightweight warmup runs first.
    */
  def runStreaming(shell: EngineShell, m: Manifest, iterCap: Option[Int] = None): Seq[Row] = {
    Manifest.casesForMetric(m, "streaming").map { c =>
      val script = Manifest.resolveScript(m, c)
      val inputs = Manifest.resolveInputs(m, c)
      val name = EngineShell.safeName(c.id)
      val primaryBytes = inputs.head.bytes.length.toLong
      val iters = iterCap.getOrElse(c.streaming)

      // Guard: if this case does NOT declare warm, warmup first so streaming isn't JIT-cold.
      if (!c.metrics.contains("warm")) {
        println(s"[streaming] ${c.id}: streaming-only, warming up first ($WARMUP_FLOOR iters)")
        var w = 0
        while (w < WARMUP_FLOOR) { shell.run(script, name, inputs, new CountingOutputStream()); w += 1 }
      }

      val mbps = new Array[Double](iters)
      var i = 0
      while (i < iters) {
        val start = nowNs()
        shell.run(script, name, inputs, new CountingOutputStream())
        mbps(i) = Stats.toMBps(primaryBytes, msSince(start))
        i += 1
      }
      Row(c.id, "streaming", "MB/s", Stats.computeStats(mbps.toSeq), iters)
    }
  }
}
