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

  def runStreaming(shell: EngineShell, m: Manifest, iterCap: Option[Int] = None): Seq[Row] = {
    Manifest.casesForMetric(m, "streaming").map { c =>
      val script = Manifest.resolveScript(m, c)
      val inputs = Manifest.resolveInputs(m, c)
      val name = EngineShell.safeName(c.id)
      val primaryBytes = inputs.head.bytes.length.toLong
      val iters = iterCap.getOrElse(c.streaming)

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
