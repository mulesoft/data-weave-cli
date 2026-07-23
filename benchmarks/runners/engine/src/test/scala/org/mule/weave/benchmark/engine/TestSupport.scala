package org.mule.weave.benchmark.engine

import java.io.File

object TestSupport {
  /** Ensure the shared large input exists (small N for tests). Best-effort:
    * requires Node, which the benchmarks already depend on. */
  def ensureGeneratedInputs(corpus: File): Boolean = {
    val gen = new File(corpus, "inputs/generated/records-large.json")
    if (gen.exists()) return true
    try {
      val pb = new ProcessBuilder("node", "corpus/gen-inputs.mjs").directory(corpus.getParentFile)
      pb.environment().put("BENCH_LARGE_N", "500")
      pb.inheritIO()
      pb.start().waitFor()
      gen.exists()
    } catch {
      case _: Throwable => false
    }
  }
}
