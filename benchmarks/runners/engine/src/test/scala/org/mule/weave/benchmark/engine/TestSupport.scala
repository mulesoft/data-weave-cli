package org.mule.weave.benchmark.engine

import java.io.File

object TestSupport {
  /** Ensure the shared large input exists (small N for tests). Best-effort:
    * requires Node, which the benchmarks already depend on.
    *
    * `synchronized` because the ScalaTest runner executes suites concurrently in
    * one JVM: without it, two suites both observe the file missing and race to
    * regenerate it, and a third reader can catch the non-atomic write mid-flight
    * (NoSuchFileException / truncated JSON). Serializing check-then-generate here
    * means the first caller writes it fully (node exits before `waitFor` returns)
    * and every later caller sees a complete file. */
  def ensureGeneratedInputs(corpus: File): Boolean = synchronized {
    val gen = new File(corpus, "inputs/generated/records-large.json")
    if (gen.isFile && gen.length() > 0) return true
    try {
      val pb = new ProcessBuilder("node", "corpus/gen-inputs.mjs").directory(corpus.getParentFile)
      pb.environment().put("BENCH_LARGE_N", "500")
      pb.inheritIO()
      pb.start().waitFor()
      gen.isFile && gen.length() > 0
    } catch {
      case _: Throwable => false
    }
  }
}
