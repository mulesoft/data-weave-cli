package org.mule.weave.benchmark.engine

import java.io.File

/** Fresh-process worker for one case. Prints a "READY" marker the instant the
  * engine is initialized, then a JSON line with the in-process first-run timing.
  * The PARENT (Emit) measures cold-start as wall-clock from spawn to the READY
  * marker, so the honest JVM cold path (process launch + classload + engine init)
  * is all included — not just the in-process `new EngineShell()` call. */
object EngineChild {

  private def nowNs(): Long = System.nanoTime()
  private def msSince(startNs: Long): Double = (System.nanoTime() - startNs) / 1e6

  def main(args: Array[String]): Unit = {
    val corpusDir = new File(args(0))
    val caseId = args(1)

    val manifest = Manifest.load(corpusDir)
    val c = manifest.cases.find(_.id == caseId).getOrElse(sys.error(s"unknown case: $caseId"))
    val script = Manifest.resolveScript(manifest, c)
    val inputs = Manifest.resolveInputs(manifest, c)

    val shell = new EngineShell()
    // Engine is ready: flush the marker now so the parent's clock stops here.
    // Emit merges stderr into stdout, so the parent tolerates other lines; this
    // exact "READY" line is the cold-start boundary.
    println("READY")
    System.out.flush()

    val runStart = nowNs()
    shell.run(script, EngineShell.safeName(caseId), inputs, new CountingOutputStream())
    val firstRunMs = msSince(runStart)

    // Single JSON line on stdout; Emit reads the last JSON line.
    println(s"""{"firstRunMs":$firstRunMs}""")
  }
}
