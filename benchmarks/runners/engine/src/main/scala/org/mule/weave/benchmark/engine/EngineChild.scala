package org.mule.weave.benchmark.engine

import java.io.File

/** Fresh-process worker. Measures a cold engine init + a cold (first) compile+exec
  * for one case, then prints a single JSON line. Spawned by Emit — the honest JVM
  * cold path (process launch + classload + engine init + first compile). */
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

    val initStart = nowNs()
    val shell = new EngineShell()
    val initMs = msSince(initStart)

    val runStart = nowNs()
    shell.run(script, EngineShell.safeName(caseId), inputs, new CountingOutputStream())
    val firstRunMs = msSince(runStart)

    // Single JSON line on stdout; Emit reads the last line.
    println(s"""{"initMs":$initMs,"firstRunMs":$firstRunMs}""")
  }
}
