package org.mule.weave.benchmark.engine

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import scala.util.control.NonFatal

final case class Env(
  runner: String,
  os: String,
  cpu: String,
  runtimeVersion: String,
  weaveVersion: String,
  commit: String,
  dwlibBuildId: String)

object EnvStamp {

  def gather(repoRoot: File): Env = Env(
    runner = "engine",
    os = s"${System.getProperty("os.name")}-${System.getProperty("os.arch")}",
    cpu = cpuModel(),
    runtimeVersion = "jvm " + System.getProperty("java.version"),
    weaveVersion = readWeaveVersion(repoRoot),
    commit = gitCommit(repoRoot),
    dwlibBuildId = "n/a-engine")

  private def readWeaveVersion(repoRoot: File): String = {
    val txt = new String(Files.readAllBytes(new File(repoRoot, "gradle.properties").toPath), StandardCharsets.UTF_8)
    """(?m)^weaveVersion=(.+)$""".r.findFirstMatchIn(txt).map(_.group(1).trim)
      .getOrElse(throw new RuntimeException("weaveVersion not found in gradle.properties"))
  }

  private def gitCommit(repoRoot: File): String =
    exec(Seq("git", "rev-parse", "--short", "HEAD"), repoRoot).getOrElse("unknown")

  private def cpuModel(): String = {
    val os = System.getProperty("os.name").toLowerCase
    val fromShell =
      if (os.contains("mac")) exec(Seq("sysctl", "-n", "machdep.cpu.brand_string"), new File("."))
      else if (os.contains("linux"))
        exec(Seq("bash", "-c", "grep -m1 'model name' /proc/cpuinfo | cut -d: -f2"), new File("."))
      else None
    fromShell.map(_.trim).filter(_.nonEmpty).getOrElse(System.getProperty("os.arch"))
  }

  private def exec(cmd: Seq[String], dir: File): Option[String] =
    try {
      val p = new ProcessBuilder(cmd: _*).directory(dir).start()
      val out = scala.io.Source.fromInputStream(p.getInputStream).mkString.trim
      if (p.waitFor() == 0 && out.nonEmpty) Some(out) else None
    } catch { case NonFatal(_) => None }
}
