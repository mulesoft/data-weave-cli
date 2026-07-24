package org.mule.weave.benchmark.engine

import org.mule.weave.v2.model.ServiceManager
import org.mule.weave.v2.model.service.CharsetProviderService
import org.mule.weave.v2.parser.ast.variables.NameIdentifier
import org.mule.weave.v2.runtime.{
  BindingValue,
  DataWeaveResult,
  DataWeaveScript,
  DataWeaveScriptingEngine,
  InputType,
  ModuleComponentsFactory,
  ParserConfiguration,
  ScriptingBindings
}
import org.mule.weave.v2.sdk.ClassLoaderWeaveResourceResolver

import java.io.{ InputStream, OutputStream }
import java.nio.charset.{ Charset, StandardCharsets }
import java.util.Properties
import scala.util.Random

/** A minimal engine harness: builds a bare DataWeaveScriptingEngine (classloader
  * resolver only) and compiles+writes a script per run(), mirroring how
  * native-cli's NativeRuntime drives the engine. Constructing this class completes
  * engine init; EngineChild prints its READY marker right after, and the parent
  * (Emit) measures cold-start as the full spawn-to-READY wall-clock. */
class EngineShell {

  EngineShell.setupEnv()

  private val engine: DataWeaveScriptingEngine = {
    val resolver = ClassLoaderWeaveResourceResolver.apply()
    new DataWeaveScriptingEngine(ModuleComponentsFactory.apply(resolver), ParserConfiguration(), new Properties())
  }

  // UTF-8 default charset service, matching NativeRuntime.createServiceManager.
  // Required so cases that don't pin a charset decode as UTF-8; per-input charsets
  // (e.g. the UTF-16 xml-to-csv case) come from the binding itself.
  private val serviceManager: ServiceManager = {
    val charsetService = new CharsetProviderService {
      override def defaultCharset(): Charset = StandardCharsets.UTF_8
    }
    val customServices: Map[Class[_], _] = Map(classOf[CharsetProviderService] -> charsetService)
    ServiceManager(customServices)
  }

  /** Compile `script` and write its output into `out`. Throws on failure. */
  def run(script: String, name: String, inputs: Seq[ResolvedInput], out: OutputStream): Unit = {
    val bindings = new ScriptingBindings()
    inputs.foreach { in =>
      val charset = Charset.forName(in.charset.getOrElse("UTF-8"))
      val bv = new BindingValue(in.bytes, Some(in.mimeType), Map.empty[String, Any], charset)
      bindings.addBinding(in.name, bv)
    }

    val config = engine.newConfig()
      .withScript(script)
      .withNameIdentifier(NameIdentifier(name))
      .withInputs(inputs.map(in => new InputType(in.name, None)).toArray)
      .withDefaultOutputType("application/json")

    val compiled: DataWeaveScript = engine.compileWith(config)
    // 3-arg write(bindings, serviceManager, target: Option[Any]) writes into `out`,
    // exactly as NativeRuntime.run does. A compile/exec failure throws here.
    compiled.write(bindings, serviceManager, Option(out))
  }

  /** Streaming variant of `run`: binds `input` as a lazy InputStream (so the
    * runtime reads it incrementally), compiles the deferred script, and drains
    * the deferred PipedInputStream result in a read loop. Returns the number of
    * output bytes drained. Throws on compile/exec failure or a non-PipedInputStream
    * result — a materialized result is rejected, so a script that forgot
    * `deferred=true` fails loudly. `inputName` must match the binding the script
    * reads (e.g. "payload"); `scriptName` is a unique compilation identifier
    * (e.g. from `safeName(caseId)`). */
  def runStreaming(script: String, scriptName: String, inputName: String, input: InputStream, inMime: String, inCharset: Option[String]): Long = {
    val bindings = new ScriptingBindings()
    val charset = Charset.forName(inCharset.getOrElse("UTF-8"))
    val bv = new BindingValue(input, Some(inMime), Map.empty[String, Any], charset)
    bindings.addBinding(inputName, bv)

    val config = engine.newConfig()
      .withScript(script)
      .withNameIdentifier(NameIdentifier(scriptName))
      .withInputs(Array(new InputType(inputName, None)))
      .withDefaultOutputType("application/json")

    val compiled: DataWeaveScript = engine.compileWith(config)
    val result: DataWeaveResult = compiled.write(bindings, serviceManager, Option.empty[Any])
    result.getContent match {
      case is: java.io.PipedInputStream =>
        try {
          val buf = new Array[Byte](65536)
          var total = 0L
          var n = is.read(buf)
          while (n > 0) { total += n; n = is.read(buf) }
          total
        } finally {
          is.close()
        }
      case other =>
        throw new RuntimeException(
          s"streaming result is not a deferred PipedInputStream (did the script declare deferred=true?): ${other.getClass.getName}")
    }
  }
}

object EngineShell {

  /** Netty init properties, adapted from NativeRuntime.setupEnv. */
  def setupEnv(): Unit = {
    System.setProperty("io.netty.processId", Math.abs(Random.nextInt()).toString)
    System.setProperty("io.netty.noUnsafe", true.toString)
  }

  /** A NameIdentifier-safe logical name derived from a case id. */
  def safeName(id: String): String = "bench_" + id.replaceAll("[^A-Za-z0-9_]", "_")
}
