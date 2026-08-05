package org.mule.weave.clinative

import org.apache.commons.io.FileUtils
import org.apache.commons.io.FilenameUtils
import org.mule.weave.v2.helper.FolderBasedTest
import org.mule.weave.v2.utils.DataWeaveVersion
import org.mule.weave.v2.version.ComponentVersion
import org.scalatest.Tag
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.io.FileFilter
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.TimeUnit
import java.util.regex.Pattern
import java.util.zip.ZipFile
import scala.collection.JavaConverters._

// TCK conformance scenarios are tagged so CI can exclude them on non-master
// (PR) builds via -PskipTCKTests=true, matching the master-only Node TCK lane.
object TckConformance extends Tag("org.mule.weave.clinative.TckConformance")

class TCKCliTest extends AnyFunSpec with Matchers
  with FolderBasedTest
  with ResourceResolver
  with OSSupport {

  private val TIMEOUT: (Int, TimeUnit) = (30, TimeUnit.SECONDS)
  private val INPUT_FILE_CONFIG_PROPERTY_PATTERN = Pattern.compile("in[0-9]+-config\\.properties")
  private val OUTPUT_FILE_CONFIG_PROPERTY_PATTERN = Pattern.compile("out[0-9]*-config\\.properties")
  private val INPUT_FILE_PATTERN = Pattern.compile("in[0-9]+\\.[a-zA-Z]+")
  private val OUTPUT_FILE_PATTERN = Pattern.compile("out\\.[a-zA-Z]+")

  private val weaveVersion = ComponentVersion.weaveVersion
  println(s"****** Running with weaveSuiteVersion: $weaveVersion *******")
  private val versionString: String = DataWeaveVersion(weaveVersion).toString()
  private val weaveTCKVersion = System.getProperty("weaveSuiteVersion", ComponentVersion.weaveTCKVersion)

  private val testSuites = Seq(
    TestSuite("runtime-tck-tests", loadTestZipFile(s"weave-suites/runtime-$weaveTCKVersion-tck.zip")),
    TestSuite("yaml-tck-tests", loadTestZipFile(s"weave-suites/yaml-module-$weaveTCKVersion-tck.zip")),
    TestSuite("core-modules-tck-tests", loadTestZipFile(s"weave-suites/core-modules-$weaveTCKVersion-tck.zip"))
  )

  private def loadTestZipFile(testSuiteExample: String): File = {
    println("loadTestZipFile" + testSuiteExample)
    val url = getResource(testSuiteExample)
    val connection = url.openConnection
    val zipFile = new File(connection.getURL.toURI)
    zipFile
  }

  println("NativeCliRuntimeTest -> " + testSuites.mkString(","))

  testSuites.foreach {
    testSuite => {
      val wd = Files.createTempDirectory(testSuite.name).toFile
      // Unzip the jar
      if (wd.exists) {
        FileUtils.deleteDirectory(wd)
      }
      wd.mkdirs
      extractArchive(testSuite.zipFile.toPath, wd.toPath)
      describe(testSuite.name) {
        runTestSuite(wd)
      }
    }
  }

  private def extractArchive(archiveFile: Path, destPath: Path): Unit = {
    Files.createDirectories(destPath)
    val archive = new ZipFile(archiveFile.toFile)
    try {
      for (entry <- archive.entries().asScala) {
        val entryDest = destPath.resolve(entry.getName)
        if (entry.isDirectory) {
          Files.createDirectory(entryDest)
        } else {
          Files.copy(archive.getInputStream(entry), entryDest)
        }
      }
    } finally {
      if (archive != null) {
        archive.close()
      }
    }
    println(s"Extract content from: $archiveFile at $destPath")
  }

  private def runTestSuite(testsSuiteFolder: File): Unit = {

    def isEmpty(source: Array[String]): Boolean = {
      source == null || source.isEmpty
    }

    val testFolders = testsSuiteFolder.listFiles(new FileFilter {
      override def accept(pathname: File): Boolean = {
        var accept = false
        if (acceptScenario(pathname)) {
          if (pathname.isDirectory && !pathname.getName.endsWith("wip")) {
            // Ignore more than one dwl file by test case (a case may bundle
            // imported modules; those are not runnable standalone here)
            val dwlFiles = pathname.list((_: File, name: String) => {
              val extension = FilenameUtils.getExtension(name)
              val isInput = INPUT_FILE_PATTERN.matcher(name).matches()
              val isOutput = OUTPUT_FILE_PATTERN.matcher(name).matches()
              "dwl" == extension && !isInput && !isOutput
            })

            // Ignore test case with inX-config.properties or outX-config.properties
            val inputOrOutputConfigProperties: Array[String] = pathname.list((_: File, name: String) => {
              val isInput = INPUT_FILE_CONFIG_PROPERTY_PATTERN.matcher(name).matches()
              val isOutput = OUTPUT_FILE_CONFIG_PROPERTY_PATTERN.matcher(name).matches()
              isInput || isOutput
            })

            // Ignore java use cases for now until we resolve classpath
            val javaCases: Array[String] = pathname.list((_: File, name: String) => {
              name.endsWith("groovy")
            })

            // Ignore config.properties test cases
            val configPropertyCase = pathname.list((_: File, name: String) => {
              "config.properties" == name
            })

            accept = dwlFiles.length == 1 && isEmpty(inputOrOutputConfigProperties) && isEmpty(javaCases) && isEmpty(configPropertyCase)
          }
        }
        accept
      }
    })
    if (testFolders != null) {
      runTestCase(testFolders)
    }
  }


  def runTestCase(testFolders: Array[File]): Unit = {
    val unsortedScenarios = testFolders.map(testFolder => {
      val output = testFolder.listFiles.find(f => isOutput(f)).orNull
      Scenario(scenarioName(testFolder, output), testFolder, inputFiles(testFolder), new File(testFolder, mainTestFile), output, configProperty(testFolder))
    })
    val scenarios = unsortedScenarios.sortBy(_.name)
    scenarios.foreach {
      scenario =>
        it(scenario.name, TckConformance) {
          var args = Array("run")

          // Add inputs
          scenario.inputs.foreach(f => {
            val name = FilenameUtils.getBaseName(f.getName)
            args = args :+ "-i"
            args = args :+ (name + s"=${f.getAbsolutePath}")

          })

          // Add output
          val outputExtension = FilenameUtils.getExtension(scenario.output.getName)
          val outputPath = Path.of(scenario.testFolder.getPath, s"cli-out.$outputExtension")
          args = args :+ s"--output=${outputPath.toString}"

          // Use transform.dwl directly - it already has the correct output directive
          val transformFile = new File(scenario.testFolder, "transform.dwl")
          args = args :+ s"--file=${transformFile.getAbsolutePath}"

          val languageLevel = versionString
          args = args :+ "--language-level=" + languageLevel

          val (exitCode, _, error) = NativeCliITTestRunner(args).execute(TIMEOUT._1, TIMEOUT._2)

          assert(exitCode == 0, error)

          // Read encoding from sidecar file if present
          val encodingFile = new File(scenario.testFolder, "encoding")
          val maybeEncoding: Option[String] = if (encodingFile.exists()) {
            Some(new String(Files.readAllBytes(encodingFile.toPath), StandardCharsets.UTF_8).trim)
          } else {
            None
          }

          AssertionHelper.doAssert(outputPath.toFile, scenario.output, maybeEncoding)
        }
    }
  }

  override def ignoreTests(): Array[String] = {
    // Scenarios to ignore. acceptScenario() matches these against the scenario
    // directory name, which in the TCK is "<scenario>-out.<ext>", so every
    // entry must be the FULL directory name (not the bare scenario name).
    // Only 2.12.2-SNAPSHOT and 2.13.0-SNAPSHOT are supported, so there is no
    // version-conditional handling.

    // Encoding issues
    Array("csv-invalid-utf8-out.csv", "splitBy-regex-out.json", "splitBy-string-out.json") ++
      // Fail in java11 because of broken backwards compatibility
      Array("coerciones_toString-out.json", "date-coercion-out.json") ++
      // Use resources (dwl files) present in the Tests but not in the CLI (e.g. org::mule::weave::v2::libs::)
      Array("full-qualified-name-ref-out.json",
        "import-component-alias-lib-out.json",
        "import-lib-out.json",
        "import-lib-with-alias-out.json",
        "import-named-lib-out.json",
        "import-star-out.json",
        "module-singleton-out.json",
        "private_scope_directives-out.xml",
        "underflow-out.json",
        "try-out.json",
        "urlEncodeDecode-out.json") ++
      // Uses resource name that is different on the CLI than in the Tests
      Array("try-recursive-call-out.json", "runtime_orElseTry-out.json") ++
      // Use readUrl from classpath
      Array("dw-binary-out.dwl", "read_lines-out.json") ++
      // Uses java module
      Array("java-big-decimal-out.xml",
        "java-field-ref-out.json",
        "java-interop-enum-out.json",
        "java-interop-function-call-out.json",
        "java_epoch_bridge-out.json",
        "runtime_run_coercionException-out.json",
        "runtime_run_fibo-out.json",
        "runtime_run_null_java-out.json",
        "sql_date_mapping-out.json",
        "write-function-with-null-out.xml") ++
      // Multipart Object has empty `parts` and expects at least one part / binary parts
      Array("multipart-mixed-message-out.multipart",
        "multipart-write-message-out.multipart",
        "multipart-write-subtype-override-out.multipart",
        "multipart-write-binary-out.json") ++
      // Reads binary files from classpath
      Array("read-binary-files-out.bin") ++
      // Fail pattern match on complex object
      Array("pattern-match-complex-type-out.json") ++
      // DataFormats descriptor query
      Array("runtime_dataFormatsDescriptors-out.json") ++
      // Cannot coerce Null (null) to Number
      Array("update-op-out.dwl") ++
      // Takes too long (fast locally but exceeds the 30s harness timeout on CI runners)
      Array("array-concat-out.json", "big_intersection-out.json", "runtime_run-out.json") ++
      // Streaming/try-handle scenarios that take too long
      Array("is-empty-using-empty-stream-out.json",
        "streaming_binary_inside_value-out.json",
        "try-handle-array-value-with-failures-out.json",
        "try-handle-attribute-delegate-with-failures-out.json",
        "try-handle-attributes-value-with-failures-out.json",
        "try-handle-binary-value-with-failures-out.json",
        "try-handle-delegate-value-with-failures-out.json",
        "try-handle-key-value-pair-value-with-failures-out.json",
        "try-handle-materialized-object-with-failures-out.json",
        "try-handle-name-value-pair-value-with-failures-out.json",
        "try-handle-schema-property-value-with-failures-out.json",
        "try-handle-schema-value-with-failures-out.json")
  }
}
