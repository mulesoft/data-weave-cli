package org.mule.weave.clinative

import org.apache.commons.io.FileUtils
import org.apache.commons.io.FilenameUtils
import org.mule.weave.v2.helper.FolderBasedTest
import org.mule.weave.v2.utils.DataWeaveVersion
import org.mule.weave.v2.version.ComponentVersion
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.io.FileFilter
import java.io.IOException
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.TimeUnit
import java.util.regex.Pattern
import java.util.zip.ZipFile
import scala.collection.JavaConverters._

class TCKCliTest extends AnyFunSpec with Matchers
  with FolderBasedTest
  with ResourceResolver
  with OSSupport {

  private val TIMEOUT: (Int, TimeUnit) = (30, TimeUnit.SECONDS)
  private val INPUT_FILE_CONFIG_PROPERTY_PATTERN = Pattern.compile("in[0-9]+-config\\.properties")
  private val OUTPUT_FILE_CONFIG_PROPERTY_PATTERN = Pattern.compile("out[0-9]*-config\\.properties")
  private val INPUT_FILE_PATTERN = Pattern.compile("in[0-9]+\\.[a-zA-Z]+")
  private val OUTPUT_FILE_PATTERN = Pattern.compile("out\\.[a-zA-Z]+")


  private val weaveVersion = System.getProperty("weaveSuiteVersion", ComponentVersion.weaveVersion)
  println(s"****** Running with weaveSuiteVersion: $weaveVersion *******")
  private val versionString: String = DataWeaveVersion(weaveVersion).toString()

  val testSuites = Seq(
    TestSuite("tck-tests", loadTestZipFile(s"weave-suites/runtime-$weaveVersion-tck.zip"))
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
            // TCK directories follow the pattern: <scenario>-out.<ext>
            // Each should contain exactly one transform.dwl
            val dwlFiles = pathname.list((_: File, name: String) => {
              "transform.dwl" == name
            })

            // Keep existing filters for unsupported scenarios
            val javaCases: Array[String] = pathname.list((_: File, name: String) => {
              name.endsWith("groovy")
            })

            val configPropertyCase = pathname.list((_: File, name: String) => {
              "config.properties" == name
            })

            accept = dwlFiles.length == 1 && isEmpty(javaCases) && isEmpty(configPropertyCase)
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
    val unsortedScenarios = for {
      testFolder <- testFolders
      output <- outputFiles(testFolder)
    } yield {
      Scenario(scenarioName(testFolder, output), testFolder, inputFiles(testFolder), new File(testFolder, "transform.dwl"), output, configProperty(testFolder))
    }
    val scenarios = unsortedScenarios.sortBy(_.name)
    scenarios.foreach {
      scenario =>
        it(scenario.name) {
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
    // Base scenarios to ignore - updated for TCK naming convention
    // TCK directories are named like: <scenario>-out.<ext>
    // Test names are derived from directory names, so ignore patterns match directory prefixes

    val baseArray =
      // Encoding issues
      Array("csv-invalid-utf8", "splitBy-regex", "splitBy-string",
            "xml-encoding-decl-near", "xml-encoding-decl-far") ++
      // Fail in java11 because of backwards compatibility
      Array("coerciones_toString", "date-coercion") ++
      // Use resources (dwl files) present in Tests but not in CLI (e.g: org::mule::weave::v2::libs::)
      Array("full-qualified-name-ref",
        "import-component-alias-lib",
        "import-lib",
        "import-lib-with-alias",
        "import-named-lib",
        "import-star",
        "lazy_metadata_definition",
        "module-singleton",
        "multipart-write-binary",
        "private_scope_directives",
        "read-binary-files",
        "underflow",
        "try",
        "urlEncodeDecode") ++
      // Uses resource name that is different on CLI than in Tests
      Array("try-recursive-call", "runtime_orElseTry") ++
      // Use readUrl from classpath
      Array("dw-binary", "read_lines") ++
      // Uses java module
      Array("java-big-decimal",
        "java-field-ref",
        "java-interop-enum",
        "java-interop-function-call",
        "runtime_run_coercionException",
        "runtime_run_fibo",
        "runtime_run_null_java",
        "sql_date_mapping",
        "write-function-with-null") ++
      // Multipart Object has empty `parts` and expects at least one part
      Array("multipart-mixed-message", "multipart-write-message",
            "multipart-write-subtype-override") ++
      // Fail pattern match on complex object
      Array("pattern-match-complex-type") ++
      // DataFormats descriptor query
      Array("runtime_dataFormatsDescriptors") ++
      // Cannot coerce Null (null) to Number
      Array("update-op") ++
      // Takes too long
      Array("array-concat", "big_intersection", "sql_date_mapping", "runtime_run") ++
      // Streaming/try-handle scenarios that take too long
      Array("is-empty-using-empty-stream",
        "streaming_binary_inside_value",
        "try-handle-array-value-with-failures",
        "try-handle-attribute-delegate-with-failures",
        "try-handle-attributes-value-with-failures",
        "try-handle-binary-value-with-failures",
        "try-handle-delegate-value-with-failures",
        "try-handle-key-value-pair-value-with-failures",
        "try-handle-materialized-object-with-failures",
        "try-handle-name-value-pair-value-with-failures",
        "try-handle-schema-property-value-with-failures",
        "try-handle-schema-value-with-failures")

    // Only 2.12.2-SNAPSHOT and 2.13.0-SNAPSHOT are supported
    // No version-specific filtering needed
    baseArray
  }


}
