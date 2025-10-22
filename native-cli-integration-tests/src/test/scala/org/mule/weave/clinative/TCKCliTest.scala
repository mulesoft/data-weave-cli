package org.mule.weave.clinative

import org.apache.commons.io.FileUtils
import org.apache.commons.io.FilenameUtils
import org.mule.weave.v2.codegen.CodeGenerator
import org.mule.weave.v2.codegen.CodeGeneratorSettings
import org.mule.weave.v2.codegen.InfixOptions
import org.mule.weave.v2.helper.FolderBasedTest
import org.mule.weave.v2.model.EvaluationContext
import org.mule.weave.v2.module.DataFormatManager
import org.mule.weave.v2.parser.MappingParser
import org.mule.weave.v2.parser.ast.header.directives.ContentType
import org.mule.weave.v2.parser.ast.header.directives.DirectiveNode
import org.mule.weave.v2.parser.ast.header.directives.OutputDirective
import org.mule.weave.v2.parser.ast.structure.StringNode
import org.mule.weave.v2.sdk.ParsingContextFactory
import org.mule.weave.v2.sdk.WeaveResourceFactory
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
    TestSuite("runtime-tests", loadTestZipFile(s"weave-suites/runtime-$weaveVersion-test.zip")),
    TestSuite("yaml-tests", loadTestZipFile(s"weave-suites/yaml-module-$weaveVersion-test.zip"))
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
            // Ignore more than one dwl file by test case
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
    val unsortedScenarios = for {
      testFolder <- testFolders
      output <- outputFiles(testFolder)
    } yield {
      Scenario(scenarioName(testFolder, output), testFolder, inputFiles(testFolder), new File(testFolder, mainTestFile), output, configProperty(testFolder))
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

          // Add transformation
          val weaveResource = WeaveResourceFactory.fromFile(scenario.transform)
          val parser = MappingParser.parse(MappingParser.parsingPhase(), weaveResource, ParsingContextFactory.createParsingContext())
          val documentNode = parser.getResult().astNode

          val headerDirectives: Seq[DirectiveNode] = documentNode.header.directives

          val maybeOutputDirective = headerDirectives.find(dn => dn.isInstanceOf[OutputDirective]).map(_.asInstanceOf[OutputDirective])

          var maybeEncoding: Option[String] = None
          var directives = headerDirectives
          implicit val ctx: EvaluationContext = EvaluationContext()
          val maybeDefaultDataFormat = DataFormatManager.byExtension(s".$outputExtension")
          val defaultDataFormat = maybeDefaultDataFormat.getOrElse(throw new IllegalArgumentException("Unable to find data-format for extension `" + outputExtension + "`"))
          val defaultMimeType = defaultDataFormat.defaultMimeType.toString()
          if (maybeOutputDirective.isEmpty) {
            val newOutputDirective = OutputDirective(None, Some(ContentType(defaultMimeType)), None, None)
            directives = directives :+ newOutputDirective
          } else {
            val outputDirective = maybeOutputDirective.get
            maybeEncoding = getEncodingFromOutputDirective(outputDirective)

            if (outputDirective.mime.isDefined) {
              val currentContentType = outputDirective.mime.get
              val maybeCurrentDataFormat = DataFormatManager.byContentType(currentContentType.mime)
              // Replace output directive if:
              // 1- declared data-format at output directive that's not exits or
              // 2- declared data-format at output directive is different from the data-format obtained by the file extension
              if (maybeCurrentDataFormat.isEmpty || maybeCurrentDataFormat.get.defaultMimeType.toString() != defaultMimeType) {
                val newOutputDirective = OutputDirective(None, Some(ContentType(defaultMimeType)), None, None)
                val index = directives.indexOf(outputDirective)
                directives = directives.take(index) ++ directives.drop(index + 1)
                directives = directives :+ newOutputDirective
                maybeEncoding = getEncodingFromOutputDirective(newOutputDirective)
              }
            }
          }

          documentNode.header.directives = directives
          val settings = CodeGeneratorSettings(InfixOptions.KEEP, alwaysInsertVersion = false, newLineBetweenFunctions = true, orderDirectives = false)
          val code = CodeGenerator.generate(documentNode, settings)
          val cliTransform = new File(scenario.testFolder, s"cli-transform-$outputExtension.dwl")

          try {
            Files.write(cliTransform.toPath, code.getBytes(StandardCharsets.UTF_8))
          } catch {
            case ioe: IOException =>
              throw ioe
          }


          args = args :+ s"--file=${cliTransform.getAbsolutePath}"
          val languageLevel = versionString
          args = args :+ "--language-level=" + languageLevel

          val (exitCode, _, error) = NativeCliITTestRunner(args).execute(TIMEOUT._1, TIMEOUT._2)

          assert(exitCode == 0, error)
          AssertionHelper.doAssert(outputPath.toFile, scenario.output, maybeEncoding)
        }
    }
  }

  private def getEncodingFromOutputDirective(outputDirective: OutputDirective): Option[String] = {
    val maybeEncodingOption = outputDirective.options.flatMap(opts => {
      opts.find(opt => {
        "encoding" == opt.name.name
      })
    })
    maybeEncodingOption.map(d => d.value.asInstanceOf[StringNode].literalValue)
  }

  override def ignoreTests(): Array[String] = {
    // Encoding issues
    val baseArray = Array("csv-invalid-utf8", "splitBy-regex", "splitBy-string", "xml-encoding-decl-near", "xml-encoding-decl-far") ++
      // Fail in java11 because broken backwards
      Array("coerciones_toString", "date-coercion") ++
      // Use resources (dwl files) that is present in the Tests but not in Cli (e.g: org::mule::weave::v2::libs::)
      Array("full-qualified-name-ref",
        "import-component-alias-lib",
        "import-lib",
        "import-lib-with-alias",
        "import-named-lib",
        "import-star",
        "lazy_metadata_definition",
        "module-singleton",
        "multipart-write-binary",
        "read-binary-files",
        "underflow",
        "try",
        "urlEncodeDecode") ++
      // Uses resource name that is different on Cli than in the Tests
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
        "write-function-with-null"
      ) ++
      // Multipart Object has empty `parts` and expects at least one part
      Array("multipart-mixed-message", "multipart-write-message", "multipart-write-subtype-override") ++
      // Fail pattern match on complex object
      Array("pattern-match-complex-type") ++
      // DataFormats
      Array("runtime_dataFormatsDescriptors") ++
      // Cannot coerce Null (null) to Number
      Array("update-op") ++
      // Take too long time
      Array("array-concat") ++
      Array("big_intersection") ++
      Array("sql_date_mapping") ++
      Array("runtime_run") ++
      Array("streaming_binary_inside_value",
        "try-handle-array-value-with-failures",
        "try-handle-attribute-delegate-with-failures",
        "try-handle-attributes-value-with-failures",
        "try-handle-binary-value-with-failures",
        "try-handle-delegate-value-with-failures",
        "try-handle-key-value-pair-value-with-failures",
        "try-handle-materialized-object-with-failures",
        "try-handle-name-value-pair-value-with-failures",
        "try-handle-schema-property-value-with-failures",
        "try-handle-schema-value-with-failures",
        "try-handle-lazy-values-with-failures",
        "math-toRadians",
      )

    val testToIgnore = if (versionString == "2.4") {
      baseArray ++
        // A change to json streaming in 2.5.0 breaks this test
        Array("default_with_extended_null_type") ++
        // Change in validations in 2.5.0 breaks these tests
        Array("logical-and",
          "logical-or"
        ) ++
        Array("coerciones_toBinary") ++
        // 2.5.0 dwl now prints metadata breaking these tests
        Array("dfl-inline-default-namespace",
          "dfl-inline-namespace",
          "dfl-maxCollectionSize",
          "dfl-overwrite-namespace",
          "multipart-base64-to-multipart",
          "xml-nill-multiple-attributes-nested",
          "xml-nill-multiple-attributes",
          "read_scalar_values"
        ) ++
        // A change of positions on dw::Core 2.5.0 breaks this test
        Array(
          "runtime_run_unhandled_compilation_exception"
        ) ++
        Array("as-operator",
          "type-equality"
        ) ++
        Array("xml_doctype", "stringutils_unwrap", "weave_ast_module")
    } else if (versionString == "2.5") {
      baseArray ++
        Array("xml_doctype", "stringutils_unwrap")
    } else if (versionString == "2.6") {
      baseArray ++
        Array("weave_ast_module")
    } else if (versionString == "2.7") {
      baseArray ++
        Array("weave_ast_module")
    } else {
      baseArray
    }
    testToIgnore
  }


}
