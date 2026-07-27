# Migration Plan: DataWeave CLI Integration Tests to TCK Artifact

> For agentic workers: implement this plan task-by-task with red-green-refactor discipline. One task, one commit. Never batch.

## Context

The DataWeave runtime now publishes a dedicated TCK artifact (`tck@zip`) in versions 2.13.0-SNAPSHOT and 2.12.2-SNAPSHOT. This artifact contains pre-expanded, self-contained test scenarios with output directives already baked in, eliminating the need for AST manipulation in the CLI integration tests.

**Current state:**
- `native-cli-integration-tests` consumes three `test@zip` artifacts (runtime, yaml-module, core-modules)
- `TCKCliTest.scala` performs AST manipulation to rewrite output directives and extract encoding from directives
- CI runs regression tests against 2.9.8 and 2.10.0 (no longer supported)

**Target state:**
- Use single `tck@zip` artifact from org.mule.weave
- Remove all AST manipulation code (CodeGenerator, MappingParser, output directive rewriting)
- Read encoding from `encoding` sidecar file when present
- Support only 2.13.0-SNAPSHOT and 2.12.2-SNAPSHOT
- Update CI regression tests to these versions

## TCK Artifact Structure

Each expanded scenario is a self-contained directory named `<scenario>-out.<ext>/`:
- `transform.dwl` - transformation with output directive already injected
- `in0.<ext>`, `in1.<ext>`, ... - input files
- `out.<ext>` - expected output
- `encoding` (optional) - text file containing charset (e.g., "UTF-8", "UTF-16") when the output directive specifies encoding
- `MyLib.dwl`, `data.txt`, ... - imported modules and resources referenced by the transform

## File Structure

```
native-cli-integration-tests/
├── build.gradle                           # Modify: switch to tck@zip dependency
└── src/test/scala/org/mule/weave/clinative/
    └── TCKCliTest.scala                   # Major refactor: remove AST code, read encoding file
```

## Tasks

### Task 1: Update build.gradle to use tck@zip artifact

**File:** `native-cli-integration-tests/build.gradle`

**Changes:**
1. Replace the three test@zip dependencies with single tck@zip:
```gradle
dependencies {
    api(project(":native-cli"))

    weaveSuite "org.mule.weave:runtime:${weaveTestSuiteVersion}:tck@zip"
    
    testRuntimeOnly 'com.vladsch.flexmark:flexmark-all:0.64.8'
    testImplementation group: 'org.scalatest', name: 'scalatest_2.12', version: '3.2.19'
    
    // Remove parser dependency - no longer needed for AST manipulation
    // testImplementation "org.mule.weave:parser:${weaveVersion}"
    testImplementation "org.mule.weave:test-helpers:${weaveVersion}"
    testImplementation 'commons-io:commons-io:2.11.0'
    testImplementation 'com.sun.mail:jakarta.mail:1.6.4'
    testImplementation 'xerces:xercesImpl:2.12.1'
    testImplementation 'xalan:xalan:2.7.2'
    testImplementation 'commons-beanutils:commons-beanutils:1.9.4'
    implementation group: 'com.sun.mail', name: 'jakarta.mail', version: '2.0.1'
}
```

2. Update downloadTestSuites task to extract the tck@zip into the expected location:
```gradle
tasks.register('downloadTestSuites', Copy) {
    from configurations.weaveSuite
    into "$projectDir/build/resources/weave-suites"
    // Rename the downloaded tck zip for easier reference
    rename { filename ->
        if (filename.endsWith(".zip")) {
            "runtime-${weaveTestSuiteVersion}-tck.zip"
        } else {
            filename
        }
    }
}
```

**Commit message:**
```
refactor(tck): switch to tck@zip artifact in build.gradle

Replace three test@zip artifacts with single runtime tck@zip.
Remove parser dependency (no longer needed for AST manipulation).

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>
```

### Task 2: Update gradle.properties for supported versions

**File:** `gradle.properties`

**Changes:**
Update version properties to 2.13.0-SNAPSHOT (or 2.12.2-SNAPSHOT for initial testing):
```properties
weaveVersion=2.13.0-SNAPSHOT
weaveTestSuiteVersion=2.13.0-SNAPSHOT
weaveSuiteVersion=2.13.0-SNAPSHOT
```

**Verification:**
- Ensure versions are aligned
- Document that both 2.13.0-SNAPSHOT and 2.12.2-SNAPSHOT are supported

**Commit message:**
```
chore(tck): update to DataWeave 2.13.0-SNAPSHOT

Set supported versions to 2.13.0-SNAPSHOT and 2.12.2-SNAPSHOT.
Drop support for versions < 2.12.

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>
```

### Task 3: Remove AST manipulation imports and dependencies from TCKCliTest.scala

**File:** `native-cli-integration-tests/src/test/scala/org/mule/weave/clinative/TCKCliTest.scala`

**Remove these imports:**
```scala
import org.mule.weave.v2.codegen.CodeGenerator
import org.mule.weave.v2.codegen.CodeGeneratorSettings
import org.mule.weave.v2.codegen.InfixOptions
import org.mule.weave.v2.model.EvaluationContext
import org.mule.weave.v2.module.DataFormatManager
import org.mule.weave.v2.parser.MappingParser
import org.mule.weave.v2.parser.ast.header.directives.ContentType
import org.mule.weave.v2.parser.ast.header.directives.DirectiveNode
import org.mule.weave.v2.parser.ast.header.directives.OutputDirective
import org.mule.weave.v2.parser.ast.structure.StringNode
import org.mule.weave.v2.sdk.ParsingContextFactory
import org.mule.weave.v2.sdk.WeaveResourceFactory
```

**Keep these imports:**
```scala
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
```

**Commit message:**
```
refactor(tck): remove AST manipulation imports

Remove parser, codegen, and directive-related imports.
TCK artifact provides pre-expanded scenarios.

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>
```

### Task 4: Update test suite loading to use tck@zip

**File:** `native-cli-integration-tests/src/test/scala/org/mule/weave/clinative/TCKCliTest.scala`

**Replace:**
```scala
val testSuites = Seq(
  TestSuite("runtime-tests", loadTestZipFile(s"weave-suites/runtime-$weaveVersion-test.zip")),
  TestSuite("yaml-tests", loadTestZipFile(s"weave-suites/yaml-module-$weaveVersion-test.zip")),
  TestSuite("core-modules-tests", loadTestZipFile(s"weave-suites/core-modules-$weaveVersion-test.zip"))
)
```

**With:**
```scala
val testSuites = Seq(
  TestSuite("tck-tests", loadTestZipFile(s"weave-suites/runtime-$weaveVersion-tck.zip"))
)
```

**Commit message:**
```
refactor(tck): load single tck@zip artifact

Replace three test@zip artifacts with single tck@zip.
All TCK scenarios are now in one archive.

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>
```

### Task 5: Simplify runTestSuite to accept pre-expanded scenarios

**File:** `native-cli-integration-tests/src/test/scala/org/mule/weave/clinative/TCKCliTest.scala`

**Changes:**

The TCK artifact contains directories like `as-operator-out.json/` where each directory is already a complete scenario. Update `runTestSuite` to recognize this structure:

1. Change directory filter logic to accept directories matching `*-out.*` pattern (TCK naming convention)
2. Remove config.properties filtering (TCK doesn't use this anymore)
3. Keep the filtering for groovy/java cases and config properties

**Current filtering logic:**
```scala
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
```

**Replace with simpler TCK-aware logic:**
```scala
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
```

**Commit message:**
```
refactor(tck): simplify directory filtering for TCK structure

TCK scenarios contain transform.dwl (not arbitrary *.dwl).
Remove config.properties filtering (not in TCK).
Keep groovy/java filters.

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>
```

### Task 6: Refactor runTestCase to use transform.dwl directly and read encoding file

**File:** `native-cli-integration-tests/src/test/scala/org/mule/weave/clinative/TCKCliTest.scala`

**Changes:**

This is the major refactor. The current implementation:
1. Parses the main DWL file with `MappingParser`
2. Manipulates the AST to inject/rewrite output directives
3. Extracts encoding from the output directive
4. Generates new code with `CodeGenerator`
5. Writes a rewritten `cli-transform-*.dwl`

The new implementation should:
1. Use `transform.dwl` directly (no parsing, no AST manipulation)
2. Read encoding from the `encoding` file if present
3. Pass `transform.dwl` directly to the CLI

**Replace the entire test scenario execution block inside the `it(scenario.name)` from line 159-234:**

```scala
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
```

**Also remove:**
- The entire `getEncodingFromOutputDirective` method (lines 238-245)
- Remove `implicit val ctx: EvaluationContext = EvaluationContext()` from line 186 (no longer needed)

**Update:**
- Change `mainTestFile` reference in Scenario construction (line 154) from "main test file" to "transform.dwl":
```scala
Scenario(scenarioName(testFolder, output), testFolder, inputFiles(testFolder), new File(testFolder, "transform.dwl"), output, configProperty(testFolder))
```

**Commit message:**
```
refactor(tck): remove AST manipulation and use transform.dwl directly

TCK scenarios include pre-expanded transform.dwl with output
directives already baked in. Read encoding from sidecar file
instead of extracting from AST. Remove CodeGenerator,
MappingParser, and getEncodingFromOutputDirective method.

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>
```

### Task 7: Revise ignoreTests() for TCK scenario names and drop old version handling

**File:** `native-cli-integration-tests/src/test/scala/org/mule/weave/clinative/TCKCliTest.scala`

**Changes:**

The TCK has different scenario naming (directories like `as-operator-out.json/`). The test name will be derived from the directory name. We need to:
1. Remove all version-conditional skips for 2.4/2.5/2.6/2.7/2.9/2.10 (no longer supported)
2. Update test case names to match TCK naming convention
3. Keep only the base ignore list that's still relevant

**Replace the entire `ignoreTests()` method (lines 247-363):**

```scala
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
```

**Note:** The test names in the ignore list should match the scenario names derived from directory names. We may need to verify actual TCK directory names and adjust the ignore list after running tests once.

**Commit message:**
```
refactor(tck): simplify ignoreTests for 2.12+ only

Remove version-conditional skips for 2.4-2.10 (no longer supported).
Keep base ignore list for known incompatible scenarios.
TCK naming convention: scenarios match directory names.

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>
```

### Task 8: Update CI workflow to run regression tests for 2.13.0-SNAPSHOT and 2.12.2-SNAPSHOT

**File:** `.github/workflows/main.yml`

**Replace lines 48-58:**
```yaml
# Run regression tests (only on master branch to save CI time on PRs)
- name: Run regression test 2.9.8
  if: github.ref == 'refs/heads/master'
  run: |
    ./gradlew --stacktrace -PweaveTestSuiteVersion=2.9.8 -DweaveSuiteVersion=2.9.8 native-cli-integration-tests:test
  shell: bash
- name: Run regression test 2.10
  if: github.ref == 'refs/heads/master'
  run: |
    ./gradlew --stacktrace -PweaveTestSuiteVersion=2.10.0 -DweaveSuiteVersion=2.10.0 native-cli-integration-tests:test
  shell: bash
```

**With:**
```yaml
# Run regression tests (only on master branch to save CI time on PRs)
# TCK artifact available in 2.12.2-SNAPSHOT and 2.13.0-SNAPSHOT
- name: Run regression test 2.12.2-SNAPSHOT
  if: github.ref == 'refs/heads/master'
  run: |
    ./gradlew --stacktrace -PweaveTestSuiteVersion=2.12.2-SNAPSHOT -DweaveSuiteVersion=2.12.2-SNAPSHOT native-cli-integration-tests:test
  shell: bash
- name: Run regression test 2.13.0-SNAPSHOT
  if: github.ref == 'refs/heads/master'
  run: |
    ./gradlew --stacktrace -PweaveTestSuiteVersion=2.13.0-SNAPSHOT -DweaveSuiteVersion=2.13.0-SNAPSHOT native-cli-integration-tests:test
  shell: bash
```

**Commit message:**
```
ci(tck): update regression tests to 2.12.2-SNAPSHOT and 2.13.0-SNAPSHOT

Replace 2.9.8 and 2.10.0 regression tests with TCK-enabled versions.
Keep master-only gating to save CI time on PRs.

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>
```

### Task 9: Verify build compiles

**Action:**
Run `./gradlew native-cli-integration-tests:compileTestScala` to ensure the refactored code compiles.

**Expected result:**
- No compilation errors
- All imports resolved
- Scala syntax valid

**If compilation fails:**
- Fix any remaining references to removed imports
- Ensure all method signatures are correct
- Verify variable types

**Commit message (if fixes needed):**
```
fix(tck): resolve compilation errors

Address remaining AST manipulation references.
Ensure all imports and types are correct.

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>
```

### Task 10: Document the migration in plan file

**Action:**
Add a section to this plan documenting what was changed and why.

**No commit** - this is documentation within the plan itself.

### Task 11: Commit the plan to the worktree's current branch

**File:** `docs/plans/2026-07-27-migrate-to-tck-artifact.md`

**Verification before commit:**
- Plan file exists at correct path
- All tasks documented with code examples
- Commit messages follow conventional commits format

**Action:**
```bash
git add docs/plans/2026-07-27-migrate-to-tck-artifact.md
git commit -m "docs(tck): add migration plan for TCK artifact

Plan migration from test@zip to tck@zip artifacts.
Remove AST manipulation, use pre-expanded scenarios.
Support only 2.12.2-SNAPSHOT and 2.13.0-SNAPSHOT.

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

**Verification after commit:**
- `git log --not master --oneline` shows the commit
- `git status --porcelain` is empty
- Current branch matches worktree starting branch

## Summary of Changes

### Dependencies
- **Before:** Three test@zip artifacts (runtime, yaml-module, core-modules)
- **After:** Single tck@zip artifact from org.mule.weave:runtime

### Code Changes
- **Removed:** AST manipulation (CodeGenerator, MappingParser, directive rewriting)
- **Removed:** Encoding extraction from output directives
- **Added:** Encoding reading from sidecar `encoding` file
- **Changed:** Use `transform.dwl` directly (no rewriting)
- **Simplified:** Directory filtering for TCK structure
- **Simplified:** ignoreTests() - removed version-conditional logic

### Version Support
- **Dropped:** 2.4, 2.5, 2.6, 2.7, 2.9, 2.10
- **Added:** 2.12.2-SNAPSHOT, 2.13.0-SNAPSHOT

### CI Changes
- **Before:** Regression tests for 2.9.8 and 2.10.0
- **After:** Regression tests for 2.12.2-SNAPSHOT and 2.13.0-SNAPSHOT

## Testing Strategy

After implementation:
1. Verify compilation: `./gradlew native-cli-integration-tests:compileTestScala`
2. Build native CLI: `./gradlew native-cli:nativeCompile` (slow, ~several minutes)
3. Run integration tests: `./gradlew native-cli-integration-tests:test`
4. Test with both versions:
   - `./gradlew -PweaveTestSuiteVersion=2.12.2-SNAPSHOT -DweaveSuiteVersion=2.12.2-SNAPSHOT native-cli-integration-tests:test`
   - `./gradlew -PweaveTestSuiteVersion=2.13.0-SNAPSHOT -DweaveSuiteVersion=2.13.0-SNAPSHOT native-cli-integration-tests:test`

## Rollback Strategy

If the migration fails:
1. Revert the commits on this branch
2. The old test@zip artifacts are still available in Maven
3. Previous version handling logic is preserved in git history

## References

- TCK expander: `~/dev/mulesoft-emu/data-weave/runtime/src/test/scala/org/mule/weave/v2/tck/ScenarioExpanderTest.scala`
- TCK structure: `~/dev/mulesoft-emu/data-weave/runtime/build/tck-expanded/`
- Original TCK scenarios: `~/dev/mulesoft-emu/data-weave/runtime/src/test/resources/org/mule/weave/v2/engine/`
