# AGENTS.md

## Purpose

This repository packages the MuleSoft DataWeave runtime as GraalVM native artifacts.
The language/runtime itself is supplied by pinned `org.mule.weave:*` dependencies;
language implementation changes generally belong in the upstream DataWeave repository.

- `native-cli`: native `dw` executable. Java/picocli parses arguments; Scala implements commands.
- `native-lib`: native `dwlib` shared library, C ABI, Python binding, and Node binding.
- `native-cli-integration-tests`: downloaded suites executed against the compiled native CLI.
- `benchmarks/`: shared corpus/schema and CLI, JVM, Node, and Python benchmark runners.

Key entry points are `DWCLI.java`, `NativeRuntime.scala`, `NativeLib.java`, and
`ScriptRuntime.java`. Read the nearest module README before broad changes.

## Toolchain

- Use the checked-in `./gradlew` wrapper.
- Native builds require GraalVM Community Java 24 with `native-image`.
- Set `GRAALVM_HOME` and `JAVA_HOME`; native-image tasks need about 6 GB heap.
- Node bindings require Node.js 18+ and a C compiler/node-gyp toolchain.
- Python bindings support Python 3.9+.
- Java source/target compatibility is 17; Scala is 2.12.

## Build Commands

Run from the repository root unless a command says otherwise.

```bash
./gradlew native-cli:test
./gradlew native-lib:test -PskipNodeTests=true -PskipPythonTests=true
./gradlew native-cli:nativeCompile
./gradlew native-lib:nativeCompile
./gradlew build -PskipNodeTests=true
./gradlew native-cli:distro
./gradlew native-lib:buildPythonWheel
./gradlew native-lib:buildNodePackage
./gradlew clean
```

Native outputs are `native-cli/build/native/nativeCompile/dw` and
`native-lib/build/native/nativeCompile/dwlib.{dylib,so,dll}`. Useful flags:
`-PskipNodeTests=true`, `-PskipPythonTests=true`, `-PskipStripDebug=true`, and
`-PpythonExe=/path/to/python`.

## Test Commands

```bash
./gradlew native-cli:test
# One ScalaTest suite (preferred single-test granularity)
./gradlew native-cli:test --tests "org.mule.weave.dwnative.cli.DataWeaveCLITest"
# One native-lib JUnit class or method; skip unrelated binding lanes
./gradlew native-lib:test --tests "org.mule.weave.lib.ScriptRuntimeTest" \
  -PskipNodeTests=true -PskipPythonTests=true
./gradlew native-lib:test --tests "org.mule.weave.lib.ScriptRuntimeTest.runSimpleScript" \
  -PskipNodeTests=true -PskipPythonTests=true
# Native CLI integration/TCK suite (builds and runs the native binary)
./gradlew -PweaveTestSuiteVersion=2.10.0 -DweaveSuiteVersion=2.10.0 \
  native-cli-integration-tests:test
./gradlew native-cli-integration-tests:test \
  --tests "org.mule.weave.clinative.NativeCliTest"
# Dependency-free benchmark JavaScript tests
./gradlew native-lib:benchmarkJsUnitTest
cd benchmarks && node --test lib/stats.test.mjs
cd benchmarks && node --test --test-name-pattern="computeStats returns" lib/stats.test.mjs
```

Node/Vitest commands (run after native staging/build when using integration tests):

```bash
cd native-lib/node
npm install
npm run build
npm test
npm run test:unit
npm run test:integration
npm test -- tests/unit/result.test.ts
npm test -- tests/integration/dataweave.test.ts -t "basic arithmetic"
```

Python binding tests are a custom executable script, not a configured pytest suite:

```bash
./gradlew native-lib:pythonTest
cd native-lib/python && python3 tests/test_dataweave_module.py
cd benchmarks/runners/python
python3 -m unittest test_bench.TestStats.test_matches_lib_stats_on_1_to_100
```

Node TCK is separate from normal `nodeTest`: run `./gradlew native-lib:stageTckSuites`,
then `cd native-lib/node && npm run test:tck`.

## Lint and Formatting

There is no repository-wide lint or formatter command: no Scalafmt, Checkstyle,
Spotless, ESLint, Prettier, Black, or Ruff configuration is checked in. Do not claim a
lint step exists or introduce formatting churn. Match neighboring code. For TypeScript,
`cd native-lib/node && npm run build:ts` is the configured strict type check.

No `.cursorrules`, `.cursor/rules/`, or `.github/copilot-instructions.md` rules exist.

## Code Style

- Preserve local import grouping; use explicit imports and avoid new wildcards.
- Java: four spaces, braces on the declaration line, braced control flow, explicit
  types, `PascalCase` classes, `camelCase` members, `UPPER_SNAKE_CASE` constants.
- Scala: two spaces, prefer `val`, idiomatic `foreach { x => ... }`, explicit public
  return types, case classes/sealed traits for domain variants, no semicolons.
- TypeScript/ESM: two spaces, double quotes, semicolons, trailing commas in multiline
  constructs, `camelCase` values, `PascalCase` types/classes.
- TypeScript is strict and emits CommonJS. Use `node:` built-in imports and `import type`;
  local `.ts` imports omit extensions.
- Benchmark `.mjs` is ESM: include `.mjs` extensions and use `import.meta.url`, never
  `require` or CommonJS-only assumptions.
- Python: PEP 8 naming, four spaces, type hints on public/boundary APIs, dataclasses for
  result/input models, and snake_case public names translated to camelCase wire fields.
- Gradle formatting varies by file; preserve the file's indentation and quote style.
- Add comments/Javadoc/TSDoc for public APIs, ABI contracts, ownership, concurrency, or
  non-obvious algorithms. Avoid comments that merely restate code.

## Types and Errors

- Model domain outcomes explicitly: Scala case classes/traits, TypeScript interfaces,
  Python dataclasses. Use loose JSON/maps only at serialization or FFI boundaries.
- Preserve wire fields exactly: `success`, `result`, `error`, `mimeType`, `charset`,
  `binary`, `streamHandle`, `content`, and `properties`.
- Script failures normally return an unsuccessful result envelope. Binding/lifecycle
  failures throw `DataWeaveError`; opt-in modes may promote script failures to
  `DataWeaveScriptError`. CLI failures need context and a nonzero exit code.
- Include operation context in errors, avoid null-only messages, and escape every string
  crossing a manually constructed JSON boundary.
- Use `try/finally` for native allocations, stream registrations, isolate attachment,
  and cleanup. Suppress cleanup errors only when they must not mask an earlier failure.
- Never let JavaScript or Python exceptions unwind across C callbacks; translate them to
  the documented callback status (`0` success, nonzero/-1 error, `0` read means EOF).

## Native and FFI Rules

- Treat exported C names, argument order, callback semantics, nullability, ownership,
  and result JSON as a stable ABI. Update Java, C addon, TypeScript, Python, tests, and
  documentation together when changing it.
- Every OS thread calling Graal must attach its own isolate thread and detach afterward.
  Never reuse an `IsolateThread` from another OS thread.
- Do not capture Graal `Word`/pointer values in Java lambdas; convert to raw addresses and
  reconstruct pointers inside explicit workers.
- Copy callback/native buffers before returning; never retain borrowed pointers or use a
  Graal-owned C string after `free_cstring`.
- Preserve bounded queues/backpressure and chunk remainders; chunks can exceed the native
  8 KiB callback buffer and do not align with logical records.
- Node worker threads must use N-API thread-safe functions; do not call V8 directly.
- Native-image `buildArgs` are load-bearing; do not simplify initialization, charset,
  locale, HTTP, reflection, or resources without native build/runtime coverage.
- Preserve SPI descriptors for `DataFormat` and `ModuleLoader`; `native-lib` deliberately
  re-materializes them when creating its fat JAR. Missing entries often appear as
  runtime "unknown mime type" failures, not build errors.

## Tests and Generated Files

- Scala CLI tests use `AnyFreeSpec`/`Matchers`; exercise picocli parsing and assert exit
  code, stdout, and stderr.
- Java library tests use JUnit 5. Node uses Vitest projects (`unit`, `integration`, `tck`).
  Benchmark JavaScript uses `node:test`; its Gradle task enumerates files explicitly, so
  wire new test files into `native-lib/build.gradle` when they belong in normal testing.
- Streaming changes need chunk integrity, terminal metadata, large-chunk, failure, and
  cleanup coverage. Shared benchmark math/schema changes need JS/Python/Scala parity.
- Never edit generated `build/genresource/.../ComponentVersion.scala`,
  `build/genjava/.../BenchmarkMode.java`, Graal headers, staged `dwlib.*`, downloaded TCK
  suites, benchmark results, or generated benchmark inputs. Change generators/sources.
- Gradle stages native libraries into Python and Node packages; do not commit staged or
  packaged artifacts (`node_modules`, `dist`, `native`, wheels, tarballs, results).

## Benchmarks and CI

Benchmarks are opt-in: `./gradlew benchmarkCompare -Pbenchmark=true`. Runner tasks carry
`ext.benchmarkRunner = true`; the root aggregator auto-discovers them. New runners must
use the shared corpus/schema and write `benchmarks/results/<runner>-<timestamp>.json`.
Do not add benchmark execution to normal `build` or `test`. The CLI benchmark requires a
binary built with `-Pbenchmark=true`; `DW_BENCH_BIN` may select a prebuilt one.

CI builds Ubuntu and Windows with GraalVM 24. Native CLI regression suites and Node TCK
run only on `master`. Run the smallest relevant suite, then the nearest module test; use
a native build for FFI, SPI, resources, initialization, packaging, or native-image changes.
Keep PRs focused, add regression tests, minimize dependencies, and report vulnerabilities
through the Salesforce portal named in `SECURITY.md`, never through public issues.
