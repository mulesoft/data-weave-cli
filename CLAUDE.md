# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

DataWeave CLI packages the MuleSoft DataWeave runtime as a **GraalVM native image** so DataWeave scripts run without a JVM. It ships two consumer surfaces from the same runtime:

- **`dw`** — a native command-line executable (`native-cli`)
- **`dwlib`** — a native shared library with a C-compatible FFI, consumed by Python and Node.js bindings (`native-lib`)

The DataWeave language/runtime itself lives in a separate repo (`org.mule.weave:*` artifacts, version pinned in `gradle.properties`). This repo is the packaging + CLI layer on top of it.

## Modules

- **`native-cli`** — the `dw` executable. Java entrypoint (`org.mule.weave.cli.DWCLI`) using picocli for arg parsing; Scala for the actual command logic (`org.mule.weave.dwnative.*`). `NativeRuntime` wraps `DataWeaveScriptingEngine` and is the heart of script execution.
- **`native-lib`** — the `dwlib` shared library. Java `@CEntryPoint` methods in `org.mule.weave.lib.NativeLib` expose `run_script`, streaming, and callback APIs to non-JVM callers. Contains `python/` and `node/` binding packages.
- **`native-cli-integration-tests`** — runs the DataWeave TCK / weave test suites against the *compiled native binary* (not the JVM). Depends on `native-cli:nativeCompile`.

## Build & prerequisites

Requires **GraalVM (Java 24, `graalvm-community`)** with `native-image`. Set `GRAALVM_HOME` and `JAVA_HOME` to it. `./install-graalvm.sh` downloads a distribution into `.graalvm/` and exports the vars (note: the script's pinned layout may lag `graalvmVersion` in `gradle.properties` — verify the path).

```bash
# Build the dw native executable (~several minutes) → native-cli/build/native/nativeCompile/dw
./gradlew native-cli:nativeCompile

# Build the shared library → native-lib/build/native/nativeCompile/dwlib.{dylib,so,dll}
./gradlew native-lib:nativeCompile

# Full build (compiles both native images + runs unit tests)
./gradlew build -PskipNodeTests=true

# Packaged OS-specific zip distribution of dw
./gradlew native-cli:distro
```

`native-lib` native builds use `-J-Xmx6G`; ensure enough memory.

## Tests

```bash
# Scala unit tests (JVM, fast) — CLI behavior
./gradlew native-cli:test

# native-lib Java tests + Python + Node bindings (Node/Python spawn subprocesses)
./gradlew native-lib:test
./gradlew native-lib:pythonTest
./gradlew native-lib:nodeTest

# Integration tests — run the DataWeave TCK against the compiled dw binary.
# Downloads test-suite zips for the given weave version, then requires native-cli:nativeCompile.
./gradlew -PweaveTestSuiteVersion=2.10.0 -DweaveSuiteVersion=2.10.0 native-cli-integration-tests:test
```

Skip flags for CI/local: `-PskipNodeTests=true`, `-PskipPythonTests=true`, `-PskipStripDebug=true` (keep debug symbols in `dwlib`).

Scala tests use **scalatest** (`AnyFreeSpec` / `Matchers`). Run a single test with the standard scalatest test filter:

```bash
./gradlew native-cli:test --tests "org.mule.weave.dwnative.cli.DataWeaveCLITest"
```

## Version wiring

Weave runtime and test-suite versions are pinned in `gradle.properties` (`weaveVersion`, `weaveTestSuiteVersion`, `ioVersion`, `weaveSuiteVersion`). At build time `native-cli`'s `genVersions` task generates `org.mule.weave.v2.version.ComponentVersion` (Scala) from these into `build/genresource/` — don't edit it by hand. `nativeVersion` (`100.100.100`) is the CLI/lib artifact version.

## Native-image gotchas

The GraalVM `buildArgs` in each `build.gradle` are load-bearing. When adding dependencies that use reflection, threads at startup, or resource loading, you'll likely need to touch these:

- `--initialize-at-run-time=...` (netty, coursier, `scala.util.Random`, the SPI module loader) — classes that must NOT be initialized during image build.
- **Data formats and module loaders are registered via SPI** (`META-INF/services/org.mule.weave.v2.module.DataFormat` and `...parser.phase.ModuleLoader`). `native-lib` re-materializes these service files during `nativeCompileClasspathJar` (see the `configureEach` block) because the fat-jar merge drops them. A missing/unregistered format shows up as a runtime "unknown mime type" rather than a build error.

## Bindings (native-lib)

`dwlib` is staged into the binding packages by Gradle, not committed built:

- `stagePythonNativeLib` / `stageNodeNativeLib` copy `dwlib.*` into `python/src/dataweave/native/` and `node/native/`.
- `buildPythonWheel` (setup.py `bdist_wheel`) and `buildNodePackage` (npm install → node-gyp rebuild → tsc → npm pack) produce distributables.
- Bindings locate the library via `DATAWEAVE_NATIVE_LIB=/abs/path/to/dwlib.*`, then fall back to the packaged/dev-build locations.

See `native-lib/README.md` for the full binding API (sync `run`, `run_streaming`, `run_transform`, callback streaming, error types).

## Domain concepts (`dw` CLI)

- **Spells** — shareable, runnable DataWeave scripts fetched from git-based "grimoire" repos (`dw spell`, `spell create/list/update`). A grimoire is named `<wizard>-data-weave-grimoire`; the built-in DW grimoire has no prefix.
- **Wizards** — trusted sources of grimoires (`dw wizard add`).
- **Dependency manager** — a spell's `dependencies.dwl` declares maven deps resolved via coursier (`DependencyManager` / `MavenDependencyManager`).
- Env vars: `DW_HOME` (default `~/.dw`), `DW_DEFAULT_INPUT_MIMETYPE`, `DW_DEFAULT_OUTPUT_MIMETYPE` (both default `application/json`).

## CI

`.github/workflows/main.yml` (push/PR to master) and `ci.yml` (weekly) build both native images and all bindings on Ubuntu + Windows. Integration/regression tests (`native-cli-integration-tests` against multiple weave suite versions) run **only on master**, not on PRs, to save CI time.