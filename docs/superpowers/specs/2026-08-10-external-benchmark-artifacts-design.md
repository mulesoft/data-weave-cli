# External benchmark artifacts — design

**Date:** 2026-08-10
**Status:** Approved

## Goal

Allow the benchmark runners to execute against existing Node and Python wrapper
artifacts without triggering local native builds. Remove the redundant legacy
Node-only `native-lib:benchmark` task.

## Supported tasks

- `native-lib:benchmarkNode` remains the Node runner task and emits a result JSON.
- `native-lib:benchmarkPython` remains the Python runner task and emits a result
  JSON.
- `benchmarkCompare` continues to invoke registered runner tasks and render one
  comparison report.
- Remove `native-lib:benchmark`; it duplicates the Node runner while also rendering
  a report, unlike the runner-task contract.

## Artifact selection

- Without an override, `benchmarkNode` depends on `buildNodePackage` and
  `benchmarkPython` depends on `stagePythonNativeLib`, preserving current local
  build behavior.
- With `DW_BENCH_NODE_PACKAGE`, `benchmarkNode` must not depend on
  `buildNodePackage`; it loads the extracted package at that path.
- With `DW_BENCH_PY_SITE`, `benchmarkPython` must not depend on
  `stagePythonNativeLib`; it imports the site-packages-style directory at that
  path.
- Invalid overrides fail immediately and never fall back to a local artifact.

## Provenance

Each result must identify the native library actually selected by its runner.
The existing `dwlibBuildId` formula remains unchanged: hash the file size and its
first 64 KiB. Node and Python environment collection resolve the library from the
configured external artifact when an override is active; otherwise they use the
existing local staging paths.

## Documentation and regression fixes

- Remove the legacy `native-lib:benchmark` invocation from benchmark documentation.
- Document external-artifact use with `benchmarkNode`, `benchmarkPython`, and
  `benchmarkCompare`.
- Correct the CLI documentation to state that `DW_BENCH_BIN` accepts a prebuilt,
  benchmark-enabled binary.
- Correct the executable Python example to use the `InputValue.mime_type` keyword.

## Testing

- Extend focused Node and Python helper tests to verify external library-path
  resolution and corresponding `dwlibBuildId` attribution.
- Add Gradle configuration-level coverage where practical to verify override paths
  do not attach local artifact build dependencies.
- Run the dependency-free Node and Python benchmark-harness test tasks and Gradle
  task discovery/help checks for the removed legacy task.
