# Explicit-Instance-Only Bindings Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove Python and Node module-level DataWeave execution/cleanup APIs and migrate every repository-owned consumer to deterministically managed `DataWeave` instances.

**Architecture:** `DataWeave` remains the sole high-level runtime owner in both bindings. Each caller constructs, initializes, uses, and cleans its own engine-backed instance; existing shared-isolate, per-engine resolver, streaming, and instance-cleanup behavior remains unchanged, while singleton state and automatic shutdown hooks are deleted.

**Tech Stack:** Python 3.9+, ctypes, pytest, TypeScript 5.5, Node.js 18+, N-API, Vitest 3, Gradle, GraalVM Community Java 24 native-image

**Spec:** `docs/superpowers/specs/2026-09-15-explicit-instance-only-bindings-design.md`

## Global Constraints

- This is an intentional breaking source-compatibility change; do not add aliases, warning stubs, deprecated wrappers, or per-call temporary runtimes.
- Preserve the native C ABI, result envelopes, wire fields, engine handles, resolver isolation, shared-isolate reference counting, streaming behavior, and explicit-instance cleanup semantics.
- Remove Python `atexit` registration and Node `beforeExit`/`exit` registration from the high-level bindings.
- Retain review-22 changes that harden explicit `DataWeave` or native lifecycle behavior; remove changes whose sole purpose is module-singleton lifecycle management.
- Every constructed runtime in repository-owned executable code must have deterministic cleanup through a Python context manager/fixture or Node `try/finally`/test teardown.
- Warm benchmark measurements reuse one initialized instance outside measured loops; they must not include per-sample initialization or cleanup.
- Custom resolvers remain unsupported for background-thread streaming/transform execution; preserve that documented limitation.
- Use the checked-in `./gradlew` wrapper. Native verification requires GraalVM Community Java 24 with `native-image` and approximately 6 GB heap.
- Match local formatting; do not introduce repository-wide formatting churn or generated/staged artifacts.

## File Map

- `native-lib/python/src/dataweave/__init__.py`: public Python exports only; delete singleton state and forwarding functions while retaining private encoding aliases required by tests.
- `native-lib/python/tests/unit/test_facade.py`: assert the exact explicit-instance public surface and retain `DataWeave` execution/resolver tests; delete singleton lifecycle tests.
- `native-lib/python/tests/conftest.py`: provide deterministic integration runtime ownership instead of resetting a hidden global.
- `native-lib/python/tests/integration/{test_execution,test_callbacks,test_streaming}.py`: exercise instance methods through the integration runtime fixture.
- `native-lib/node/src/dataweave.ts`: retain the `DataWeave` class and explicit-instance cleanup hardening; delete all module singleton state, helpers, and hooks.
- `native-lib/node/src/index.ts`: export `DataWeave` but no module execution/cleanup functions.
- `native-lib/node/tests/unit/dataweave-initialize.test.ts`: retain instance lifecycle tests and replace singleton tests with public-surface/hook-absence coverage.
- `native-lib/node/tests/integration/{dataweave,edge-cases,instance-lifecycle,teardown-deadlock,independent-engines,dataweave-resolver}.test.ts`: migrate behavior tests to explicit instances and remove singleton-specific scenarios.
- `native-lib/example_dataweave_module.py`, `native-lib/example_streaming.py`, `native-lib/python/examples/{simple_demo,streaming_demo}.py`: own Python runtimes explicitly.
- `native-lib/example_streaming.mjs`: own one initialized Node runtime explicitly.
- `benchmarks/runners/node/{wrapper,warm-bench,emit,coldstart-child}.mjs` and tests: load/construct `DataWeave`, reuse one warm runtime, and clean it explicitly.
- `benchmarks/runners/python/{wrapper,emit,coldstart_child,test_bench}.py`: align test doubles and loader assertions with the already explicit Python benchmark runtime.
- `native-lib/{README.md,python/README.md,node/README.md,node/docs/external-modules.md,node/node-api-plan.md}`, `CLAUDE.md`, and current architecture specs: document explicit ownership and remove current singleton promises.
- `native-lib/python/tests/unit/test_ci_structure.py`: enforce the revised README resolver/lifecycle contract and reject removed module-level example usage.

---

### Task 1: Remove the Python module singleton

**Files:**
- Modify: `native-lib/python/src/dataweave/__init__.py:1-95`
- Modify: `native-lib/python/tests/unit/test_facade.py:1-515`

**Interfaces:**
- Consumes: Existing `dataweave.runtime.DataWeave` constructor and instance methods.
- Produces: Python package exports with `DataWeave` and supporting types/utilities, but no `run`, `run_streaming`, `run_transform`, `run_callback`, `run_input_output_callback`, or `cleanup` attributes.

- [ ] **Step 1: Replace the legacy export assertion with removal and retained-export assertions**

In `test_facade.py`, replace `test_facade_preserves_fixed_legacy_public_exports`, `test_module_level_run_does_not_accept_module_resolver`, and all tests beginning `test_global_` or otherwise exercising `_get_global_instance` with these public-surface tests. Keep the existing instance-level constructor, execution, resolver, callback-reentrancy, and lifecycle tests in this file.

```python
@pytest.mark.unit
def test_facade_exports_explicit_instance_api():
    expected_exports = {
        "DataWeave",
        "DataWeaveError",
        "DataWeaveLibraryNotFoundError",
        "DataWeaveScriptError",
        "ExecutionResult",
        "InputValue",
        "ReadCallback",
        "Stream",
        "StreamingResult",
        "WriteCallback",
        "READ_CALLBACK",
        "RESOLVE_MODULE_CALLBACK",
        "WRITE_CALLBACK",
        "ModuleResolver",
        "compose_resolvers",
        "modules_from_directory",
        "modules_from_jars",
        "modules_from_map",
    }

    assert set(dataweave.__all__) == expected_exports
    for name in expected_exports:
        getattr(dataweave, name)


@pytest.mark.unit
@pytest.mark.parametrize(
    "name",
    [
        "run",
        "run_streaming",
        "run_transform",
        "run_callback",
        "run_input_output_callback",
        "cleanup",
    ],
)
def test_facade_does_not_export_module_execution_or_cleanup(name):
    assert name not in dataweave.__all__
    assert not hasattr(dataweave, name)
```

- [ ] **Step 2: Run the facade tests and verify the removed names still fail the contract**

Run:

```bash
cd native-lib/python && python3 -m pytest tests/unit/test_facade.py -q
```

Expected: FAIL because the six module-level functions still exist and remain in `__all__`.

- [ ] **Step 3: Delete Python singleton implementation and unused imports**

In `native-lib/python/src/dataweave/__init__.py`:

- Delete `threading`, `Any`, `Dict`, `Iterable`, `Optional`, callback input types used only by forwarding functions, and `_raise_if_native_callback_active` imports when no longer used.
- Delete `_global_instance`, `_global_lock`, `_get_global_instance`, all six forwarding/cleanup functions, and all `atexit` registration behavior.
- Preserve `ctypes` if required by the package's compatibility imports, the three private encoding aliases used by `test_encoding.py`, model/error/callback constants, resolver exports, and `DataWeave`.
- Replace `__all__` with the exact retained set from Step 1, maintaining the file's compact list style.

The resulting module must contain no call to `atexit.register` and no runtime object stored at module scope.

- [ ] **Step 4: Run focused Python unit tests**

Run:

```bash
cd native-lib/python && python3 -m pytest tests/unit/test_facade.py tests/unit/test_encoding.py tests/unit/test_runtime.py -q
```

Expected: PASS.

- [ ] **Step 5: Commit the Python public-surface removal**

```bash
git add native-lib/python/src/dataweave/__init__.py native-lib/python/tests/unit/test_facade.py
git commit -m "refactor: require explicit Python runtimes"
```

---

### Task 2: Migrate Python integration tests to an owned fixture

**Files:**
- Modify: `native-lib/python/tests/conftest.py:160-186`
- Modify: `native-lib/python/tests/integration/test_execution.py:1-63`
- Modify: `native-lib/python/tests/integration/test_callbacks.py:1-135`
- Modify: `native-lib/python/tests/integration/test_streaming.py:55-161`

**Interfaces:**
- Consumes: `dataweave.DataWeave.initialize()`, instance execution methods, and synchronous `DataWeave.cleanup()`.
- Produces: Function-scoped pytest fixture `runtime` yielding one initialized `dataweave.DataWeave` and cleaning it in `finally`.

- [ ] **Step 1: Add a fixture-contract test before replacing the old cleanup fixture**

Add this integration test to `test_execution.py`:

```python
@pytest.mark.integration
def test_runtime_fixture_is_initialized(runtime):
    assert runtime.run("2 + 2").get_string() == "4"
```

- [ ] **Step 2: Run the new test and verify fixture lookup fails**

Run:

```bash
cd native-lib/python && python3 -m pytest tests/integration/test_execution.py::test_runtime_fixture_is_initialized -q
```

Expected: ERROR with `fixture 'runtime' not found`.

- [ ] **Step 3: Replace hidden-global cleanup with explicit fixture ownership**

Delete `clean_dataweave_runtime` from `tests/conftest.py` and add:

```python
@pytest.fixture
def runtime():
    instance = dataweave.DataWeave()
    instance.initialize()
    try:
        yield instance
    finally:
        instance.cleanup()
```

Keep the session-scoped resolver-backed `tck_runtime` unchanged.

- [ ] **Step 4: Convert buffered and callback integration tests to the fixture**

Add `runtime` to every execution/callback test signature and replace calls mechanically by method:

```python
result = runtime.run("2 + 2", {})
result = runtime.run_callback("2 + 2", on_write)
result = runtime.run_input_output_callback(
    "output application/json\n---\npayload",
    input_name="payload",
    input_mime_type="application/json",
    read_callback=on_read,
    write_callback=on_write,
)
```

Do not alter assertions or callback behavior.

- [ ] **Step 5: Convert module-level streaming tests to the fixture**

Add `runtime` to tests at `test_streaming.py:55-161` and replace only module-level calls:

```python
output, metadata = collect_stream(runtime.run_streaming(script, inputs))
stream = runtime.run_transform(
    script,
    input_stream=chunks,
    input_mime_type="application/json",
)
```

Leave `test_real_native_precreated_stream_rejects_stale_generation` using its two explicitly owned instances.

- [ ] **Step 6: Run all Python integration tests**

Run:

```bash
cd native-lib/python && python3 -m pytest tests/integration -q
```

Expected: PASS when staged `dwlib` is available. If unavailable, record the missing native artifact and defer this command to Task 8; do not weaken the tests.

- [ ] **Step 7: Commit the Python test migration**

```bash
git add native-lib/python/tests/conftest.py native-lib/python/tests/integration/test_execution.py native-lib/python/tests/integration/test_callbacks.py native-lib/python/tests/integration/test_streaming.py
git commit -m "test: own Python DataWeave fixtures explicitly"
```

---

### Task 3: Remove the Node module singleton and hooks

**Files:**
- Modify: `native-lib/node/src/dataweave.ts:22,52-61,599-796`
- Modify: `native-lib/node/src/index.ts:1`
- Modify: `native-lib/node/tests/unit/dataweave-initialize.test.ts:25,274-302,330-end`
- Create: `native-lib/node/tests/unit/index-exports.test.ts`

**Interfaces:**
- Consumes: Existing exported class `DataWeave` and its instance methods.
- Produces: Package entry point exporting `DataWeave` without `run`, `runStreaming`, `runTransform`, or `cleanup`; constructing/initializing an explicit instance registers no process listeners.

- [ ] **Step 1: Write package export and listener tests**

Create `index-exports.test.ts`:

```typescript
import { describe, expect, it } from "vitest";
import * as api from "../../src/index";

describe("public package exports", () => {
  it("exports DataWeave without module-level execution or cleanup", () => {
    expect(api.DataWeave).toBeTypeOf("function");
    for (const name of ["run", "runStreaming", "runTransform", "cleanup"]) {
      expect(api).not.toHaveProperty(name);
    }
  });
});
```

In `dataweave-initialize.test.ts`, replace `test("does not accumulate process exit listeners...")` and delete each test whose subject is module `run`, module `cleanup`, singleton revival/generations, or module exit hooks. Do not delete later tests solely because they appear after the first singleton test: retain every test whose subject is an explicitly constructed `DataWeave` instance, stream cancellation, engine destruction, or native reference release. Add:

```typescript
it("does not register process lifecycle listeners for explicit instances", async () => {
  const beforeExitCount = process.listenerCount("beforeExit");
  const exitCount = process.listenerCount("exit");
  const dw = new DataWeave();

  dw.initialize();
  await dw.cleanup();

  expect(process.listenerCount("beforeExit")).toBe(beforeExitCount);
  expect(process.listenerCount("exit")).toBe(exitCount);
});
```

Retain every test before/after those blocks that directly exercises `DataWeave.initialize()`, `DataWeave.cleanup()`, stream cancellation, engine destruction, or native reference release. Remove imports and test helpers used only to capture singleton listeners.

- [ ] **Step 2: Run focused Node unit tests and verify failure**

Run:

```bash
cd native-lib/node && npm run test:unit -- tests/unit/index-exports.test.ts tests/unit/dataweave-initialize.test.ts
```

Expected: FAIL because the package still exports the four module functions and explicit first use may still coexist with singleton code in the module.

- [ ] **Step 3: Delete all singleton-only Node implementation**

In `dataweave.ts`:

- Update the class JSDoc to require explicit `initialize()` and awaited `cleanup()`; remove links to module-level functions.
- Delete `retryableCleanupInstances` only if no explicit-instance cleanup test/path uses it after singleton deletion. In the current worktree it is read solely by module cleanup, so delete it and its `add`/`delete` calls without changing `doCleanup()` state transitions.
- Delete everything from `// Module-level convenience API with lazy singleton` through the end of the file.
- Preserve the complete `DataWeave` class, including explicit cleanup coalescing, stream cancellation, stale-generation checks, resolver ownership, and cleanup retry state.

In `index.ts`, change the first line to:

```typescript
export { DataWeave } from "./dataweave";
```

- [ ] **Step 4: Run focused tests and strict type checking**

Run:

```bash
cd native-lib/node && npm run test:unit -- tests/unit/index-exports.test.ts tests/unit/dataweave-initialize.test.ts
cd native-lib/node && npm run build:ts
```

Expected: the focused tests PASS; `build:ts` may still fail only at repository consumers importing removed symbols, which Tasks 4-6 address. Record exact failing files before proceeding.

- [ ] **Step 5: Commit the Node public-surface removal**

```bash
git add native-lib/node/src/dataweave.ts native-lib/node/src/index.ts native-lib/node/tests/unit/dataweave-initialize.test.ts native-lib/node/tests/unit/index-exports.test.ts
git commit -m "refactor: require explicit Node runtimes"
```

---

### Task 4: Migrate Node integration tests to explicit instances

**Files:**
- Modify: `native-lib/node/tests/integration/dataweave.test.ts`
- Modify: `native-lib/node/tests/integration/edge-cases.test.ts`
- Modify: `native-lib/node/tests/integration/instance-lifecycle.test.ts`
- Modify: `native-lib/node/tests/integration/teardown-deadlock.test.ts`
- Modify: `native-lib/node/tests/integration/independent-engines.test.ts`
- Modify: `native-lib/node/tests/integration/dataweave-resolver.test.ts`

**Interfaces:**
- Consumes: `DataWeave`, `DataWeave.initialize()`, `run`, `runStreaming`, `runTransform`, and `cleanup` instance methods.
- Produces: Native integration tests with no imports or calls to removed module-level APIs.

- [ ] **Step 1: Add an explicit runtime owner to broad API suites**

For `dataweave.test.ts` and `edge-cases.test.ts`, import only `DataWeave` from the package API, create a suite runtime, initialize it in `beforeAll`, and clean it in `afterAll`:

```typescript
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { DataWeave } from "../../src/index";

const dw = new DataWeave();

beforeAll(() => {
  dw.initialize();
});

afterAll(async () => {
  await dw.cleanup();
});
```

Replace `run(...)`, `runStreaming(...)`, and `runTransform(...)` with `dw.run(...)`, `dw.runStreaming(...)`, and `dw.runTransform(...)`. Rename the edge-case test description `shared singleton` to `shared explicit instance`. Keep tests that intentionally construct additional instances and their local `finally` cleanup unchanged.

- [ ] **Step 2: Remove redundant module cleanup from explicit-engine suites**

In `independent-engines.test.ts` and `dataweave-resolver.test.ts`, import only `DataWeave`; retain tracked-instance teardown but delete the final module `cleanup()` call and comments claiming it cleans a singleton:

```typescript
afterAll(async () => {
  for (const dw of instances) {
    await dw.cleanup();
  }
});
```

- [ ] **Step 3: Delete module-only lifecycle scenarios while retaining instance contracts**

In `instance-lifecycle.test.ts`, remove `run` and module `cleanup` imports. Preserve all tests that construct `DataWeave` or call raw `ffi`. Delete only any scenario whose subject is singleton recreation or module cleanup; do not delete same-instance pending-cleanup, stale-generation, transform-admission, or raw last-reference tests.

In `teardown-deadlock.test.ts`:

- Change the callback reentrancy test to construct and initialize `dw`, invoke `dw.run(...)` from the input callback, start the outer operation with `dw.runTransform(...)`, and await `dw.cleanup()` in `finally`.
- Rewrite the pending-teardown adoption test with two explicit instances: `outerDw` starts the held stream, `cleanupPromise = outerDw.cleanup()` releases its reference, then `replacementDw.initialize()` adopts the live isolate and `replacementDw.run(...)` succeeds. Release the gate and clean both instances in `finally`.

The callback body must remain:

```typescript
try {
  dw.run("%dw 2.0\noutput application/json\n---\n1 + 1");
} catch (error) {
  runError = error;
}
```

- [ ] **Step 4: Run TypeScript checking to prove removed imports are gone**

Run:

```bash
cd native-lib/node && npm run build:ts
```

Expected: PASS.

- [ ] **Step 5: Run the migrated integration files**

Run:

```bash
cd native-lib/node && npm run test:integration -- tests/integration/dataweave.test.ts tests/integration/edge-cases.test.ts tests/integration/instance-lifecycle.test.ts tests/integration/teardown-deadlock.test.ts tests/integration/independent-engines.test.ts tests/integration/dataweave-resolver.test.ts
```

Expected: PASS with the native addon and staged `dwlib`. If artifacts are unavailable, defer to Task 8 without modifying expected behavior.

- [ ] **Step 6: Commit the Node integration migration**

```bash
git add native-lib/node/tests/integration/dataweave.test.ts native-lib/node/tests/integration/edge-cases.test.ts native-lib/node/tests/integration/instance-lifecycle.test.ts native-lib/node/tests/integration/teardown-deadlock.test.ts native-lib/node/tests/integration/independent-engines.test.ts native-lib/node/tests/integration/dataweave-resolver.test.ts
git commit -m "test: migrate Node suites to explicit runtimes"
```

---

### Task 5: Migrate executable examples

**Files:**
- Modify: `native-lib/example_dataweave_module.py`
- Modify: `native-lib/example_streaming.py`
- Modify: `native-lib/python/examples/simple_demo.py`
- Modify: `native-lib/python/examples/streaming_demo.py`
- Modify: `native-lib/example_streaming.mjs`

**Interfaces:**
- Consumes: Python context-manager `DataWeave`; Node `DataWeave` explicit lifecycle.
- Produces: Examples with no removed package-level calls and deterministic cleanup.

- [ ] **Step 1: Add a static regression test for examples**

Add to `native-lib/python/tests/unit/test_ci_structure.py`:

```python
@pytest.mark.unit
def test_examples_do_not_use_removed_module_level_runtime_api():
    root = Path(__file__).resolve().parents[3]
    examples = [
        root / "example_dataweave_module.py",
        root / "example_streaming.py",
        root / "python" / "examples" / "simple_demo.py",
        root / "python" / "examples" / "streaming_demo.py",
        root / "example_streaming.mjs",
    ]
    removed = (
        "dataweave.run(",
        "dataweave.run_streaming(",
        "dataweave.run_transform(",
        "dataweave.run_callback(",
        "dataweave.run_input_output_callback(",
        "dataweave.cleanup(",
        "import { runTransform, cleanup }",
    )

    for example in examples:
        content = example.read_text(encoding="utf-8")
        assert not any(symbol in content for symbol in removed), example
```

Ensure `Path` and `pytest` are imported in the test file.

- [ ] **Step 2: Run the static test and verify existing examples fail**

Run:

```bash
cd native-lib/python && python3 -m pytest tests/unit/test_ci_structure.py::test_examples_do_not_use_removed_module_level_runtime_api -q
```

Expected: FAIL and identify the listed Python/Node examples.

- [ ] **Step 3: Migrate Python examples with one lexical runtime**

For each Python example, pass `dw` into helper functions or construct it once in `main`, then use instance methods. The top-level shape must be:

```python
def main():
    with dataweave.DataWeave() as dw:
        run_examples(dw)


def run_examples(dw: dataweave.DataWeave):
    result = dw.run("2 + 2")
```

For streaming examples, call `dw.run_streaming`, `dw.run_transform`, and `dw.run_input_output_callback`. Remove explicit module cleanup calls. Preserve example output, memory measurements, callback logic, and error demonstrations.

- [ ] **Step 4: Migrate the Node streaming example**

Change its import and main lifecycle to:

```javascript
import { DataWeave } from "./node/dist/index.js";

const dw = new DataWeave();
dw.initialize();
try {
  await testRunTransform(dw);
} finally {
  await dw.cleanup();
}
```

Pass `dw` to the transform helper and replace `runTransform(...)` with `dw.runTransform(...)`. Preserve the generator drain and reporting behavior.

- [ ] **Step 5: Run static and syntax validation**

Run:

```bash
cd native-lib/python && python3 -m pytest tests/unit/test_ci_structure.py -q
python3 -m py_compile native-lib/example_dataweave_module.py native-lib/example_streaming.py native-lib/python/examples/simple_demo.py native-lib/python/examples/streaming_demo.py
node --check native-lib/example_streaming.mjs
```

Expected: PASS.

- [ ] **Step 6: Commit the example migration**

```bash
git add native-lib/python/tests/unit/test_ci_structure.py native-lib/example_dataweave_module.py native-lib/example_streaming.py native-lib/python/examples/simple_demo.py native-lib/python/examples/streaming_demo.py native-lib/example_streaming.mjs
git commit -m "docs: use explicit runtimes in binding examples"
```

---

### Task 6: Migrate benchmark wrappers and tests

**Files:**
- Modify: `benchmarks/runners/node/wrapper.mjs`
- Modify: `benchmarks/runners/node/wrapper.test.mjs`
- Modify: `benchmarks/runners/node/warm-bench.mjs`
- Modify: `benchmarks/runners/node/warm-bench.test.mjs`
- Modify: `benchmarks/runners/node/emit.mjs`
- Modify: `benchmarks/runners/node/coldstart-child.mjs`
- Modify: `benchmarks/runners/python/wrapper.py`
- Modify: `benchmarks/runners/python/test_bench.py`

**Interfaces:**
- Consumes: Package-exported `DataWeave` constructors.
- Produces: Node `loadWrapper()` returning an API object with `DataWeave`; `runWarmAndStreaming(runtime, manifest)` consuming an initialized instance; Python loader tests requiring `DataWeave` rather than removed functions.

- [ ] **Step 1: Update benchmark loader tests to demand constructors**

In `wrapper.test.mjs`, make generated fixture modules export a class:

```javascript
writeFileSync(
  join(distDir, "index.js"),
  "export class DataWeave { initialize() {} run() { return null; } cleanup() {} }",
);
```

Assert:

```javascript
assert.equal(typeof api.DataWeave, "function");
```

Update missing-export errors to expect `did not export a DataWeave constructor`.

In `test_bench.py`, change wrapper fixture modules and required attributes so loaders assert `DataWeave` plus result/types as applicable, never module `run`, `run_streaming`, or `run_transform`.

- [ ] **Step 2: Run benchmark unit tests and verify loader failures**

Run:

```bash
cd benchmarks && node --test runners/node/wrapper.test.mjs runners/node/warm-bench.test.mjs
cd benchmarks/runners/python && python3 -m unittest test_bench
```

Expected: Node loader tests FAIL until `loadWrapper` validates `DataWeave`; Python failures, if any, identify stale required module functions.

- [ ] **Step 3: Update Node wrapper loading and warm benchmark contract**

In `wrapper.mjs`, select ESM/CommonJS exports by constructor and validate them:

```javascript
const mod = await import(pathToFileURL(wrapperPath).href);
const api = mod.DataWeave ? mod : mod.default;
if (!api || typeof api.DataWeave !== "function") {
  throw new Error(`Wrapper at ${wrapperPath} did not export a DataWeave constructor`);
}
return api;
```

Rename `runWarmAndStreaming(api, manifest)` to `runWarmAndStreaming(dw, manifest)` and replace `api.run`/`api.runTransform` with instance calls. Do not initialize or clean inside this function so timing setup remains controlled by its caller.

- [ ] **Step 4: Own one warm Node runtime in the emitter**

In `emit.mjs`:

```javascript
const api = await loadWrapper();
const dw = new api.DataWeave();
dw.initialize();
let warmRows;
try {
  warmRows = await runWarmAndStreaming(dw, manifest);
} finally {
  await dw.cleanup();
}
```

The instance must be initialized after cold-process measurements and before warm measurements. Cleanup remains outside all measured loops.

In `coldstart-child.mjs`, retain its explicit `DataWeave` construction and initialization but change the final call to `await dw.cleanup()` so asynchronous cleanup completes before child exit.

- [ ] **Step 5: Update benchmark test doubles and Python loader checks**

Change `warm-bench.test.mjs` fake APIs into runtime-like objects with `run` and `runTransform`; remove `api.cleanup()` from the test because ownership belongs to `emit.mjs`, not `runWarmAndStreaming`.

In Python benchmark wrapper/tests, preserve the existing explicit `DataWeave` runtime used by `emit.py` and `coldstart_child.py`. Remove only assertions and synthetic fixture exports that require package-level execution functions.

- [ ] **Step 6: Run all dependency-free benchmark tests**

Run:

```bash
cd benchmarks && node --test lib/stats.test.mjs runners/node/*.test.mjs
cd benchmarks/runners/python && python3 -m unittest test_bench
```

Expected: PASS.

- [ ] **Step 7: Commit the benchmark migration**

```bash
git add benchmarks/runners/node/wrapper.mjs benchmarks/runners/node/wrapper.test.mjs benchmarks/runners/node/warm-bench.mjs benchmarks/runners/node/warm-bench.test.mjs benchmarks/runners/node/emit.mjs benchmarks/runners/node/coldstart-child.mjs benchmarks/runners/python/wrapper.py benchmarks/runners/python/test_bench.py
git commit -m "refactor: benchmark explicit DataWeave instances"
```

---

### Task 7: Rewrite binding and architecture documentation

**Files:**
- Modify: `native-lib/README.md`
- Modify: `native-lib/python/README.md`
- Modify: `native-lib/node/README.md`
- Modify: `native-lib/node/docs/external-modules.md`
- Modify: `native-lib/node/node-api-plan.md`
- Modify: `CLAUDE.md`
- Modify: `native-lib/python/tests/unit/test_ci_structure.py:21-134`
- Modify: `docs/superpowers/specs/2026-08-07-native-lib-multi-engine-design.md`
- Modify: `docs/superpowers/specs/2026-08-19-python-binding-modernization-design.md`
- Modify: `docs/superpowers/specs/2026-08-24-python-module-resolver-design.md`

**Interfaces:**
- Consumes: Explicit-instance Python and Node APIs established in Tasks 1 and 3.
- Produces: Current documentation whose runnable examples and lifecycle statements use only explicit instances; historical rationale remains accurate and clearly historical.

- [ ] **Step 1: Rewrite first-use and execution examples**

Use these canonical patterns consistently.

Python:

```python
import dataweave

with dataweave.DataWeave() as dw:
    result = dw.run("2 + 2")
```

Node:

```typescript
import { DataWeave } from "dataweave-native";

const dw = new DataWeave();
dw.initialize();
try {
  const result = dw.run("2 + 2");
} finally {
  await dw.cleanup();
}
```

Convert all buffered, streaming, transform, callback, resolver, error-handling, worker, and signal-handling snippets to invoke the instance. Do not imply that Node cleanup is synchronous.

- [ ] **Step 2: Rewrite lifecycle and resolver guidance**

Delete current statements that:

- advertise a module-level convenience API;
- describe lazy singleton creation/recreation or singleton generation bounds;
- claim Python registers `atexit` or Node registers `beforeExit`/`exit`;
- instruct users to import module `cleanup`; or
- define instance methods as “same as module-level”.

Replace them with direct instance-method descriptions and deterministic cleanup guidance. In Worker guidance, require one independently constructed `DataWeave` per Worker thread; do not recommend module functions.

Keep the external-module streaming warning but remove its obsolete singleton comparison:

```markdown
Configure external modules with the `resolveModule` constructor option. Custom
modules resolve for `dw.run()`. They do not resolve inside
`dw.runStreaming()`/`dw.runTransform()` because those operations execute on a
background thread that cannot safely call the JavaScript resolver.
```

- [ ] **Step 3: Update architecture specifications without erasing history**

In the consolidated multi-engine design:

- Update scope statements that say the Python public API is unchanged.
- Replace the current Node module-singleton lifecycle subsection with an explicit-instance-only note referencing the new design.
- Remove module-singleton implementation/test entries from the final-state file/provenance maps while retaining historical descriptions of the removed Java/C singleton where needed for rationale.
- Keep lower-level shared-isolate module globals; “no singleton” refers to the high-level execution facade, not ref-count state.

In the Python modernization and resolver designs, add a dated supersession note linking to the new design and change current-state assertions from “functions remain” to “subsequently removed.” Do the same in `node-api-plan.md`, which is historical but currently presents module functions as the final API.

- [ ] **Step 4: Update documentation contract tests**

In `test_ci_structure.py`, replace the singleton-specific resolver contract with the explicit-instance contract:

```python
assert (
    "Custom resolver configuration is provided through the `resolve_module` "
    "option on each `DataWeave` instance."
) in normalized
assert (
    "`run_streaming()`, `run_transform()`, and the low-level callback "
    "streaming API do not use custom resolvers and can import only built-in "
    "modules."
) in normalized
```

Update the negation parameter cases to mutate `provided through the \`resolve_module\` option` and confirm `assert_resolver_restrictions` rejects the mutation. Retain the shared-isolate and resolver-retention assertions.

- [ ] **Step 5: Run a focused stale-guidance search**

Run:

```bash
rg -n 'module-level (API|convenience)|global singleton|shared singleton|import \{ (run|runStreaming|runTransform|cleanup)|dataweave\.(run|run_streaming|run_transform|run_callback|run_input_output_callback|cleanup)\(' native-lib CLAUDE.md docs/superpowers/specs
```

Expected: no matches in current runnable examples or present-tense API guidance. Manually inspect remaining historical/spec matches; retain only text that explicitly describes prior behavior, a DataWeave language concept such as module-singleton fixtures, or an instance method.

- [ ] **Step 6: Run documentation contract tests**

Run:

```bash
cd native-lib/python && python3 -m pytest tests/unit/test_ci_structure.py -q
```

Expected: PASS.

- [ ] **Step 7: Commit documentation updates**

```bash
git add CLAUDE.md native-lib/README.md native-lib/python/README.md native-lib/node/README.md native-lib/node/docs/external-modules.md native-lib/node/node-api-plan.md native-lib/python/tests/unit/test_ci_structure.py docs/superpowers/specs/2026-08-07-native-lib-multi-engine-design.md docs/superpowers/specs/2026-08-19-python-binding-modernization-design.md docs/superpowers/specs/2026-08-24-python-module-resolver-design.md docs/superpowers/specs/2026-09-15-explicit-instance-only-bindings-design.md
git add -f docs/superpowers/plans/2026-09-15-explicit-instance-only-bindings.md
git commit -m "docs: document explicit binding lifecycle"
```

---

### Task 8: Run complete verification and inspect the final diff

**Files:**
- Modify only files implicated by verification failures caused by this change.

**Interfaces:**
- Consumes: All deliverables from Tasks 1-7.
- Produces: Verified explicit-instance-only bindings with no stale executable consumers or accidental singleton-hardening remnants.

- [ ] **Step 1: Run the source/export search gate**

Run:

```bash
rg -n 'dataweave\.(run|run_streaming|run_transform|run_callback|run_input_output_callback|cleanup)\(' native-lib benchmarks
rg -n 'import \{[^}]*\b(run|runStreaming|runTransform|cleanup)\b[^}]*\} from ["'"'](?:dataweave-native|.*src/(?:index|dataweave))["'"']' native-lib benchmarks
rg -n '^(export )?(async )?function (run|runStreaming|runTransform|cleanup)\b|^def (run|run_streaming|run_transform|run_callback|run_input_output_callback|cleanup)\b' native-lib/python/src/dataweave/__init__.py native-lib/node/src/dataweave.ts
```

Expected: no executable module-level consumers or definitions. Instance methods may appear in the final definition search only when indented; inspect all output rather than deleting valid methods.

- [ ] **Step 2: Run Python unit and benchmark suites**

Run:

```bash
cd native-lib/python && python3 -m pytest tests/unit -q
cd benchmarks/runners/python && python3 -m unittest test_bench
```

Expected: all tests PASS.

- [ ] **Step 3: Run Node type checking, unit tests, and benchmark suites**

Run:

```bash
cd native-lib/node && npm run build:ts
cd native-lib/node && npm run test:unit
cd benchmarks && node --test lib/stats.test.mjs runners/node/*.test.mjs
```

Expected: all commands PASS.

- [ ] **Step 4: Run non-binding Java native-lib tests**

Run:

```bash
./gradlew native-lib:test -PskipNodeTests=true -PskipPythonTests=true
```

Expected: PASS.

- [ ] **Step 5: Build the native library and run Python native tests**

With `GRAALVM_HOME` and `JAVA_HOME` pointing to GraalVM Community Java 24, run:

```bash
./gradlew native-lib:nativeCompile
./gradlew native-lib:pythonTest
```

Expected: native-image build and Python binding tests PASS.

- [ ] **Step 6: Run Node native integration and TCK lanes**

Run:

```bash
cd native-lib/node && npm run build
cd native-lib/node && npm run test:integration
./gradlew native-lib:stageTckSuites
cd native-lib/node && npm run test:tck
```

Expected: TypeScript/addon build, integration tests, and TCK PASS, subject only to documented corpus/toolchain availability.

- [ ] **Step 7: Inspect status and diff for accidental loss or unrelated edits**

Run:

```bash
git status --short
git diff --check
git diff --stat master...HEAD
git diff master...HEAD -- native-lib/python/src/dataweave/__init__.py native-lib/node/src/dataweave.ts native-lib/node/src/index.ts
```

Expected: no whitespace errors; singleton implementation is absent; explicit-instance cleanup hardening remains; no unrelated user changes are included.

- [ ] **Step 8: Commit verification-driven corrections if necessary**

If verification required source changes, stage only those files and commit:

```bash
git add <exact-files-fixed>
git commit -m "fix: complete explicit runtime migration"
```

If no corrections were needed, do not create an empty commit.
