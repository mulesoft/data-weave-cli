# Task 6 Report: Migrate benchmark wrappers and tests

## Outcome

- Node wrapper loading now requires an exported `DataWeave` constructor for ESM and default/CommonJS-shaped modules.
- Node warm and streaming benchmarks consume one caller-owned initialized runtime.
- Node emitter preserves cold rows first, initializes once before warm measurements, and awaits cleanup after all measured loops.
- Node cold-start children await runtime cleanup before exiting.
- Python wrapper loading and fixtures require `DataWeave` without stale package-level execution functions; existing benchmark runtime ownership is unchanged.

## TDD Evidence

RED:

- Node targeted tests failed because `loadWrapper()` still required `run()` and accepted a module without `DataWeave`.
- Python targeted tests failed because `load_wrapper()` accepted a module without `DataWeave`.

GREEN:

- `cd benchmarks && node --test runners/node/wrapper.test.mjs runners/node/warm-bench.test.mjs`: 6 passed.
- `cd benchmarks/runners/python && python3 -m unittest test_bench`: 25 passed.

## Final Verification

- `cd benchmarks && node --test lib/stats.test.mjs runners/node/*.test.mjs`: 13 passed.
- `cd benchmarks/runners/python && python3 -m unittest test_bench`: 25 passed.
- `git diff --check`: passed.

## Self-Review

- No singleton or implicit runtime was introduced.
- Initialization and cleanup remain outside warm and streaming timing samples.
- Cold-start collection still precedes warm runtime creation.
- Python emitter and cold-start runtime ownership were not changed.
- Changes are limited to the eight Task 6 benchmark files; unrelated documentation changes remain untouched and unstaged.
