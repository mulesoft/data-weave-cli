# DataWeave native-lib benchmarks

Language-agnostic benchmark harness for the DataWeave native-lib wrappers.

## Layout

- `corpus/` — shared benchmark cases: `manifest.json` (the contract), `scripts/*.dwl`,
  `inputs/` (committed small inputs), `inputs/generated/` (regenerated large inputs),
  `gen-inputs.mjs` (deterministic generator).
- `schema/result.schema.json` — the JSON schema every runner's output conforms to.
- `lib/` — dependency-free shared modules (stats, manifest, env).
- `runners/node/` — the Node reference runner. `runners/engine/` is the JVM baseline
  (Scala/Gradle subproject `:benchmarks-engine`, depends on `org.mule.weave:runtime` at
  the same `weaveVersion` the native image is built from). `runners/python/` is a follow-up.
- `report/report.mjs` — joins result files against the manifest and prints a comparison table.
- `results/` — gitignored per-run output.

## Metrics

`cold-start` and `first-run` (fresh process per sample), `warm` (in-process steady state),
`streaming` (MB/s). Each case declares which apply via `metrics[]`.

## Running

    ./gradlew native-lib:benchmark -Pbenchmark=true      # build wrapper, run, report

Or directly, once the wrapper is built (`./gradlew native-lib:buildNodePackage`):

    node runners/node/emit.mjs                            # writes results/node-<ts>.json
    node report/report.mjs results/*.json                 # renders the table

Generate large inputs first (idempotent):

    node corpus/gen-inputs.mjs

Run the engine (JVM) baseline and let the report pick it up as the comparison anchor:

    ./gradlew benchmarks-engine:benchmarkEngine -Pbenchmark=true   # writes results/engine-<ts>.json
    node report/report.mjs results/*.json                          # engine is auto-selected as baseline

Results are local-only; no history is accumulated (see the design spec).
