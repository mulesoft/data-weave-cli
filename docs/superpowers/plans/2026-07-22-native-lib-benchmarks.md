# Native-lib Wrapper Benchmarks Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a benchmark harness for the DataWeave native-lib Node wrapper that measures cold-start, first-run, warm steady-state, and streaming throughput, emits results in a shared JSON schema, and renders a comparison report — designed so Python and the Scala engine plug in later unchanged.

**Architecture:** A language-agnostic corpus (`.dwl` scripts + inputs + `manifest.json`) is the contract. Dependency-free ESM modules under `benchmarks/lib/` hold shared logic (stats, manifest, env). The Node runner under `benchmarks/runners/node/` consumes the corpus, times the four metrics, and emits schema-conformant JSON validated by case `id`. A `report/report.mjs` joins result files against the manifest and prints a comparison table with a weave-version skew banner. Gradle exposes an opt-in `native-lib:benchmark` task.

**Tech Stack:** Node.js ≥18 (ESM `.mjs`, zero new npm deps), built-in `node:test` for unit tests, `process.hrtime.bigint()` for timing, the existing `@dataweave/native` wrapper (built to `native-lib/node/dist`), Gradle.

## Global Constraints

- **No new npm dependencies.** Use only Node built-ins (`node:test`, `node:assert/strict`, `node:fs`, `node:child_process`, `node:os`, `node:path`, `node:url`) and the already-built `@dataweave/native` wrapper.
- **Timing methodology (applies to every metric):** measure with `process.hrtime.bigint()`; convert to ms as `Number(endNs - startNs) / 1e6`. This is the methodology a future engine harness must mirror, so keep it identical everywhere.
- **`id` is the immutable join key.** Runners emit case `id` verbatim from the manifest; never invent or rename ids. Deprecate, never rename.
- **Fail-fast on orphan ids.** A runner MUST abort before writing output if it emits any `id` not present in the manifest.
- **Explicit metrics.** Each manifest case declares `metrics[]` from exactly `["cold-start","first-run","warm","streaming"]`. A runner runs only the metrics a case declares.
- **`env.weaveVersion` is mandatory** in every result file, read from `gradle.properties` (`weaveVersion=`, currently `2.12.0-20260413`). `env.commit` and `env.dwlibBuildId` are also mandatory (best-effort values allowed) for future attributable history.
- **Metric refinement vs. spec (approved deviation):** `cold-start` **and** `first-run` are measured by the spawn harness (fresh process per sample → cold isolate + cold compilation); `warm` and `streaming` are measured in-process. The spec placed first-run in the in-process runner; measuring it fresh yields a real cold-compilation distribution.
- **Results are local-only.** `benchmarks/results/` and generated inputs are gitignored; only corpus, schema, lib, runners, report, and README are committed.
- **Units are per-row:** `ms` for latency metrics (cold-start, first-run, warm), `MB/s` for streaming.

---

### Task 1: Scaffolding, schema, `.gitignore`, README

**Files:**
- Create: `benchmarks/.gitignore`
- Create: `benchmarks/README.md`
- Create: `benchmarks/schema/result.schema.json`
- Create: `benchmarks/schema/schema.test.mjs`

**Interfaces:**
- Consumes: nothing.
- Produces: `benchmarks/schema/result.schema.json` — the JSON Schema (draft-07) that every runner's output validates against. Consumers reference it as documentation; runtime validation of ids happens in code (Task 3/6).

- [ ] **Step 1: Write the failing test**

Create `benchmarks/schema/schema.test.mjs`:

```js
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));

test("result schema is valid JSON with the required shape", () => {
  const schema = JSON.parse(readFileSync(join(__dirname, "result.schema.json"), "utf-8"));
  assert.equal(schema.$schema, "http://json-schema.org/draft-07/schema#");
  const props = schema.properties;
  for (const key of ["schemaVersion", "runner", "env", "timestamp", "cases"]) {
    assert.ok(props[key], `schema must declare property ${key}`);
  }
  const envProps = props.env.properties;
  for (const key of ["os", "cpu", "runtimeVersion", "weaveVersion", "commit", "dwlibBuildId"]) {
    assert.ok(envProps[key], `env must declare ${key}`);
  }
  const metricEnum = props.cases.items.properties.metric.enum;
  assert.deepEqual(metricEnum.sort(), ["cold-start", "first-run", "streaming", "warm"]);
  const unitEnum = props.cases.items.properties.unit.enum;
  assert.deepEqual(unitEnum.sort(), ["MB/s", "ms"]);
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `node --test benchmarks/schema/`
Expected: FAIL — `ENOENT` opening `result.schema.json`.

- [ ] **Step 3: Create the schema**

Create `benchmarks/schema/result.schema.json`:

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "DataWeave benchmark result",
  "type": "object",
  "required": ["schemaVersion", "runner", "env", "timestamp", "cases"],
  "additionalProperties": false,
  "properties": {
    "schemaVersion": { "const": "1.0" },
    "runner": { "type": "string" },
    "timestamp": { "type": "string" },
    "env": {
      "type": "object",
      "required": ["os", "cpu", "runtimeVersion", "weaveVersion", "commit", "dwlibBuildId"],
      "additionalProperties": true,
      "properties": {
        "os": { "type": "string" },
        "cpu": { "type": "string" },
        "runtimeVersion": { "type": "string" },
        "weaveVersion": { "type": "string" },
        "commit": { "type": "string" },
        "dwlibBuildId": { "type": "string" }
      }
    },
    "cases": {
      "type": "array",
      "items": {
        "type": "object",
        "required": ["id", "metric", "unit", "stats", "iterations"],
        "additionalProperties": false,
        "properties": {
          "id": { "type": "string" },
          "metric": { "enum": ["cold-start", "first-run", "warm", "streaming"] },
          "unit": { "enum": ["ms", "MB/s"] },
          "iterations": { "type": "integer" },
          "stats": {
            "type": "object",
            "required": ["median"],
            "additionalProperties": false,
            "properties": {
              "min": { "type": "number" },
              "median": { "type": "number" },
              "p90": { "type": "number" },
              "p99": { "type": "number" },
              "mean": { "type": "number" }
            }
          }
        }
      }
    }
  }
}
```

- [ ] **Step 4: Create `.gitignore` and README**

Create `benchmarks/.gitignore`:

```gitignore
# Ephemeral per-run result files
results/
# Deterministically regenerated large inputs
corpus/inputs/generated/
```

Create `benchmarks/README.md`:

```markdown
# DataWeave native-lib benchmarks

Language-agnostic benchmark harness for the DataWeave native-lib wrappers.

## Layout

- `corpus/` — shared benchmark cases: `manifest.json` (the contract), `scripts/*.dwl`,
  `inputs/` (committed small inputs), `inputs/generated/` (regenerated large inputs),
  `gen-inputs.mjs` (deterministic generator).
- `schema/result.schema.json` — the JSON schema every runner's output conforms to.
- `lib/` — dependency-free shared modules (stats, manifest, env).
- `runners/node/` — the Node reference runner. `runners/python/` and `runners/engine/`
  are follow-ups; the engine harness lives in the `data-weave` repo but reads this corpus.
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

Results are local-only; no history is accumulated (see the design spec).
```

- [ ] **Step 5: Run test to verify it passes**

Run: `node --test benchmarks/schema/`
Expected: PASS (1 test).

- [ ] **Step 6: Commit**

```bash
git add benchmarks/.gitignore benchmarks/README.md benchmarks/schema/
git commit -m "W-23545283: Scaffold benchmarks dir with result JSON schema"
```

---

### Task 2: Stats library (`lib/stats.mjs`)

**Files:**
- Create: `benchmarks/lib/stats.mjs`
- Create: `benchmarks/lib/stats.test.mjs`

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `computeStats(samples: number[]): { min, median, p90, p99, mean }` — throws on an empty/non-array input.
  - `toMBps(totalBytes: number, elapsedMs: number): number` — throughput in megabytes/sec (bytes ÷ 1e6 ÷ seconds).

- [ ] **Step 1: Write the failing test**

Create `benchmarks/lib/stats.test.mjs`:

```js
import { test } from "node:test";
import assert from "node:assert/strict";
import { computeStats, toMBps } from "./stats.mjs";

test("computeStats returns min/median/p90/p99/mean", () => {
  const s = computeStats([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
  assert.equal(s.min, 1);
  assert.equal(s.mean, 5.5);
  assert.equal(s.median, 5);
  assert.equal(s.p90, 9);
  assert.equal(s.p99, 10);
});

test("computeStats handles a single sample", () => {
  const s = computeStats([42]);
  assert.deepEqual(s, { min: 42, median: 42, p90: 42, p99: 42, mean: 42 });
});

test("computeStats rejects empty input", () => {
  assert.throws(() => computeStats([]), /non-empty/);
  assert.throws(() => computeStats(null), /non-empty/);
});

test("toMBps converts bytes and elapsed to MB/s", () => {
  // 10 MB in 1000 ms => 10 MB/s
  assert.equal(toMBps(10_000_000, 1000), 10);
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `node --test benchmarks/lib/stats.test.mjs`
Expected: FAIL — cannot find module `./stats.mjs`.

- [ ] **Step 3: Write the implementation**

Create `benchmarks/lib/stats.mjs`:

```js
/**
 * Aggregate a list of samples into min/median/p90/p99/mean.
 * Percentiles use the nearest-rank method on a sorted copy.
 * @param {number[]} samples
 * @returns {{min:number, median:number, p90:number, p99:number, mean:number}}
 */
export function computeStats(samples) {
  if (!Array.isArray(samples) || samples.length === 0) {
    throw new Error("computeStats requires a non-empty array of numbers");
  }
  const sorted = [...samples].sort((a, b) => a - b);
  const n = sorted.length;
  const pct = (p) => sorted[Math.min(n - 1, Math.max(0, Math.ceil((p / 100) * n) - 1))];
  const sum = sorted.reduce((a, b) => a + b, 0);
  return { min: sorted[0], median: pct(50), p90: pct(90), p99: pct(99), mean: sum / n };
}

/**
 * Throughput in megabytes per second (decimal MB, i.e. 1e6 bytes).
 * @param {number} totalBytes
 * @param {number} elapsedMs
 * @returns {number}
 */
export function toMBps(totalBytes, elapsedMs) {
  if (elapsedMs <= 0) throw new Error("elapsedMs must be > 0");
  return totalBytes / 1e6 / (elapsedMs / 1000);
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `node --test benchmarks/lib/stats.test.mjs`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add benchmarks/lib/stats.mjs benchmarks/lib/stats.test.mjs
git commit -m "W-23545283: Add stats lib (percentiles + throughput)"
```

---

### Task 3: Corpus, generator, and manifest library (`lib/manifest.mjs`)

**Files:**
- Create: `benchmarks/lib/manifest.mjs`
- Create: `benchmarks/lib/manifest.test.mjs`
- Create: `benchmarks/corpus/manifest.json`
- Create: `benchmarks/corpus/scripts/trivial.dwl`
- Create: `benchmarks/corpus/scripts/object-transform.dwl`
- Create: `benchmarks/corpus/scripts/map-scale.dwl`
- Create: `benchmarks/corpus/scripts/xml-to-csv.dwl`
- Create: `benchmarks/corpus/scripts/json-stream.dwl`
- Create: `benchmarks/corpus/scripts/compile-heavy.dwl`
- Create: `benchmarks/corpus/inputs/person.xml`
- Create: `benchmarks/corpus/gen-inputs.mjs`

**Interfaces:**
- Consumes: `METRICS` concept from the Global Constraints.
- Produces:
  - `METRICS: string[]` — `["cold-start","first-run","warm","streaming"]`.
  - `loadManifest(corpusDir: string): { corpusDir, cases: Case[], ids: Set<string> }` — validates each case (unique id, non-empty metrics from `METRICS`, script file exists, non-generated input files exist) and throws on any violation.
  - `casesForMetric(manifest, metric: string): Case[]` — cases whose `metrics[]` includes `metric`.
  - `resolveInputs(manifest, caseObj): Record<string,{buffer:Buffer, mimeType:string, charset?:string}>` — reads each input file into a Buffer.
  - `validateResultIds(manifest, resultCases: {id:string}[]): void` — throws on any orphan id (fail-fast).
  - `Case` shape: `{ id, script, inputs?: Record<string,{file, mimeType, charset?, generated?}>, metrics: string[], iterations?: { warm?, warmup?, streaming?, samples? } }`.

- [ ] **Step 1: Write the failing test**

Create `benchmarks/lib/manifest.test.mjs`:

```js
import { test } from "node:test";
import assert from "node:assert/strict";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import {
  METRICS,
  loadManifest,
  casesForMetric,
  validateResultIds,
} from "./manifest.mjs";

const __dirname = dirname(fileURLToPath(import.meta.url));
const CORPUS = join(__dirname, "..", "corpus");

test("METRICS lists exactly the four metrics", () => {
  assert.deepEqual([...METRICS].sort(), ["cold-start", "first-run", "streaming", "warm"]);
});

test("loadManifest validates the committed corpus", () => {
  const m = loadManifest(CORPUS);
  assert.ok(m.cases.length >= 6, "expected at least 6 corpus cases");
  assert.ok(m.ids.has("trivial"));
});

test("casesForMetric filters by declared metric", () => {
  const m = loadManifest(CORPUS);
  const streaming = casesForMetric(m, "streaming");
  assert.ok(streaming.every((c) => c.metrics.includes("streaming")));
  assert.ok(streaming.length >= 1);
});

test("validateResultIds throws on an orphan id", () => {
  const m = loadManifest(CORPUS);
  assert.throws(() => validateResultIds(m, [{ id: "does-not-exist" }]), /orphan id/);
  assert.doesNotThrow(() => validateResultIds(m, [{ id: "trivial" }]));
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `node --test benchmarks/lib/manifest.test.mjs`
Expected: FAIL — cannot find module `./manifest.mjs`.

- [ ] **Step 3: Write the manifest library**

Create `benchmarks/lib/manifest.mjs`:

```js
import { readFileSync, existsSync } from "node:fs";
import { join } from "node:path";

/** The only metrics a case may declare. */
export const METRICS = ["cold-start", "first-run", "warm", "streaming"];

/**
 * Load and validate corpus/manifest.json.
 * @param {string} corpusDir absolute path to the corpus directory
 */
export function loadManifest(corpusDir) {
  const raw = JSON.parse(readFileSync(join(corpusDir, "manifest.json"), "utf-8"));
  if (!Array.isArray(raw.cases)) throw new Error("manifest.cases must be an array");
  const ids = new Set();
  for (const c of raw.cases) {
    if (!c.id) throw new Error("manifest case is missing an id");
    if (ids.has(c.id)) throw new Error(`duplicate case id: ${c.id}`);
    ids.add(c.id);
    if (!Array.isArray(c.metrics) || c.metrics.length === 0) {
      throw new Error(`case ${c.id} must declare a non-empty metrics[]`);
    }
    for (const metric of c.metrics) {
      if (!METRICS.includes(metric)) throw new Error(`case ${c.id} has unknown metric: ${metric}`);
    }
    if (!c.script || !existsSync(join(corpusDir, c.script))) {
      throw new Error(`case ${c.id} script not found: ${c.script}`);
    }
    for (const [name, inp] of Object.entries(c.inputs ?? {})) {
      if (inp.file && !inp.generated && !existsSync(join(corpusDir, inp.file))) {
        throw new Error(`case ${c.id} input '${name}' file not found: ${inp.file}`);
      }
    }
  }
  return { corpusDir, cases: raw.cases, ids };
}

/** Cases whose declared metrics[] includes `metric`. */
export function casesForMetric(manifest, metric) {
  return manifest.cases.filter((c) => c.metrics.includes(metric));
}

/**
 * Read a case's declared inputs into Buffers.
 * @returns {Record<string,{buffer:Buffer, mimeType:string, charset?:string}>}
 */
export function resolveInputs(manifest, caseObj) {
  const out = {};
  for (const [name, inp] of Object.entries(caseObj.inputs ?? {})) {
    const buffer = readFileSync(join(manifest.corpusDir, inp.file));
    out[name] = { buffer, mimeType: inp.mimeType, charset: inp.charset };
  }
  return out;
}

/** Fail-fast: throw if any result case carries an id not present in the manifest. */
export function validateResultIds(manifest, resultCases) {
  for (const rc of resultCases) {
    if (!manifest.ids.has(rc.id)) {
      throw new Error(`result contains orphan id not in manifest: ${rc.id}`);
    }
  }
}
```

- [ ] **Step 4: Create the corpus scripts**

Create `benchmarks/corpus/scripts/trivial.dwl`:

```dataweave
2 + 2
```

Create `benchmarks/corpus/scripts/object-transform.dwl`:

```dataweave
output application/json
---
{
  fullName: payload.first ++ " " ++ payload.last,
  adult: payload.age >= 18,
  initials: payload.first[0] ++ payload.last[0]
}
```

Create `benchmarks/corpus/scripts/map-scale.dwl`:

```dataweave
output application/json
---
payload map (item) -> { id: item.id, doubled: item.value * 2, label: "item_" ++ item.id }
```

Create `benchmarks/corpus/scripts/xml-to-csv.dwl`:

```dataweave
output application/csv header=true
---
[payload.person]
```

Create `benchmarks/corpus/scripts/json-stream.dwl`:

```dataweave
output application/json
---
payload map (item) -> { id: item.id, name: item.name }
```

Create `benchmarks/corpus/scripts/compile-heavy.dwl`:

```dataweave
output application/json
---
{
  a: (1 to 100) reduce ((i, acc = 0) -> acc + i),
  b: [1, 2, 3, 4, 5] map ($ * 2) filter ($ > 4) reduce ((i, acc = 0) -> acc + i),
  c: { x: 1, y: 2, z: 3 } mapObject (v, k) -> { (k): v * 10 },
  d: "hello world" splitBy " " map upper($),
  e: (1 to 50) map (n) -> { n: n, sq: n * n, even: (n mod 2) == 0 }
}
```

- [ ] **Step 5: Create the committed small input**

Create `benchmarks/corpus/inputs/person.xml` (UTF-8; the case declares its charset):

```xml
<?xml version="1.0" encoding="UTF-8"?>
<person>
  <name>Billy</name>
  <age>31</age>
</person>
```

- [ ] **Step 6: Create the input generator**

Create `benchmarks/corpus/gen-inputs.mjs`:

```js
// Deterministically regenerate large inputs. No randomness -> comparable across
// machines and runners. Size overridable via BENCH_LARGE_N (default 50000).
import { writeFileSync, mkdirSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const outDir = join(__dirname, "inputs", "generated");
mkdirSync(outDir, { recursive: true });

const n = Number(process.env.BENCH_LARGE_N ?? 50000);
const records = [];
for (let i = 1; i <= n; i++) {
  records.push({ id: i, name: `item_${i}`, value: i * 3 });
}
const path = join(outDir, "records-large.json");
writeFileSync(path, JSON.stringify(records));
console.log(`wrote ${n} records to ${path}`);
```

- [ ] **Step 7: Create the manifest**

Create `benchmarks/corpus/manifest.json`:

```json
{
  "cases": [
    {
      "id": "trivial",
      "script": "scripts/trivial.dwl",
      "metrics": ["cold-start", "first-run", "warm"],
      "iterations": { "warm": 200, "warmup": 20, "samples": 30 }
    },
    {
      "id": "object-transform",
      "script": "scripts/object-transform.dwl",
      "inputs": {
        "payload": { "file": "inputs/person-record.json", "mimeType": "application/json" }
      },
      "metrics": ["first-run", "warm"],
      "iterations": { "warm": 200, "warmup": 20, "samples": 20 }
    },
    {
      "id": "map-scale",
      "script": "scripts/map-scale.dwl",
      "inputs": {
        "payload": { "file": "inputs/generated/records-large.json", "mimeType": "application/json", "generated": true }
      },
      "metrics": ["first-run", "warm", "streaming"],
      "iterations": { "warm": 30, "warmup": 3, "streaming": 10, "samples": 15 }
    },
    {
      "id": "xml-to-csv",
      "script": "scripts/xml-to-csv.dwl",
      "inputs": {
        "payload": { "file": "inputs/person.xml", "mimeType": "application/xml", "charset": "UTF-8" }
      },
      "metrics": ["first-run", "warm"],
      "iterations": { "warm": 100, "warmup": 10, "samples": 20 }
    },
    {
      "id": "json-stream",
      "script": "scripts/json-stream.dwl",
      "inputs": {
        "payload": { "file": "inputs/generated/records-large.json", "mimeType": "application/json", "generated": true }
      },
      "metrics": ["first-run", "warm", "streaming"],
      "iterations": { "warm": 30, "warmup": 3, "streaming": 10, "samples": 15 }
    },
    {
      "id": "compile-heavy",
      "script": "scripts/compile-heavy.dwl",
      "metrics": ["first-run", "warm"],
      "iterations": { "warm": 100, "warmup": 10, "samples": 20 }
    }
  ]
}
```

- [ ] **Step 8: Create the committed `person-record.json` input**

The `object-transform` case references `inputs/person-record.json`. Create `benchmarks/corpus/inputs/person-record.json`:

```json
{ "first": "Ada", "last": "Lovelace", "age": 36 }
```

- [ ] **Step 9: Run tests to verify they pass**

Run: `node --test benchmarks/lib/manifest.test.mjs`
Expected: PASS (4 tests). Non-generated inputs (`person.xml`, `person-record.json`) exist; the generated `records-large.json` is marked `"generated": true` so validation does not require it yet.

- [ ] **Step 10: Commit**

```bash
git add benchmarks/lib/manifest.mjs benchmarks/lib/manifest.test.mjs benchmarks/corpus/
git commit -m "W-23545283: Add corpus, input generator, and manifest lib"
```

---

### Task 4: Environment stamp (`lib/env.mjs`)

**Files:**
- Create: `benchmarks/lib/env.mjs`
- Create: `benchmarks/lib/env.test.mjs`

**Interfaces:**
- Consumes: repo `gradle.properties` (weave version), git (commit), the staged `dwlib` file (build id).
- Produces: `gatherEnv({ runner: string, runtimeVersion: string }): { os, cpu, runtimeVersion, weaveVersion, commit, dwlibBuildId }`. Reads the repo root internally (three levels up from `benchmarks/lib/` is the repo root's parent — see code; repo root is two levels up).

- [ ] **Step 1: Write the failing test**

Create `benchmarks/lib/env.test.mjs`:

```js
import { test } from "node:test";
import assert from "node:assert/strict";
import { gatherEnv } from "./env.mjs";

test("gatherEnv returns all required fields", () => {
  const env = gatherEnv({ runner: "node-wrapper", runtimeVersion: "node vX" });
  for (const key of ["os", "cpu", "runtimeVersion", "weaveVersion", "commit", "dwlibBuildId"]) {
    assert.ok(env[key] !== undefined && env[key] !== "", `env.${key} must be set`);
  }
});

test("gatherEnv reads the pinned weaveVersion from gradle.properties", () => {
  const env = gatherEnv({ runner: "node-wrapper", runtimeVersion: "node vX" });
  // gradle.properties pins e.g. 2.12.0-YYYYMMDD; assert it looks like a weave version.
  assert.match(env.weaveVersion, /^\d+\.\d+\.\d+/);
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `node --test benchmarks/lib/env.test.mjs`
Expected: FAIL — cannot find module `./env.mjs`.

- [ ] **Step 3: Write the implementation**

Create `benchmarks/lib/env.mjs`:

```js
import { readFileSync, existsSync, createReadStream } from "node:fs";
import { execSync } from "node:child_process";
import { createHash } from "node:crypto";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import os from "node:os";

const __dirname = dirname(fileURLToPath(import.meta.url));
// benchmarks/lib -> benchmarks -> repo root
const REPO_ROOT = join(__dirname, "..", "..");

function readWeaveVersion() {
  const txt = readFileSync(join(REPO_ROOT, "gradle.properties"), "utf-8");
  const m = txt.match(/^weaveVersion=(.+)$/m);
  if (!m) throw new Error("weaveVersion not found in gradle.properties");
  return m[1].trim();
}

function readCommit() {
  try {
    return execSync("git rev-parse --short HEAD", { cwd: REPO_ROOT }).toString().trim();
  } catch {
    return "unknown";
  }
}

// Best-effort identity of the staged dwlib: first 8 hex of a sha256 over
// (size + first 64KB). Cheap, stable, and enough to detect a lib swap.
function readDwlibBuildId() {
  const base = join(REPO_ROOT, "native-lib", "node", "native");
  for (const ext of [".dylib", ".so", ".dll"]) {
    const p = join(base, `dwlib${ext}`);
    if (existsSync(p)) {
      const buf = readFileSync(p).subarray(0, 65536);
      const { size } = statSafe(p);
      return "dwlib-" + createHash("sha256").update(String(size)).update(buf).digest("hex").slice(0, 8);
    }
  }
  return "unknown";
}

function statSafe(p) {
  // eslint-disable-next-line
  const { statSync } = require("node:fs");
  return statSync(p);
}

/**
 * @param {{runner:string, runtimeVersion:string}} opts
 */
export function gatherEnv({ runner, runtimeVersion }) {
  const cpus = os.cpus();
  return {
    runner,
    os: `${process.platform}-${process.arch}`,
    cpu: cpus.length ? cpus[0].model : "unknown",
    runtimeVersion,
    weaveVersion: readWeaveVersion(),
    commit: readCommit(),
    dwlibBuildId: readDwlibBuildId(),
  };
}
```

> Note: `require` is not available in ESM. Replace the `statSafe` helper with a top-level import. Use this corrected version of the two relevant lines instead:
>
> add to the imports at the top: `import { statSync } from "node:fs";`
> and replace `readDwlibBuildId`'s `statSafe(p)` call and the helper with a direct `statSync(p)`:
>
> ```js
> const size = statSync(p).size;
> return "dwlib-" + createHash("sha256").update(String(size)).update(buf).digest("hex").slice(0, 8);
> ```
>
> Delete the `statSafe` function entirely. (Implement env.mjs with the corrected imports — do not ship the `require` form.)

- [ ] **Step 4: Run test to verify it passes**

Run: `node --test benchmarks/lib/env.test.mjs`
Expected: PASS (2 tests). `dwlibBuildId` will be `"unknown"` until the wrapper is built and `dwlib` staged — the test only asserts it is non-empty, which `"unknown"` satisfies.

- [ ] **Step 5: Commit**

```bash
git add benchmarks/lib/env.mjs benchmarks/lib/env.test.mjs
git commit -m "W-23545283: Add env stamp (weaveVersion, commit, dwlibBuildId)"
```

---

### Task 5: Wrapper resolution + in-process warm/streaming runner

**Files:**
- Create: `benchmarks/runners/node/wrapper.mjs`
- Create: `benchmarks/runners/node/warm-bench.mjs`
- Create: `benchmarks/runners/node/warm-bench.test.mjs`

**Prerequisite:** the Node wrapper must be built (`dist/` + addon + staged `dwlib`). Run `./gradlew native-lib:buildNodePackage` (or, in `native-lib/node`: `npm install && npx node-gyp rebuild && npx tsc`) before running the integration test in this task. The generated large input must exist: `node benchmarks/corpus/gen-inputs.mjs`.

**Interfaces:**
- Consumes: `loadManifest`, `casesForMetric`, `resolveInputs` (Task 3); `computeStats`, `toMBps` (Task 2).
- Produces:
  - `loadWrapper(): Promise<{ run, runStreaming, runTransform, DataWeave, cleanup }>` — imports the built wrapper `dist/index.js`; throws a clear "not built" error otherwise.
  - `runWarmAndStreaming(api, manifest): Promise<ResultCase[]>` where `ResultCase = { id, metric, unit, stats, iterations }`. Emits `warm` (unit `ms`) and `streaming` (unit `MB/s`) rows for cases declaring those metrics.

- [ ] **Step 1: Write the wrapper resolver**

Create `benchmarks/runners/node/wrapper.mjs`:

```js
import { existsSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));
// benchmarks/runners/node -> benchmarks/runners -> benchmarks -> repo root
const REPO_ROOT = join(__dirname, "..", "..", "..");
const WRAPPER_DIST = join(REPO_ROOT, "native-lib", "node", "dist", "index.js");

/**
 * Import the built @dataweave/native wrapper. The wrapper locates dwlib itself
 * (staged at native-lib/node/native/dwlib.*), so no env var is required here.
 */
export async function loadWrapper() {
  if (!existsSync(WRAPPER_DIST)) {
    throw new Error(
      `Node wrapper not built at ${WRAPPER_DIST}. ` +
        `Run: ./gradlew native-lib:buildNodePackage`
    );
  }
  const mod = await import(pathToFileURL(WRAPPER_DIST).href);
  const api = mod.run ? mod : mod.default;
  if (!api || typeof api.run !== "function") {
    throw new Error(`Wrapper at ${WRAPPER_DIST} did not export a run() function`);
  }
  return api;
}
```

- [ ] **Step 2: Write the warm/streaming runner**

Create `benchmarks/runners/node/warm-bench.mjs`:

```js
import { casesForMetric, resolveInputs } from "../../lib/manifest.mjs";
import { computeStats, toMBps } from "../../lib/stats.mjs";

const nowNs = () => process.hrtime.bigint();
const msSince = (start) => Number(nowNs() - start) / 1e6;

/** Build the wrapper Inputs map from resolved corpus inputs. */
function toInputsMap(resolved) {
  const inputs = {};
  for (const [name, v] of Object.entries(resolved)) {
    inputs[name] = { content: v.buffer, mimeType: v.mimeType, charset: v.charset ?? "utf-8" };
  }
  return inputs;
}

/** Split a Buffer into fixed-size chunks for streaming input. */
function* chunked(buffer, size = 65536) {
  for (let i = 0; i < buffer.length; i += size) yield buffer.subarray(i, i + size);
}

async function drain(gen) {
  let total = 0;
  let step = await gen.next();
  while (!step.done) {
    total += step.value.length;
    step = await gen.next();
  }
  if (!step.value.success) throw new Error(`stream failed: ${step.value.error}`);
  return total;
}

/**
 * @returns {Promise<Array<{id,metric,unit,stats,iterations}>>}
 */
export async function runWarmAndStreaming(api, manifest) {
  const rows = [];

  for (const c of casesForMetric(manifest, "warm")) {
    const script = readScript(manifest, c);
    const inputs = toInputsMap(resolveInputs(manifest, c));
    const warmup = c.iterations?.warmup ?? 10;
    const iters = c.iterations?.warm ?? 100;

    for (let i = 0; i < warmup; i++) assertOk(api.run(script, inputs));
    const samples = [];
    for (let i = 0; i < iters; i++) {
      const start = nowNs();
      assertOk(api.run(script, inputs));
      samples.push(msSince(start));
    }
    rows.push({ id: c.id, metric: "warm", unit: "ms", stats: computeStats(samples), iterations: iters });
  }

  for (const c of casesForMetric(manifest, "streaming")) {
    const script = readScript(manifest, c);
    const resolved = resolveInputs(manifest, c);
    const [primaryName, primary] = Object.entries(resolved)[0];
    const iters = c.iterations?.streaming ?? 10;

    const mbps = [];
    for (let i = 0; i < iters; i++) {
      const start = nowNs();
      const gen = api.runTransform(script, chunked(primary.buffer), {
        inputName: primaryName,
        mimeType: primary.mimeType,
        charset: primary.charset,
      });
      await drain(gen);
      mbps.push(toMBps(primary.buffer.length, msSince(start)));
    }
    rows.push({ id: c.id, metric: "streaming", unit: "MB/s", stats: computeStats(mbps), iterations: iters });
  }

  return rows;
}

function assertOk(result) {
  if (!result.success) throw new Error(`run failed: ${result.error}`);
  return result;
}

function readScript(manifest, c) {
  // Local import to keep the module dependency-light and avoid a top-level fs import.
  const { readFileSync } = require("node:fs");
  const { join } = require("node:path");
  return readFileSync(join(manifest.corpusDir, c.script), "utf-8");
}
```

> Note: `require` is unavailable in ESM. Implement `readScript` with top-level imports instead — add `import { readFileSync } from "node:fs";` and `import { join } from "node:path";` at the top of `warm-bench.mjs`, and make `readScript` just `return readFileSync(join(manifest.corpusDir, c.script), "utf-8");`. Do not ship the `require` form.

- [ ] **Step 3: Write the integration test**

Create `benchmarks/runners/node/warm-bench.test.mjs`:

```js
import { test } from "node:test";
import assert from "node:assert/strict";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { loadManifest } from "../../lib/manifest.mjs";
import { loadWrapper } from "./wrapper.mjs";
import { runWarmAndStreaming } from "./warm-bench.mjs";

const __dirname = dirname(fileURLToPath(import.meta.url));
const CORPUS = join(__dirname, "..", "..", "corpus");

test("warm + streaming rows are produced with valid stats", async () => {
  const api = await loadWrapper();
  try {
    const manifest = loadManifest(CORPUS);
    const rows = await runWarmAndStreaming(api, manifest);

    const warm = rows.filter((r) => r.metric === "warm");
    const streaming = rows.filter((r) => r.metric === "streaming");
    assert.ok(warm.length >= 1, "expected at least one warm row");
    assert.ok(streaming.length >= 1, "expected at least one streaming row");

    for (const r of warm) {
      assert.equal(r.unit, "ms");
      assert.ok(r.stats.median >= 0);
      assert.ok(r.stats.p99 >= r.stats.median);
    }
    for (const r of streaming) {
      assert.equal(r.unit, "MB/s");
      assert.ok(r.stats.median > 0);
    }
  } finally {
    api.cleanup();
  }
});
```

- [ ] **Step 4: Build wrapper + generate input, then run the test**

Run:
```bash
./gradlew native-lib:buildNodePackage
node benchmarks/corpus/gen-inputs.mjs
node --test benchmarks/runners/node/warm-bench.test.mjs
```
Expected: PASS (1 test). If it fails with "Node wrapper not built", the gradle build did not produce `native-lib/node/dist/index.js` — re-run the build step.

- [ ] **Step 5: Commit**

```bash
git add benchmarks/runners/node/wrapper.mjs benchmarks/runners/node/warm-bench.mjs benchmarks/runners/node/warm-bench.test.mjs
git commit -m "W-23545283: Add Node warm + streaming benchmark runner"
```

---

### Task 6: Cold-start + first-run spawn harness

**Files:**
- Create: `benchmarks/runners/node/coldstart-child.mjs`
- Create: `benchmarks/runners/node/coldstart.mjs`
- Create: `benchmarks/runners/node/coldstart.test.mjs`

**Prerequisite:** wrapper built (Task 5 prerequisite) and `node benchmarks/corpus/gen-inputs.mjs` run.

**Interfaces:**
- Consumes: `loadManifest`, `casesForMetric`, `resolveInputs` (Task 3); `computeStats` (Task 2); `loadWrapper` (Task 5).
- Produces:
  - `coldstart-child.mjs` — a CLI child: `node coldstart-child.mjs <corpusDir> <caseId>` initializes a fresh `DataWeave` instance (timing `initMs`), runs the case's script once (timing `firstRunMs`), and prints exactly one JSON line `{"initMs":<n>,"firstRunMs":<n>}` to stdout.
  - `runColdStartAndFirstRun(manifest, { samplesOverride? }): Promise<ResultCase[]>` — spawns N fresh child processes per applicable case, aggregates `initMs` into a `cold-start` row (for cases declaring `cold-start`) and `firstRunMs` into a `first-run` row (for cases declaring `first-run`), both unit `ms`.

- [ ] **Step 1: Write the child process script**

Create `benchmarks/runners/node/coldstart-child.mjs`:

```js
// Fresh-process worker. Measures a cold isolate init + a cold (first) compile+exec
// for one case, then prints a single JSON line. Invoked by coldstart.mjs.
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { loadManifest, resolveInputs } from "../../lib/manifest.mjs";
import { loadWrapper } from "./wrapper.mjs";

const [, , corpusDir, caseId] = process.argv;
const nowNs = () => process.hrtime.bigint();
const msSince = (s) => Number(nowNs() - s) / 1e6;

const manifest = loadManifest(corpusDir);
const c = manifest.cases.find((x) => x.id === caseId);
if (!c) throw new Error(`unknown case: ${caseId}`);
const script = readFileSync(join(corpusDir, c.script), "utf-8");

const resolved = resolveInputs(manifest, c);
const inputs = {};
for (const [name, v] of Object.entries(resolved)) {
  inputs[name] = { content: v.buffer, mimeType: v.mimeType, charset: v.charset ?? "utf-8" };
}

const api = await loadWrapper();
const dw = new api.DataWeave();

const initStart = nowNs();
dw.initialize();
const initMs = msSince(initStart);

const runStart = nowNs();
const result = dw.run(script, inputs);
const firstRunMs = msSince(runStart);
if (!result.success) throw new Error(`first run failed: ${result.error}`);

dw.cleanup();
process.stdout.write(JSON.stringify({ initMs, firstRunMs }) + "\n");
```

- [ ] **Step 2: Write the spawn harness**

Create `benchmarks/runners/node/coldstart.mjs`:

```js
import { execFileSync } from "node:child_process";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { casesForMetric } from "../../lib/manifest.mjs";
import { computeStats } from "../../lib/stats.mjs";

const __dirname = dirname(fileURLToPath(import.meta.url));
const CHILD = join(__dirname, "coldstart-child.mjs");

/** Spawn one fresh process and parse its single JSON line. */
function sampleOnce(corpusDir, caseId) {
  const out = execFileSync(process.execPath, [CHILD, corpusDir, caseId], { encoding: "utf-8" });
  const line = out.trim().split("\n").pop();
  return JSON.parse(line);
}

/**
 * @returns {Promise<Array<{id,metric,unit,stats,iterations}>>}
 */
export async function runColdStartAndFirstRun(manifest, { samplesOverride } = {}) {
  const rows = [];
  const ids = new Set([
    ...casesForMetric(manifest, "cold-start").map((c) => c.id),
    ...casesForMetric(manifest, "first-run").map((c) => c.id),
  ]);

  for (const id of ids) {
    const c = manifest.cases.find((x) => x.id === id);
    const n = samplesOverride ?? c.iterations?.samples ?? 20;
    const inits = [];
    const firsts = [];
    for (let i = 0; i < n; i++) {
      const { initMs, firstRunMs } = sampleOnce(manifest.corpusDir, id);
      inits.push(initMs);
      firsts.push(firstRunMs);
    }
    if (c.metrics.includes("cold-start")) {
      rows.push({ id, metric: "cold-start", unit: "ms", stats: computeStats(inits), iterations: n });
    }
    if (c.metrics.includes("first-run")) {
      rows.push({ id, metric: "first-run", unit: "ms", stats: computeStats(firsts), iterations: n });
    }
  }
  return rows;
}
```

- [ ] **Step 3: Write the integration test**

Create `benchmarks/runners/node/coldstart.test.mjs`:

```js
import { test } from "node:test";
import assert from "node:assert/strict";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { loadManifest } from "../../lib/manifest.mjs";
import { runColdStartAndFirstRun } from "./coldstart.mjs";

const __dirname = dirname(fileURLToPath(import.meta.url));
const CORPUS = join(__dirname, "..", "..", "corpus");

test("cold-start and first-run rows are produced from fresh processes", async () => {
  const manifest = loadManifest(CORPUS);
  // Keep sample count tiny so the test stays fast (each sample spawns a process).
  const rows = await runColdStartAndFirstRun(manifest, { samplesOverride: 3 });

  const cold = rows.filter((r) => r.metric === "cold-start");
  const first = rows.filter((r) => r.metric === "first-run");
  assert.ok(cold.length >= 1, "expected a cold-start row (trivial declares it)");
  assert.ok(first.length >= 1, "expected first-run rows");
  for (const r of [...cold, ...first]) {
    assert.equal(r.unit, "ms");
    assert.ok(r.stats.median > 0);
    assert.equal(r.iterations, 3);
  }
});
```

- [ ] **Step 4: Run the test**

Run:
```bash
node --test benchmarks/runners/node/coldstart.test.mjs
```
Expected: PASS (1 test). Requires the wrapper to be built and `records-large.json` generated (Task 5 prerequisites).

- [ ] **Step 5: Commit**

```bash
git add benchmarks/runners/node/coldstart-child.mjs benchmarks/runners/node/coldstart.mjs benchmarks/runners/node/coldstart.test.mjs
git commit -m "W-23545283: Add cold-start + first-run spawn harness"
```

---

### Task 7: Emit — orchestrate, validate ids, write result file

**Files:**
- Create: `benchmarks/runners/node/emit.mjs`
- Create: `benchmarks/runners/node/emit.test.mjs`

**Prerequisite:** wrapper built; `records-large.json` generated.

**Interfaces:**
- Consumes: `loadManifest`, `validateResultIds` (Task 3); `gatherEnv` (Task 4); `loadWrapper`, `runWarmAndStreaming` (Task 5); `runColdStartAndFirstRun` (Task 6).
- Produces:
  - `buildResult(env, cases): object` — assembles the full schema object (`schemaVersion:"1.0"`, `runner`, `env`, `timestamp`, `cases`).
  - `main()` — runs both harnesses, merges rows, calls `validateResultIds` (fail-fast), writes `benchmarks/results/node-<timestamp>.json`, prints the path. Run via `node benchmarks/runners/node/emit.mjs`.

- [ ] **Step 1: Write the failing unit test (fail-fast + shape, no native needed)**

Create `benchmarks/runners/node/emit.test.mjs`:

```js
import { test } from "node:test";
import assert from "node:assert/strict";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { loadManifest, validateResultIds } from "../../lib/manifest.mjs";
import { buildResult } from "./emit.mjs";

const __dirname = dirname(fileURLToPath(import.meta.url));
const CORPUS = join(__dirname, "..", "..", "corpus");

test("buildResult produces a schema-shaped object", () => {
  const env = {
    runner: "node-wrapper", os: "x", cpu: "y", runtimeVersion: "node vX",
    weaveVersion: "2.12.0-x", commit: "abc", dwlibBuildId: "dwlib-x",
  };
  const cases = [{ id: "trivial", metric: "warm", unit: "ms", stats: { median: 1 }, iterations: 10 }];
  const r = buildResult(env, cases);
  assert.equal(r.schemaVersion, "1.0");
  assert.equal(r.runner, "node-wrapper");
  assert.ok(typeof r.timestamp === "string");
  assert.deepEqual(r.cases, cases);
});

test("orphan ids are rejected before writing", () => {
  const manifest = loadManifest(CORPUS);
  assert.throws(
    () => validateResultIds(manifest, [{ id: "totally-made-up" }]),
    /orphan id/
  );
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `node --test benchmarks/runners/node/emit.test.mjs`
Expected: FAIL — cannot find module `./emit.mjs`.

- [ ] **Step 3: Write the implementation**

Create `benchmarks/runners/node/emit.mjs`:

```js
import { writeFileSync, mkdirSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { loadManifest, validateResultIds } from "../../lib/manifest.mjs";
import { gatherEnv } from "../../lib/env.mjs";
import { loadWrapper } from "./wrapper.mjs";
import { runWarmAndStreaming } from "./warm-bench.mjs";
import { runColdStartAndFirstRun } from "./coldstart.mjs";

const __dirname = dirname(fileURLToPath(import.meta.url));
const CORPUS = join(__dirname, "..", "..", "corpus");
const RESULTS_DIR = join(__dirname, "..", "..", "results");

/** Assemble the full schema object. */
export function buildResult(env, cases) {
  return {
    schemaVersion: "1.0",
    runner: env.runner,
    env,
    timestamp: new Date().toISOString(),
    cases,
  };
}

export async function main() {
  const manifest = loadManifest(CORPUS);
  const env = gatherEnv({ runner: "node-wrapper", runtimeVersion: `node ${process.version}` });

  // Cold-start / first-run first (fresh processes), then warm/streaming in-process.
  const coldRows = await runColdStartAndFirstRun(manifest);
  const api = await loadWrapper();
  let warmRows;
  try {
    warmRows = await runWarmAndStreaming(api, manifest);
  } finally {
    api.cleanup();
  }

  const cases = [...coldRows, ...warmRows];
  validateResultIds(manifest, cases); // fail-fast on any orphan id

  mkdirSync(RESULTS_DIR, { recursive: true });
  const stamp = new Date().toISOString().replace(/[:.]/g, "-");
  const outPath = join(RESULTS_DIR, `node-${stamp}.json`);
  writeFileSync(outPath, JSON.stringify(buildResult(env, cases), null, 2));
  console.log(`wrote ${outPath} (${cases.length} rows)`);
  return outPath;
}

// Run when invoked directly.
if (import.meta.url === pathToFileURLSafe(process.argv[1])) {
  main().catch((e) => {
    console.error(e.message);
    process.exit(1);
  });
}

function pathToFileURLSafe(p) {
  try {
    return new URL(`file://${p}`).href;
  } catch {
    return "";
  }
}
```

> Note: the direct-invocation guard is more robust using Node's helper. Implement the guard with `import { pathToFileURL } from "node:url";` at the top and `if (import.meta.url === pathToFileURL(process.argv[1]).href)`, and delete the `pathToFileURLSafe` helper. Do not ship the hand-rolled `file://` concatenation.

- [ ] **Step 4: Run unit test to verify it passes**

Run: `node --test benchmarks/runners/node/emit.test.mjs`
Expected: PASS (2 tests).

- [ ] **Step 5: Run the full emit end-to-end (integration)**

Run:
```bash
node benchmarks/corpus/gen-inputs.mjs
node benchmarks/runners/node/emit.mjs
```
Expected: prints `wrote .../benchmarks/results/node-<timestamp>.json (N rows)`. Open the file and confirm it has `schemaVersion`, `env.weaveVersion`, and case rows for all four metrics.

- [ ] **Step 6: Commit**

```bash
git add benchmarks/runners/node/emit.mjs benchmarks/runners/node/emit.test.mjs
git commit -m "W-23545283: Add emit orchestrator writing schema-conformant results"
```

---

### Task 8: Report script

**Files:**
- Create: `benchmarks/report/report.mjs`
- Create: `benchmarks/report/report.test.mjs`
- Create: `benchmarks/report/fixtures/node-a.json`
- Create: `benchmarks/report/fixtures/engine-b.json`

**Interfaces:**
- Consumes: `loadManifest` (Task 3). Result JSON files (schema from Task 1).
- Produces (all unit-testable without native):
  - `computeDelta(value: number, baseline: number, unit: string): number` — percent change; sign is raw `(value-baseline)/baseline*100`. Interpretation (lower-better for `ms`, higher-better for `MB/s`) is applied by the caller/formatter.
  - `detectSkew(results: Result[]): string[]` — distinct `env.weaveVersion` values across results (length > 1 ⇒ skew).
  - `buildTable(manifest, results, baselineRunner): { header, rows }` — one row per `(id, metric)`, columns per runner plus a delta-vs-baseline column.
  - `main(argv)` — parses `[files...] [--baseline <runner>] [--emit <target>]`, prints the skew banner + table; `--emit` throws `not implemented` (documented seam).

- [ ] **Step 1: Write fixtures**

Create `benchmarks/report/fixtures/node-a.json`:

```json
{
  "schemaVersion": "1.0",
  "runner": "node-wrapper",
  "env": { "os": "darwin-arm64", "cpu": "M1", "runtimeVersion": "node v20", "weaveVersion": "2.12.0-20260413", "commit": "abc", "dwlibBuildId": "dwlib-1" },
  "timestamp": "2026-07-22T12:00:00.000Z",
  "cases": [
    { "id": "trivial", "metric": "warm", "unit": "ms", "stats": { "median": 2.0 }, "iterations": 100 },
    { "id": "map-scale", "metric": "streaming", "unit": "MB/s", "stats": { "median": 300 }, "iterations": 10 }
  ]
}
```

Create `benchmarks/report/fixtures/engine-b.json`:

```json
{
  "schemaVersion": "1.0",
  "runner": "engine",
  "env": { "os": "darwin-arm64", "cpu": "M1", "runtimeVersion": "jvm 24", "weaveVersion": "2.13.0-SNAPSHOT", "commit": "def", "dwlibBuildId": "n/a" },
  "timestamp": "2026-07-22T12:05:00.000Z",
  "cases": [
    { "id": "trivial", "metric": "warm", "unit": "ms", "stats": { "median": 4.0 }, "iterations": 100 },
    { "id": "map-scale", "metric": "streaming", "unit": "MB/s", "stats": { "median": 150 }, "iterations": 10 }
  ]
}
```

- [ ] **Step 2: Write the failing test**

Create `benchmarks/report/report.test.mjs`:

```js
import { test } from "node:test";
import assert from "node:assert/strict";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { readFileSync } from "node:fs";
import { loadManifest } from "../lib/manifest.mjs";
import { computeDelta, detectSkew, buildTable } from "./report.mjs";

const __dirname = dirname(fileURLToPath(import.meta.url));
const CORPUS = join(__dirname, "..", "corpus");
const load = (f) => JSON.parse(readFileSync(join(__dirname, "fixtures", f), "utf-8"));

test("computeDelta is raw percent change", () => {
  assert.equal(computeDelta(2, 4, "ms"), -50);   // node 2ms vs engine 4ms
  assert.equal(computeDelta(300, 150, "MB/s"), 100);
});

test("detectSkew finds differing weave versions", () => {
  const skew = detectSkew([load("node-a.json"), load("engine-b.json")]);
  assert.equal(skew.length, 2);
  assert.ok(skew.includes("2.12.0-20260413"));
  assert.ok(skew.includes("2.13.0-SNAPSHOT"));
});

test("buildTable joins by (id, metric) with a delta vs baseline", () => {
  const manifest = loadManifest(CORPUS);
  const { rows } = buildTable(manifest, [load("node-a.json"), load("engine-b.json")], "engine");
  const trivialWarm = rows.find((r) => r.id === "trivial" && r.metric === "warm");
  assert.ok(trivialWarm);
  assert.equal(trivialWarm.values["node-wrapper"], 2.0);
  assert.equal(trivialWarm.values["engine"], 4.0);
  assert.equal(trivialWarm.delta, -50); // node is 50% lower (faster) than engine baseline
});
```

- [ ] **Step 3: Run test to verify it fails**

Run: `node --test benchmarks/report/report.test.mjs`
Expected: FAIL — cannot find module `./report.mjs`.

- [ ] **Step 4: Write the implementation**

Create `benchmarks/report/report.mjs`:

```js
import { readFileSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { loadManifest } from "../lib/manifest.mjs";

const __dirname = dirname(fileURLToPath(import.meta.url));
const CORPUS = join(__dirname, "..", "corpus");

/** Raw percent change of value vs baseline. Sign interpretation is per-unit (caller decides). */
export function computeDelta(value, baseline, _unit) {
  if (baseline === 0) return NaN;
  return ((value - baseline) / baseline) * 100;
}

/** Distinct weaveVersions across results; length > 1 means the comparison spans versions. */
export function detectSkew(results) {
  return [...new Set(results.map((r) => r.env.weaveVersion))];
}

/** True if lower is better for this unit. */
function lowerIsBetter(unit) {
  return unit === "ms";
}

/**
 * Join result files against the manifest into one row per (id, metric).
 * @param baselineRunner runner name whose value anchors the delta column
 */
export function buildTable(manifest, results, baselineRunner) {
  const runners = results.map((r) => r.runner);
  const cellByRunner = new Map(); // `${runner}|${id}|${metric}` -> {value, unit}
  for (const r of results) {
    for (const c of r.cases) {
      cellByRunner.set(`${r.runner}|${c.id}|${c.metric}`, { value: c.stats.median, unit: c.unit });
    }
  }

  const rows = [];
  for (const c of manifest.cases) {
    for (const metric of c.metrics) {
      const values = {};
      let unit = null;
      for (const runner of runners) {
        const cell = cellByRunner.get(`${runner}|${c.id}|${metric}`);
        if (cell) {
          values[runner] = cell.value;
          unit = cell.unit;
        }
      }
      if (Object.keys(values).length === 0) continue; // metric declared but no runner ran it
      const base = values[baselineRunner];
      const other = runners.find((r) => r !== baselineRunner && values[r] !== undefined);
      const delta =
        base !== undefined && other !== undefined ? computeDelta(values[other], base, unit) : null;
      rows.push({ id: c.id, metric, unit, values, delta, lowerIsBetter: lowerIsBetter(unit) });
    }
  }
  return { header: ["case", "metric", "unit", ...runners, `Δ vs ${baselineRunner}`], rows };
}

function fmt(n) {
  return n === undefined ? "—" : Number(n).toFixed(2);
}

export function main(argv) {
  const files = [];
  let baseline = null;
  let emit = null;
  for (let i = 0; i < argv.length; i++) {
    if (argv[i] === "--baseline") baseline = argv[++i];
    else if (argv[i] === "--emit") emit = argv[++i];
    else files.push(argv[i]);
  }
  if (emit) throw new Error(`--emit ${emit} not implemented (exporter seam reserved for future history/dashboard)`);
  if (files.length === 0) throw new Error("usage: report.mjs <result.json...> [--baseline <runner>]");

  const results = files.map((f) => JSON.parse(readFileSync(f, "utf-8")));
  const manifest = loadManifest(CORPUS);
  const baselineRunner = baseline ?? (results.find((r) => r.runner === "engine")?.runner ?? results[0].runner);

  const skew = detectSkew(results);
  if (skew.length > 1) {
    console.log(`⚠️  WEAVE VERSION SKEW: comparing across ${skew.join(" vs ")} — deltas are not clean.`);
    console.log("");
  }

  const { header, rows } = buildTable(manifest, results, baselineRunner);
  console.log("| " + header.join(" | ") + " |");
  console.log("| " + header.map(() => "---").join(" | ") + " |");
  for (const row of rows) {
    const runnerCols = header.slice(3, header.length - 1).map((runner) => fmt(row.values[runner]));
    const deltaStr = row.delta === null ? "—" : `${row.delta > 0 ? "+" : ""}${row.delta.toFixed(1)}%`;
    console.log(`| ${row.id} | ${row.metric} | ${row.unit} | ${runnerCols.join(" | ")} | ${deltaStr} |`);
  }
}

if (import.meta.url === pathToFileURL(process.argv[1]).href) {
  try {
    main(process.argv.slice(2));
  } catch (e) {
    console.error(e.message);
    process.exit(1);
  }
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `node --test benchmarks/report/report.test.mjs`
Expected: PASS (3 tests).

- [ ] **Step 6: Smoke the CLI against fixtures**

Run:
```bash
node benchmarks/report/report.mjs benchmarks/report/fixtures/node-a.json benchmarks/report/fixtures/engine-b.json --baseline engine
```
Expected: a skew banner (2.12 vs 2.13) followed by a markdown table with `node-wrapper`, `engine`, and a `Δ vs engine` column.

- [ ] **Step 7: Commit**

```bash
git add benchmarks/report/
git commit -m "W-23545283: Add report script (join, delta, skew banner)"
```

---

### Task 9: Gradle wiring (`native-lib:benchmark`)

**Files:**
- Modify: `native-lib/build.gradle` (add a `benchmark` task after the `nodeTest` task, around line 199)

**Interfaces:**
- Consumes: `buildNodePackage` (existing task — builds wrapper dist + addon + stages dwlib); the Node runner (`emit.mjs`) and report (`report.mjs`).
- Produces: an opt-in `native-lib:benchmark` Gradle task, skipped unless `-Pbenchmark=true`, not wired into `build`/`test`.

- [ ] **Step 1: Add the task**

In `native-lib/build.gradle`, immediately after the `nodeTest` task registration (the block ending at line 199), add:

```groovy
tasks.register('benchmark', Exec) {
  // Opt-in only: skipped unless -Pbenchmark=true. Never part of build/test.
  onlyIf { project.findProperty('benchmark')?.toString()?.toBoolean() == true }

  dependsOn tasks.named('buildNodePackage')
  workingDir("${rootDir}/benchmarks")

  def script = 'node corpus/gen-inputs.mjs && node runners/node/emit.mjs && node report/report.mjs results/*.json'
  if (System.getProperty('os.name').toLowerCase().contains('windows')) {
    commandLine('cmd', '/c', script)
  } else {
    commandLine('bash', '-c', script)
  }
}
```

- [ ] **Step 2: Verify the task is registered and skipped by default**

Run:
```bash
./gradlew native-lib:benchmark --dry-run
```
Expected: the task list includes `:native-lib:benchmark` (dry-run prints the task graph without executing). Running without `-Pbenchmark=true` and without dry-run shows the task `SKIPPED` due to `onlyIf`.

- [ ] **Step 3: Verify the full opt-in run (integration)**

Run:
```bash
./gradlew native-lib:benchmark -Pbenchmark=true
```
Expected: builds the wrapper, generates inputs, writes a `benchmarks/results/node-*.json`, and prints the comparison table (single runner — no skew banner). This is slow (native build); acceptable for a manual/opt-in benchmark run.

- [ ] **Step 4: Commit**

```bash
git add native-lib/build.gradle
git commit -m "W-23545283: Wire opt-in native-lib:benchmark Gradle task"
```

---

## Follow-up scope (not in this plan)

- `benchmarks/runners/python/` — Python runner emitting the same schema.
- `benchmarks/runners/engine/` — pointer/README here; the `-Ddw.perf`-gated scalatest harness lives in the `data-weave` repo, reads this corpus, emits this schema.
- Results accumulation / dashboard (the `--emit` seam is reserved but unimplemented).

## Self-Review

**Spec coverage:**
- Shared corpus + manifest → Task 3. ✓
- Common JSON schema (incl. `env.commit`/`dwlibBuildId`, immutable id, `schemaVersion`) → Tasks 1, 4, 7. ✓
- Four metrics → Tasks 5 (warm, streaming) + 6 (cold-start, first-run). ✓
- Explicit `metrics[]` + fail-fast orphan id → Task 3 (`loadManifest`, `validateResultIds`) enforced in Task 7. ✓
- Node reference runner (in-process + spawn harness) → Tasks 5, 6, 7. ✓
- Report with per-unit delta direction + skew banner + `--baseline` + `--emit` seam → Task 8. ✓
- Gradle opt-in task, off by default → Task 9. ✓
- Results local-only / gitignored → Task 1 `.gitignore`. ✓
- `benchmarks/runners/` extension point for Python/engine → layout in Task 1 README + Follow-up scope. ✓

**Placeholder scan:** No "TBD"/"handle edge cases" placeholders. Two ESM-correctness notes (env.mjs, warm-bench.mjs, emit.mjs) explicitly give the corrected code to ship rather than leaving it vague. ✓

**Type consistency:** `ResultCase = {id, metric, unit, stats, iterations}` is used identically in Tasks 5, 6, 7, 8. `computeStats` return shape matches the schema `stats` object (Task 1). `loadWrapper()` return `{run, runStreaming, runTransform, DataWeave, cleanup}` matches the wrapper's actual exports and is consumed consistently in Tasks 5–7. `gatherEnv` output fields match the schema `env` required list. ✓

---

**Deviations flagged for reviewer veto:**
1. **Timing harness:** self-contained `process.hrtime.bigint()` sampling instead of vitest `bench` — so the future engine harness can mirror the exact methodology (the spec named vitest `bench`).
2. **first-run placement:** measured in the spawn harness (fresh process = cold compilation) rather than in-process (spec §4). Yields a real cold-compilation distribution.
