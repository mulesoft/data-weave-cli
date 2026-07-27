# Node.js TCK Migration to `tck@zip` Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Migrate the Node.js binding's TCK conformance harness from the old `:test@zip` artifacts to the self-contained `:tck@zip` artifact, deleting the transform-normalization layer, honoring the `encoding` sidecar, and running both supported versions in CI.

**Architecture:** The harness stages runtime + core-modules `tck@zip` corpora into `node/tests/tck/suites/`, discovers one scenario per `<scenario>-out.<ext>` case directory, runs each case's `transform.dwl` verbatim in-process via `DataWeave.run()`, and compares output format-aware — decoding both sides with the case's `encoding` sidecar charset when present. An empirically-derived ignore list (keyed by full directory name) skips cases that fail for documented runtime/dwlib limitations.

**Tech Stack:** TypeScript, vitest (projects: unit/integration/tck), Gradle (`native-lib`), the `@dataweave/native` binding, `fast-xml-parser`.

## Global Constraints

- Corpus versions supported: `2.12.2-SNAPSHOT` and `2.13.0-SNAPSHOT` only. `gradle.properties` `weaveTestSuiteVersion` is already `2.13.0-SNAPSHOT`.
- No yaml-module suite: YAML is not compiled into dwlib; stage runtime + core-modules only.
- No language-level knob: the Node `run()` API exposes none; the 2.12.2 corpus replays against the single 2.13 dwlib build.
- TCK case layout: directory `<scenario>-out.<ext>/` containing exactly one `transform.dwl` (output directive already pinned), `inN.<ext>` inputs, exactly one `out.<ext>`, optional `encoding` sidecar (charset name), optional `config.properties` / bundled `.dwl` (skipped).
- Scenario id == case directory name (e.g. `big_intersection-out.json`); ignore-list keys are full directory names.
- Encoding: decode BOTH actual and expected with the sidecar charset (default `UTF-8`), mirroring the CLI `AssertionHelper`. `decodeBytes(bytes, charset)` is exported from `native-lib/node/src/result.ts:24`.
- Commits are GPG-signed; the plan uses `git commit -S` implicitly via repo config. End commit messages with the Co-Authored-By trailer.

## File Structure

| File | Responsibility after migration |
|------|-------------------------------|
| `native-lib/build.gradle` | `weaveTckSuite` → `:tck@zip` (runtime + core-modules); `stageTckSuites` unchanged (already version-parameterized via `weaveTestSuiteVersion`) |
| `native-lib/node/tests/tck/formats.ts` | extension↔MIME only; `familyForMime` removed |
| `native-lib/node/tests/tck/case-loader.ts` | parse case dir → one scenario named by the dir |
| `native-lib/node/tests/tck/compare.ts` | format-aware compare; optional charset decodes both sides |
| `native-lib/node/tests/tck/ignore-list.ts` | full-dir-name keys; empirically re-derived |
| `native-lib/node/tests/tck/tck.test.ts` | discover, run `transform.dwl` verbatim, read `encoding` sidecar |
| `native-lib/node/tests/tck/transform.ts` | **deleted** |
| `native-lib/node/tests/unit/tck-transform.test.ts` | **deleted** |
| `native-lib/node/tests/unit/tck-case-loader.test.ts` | scenario-naming already matches new scheme (see Task 3) |
| `native-lib/node/tests/unit/tck-compare.test.ts` | + encoding-aware cases |
| `.github/workflows/main.yml` | TCK step stages+runs both versions, master-only |

---

## Task 1: Switch Gradle staging to `tck@zip`

**Files:**
- Modify: `native-lib/build.gradle:176-179`

**Interfaces:**
- Produces: staged corpus at `native-lib/node/tests/tck/suites/{runtime,core-modules}/` from the `tck` classifier. No API change; `stageTckSuites` already maps by artifact name and honors `weaveTestSuiteVersion`.

- [ ] **Step 1: Change the two `weaveTckSuite` dependencies to the `tck` classifier**

In `native-lib/build.gradle`, replace the `dependencies { weaveTckSuite ... }` block (currently at lines 176-179):

```groovy
dependencies {
  weaveTckSuite "org.mule.weave:runtime:${weaveTestSuiteVersion}:tck@zip"
  weaveTckSuite "org.mule.weave:core-modules:${weaveTestSuiteVersion}:tck@zip"
}
```

(Only the classifier changes: `:test@zip` → `:tck@zip`. yaml-module is intentionally absent.)

- [ ] **Step 2: Stage the corpus and verify the new layout**

Run:
```bash
cd /Users/lmariano/dev/mulesoft/data-weave-cli
./gradlew --no-problems-report native-lib:stageTckSuites
ls native-lib/node/tests/tck/suites/runtime | head
```
Expected: directory names of the form `<scenario>-out.<ext>` (e.g. `access_raw_value-out.json/`), and `native-lib/node/tests/tck/suites/core-modules/` also populated. Confirm a sample case contains `transform.dwl` + `out.*`:
```bash
ls native-lib/node/tests/tck/suites/runtime/access_raw_value-out.json
```
Expected: `in0.xml  out.json  transform.dwl` (or similar).

- [ ] **Step 3: Verify the encoding sidecar is present in the staged corpus**

Run:
```bash
find native-lib/node/tests/tck/suites -name encoding
```
Expected: 7 hits including `core-modules/xml-encoding-out.xml/encoding` (value `UTF-16`) and `runtime/nested_map_with_filter-out.json/encoding` (value `UTF-8`).

- [ ] **Step 4: Commit**

```bash
git add native-lib/build.gradle
git commit -m "build(native-lib): stage Node TCK corpus from tck@zip

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

Note: the staged `suites/` directory is gitignored (corpus is not committed).

---

## Task 2: Delete the transform normalizer

**Files:**
- Delete: `native-lib/node/tests/tck/transform.ts`
- Delete: `native-lib/node/tests/unit/tck-transform.test.ts`
- Modify: `native-lib/node/tests/tck/formats.ts` (remove `familyForMime`)

**Interfaces:**
- Produces: `formats.ts` exports `EXTENSION_TO_MIME`, `mimeForExtension`, `isSupportedExtension` only. `familyForMime` no longer exists. `transform.ts`/`ensureOutputDirective` no longer exist.
- Consumes: nothing new.

- [ ] **Step 1: Confirm `familyForMime` and `ensureOutputDirective` have no consumers outside the deleted files**

Run:
```bash
cd native-lib/node
rg -n 'familyForMime|ensureOutputDirective' src tests
```
Expected: matches only in `tests/tck/transform.ts` (to be deleted), `tests/tck/formats.ts` (the definition), and `tests/tck/tck.test.ts` (the call, removed in Task 5). If any *other* file references them, STOP — the plan assumption is wrong.

- [ ] **Step 2: Delete the normalizer and its test**

Run:
```bash
git rm native-lib/node/tests/tck/transform.ts native-lib/node/tests/unit/tck-transform.test.ts
```

- [ ] **Step 3: Remove `familyForMime` from `formats.ts`**

In `native-lib/node/tests/tck/formats.ts`, delete the entire `familyForMime` function (the doc comment block plus the function, currently the last export in the file — starts at the `/**` above `export function familyForMime` and ends at its closing `}`). Leave `EXTENSION_TO_MIME`, `mimeForExtension`, and `isSupportedExtension` untouched.

- [ ] **Step 4: Type-check to confirm nothing else referenced the deletions**

Run:
```bash
cd native-lib/node && npx tsc --noEmit
```
Expected: no errors. (`tck.test.ts` still imports `ensureOutputDirective` at this point — if tsc flags that, it is expected and fixed in Task 5. To keep this task self-contained, if tsc errors ONLY on the `transform` import in `tck.test.ts`, that is acceptable and resolved next task; any other error means STOP.)

- [ ] **Step 5: Commit**

```bash
git add -A native-lib/node/tests/tck/formats.ts
git commit -m "test(native-lib): drop TCK transform normalizer (tck@zip pins the directive)

transform.dwl in the tck@zip artifact already carries the correct output
directive, so the text-level ensureOutputDirective normalizer and its only
consumer (familyForMime) are no longer needed.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: Confirm `case-loader.ts` scenario naming (verify-only, likely no change)

**Files:**
- Verify: `native-lib/node/tests/tck/case-loader.ts`
- Verify: `native-lib/node/tests/unit/tck-case-loader.test.ts`

**Interfaces:**
- Produces: `parseCase(caseName, fileNames)` returns scenarios whose `name` is `${caseName}-${outputFileName}`. Because in `tck@zip` the *case directory* is already `<scenario>-out.<ext>` and `tck.test.ts` passes the directory name as `caseName`, the resulting scenario name is `<scenario>-out.<ext>-out.<ext>` — WRONG. This task fixes that.

**Rationale:** The existing loader was written for `test@zip`, where the case dir was the bare scenario (`as-operator`) and produced `as-operator-out.json`. Under `tck@zip` the dir is already `as-operator-out.json`, so re-appending the output file name double-suffixes. The scenario name must become the case directory name itself.

- [ ] **Step 1: Update the failing unit test to the new naming**

In `native-lib/node/tests/unit/tck-case-loader.test.ts`, the happy-path tests currently pass a bare `caseName` (`"as-operator"`) and expect `"as-operator-out.json"`. Update them to reflect that the caseName is now the full dir name and the scenario name equals it. Replace the `parseCase — happy paths` block's first test:

```typescript
  it("parses a single-input single-output case", () => {
    const r = parseCase("as-operator-out.json", [MAIN_TRANSFORM, "in0.json", "out.json"]);
    expect(r.kind).toBe("scenarios");
    if (r.kind !== "scenarios") return;
    expect(r.scenarios).toHaveLength(1);
    const s = r.scenarios[0];
    expect(s.name).toBe("as-operator-out.json");
    expect(s.inputs).toEqual([{ name: "in0", fileName: "in0.json", mimeType: "application/json" }]);
    expect(s.outputMime).toBe("application/json");
    expect(s.outputExtension).toBe("json");
  });
```

And update the multi-input test's caseName and the `emits one scenario per output file` / `no-input` tests to pass full dir names and expect the dir name as the scenario name:

```typescript
  it("binds multiple inputs by base name, sorted", () => {
    const r = parseCase("multi-out.json", [MAIN_TRANSFORM, "in1.xml", "in0.json", "out.json"]);
    if (r.kind !== "scenarios") throw new Error("expected scenarios");
    expect(r.scenarios[0].name).toBe("multi-out.json");
    expect(r.scenarios[0].inputs.map((i) => i.name)).toEqual(["in0", "in1"]);
    expect(r.scenarios[0].inputs.map((i) => i.mimeType)).toEqual(["application/json", "application/xml"]);
  });

  it("names the scenario after the case directory", () => {
    const r = parseCase("literal-out.json", [MAIN_TRANSFORM, "out.json"]);
    if (r.kind !== "scenarios") throw new Error("expected scenarios");
    expect(r.scenarios[0].name).toBe("literal-out.json");
    expect(r.scenarios[0].inputs).toEqual([]);
  });
```

Delete the old `emits one scenario per output file` test (multi-output cases do not occur in `tck@zip` — each dir has exactly one `out.*`; keeping a double-suffix expectation would be misleading). Keep the `structural skips` and `unsupported formats` blocks as-is except update any `caseName` first arg to a full-dir-name form where the test asserts a scenario name; skip-reason tests need no change.

- [ ] **Step 2: Run the test to confirm it fails against the current loader**

Run:
```bash
cd native-lib/node && npx vitest run --project unit tests/unit/tck-case-loader.test.ts
```
Expected: FAIL — current loader produces `as-operator-out.json-out.json` (double suffix).

- [ ] **Step 3: Change the scenario name to the case directory name**

In `native-lib/node/tests/tck/case-loader.ts`, in the `scenarios` map, change the `name` field:

```typescript
  const scenarios: TckScenario[] = outputFiles
    .filter((out) => isSupportedExtension(extensionOf(out)))
    .map((outputFileName) => {
      const outputExtension = extensionOf(outputFileName);
      return {
        name: caseName,
        inputs,
        outputFileName,
        outputExtension,
        outputMime: mimeOrThrow(outputFileName),
      };
    });
```

(Only `name: \`${caseName}-${outputFileName}\`` → `name: caseName`.) Update the `TckScenario.name` doc comment from ``Scenario id: `<case>-<outputFileName>`.`` to ``Scenario id: the case directory name (`<scenario>-out.<ext>`).``

- [ ] **Step 4: Run the unit test to confirm it passes**

Run:
```bash
cd native-lib/node && npx vitest run --project unit tests/unit/tck-case-loader.test.ts
```
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add native-lib/node/tests/tck/case-loader.ts native-lib/node/tests/unit/tck-case-loader.test.ts
git commit -m "test(native-lib): name TCK scenario after the case directory

Under tck@zip the case directory is already <scenario>-out.<ext>, so the
scenario id is the directory name itself; appending the output file name
double-suffixed it.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: Encoding-aware comparison in `compare.ts`

**Files:**
- Modify: `native-lib/node/tests/tck/compare.ts`
- Modify: `native-lib/node/tests/unit/tck-compare.test.ts`

**Interfaces:**
- Consumes: `decodeBytes(bytes: Buffer, charset: string | null): string` from `../../src/result`.
- Produces: `compareOutput(extension: string, actual: Buffer, expected: Buffer, charset?: string | null): CompareResult`. When `charset` is given both sides are decoded with it (default UTF-8 when omitted/null). `bin` ignores charset. `deepEqual` signature unchanged.

- [ ] **Step 1: Write failing tests for charset decoding**

In `native-lib/node/tests/unit/tck-compare.test.ts`, add a new describe block at the end (after the `deepEqual` block). It builds UTF-16LE bytes for both sides and asserts they compare equal when the charset is supplied, and that omitting the charset defaults to UTF-8:

```typescript
describe("compareOutput — encoding sidecar", () => {
  const utf16le = (s: string) => Buffer.from(s, "utf16le");

  it("decodes both sides as UTF-16 when charset is supplied (json)", () => {
    const actual = utf16le(JSON.stringify({ v: "café" }));
    const expected = utf16le(JSON.stringify({ v: "café" }));
    // Without the charset these UTF-16 bytes would parse as garbage → mismatch.
    expect(compareOutput("json", actual, expected, "UTF-16").match).toBe(true);
  });

  it("decodes both sides as UTF-16 for xml", () => {
    const actual = utf16le("<a>x</a>");
    const expected = utf16le("<a>x</a>");
    expect(compareOutput("xml", actual, expected, "UTF-16").match).toBe(true);
  });

  it("defaults to UTF-8 when no charset is given", () => {
    expect(compareOutput("json", Buffer.from('{"a":1}', "utf-8"), Buffer.from('{"a":1}', "utf-8")).match).toBe(true);
  });

  it("ignores charset for bin (raw bytes)", () => {
    expect(compareOutput("bin", Buffer.from([0, 1]), Buffer.from([0, 1]), "UTF-16").match).toBe(true);
  });
});
```

- [ ] **Step 2: Run the new tests to confirm they fail**

Run:
```bash
cd native-lib/node && npx vitest run --project unit tests/unit/tck-compare.test.ts -t "encoding sidecar"
```
Expected: FAIL — `compareOutput` currently ignores the 4th arg and decodes UTF-8, so the UTF-16 bytes parse as invalid JSON/XML.

- [ ] **Step 3: Thread the charset through `compareOutput`**

In `native-lib/node/tests/tck/compare.ts`:

Add the import at the top (after the `fast-xml-parser` import):
```typescript
import { decodeBytes } from "../../src/result";
```

Change the signature and the actual/expected decoding. Replace the head of `compareOutput` (the `bin` branch and the two `toString("utf-8")` lines) with:

```typescript
export function compareOutput(
  extension: string,
  actual: Buffer,
  expected: Buffer,
  charset?: string | null
): CompareResult {
  const ext = extension.replace(/^\./, "").toLowerCase();

  if (ext === "bin") {
    return actual.equals(expected) ? ok : fail(`binary mismatch: ${actual.length} vs ${expected.length} bytes`);
  }

  const a = decodeBytes(actual, charset ?? null);
  const e = decodeBytes(expected, charset ?? null);
```

Leave the rest of the function (the `switch (ext)` dispatch) unchanged. Update the doc comment's `@param` list to add:
```
 * @param charset - Optional charset (from a case's `encoding` sidecar) used to
 *                  decode BOTH sides; defaults to UTF-8. Ignored for `bin`.
```

- [ ] **Step 4: Run the compare tests to confirm all pass**

Run:
```bash
cd native-lib/node && npx vitest run --project unit tests/unit/tck-compare.test.ts
```
Expected: PASS (new encoding block + all prior tests, since `decodeBytes(x, null)` === `x.toString("utf-8")`).

- [ ] **Step 5: Commit**

```bash
git add native-lib/node/tests/tck/compare.ts native-lib/node/tests/unit/tck-compare.test.ts
git commit -m "test(native-lib): decode TCK output with the encoding sidecar charset

compareOutput now decodes both actual and expected with an optional charset
(default UTF-8), mirroring the CLI AssertionHelper. Needed for the UTF-16
core-modules cases where dwlib emits UTF-16 output bytes.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 5: Wire `tck.test.ts` to run `transform.dwl` verbatim + read the sidecar

**Files:**
- Modify: `native-lib/node/tests/tck/tck.test.ts`

**Interfaces:**
- Consumes: `parseCase`, `MAIN_TRANSFORM` (case-loader); `compareOutput` (now charset-aware); `isIgnored`, `ignoreReason` (ignore-list, re-keyed in Task 6).
- Produces: the runnable `tck` vitest lane.

- [ ] **Step 1: Remove the normalizer import and call; read the transform verbatim; read the encoding sidecar**

In `native-lib/node/tests/tck/tck.test.ts`:

Delete the import line:
```typescript
import { ensureOutputDirective } from "./transform";
```

In the scenario test body, replace the transform read + normalize:
```typescript
          const src = readFileSync(join(c.dir, MAIN_TRANSFORM), "utf-8");
          const script = ensureOutputDirective(src, scenario.outputMime);
```
with a verbatim read:
```typescript
          const script = readFileSync(join(c.dir, MAIN_TRANSFORM), "utf-8");
```

Replace the comparison tail:
```typescript
          const actual = result.getBytes()!;
          const expected = readFileSync(join(c.dir, scenario.outputFileName));
          const cmp = compareOutput(scenario.outputExtension, actual, expected);
          expect(cmp.match, cmp.detail).toBe(true);
```
with a sidecar-aware version:
```typescript
          const actual = result.getBytes()!;
          const expected = readFileSync(join(c.dir, scenario.outputFileName));
          const encodingFile = join(c.dir, "encoding");
          const charset = existsSync(encodingFile)
            ? readFileSync(encodingFile, "utf-8").trim()
            : null;
          const cmp = compareOutput(scenario.outputExtension, actual, expected, charset);
          expect(cmp.match, cmp.detail).toBe(true);
```

(`existsSync` and `readFileSync` are already imported from `node:fs` at the top of the file.)

- [ ] **Step 2: Type-check the whole Node package**

Run:
```bash
cd native-lib/node && npx tsc --noEmit
```
Expected: no errors (the `transform` import from Task 2 is now gone).

- [ ] **Step 3: Commit**

```bash
git add native-lib/node/tests/tck/tck.test.ts
git commit -m "test(native-lib): run TCK transform.dwl verbatim and honor encoding sidecar

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 6: Re-derive and re-key the ignore list against `tck@zip`

**Files:**
- Modify: `native-lib/node/tests/tck/ignore-list.ts`

**Interfaces:**
- Produces: `IGNORED_CASES` keyed by full directory names (`<scenario>-out.<ext>`); `isIgnored(caseName)` / `ignoreReason(caseName)` unchanged in signature (now matched against scenario ids, which equal dir names after Task 3).

**Rationale:** Every current key is a bare scenario name (`big_intersection`), which will never match the new scenario ids (`big_intersection-out.json`) — the exact silent-no-match bug found on the CLI. The list must be re-keyed AND re-derived empirically, because deleting the normalizer changes which cases pass.

**Prerequisite:** Tasks 1–5 committed, corpus staged (Task 1), and the Node package built against the current dwlib. Build it:
```bash
cd /Users/lmariano/dev/mulesoft/data-weave-cli
export GRAALVM_HOME="$PWD/.graalvm/graalvm-community-openjdk-24.0.2+11.1/Contents/Home"
export JAVA_HOME="$GRAALVM_HOME"
./gradlew --no-problems-report native-lib:buildNodePackage
```
(If dwlib was already built this session and unchanged, `buildNodePackage` is fast. A full `native-lib:nativeCompile` is only needed if dwlib is stale.)

- [ ] **Step 1: Run the tck lane with the current (bare-name) ignore list to get the raw failure set**

Run:
```bash
cd native-lib/node && npx vitest run --project tck 2>&1 | tee /tmp/tck-node-run1.txt | tail -40
```
Because keys don't match, nothing is skipped; every genuinely-failing case fails openly. Collect the failing scenario ids:
```bash
rg -oN '✗|FAIL|×' /tmp/tck-node-run1.txt >/dev/null 2>&1 || true
rg -N '^\s*[×✗] ' /tmp/tck-node-run1.txt | sed -E 's/^\s*[×✗] //' | sort -u > /tmp/tck-node-failures.txt
cat /tmp/tck-node-failures.txt
```
(If the reporter format differs, extract failing test names from the vitest summary — each failing scenario id is a full `<scenario>-out.<ext>` name.)

- [ ] **Step 2: Re-key existing entries to full directory names and classify each failure**

For every failure in `/tmp/tck-node-failures.txt`, add/keep an entry in `IGNORED_CASES` keyed by the FULL scenario id, grouped by the existing reason categories (unresolved-module, java, dw::Runtime, dwl-output-format, multipart, nondeterministic, slow, coercion/runtime, xml). Cross-check against the CLI's `TCKCliTest.ignoreTests()` (`native-cli-integration-tests/.../TCKCliTest.scala`) which is already keyed by full dir names — a case ignored there for a runtime/dwlib reason is very likely ignored here too. Concretely, convert each current bare key to its `-out.<ext>` form, e.g.:

```typescript
export const IGNORED_CASES: Readonly<Record<string, IgnoreEntry>> = {
  // unresolved-module — library/resource not present in dwlib
  "dw-binary-out.dwl": { reason: "unresolved-module: readUrl/classpath resource" },
  "full-qualified-name-ref-out.json": { reason: "unresolved-module: org::mule::weave::v2::libs" },
  "import-lib-out.json": { reason: "unresolved-module: import lib not in dwlib" },
  // …one entry per confirmed failure, full dir name as key…
  "big_intersection-out.json": { reason: "slow: 500-way intersection type exceeds the test timeout" },
};
```

Determine the exact extension suffix for each key from the staged corpus (the failure ids already carry it). Do NOT invent keys: only include a case if (a) it appears in `/tmp/tck-node-failures.txt`, or (b) it is a known-slow case that passes but risks the 30s timeout on CI (currently `big_intersection-out.json` — carry it forward like the CLI does).

- [ ] **Step 3: Drop stale entries and update the header comment**

Remove any bare-name entry whose case no longer fails (e.g. do-block / output-format-mismatch cases that the normalizer used to fix and now pass verbatim, if they are absent from the failure set). Update the file header comment: replace the paragraph about `ensureOutputDirective` / do-block recoveries (no longer relevant — the normalizer is deleted) with a note that keys are full `<scenario>-out.<ext>` directory names matched against scenario ids, seeded empirically against the `tck@zip` corpus.

- [ ] **Step 4: Re-run the tck lane; confirm green**

Run:
```bash
cd native-lib/node && npx vitest run --project tck 2>&1 | tail -20
```
Expected: 0 failures; skipped count equals the number of ignore-list entries whose case is present. If any case still fails, classify it (add to the list with a documented reason) or fix the harness — do NOT leave a red lane. Re-run until green.

- [ ] **Step 5: Sanity-check the UTF-16 case actually ran and passed**

Run:
```bash
cd native-lib/node && npx vitest run --project tck -t "xml-encoding-out.xml" 2>&1 | tail -15
```
Expected: the `xml-encoding-out.xml` scenario PASSES (not skipped) — proving the encoding sidecar path works end-to-end. If it is on the ignore list, remove it and confirm it passes on its own merits; if it genuinely cannot pass, document the reason explicitly.

- [ ] **Step 6: Commit**

```bash
git add native-lib/node/tests/tck/ignore-list.ts
git commit -m "test(native-lib): re-key and re-derive the TCK ignore list for tck@zip

Keys are now full <scenario>-out.<ext> directory names (bare names silently
never matched under the new naming, as on the CLI). List re-derived empirically
against the tck@zip corpus after removing the transform normalizer.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 7: Run both corpus versions in CI

**Files:**
- Modify: `.github/workflows/main.yml:97-102`

**Interfaces:**
- Produces: a master-only CI step that stages + runs the tck lane once per supported version against the single built dwlib.

- [ ] **Step 1: Replace the single-version TCK step with a two-version step**

In `.github/workflows/main.yml`, replace the `Run Node.js TCK Conformance` step (currently lines 97-102) with two staged runs, re-staging the corpus between versions (each `stageTckSuites` cleans `suites/` first via `cleanTckSuites`):

```yaml
      # Run the Node.js TCK conformance lane (only on master to save CI time on
      # PRs — mirrors the native-cli regression gating). Stages the DataWeave
      # tck@zip corpus per supported version and runs the tck vitest project
      # against the built package. The Node package was already built by "Create
      # Native Lib Node Package" above; dwlib is the single 2.13.0 build and the
      # 2.12.2 corpus replays against it (the binding exposes no language-level).
      - name: Run Node.js TCK Conformance 2.12.2-SNAPSHOT
        if: github.ref == 'refs/heads/master'
        run: |
          ./gradlew --stacktrace --no-problems-report -PweaveTestSuiteVersion=2.12.2-SNAPSHOT native-lib:stageTckSuites
          cd native-lib/node && npm run test:tck
        shell: bash
      - name: Run Node.js TCK Conformance 2.13.0-SNAPSHOT
        if: github.ref == 'refs/heads/master'
        run: |
          ./gradlew --stacktrace --no-problems-report -PweaveTestSuiteVersion=2.13.0-SNAPSHOT native-lib:stageTckSuites
          cd native-lib/node && npm run test:tck
        shell: bash
```

- [ ] **Step 2: Verify `weaveTestSuiteVersion` is honored as a `-P` project property**

Run:
```bash
cd /Users/lmariano/dev/mulesoft/data-weave-cli
./gradlew --no-problems-report -PweaveTestSuiteVersion=2.12.2-SNAPSHOT native-lib:stageTckSuites
ls native-lib/node/tests/tck/suites/runtime | head -3
```
Expected: staging succeeds and pulls the 2.12.2 corpus (Gradle resolves `runtime-2.12.2-SNAPSHOT-tck.zip`). Confirm the resolved version in the download by checking the Gradle output mentions `2.12.2-SNAPSHOT`, or:
```bash
find ~/.gradle/caches -name 'runtime-2.12.2-SNAPSHOT-tck.zip' | head -1
```

- [ ] **Step 3: Run the tck lane against the 2.12.2 corpus locally to catch cross-version divergence**

Run (corpus from Step 2 is staged to 2.12.2):
```bash
cd native-lib/node && npx vitest run --project tck 2>&1 | tail -20
```
Expected: green. If a case fails only under 2.12.2, add it to the ignore list with a version note in its reason (e.g. `"…: 2.12.2 divergence"`) — amend Task 6's file — then re-run. Re-stage back to 2.13.0 afterward:
```bash
cd /Users/lmariano/dev/mulesoft/data-weave-cli
./gradlew --no-problems-report -PweaveTestSuiteVersion=2.13.0-SNAPSHOT native-lib:stageTckSuites
```

- [ ] **Step 4: Commit**

```bash
git add .github/workflows/main.yml
# include ignore-list.ts too if Step 3 required a divergence entry
git commit -m "ci: run Node TCK conformance for both 2.12.2 and 2.13.0 (master-only)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 8: Full-lane verification & finish

**Files:** none (verification only)

- [ ] **Step 1: Run the complete Node test matrix (unit + integration + tck) against 2.13.0**

Run:
```bash
cd /Users/lmariano/dev/mulesoft/data-weave-cli
export GRAALVM_HOME="$PWD/.graalvm/graalvm-community-openjdk-24.0.2+11.1/Contents/Home"
export JAVA_HOME="$GRAALVM_HOME"
./gradlew --no-problems-report native-lib:stageTckSuites   # 2.13.0 (default)
cd native-lib/node
npx tsc --noEmit
npx vitest run   # all projects; tck lane included since suites/ is staged
```
Expected: all projects green; the tck lane reports `N runnable cases, M structurally skipped` and 0 failures.

- [ ] **Step 2: Confirm no lingering references to the deleted normalizer**

Run:
```bash
cd native-lib/node && rg -n 'transform\.ts|ensureOutputDirective|familyForMime' . || echo "clean"
```
Expected: `clean` (or only the plan/spec docs under `docs/`).

- [ ] **Step 3: Finish the branch**

Announce: "I'm using the finishing-a-development-branch skill to complete this work." Then use superpowers:finishing-a-development-branch to verify tests, present integration options, and open the PR. PR description must cover: the artifact change (`test@zip` → `tck@zip`, runtime + core-modules), normalizer deletion, encoding-sidecar handling, ignore-list re-key/re-derivation, and the two-version master-only CI. (EMU may block programmatic PR creation — open in browser if `gh`/MCP returns 403.)

---

## Self-Review

**Spec coverage:**
- §1 artifact sourcing → Task 1 ✓
- §2 delete transform.ts → Task 2 ✓ (+ familyForMime removal)
- §3 one scenario per dir / naming → Task 3 ✓
- §4 encoding sidecar (both sides) → Task 4 (compare) + Task 5 (sidecar read) ✓
- §5 ignore-list re-key + re-derive → Task 6 ✓
- §6 both versions, master-only CI → Task 7 ✓
- Validation §Testing/§Risks → Task 6 (empirical), Task 7 Step 3 (cross-version), Task 8 (full matrix) ✓

**Placeholder scan:** No TBD/TODO; every code step shows exact edits. Task 6 is inherently empirical (the failure set can't be known without running dwlib), but the procedure, keying rule, classification categories, and green-gate are fully specified — no "add appropriate handling" hand-waving.

**Type consistency:** `compareOutput` 4th param `charset?: string | null` matches `decodeBytes(bytes, charset: string | null)` (call passes `charset ?? null`). Scenario `name` = `caseName` (Task 3) equals ignore-list keys (Task 6) and the CI-run scenario ids. `existsSync`/`readFileSync` already imported in `tck.test.ts`. `MAIN_TRANSFORM` unchanged.
