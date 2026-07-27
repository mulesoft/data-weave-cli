# Node.js TCK migration to the `tck@zip` artifact

**Date:** 2026-07-27
**Module:** `native-lib` (Node.js binding TCK harness)
**Related work:** CLI equivalent already merged — `W-23595942` / PR #141 (`native-cli-integration-tests`).

## Problem

The Node.js binding's TCK conformance harness (`native-lib/node/tests/tck/`) was
built against the old classified test-suite zips (`org.mule.weave:*:test@zip`).
Those artifacts did not ship runnable, self-contained cases: the harness had to
reconstruct the output directive itself (a text-level normalizer standing in for
the CLI's AST rewrite) and had no way to honor a case's declared encoding.

A dedicated `tck@zip` artifact is now published (present in `2.12.2-SNAPSHOT` and
`2.13.0-SNAPSHOT`). Each case is a self-contained directory named
`<scenario>-out.<ext>` containing:

- `transform.dwl` — the transform, with the correct `output` directive already
  pinned (no rewriting needed);
- `inN.<ext>` — inputs (basename is the DataWeave variable, extension selects the
  reader format);
- exactly one `out.<ext>` — the expected output;
- an optional `encoding` sidecar file — a charset name (e.g. `UTF-8`, `UTF-16`)
  the expected output is written in;
- optional `config.properties` / bundled module `.dwl` files (still skipped).

The CLI integration tests already migrated to this artifact. This spec migrates
the Node binding to the same source, deleting the compensating logic the old
artifact forced on it.

## Goals

1. Source TCK cases from `tck@zip` (runtime + core-modules) instead of `test@zip`.
2. Delete the transform-normalization layer — `transform.dwl` is now runnable
   verbatim.
3. Honor the `encoding` sidecar in output comparison.
4. Re-key the ignore list to the new full directory names and re-derive it
   empirically against the `tck@zip` corpus.
5. Run the Node TCK CI lane against both supported versions (`2.12.2-SNAPSHOT`
   and `2.13.0-SNAPSHOT`), master-only, mirroring the CLI regression matrix.

## Non-goals

- **yaml-module suite** — NOT included. YAML is not compiled into this dwlib
  build, so those cases are unsupported. The Node harness stages runtime +
  core-modules only.
- No change to the comparison strategies themselves (`compareJson`, `compareXml`,
  normalized-string, byte-equal for `bin`) beyond adding encoding awareness.
- No change to the in-process execution model (one shared `DataWeave` runtime,
  `run()` per scenario).
- No language-level pinning. The CLI passes `--language-level=<corpusVersion>` to
  the `dw` subprocess; the Node `run()` API exposes no such option. dwlib is a
  single `2.13.0` build, the two corpora are near-identical (613/613 runtime
  cases) and `transform.dwl` files carry no `%dw` version directive, so replaying
  the 2.12.2 corpus against the 2.13 runtime is safe; any divergence is absorbed
  per-version in the ignore list.

## Design

### 1. Artifact sourcing — `native-lib/build.gradle`

Change the `weaveTckSuite` configuration dependencies from `:test@zip` to
`:tck@zip`, keeping runtime + core-modules only:

```groovy
dependencies {
  weaveTckSuite "org.mule.weave:runtime:${weaveTestSuiteVersion}:tck@zip"
  weaveTckSuite "org.mule.weave:core-modules:${weaveTestSuiteVersion}:tck@zip"
}
```

`stageTckSuites` already resolves each artifact, derives the suite name from the
artifact name (`runtime`, `core-modules`), and unzips into
`node/tests/tck/suites/<name>/`. No structural change is needed there beyond the
CI version parameterization in §6. `gradle.properties` is already pinned to
`2.13.0-SNAPSHOT` (from the CLI migration).

### 2. Delete `transform.ts`

`tck@zip`'s `transform.dwl` already carries the correct `output` directive, so
the text-level `ensureOutputDirective` normalizer is dead weight and a source of
subtle bugs.

- Delete `native-lib/node/tests/tck/transform.ts` and its unit test.
- In `tck.test.ts`, drop the `ensureOutputDirective` import/call and run the raw
  `transform.dwl` contents.
- Remove `familyForMime` from `formats.ts` (only `transform.ts` consumed it) and
  its unit-test coverage. `EXTENSION_TO_MIME`, `mimeForExtension`, and
  `isSupportedExtension` stay.

### 3. `case-loader.ts` — one scenario per directory

In `tck@zip` a case directory *is* the scenario: named `<scenario>-out.<ext>`
with exactly one `out.*`. The loader collapses accordingly:

- Keep the structural skip filters unchanged: `_wip` marker, per-input/output
  `config.properties`, bare `config.properties`, `.groovy`/java cases, the
  "exactly one non-input/output `.dwl` named `transform.dwl`" rule, and dropping
  cases whose input/output extension maps to an unsupported format.
- The scenario name becomes the **case directory name itself** (e.g.
  `big_intersection-out.json`), not the old `${caseName}-${outputFileName}`
  composite. This makes scenario ids align 1:1 with ignore-list keys.
- Since there is exactly one expected output per case, the loader yields a single
  scenario per runnable directory (the multi-output loop degenerates but the
  code stays tolerant of >1 `out.*` for safety).

### 4. Encoding sidecar — `compare.ts` + `tck.test.ts`

When an `encoding` sidecar file is present in the case directory, decode the
expected output bytes with that charset before comparison. Observed in the 2.13
corpus: 7 cases carry a sidecar (6 in core-modules incl. one `UTF-16`, 1 in
runtime), all others default to UTF-8.

- `tck.test.ts` reads the optional `encoding` sidecar (trimmed) alongside the
  case files and passes the charset into the comparison call.
- `compare.ts` accepts an optional charset for the *expected* side. For the
  text/structural strategies (json, xml, csv, txt, dwl, properties, urlencoded)
  it decodes the expected bytes via `decodeBytes()` (exported from
  `src/result.ts:24`, which already handles IANA names, UTF-16 BOM/byte-order,
  and fallback) instead of a hardcoded `toString("utf-8")`. `bin` stays a raw
  byte compare (encoding is irrelevant to octet-stream).
- The actual output produced by `run()` is compared as-is; the sidecar governs
  only how the *expected fixture* is decoded, mirroring the CLI's `maybeEncoding`
  → `AssertionHelper` contract.

### 5. `ignore-list.ts` — re-key and re-derive

- Re-key every entry from the bare case name (`big_intersection`) to the full
  directory name (`big_intersection-out.json`). The bare-name keys silently
  never matched under the new naming — the exact bug caught on the CLI side.
- Re-derive the list empirically by running the migrated harness against the
  staged `tck@zip` corpus. Keep entries that still fail for a documented root
  cause (unresolved-module, java, dw::Runtime, multipart, nondeterministic,
  coercion/runtime, slow); drop entries whose cases no longer exist or now pass
  (e.g. the do-block / output-format-mismatch cases previously recovered by the
  normalizer — verify these pass now that `transform.dwl` runs verbatim).
- Preserve the grouped-by-reason structure and the `IgnoreEntry { reason }`
  shape; `isIgnored` / `ignoreReason` keep working on the new keys.

### 6. CI — `main.yml`, both versions, master-only

The current Node TCK step stages once and runs `npm run test:tck`. Update it to
run against both supported corpus versions, master-only (as today):

- Parameterize `stageTckSuites` by a suite-version property so the corpus can be
  re-staged per version (e.g. `-PweaveTestSuiteVersion=<ver>` or an equivalent
  system property honored by the task).
- Run the tck lane once per version — `2.12.2-SNAPSHOT` then `2.13.0-SNAPSHOT` —
  against the single `2.13.0` dwlib build (re-staging the corpus between runs).
- No language-level flag (the binding doesn't expose one).
- Keep the existing master-only gating on the step.

## Files touched

| File | Change |
|------|--------|
| `native-lib/build.gradle` | `weaveTckSuite` deps → `:tck@zip`; parameterize `stageTckSuites` by version |
| `native-lib/node/tests/tck/transform.ts` | **delete** |
| `native-lib/node/tests/unit/tck-transform.test.ts` | **delete** |
| `native-lib/node/tests/tck/case-loader.ts` | scenario name = dir name; one scenario per dir |
| `native-lib/node/tests/unit/tck-case-loader.test.ts` | update to new scenario naming |
| `native-lib/node/tests/tck/formats.ts` | remove `familyForMime` (no dedicated test to remove) |
| `native-lib/node/tests/tck/compare.ts` | accept expected-side charset; decode via `decodeBytes` |
| `native-lib/node/tests/unit/tck-compare.test.ts` | add encoding-aware comparison cases |
| `native-lib/node/tests/tck/tck.test.ts` | drop normalizer; read `encoding` sidecar; run `transform.dwl` verbatim |
| `native-lib/node/tests/tck/ignore-list.ts` | re-key to full dir names; re-derive empirically |
| `.github/workflows/main.yml` | Node TCK step runs both versions, master-only |

## Testing / validation

1. `native-lib:stageTckSuites` stages runtime + core-modules `tck@zip` into
   `node/tests/tck/suites/`.
2. Build the Node package (`native-lib:buildNodePackage`) against the current
   dwlib, then `cd native-lib/node && npm run test:tck`.
3. The lane should be green with the re-derived ignore list — target parity with
   the pre-migration pass rate, minus/plus cases that legitimately changed.
4. Unit tests updated: `tests/unit/tck-case-loader.test.ts` to the new
   scenario-naming, `tests/unit/tck-compare.test.ts` for encoding-aware
   comparison; `tests/unit/tck-transform.test.ts` deleted.
5. Repeat step 1–3 for the `2.12.2-SNAPSHOT` corpus to confirm cross-version
   replay passes before wiring it into CI.

## Risks

- **Cross-version replay divergence.** Running the 2.12.2 corpus against the 2.13
  dwlib could surface a case that only passes under one version. Mitigation:
  re-derive the ignore list against *both* corpora; document any version-specific
  skip inline.
- **Encoding decode edge cases.** `decodeBytes` is already battle-tested from the
  charset fix; the sidecar set is small (7 cases) and directly inspectable.
