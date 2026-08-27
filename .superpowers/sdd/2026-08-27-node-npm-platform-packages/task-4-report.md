**Task:** Docs and leftover `@dataweave/native` strings

**Files changed:**
- `native-lib/node/README.md` — documents unscoped npm installation, local meta/platform tarballs, supported platforms, and updated imports.
- `native-lib/README.md` — documents unscoped npm installation, local meta/platform tarballs, supported platforms, and updated imports.
- `native-lib/node/docs/external-modules.md` — updates package imports and references to `dataweave-native`.
- `benchmarks/README.md` — updates the extracted Node package name.
- `benchmarks/runners/node/wrapper.mjs` — updates the customer-facing error and wrapper comment.
- `native-lib/node/tests/tck/tck.test.ts` — updates the binding name in the TCK comment.

**Test added/modified:**
- No executable test was appropriate for documentation/string-only changes; the required grep is the acceptance check.
- Failing output before fix: customer-facing hits appeared in the listed READMEs, external-module docs, benchmark files, and TCK comment.
- Passing output after fix: only historical/intentional hits remain (design spec, `node-api-plan.md`, and the legacy-input fixture in `pack-packages.test.mjs`).

**Acceptance command output:**

```text
$ rg '@dataweave/native' --glob '!**/node_modules/**' --glob '!**/package-lock.json'
docs/superpowers/specs/2026-08-27-node-npm-platform-packages-design.md:- Keeping `@dataweave/native` as a published name (nothing is on npm yet;
docs/superpowers/specs/2026-08-27-node-npm-platform-packages-design.md:  relative; docs and comments that mention `@dataweave/native` are updated to
docs/superpowers/specs/2026-08-27-node-npm-platform-packages-design.md:- Single package `@dataweave/native@0.0.1`.
docs/superpowers/specs/2026-08-27-node-npm-platform-packages-design.md:Benchmark `wrapper.mjs` and READMEs: replace `@dataweave/native` with
docs/superpowers/specs/2026-08-27-node-npm-platform-packages-design.md:- Deprecation alias `@dataweave/native`.
native-lib/node/node-api-plan.md:Add a Node.js package (`@dataweave/native`) that mirrors the existing Python API, exposing the DataWeave native shared library (`dwlib`) via a N-API native addon. The package will be built as a platform-specific tarball (`.tgz`) and uploaded to GitHub Releases alongside the Python wheel.
native-lib/node/node-api-plan.md:import { run, runStreaming, runTransform, cleanup } from '@dataweave/native';
native-lib/node/node-api-plan.md:import { DataWeave } from '@dataweave/native';
native-lib/node/scripts/pack-packages.test.mjs:    name: "@dataweave/native",

$ git diff --check
[no output]
```

**Out-of-scope observations:**
- `native-lib/node/node-api-plan.md` retains historical references as explicitly allowed.
- `native-lib/node/scripts/pack-packages.test.mjs` intentionally uses the old scoped name as an input fixture proving package metadata is rewritten.

**Surprises:**
- None.

**Commit:** `1770907 docs(node): document unscoped dataweave-native packages`

## Review follow-up

- Corrected local install examples to use tarballs emitted directly under `native-lib/node/`.
- Clarified that published npm packages support only `linux-x64`, `win32-x64`, and `darwin-arm64`; macOS x86_64 remains available only as a source build.
