# CI Layered Actions: Foundation + Per-Artifact Packaging

**Date:** 2026-07-30

## Goal

Restructure the CI build orchestration from one monolithic `build-native`
composite action into a **two-layer** design:

- **One shared foundation action** — the parts every artifact needs (Gradle +
  GraalVM setup, the single `gradlew build` that compiles both native images).
- **One packaging action per product artifact** — CLI, Python, Node today; Go,
  Rust, etc. later — each owning its own toolchain setup and build/test steps.

This makes each artifact an independently-composable unit so a future binding
is *one new action file + one line per workflow that wants it*, dragging in no
setup it doesn't need.

## Context & motivation

PR #150 (`build-native` composite action) removed the byte-for-byte duplicated
build prefix across `ci.yml`/`main.yml`/`release.yml`. It is correct but
monolithic: it bakes **Node-specific** (`actions/setup-node`) and
**Python-specific** (`pip install … --break-system-packages`) setup into a
block that presents as generic. That is invisible today because all three
workflows build all three artifacts — but the roadmap adds Go/Rust bindings,
at which point a binding-specific build would drag in unrelated toolchains and
new bindings would mean editing one growing action.

PR #150 is **not yet merged**, so this evolves that PR in place — master never
carries the monolith. Same branch (`ci-composite-action`), same PR (#150).

### Gradle dependency facts (verified in `native-lib/build.gradle`)

The layering is safe because the packaging tasks already declare their own
dependencies back to compilation:

- `native-cli:distro` dependsOn `nativeCompile` (the CLI image).
- `native-lib:buildPythonWheel` dependsOn `stagePythonNativeLib` →
  `stripNativeLibrary` → `nativeCompile` (the `dwlib` shared library).
- `native-lib:buildNodePackage` dependsOn `stageNodeNativeLib` →
  `stripNativeLibrary` → `nativeCompile`.

So the foundation's `gradlew build` compiles both native images once; each
packaging action is a thin layer Gradle will not recompile (up-to-date checks
short-circuit the native builds). `setup-gradle`/`setup-graalvm` configure the
**job** environment (PATH, `JAVA_HOME`), so packaging actions later in the same
job invoke `./gradlew` with no re-setup.

## Current state (PR #150, to be evolved)

`.github/actions/build-native/action.yml` — one composite action, 9 steps:
Setup Gradle, Setup GraalVM, Run Build, Create Distro, Install Python deps,
Create Python Wheel, Setup Node, Create Node Package, Run Node Tests. Inputs:
`github-token`, `native-version`, `break-system-packages`.

Workflows call it as: `checkout → build-native(...) → <tail>`.

## Design

### Layer 1 — foundation action

`.github/actions/build-foundation/action.yml` (composite). Renamed from
`build-native`.

Steps:
1. Setup Gradle (`gradle/actions/setup-gradle@v3`)
2. Setup GraalVM (`graalvm/setup-graalvm@v1`, Java 24, `graalvm-community`,
   `github-token: ${{ inputs.github-token }}`)
3. Run Build — `./gradlew --stacktrace --no-problems-report -PskipNodeTests=true build <version>`

Inputs:
- `github-token` (required) — for graalvm setup component downloads.
- `native-version` (default `''`) — empty omits `-PnativeVersion`.

The conditional version arg: `${{ inputs.native-version != '' &&
format('-PnativeVersion={0}', inputs.native-version) || '' }}`.

### Layer 2 — per-artifact packaging actions

Each is a composite action; each `run` step declares `shell: bash`; each takes
`native-version` (default `''`) and applies the same conditional version arg.

**`.github/actions/package-cli/action.yml`**
- Step: Create Distro — `./gradlew … native-cli:distro <version>`
- Inputs: `native-version`.

**`.github/actions/package-python/action.yml`**
- Step: Install Python build dependencies —
  `python3 -m pip install <break-flag> --upgrade setuptools wheel`
- Step: Create Native Lib Python Wheel — `./gradlew … native-lib:buildPythonWheel <version>`
- Inputs: `native-version`; `break-system-packages` (default `'false'`) — the
  flag now lives with the artifact that needs it (macOS PEP 668), not in a
  generic block. Conditional: `${{ inputs.break-system-packages == 'true' &&
  '--break-system-packages' || '' }}`.

**`.github/actions/package-node/action.yml`**
- Step: Setup Node.js (`actions/setup-node@v4`, node 18)
- Step: Create Native Lib Node Package — `./gradlew … native-lib:buildNodePackage <version>`
- Step: Run Node.js Tests — `./gradlew … native-lib:nodeTest <version>`
- Inputs: `native-version`.

**Future** `package-go`, `package-rust`: same shape — own toolchain setup +
build/test — added without touching foundation or the other packaging actions.

### Workflow composition

Every calling job stays: `actions/checkout@v4` first (local `uses:` needs the
workspace on disk), then the pipeline, then that workflow's divergent tail.

**`ci.yml`** (self-hosted mulesoft matrix, no tail):
```
checkout
build-foundation      (github-token)
package-cli
package-python        (break-system-packages omitted → 'false')
package-node
```

**`main.yml`** (ubuntu-latest/windows-2022/macos-latest):
```
checkout
build-foundation      (github-token)
package-cli
package-python        (break-system-packages: 'true')
package-node
<master-only regression 2.12.2 / 2.13.0>
<master-only Node TCK 2.12.2 / 2.13.0>
<Derive platform tokens, staging, upload-artifact steps>
```

**`release.yml`** (tag builds):
```
checkout
Guess Extension Version           (sets NATIVE_VERSION, ARCH — must precede)
build-foundation      (github-token, native-version: ${{ env.NATIVE_VERSION }})
package-cli           (native-version: ${{ env.NATIVE_VERSION }})
package-python        (native-version: …, break-system-packages: 'true')
package-node          (native-version: …)
<svenstaro upload steps: binaries, wheel, node, per-OS dwlib, header>
```

### Behavioral equivalence

The rendered commands per workflow must be **identical** to PR #150's current
(green) state — this is a pure restructuring, no behavior change:
- `native-version` empty for ci/main, `${{ env.NATIVE_VERSION }}` for release
  (on foundation + all three packaging actions).
- `--break-system-packages` present only on main/release `package-python`,
  absent on ci `package-python`.
- Regression/TCK/upload tails byte-identical to today.

## Naming decisions

- Foundation action: **`build-foundation`** (signals shared base layer, paired
  with `package-*`).
- CLI gets a full **`package-cli`** action for symmetry (even though it's one
  Gradle step), so all artifacts are uniform and equally extensible.

## Risks / notes

- **Local composite actions require checkout first** — already true in all
  three workflows; unchanged.
- **`shell: bash` mandatory** on every composite `run` step.
- **Job-scoped setup:** foundation's `setup-gradle`/`setup-graalvm` and
  `package-node`'s `setup-node` configure the job env; ordering guarantees each
  `./gradlew`/npm call sees its toolchain. Foundation must run before any
  packaging action (it produces the compiled images they package).
- **`release.yml` ordering:** `Guess Extension Version` must stay before
  `build-foundation` so `env.NATIVE_VERSION` is populated when action inputs
  evaluate.
- Slightly more files (4 actions vs 1). Justified by the stated Go/Rust
  roadmap — this is designing a known seam, not speculative generality.
- More actions per job = marginally more step-group overhead in the CI UI;
  negligible next to native-image compile time.

## Testing / validation

No unit tests apply. Validation:
- YAML parse for all four action files and all three workflows.
- Confirm rendered commands per workflow equal PR #150's: version arg presence
  per workflow; `--break-system-packages` only on main/release python; tails
  intact.
- Confirm no `build-native` reference remains; foundation is `build-foundation`.
- Confirm each workflow composes `build-foundation → package-cli →
  package-python → package-node` in order, foundation first.
- End-to-end: the `Build Native CLI` (`main.yml`) PR run is green on all three
  legs (ubuntu-latest, windows-2022, macos-latest) — the live proof, same bar
  PR #150 already cleared. `ci.yml`/`release.yml` validated by parse + command
  inspection (they don't run on PRs).
