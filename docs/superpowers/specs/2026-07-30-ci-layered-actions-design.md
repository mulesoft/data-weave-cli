# CI Layered Actions: Foundation + Per-Artifact Actions

**Date:** 2026-07-30

## Goal

Restructure CI so **adding a new product artifact touches the workflows as
little as possible** and **everything about one artifact lives in one place**.

Two layers:

- **One shared foundation action** — Gradle + GraalVM setup and the single
  `gradlew build` that compiles both native images.
- **One action per artifact** (`cli`, `python`, `node`, later `go`/`rust`) —
  each owns its full lifecycle (produce → optional test/tck → optional
  publish), with the phases selected by inputs.

Target outcome: a workflow reads `checkout → build-foundation → cli → python →
node`, one call per artifact, and adding **Go** is *one new action directory +
one line per workflow that wants it*.

## Context & motivation

PR #150 (`build-native`) removed the duplicated build prefix but is a monolith
that bakes Node- and Python-specific setup into a "generic" block. The roadmap
adds Go/Rust bindings; we want new artifacts to be drop-in. This evolves PR #150
in place (branch `ci-composite-action`, PR #150) — master never carries the
monolith.

### GitHub Actions constraint (decisive for the shape)

A composite action is **one directory / one `action.yml` / one callable unit** —
you cannot define several named sub-actions (`produce-cli`, `upload-cli`, …) in
one file. The other primitive, a reusable *workflow* (`workflow_call`), runs as
its **own job on its own runner**, so it would not see the native images the
foundation compiled without uploading/downloading them between jobs — which
breaks "build once, package many."

Therefore each artifact is **one composite action** that performs all its
phases, gated by inputs (Shape A). Naming is **by artifact, not phase** (`cli`,
not `produce-cli`), because the action does more than produce.

### Gradle dependency facts (verified in `native-lib/build.gradle`)

The layering is safe because packaging/test tasks declare their own
dependencies back to compilation, and Gradle up-to-date checks prevent
recompilation:

- `native-cli:distro` → `nativeCompile` (CLI image).
- `native-lib:buildPythonWheel` → `stagePythonNativeLib` → `stripNativeLibrary`
  → `nativeCompile` (`dwlib`).
- `native-lib:buildNodePackage` / `nodeTest` → `stageNodeNativeLib` →
  `stripNativeLibrary` → `nativeCompile`.

`gradlew build` in the foundation compiles both native images once;
`setup-gradle`/`setup-graalvm` configure the **job** env (PATH, `JAVA_HOME`),
so later actions in the same job invoke `./gradlew` with no re-setup. The
foundation must run before any artifact action.

### Per-artifact test/tck phases are asymmetric (verified)

- **CLI:** regression/TCK = `native-cli-integration-tests:test` (a *separate*
  Gradle module), master-only, two weave suite versions (2.12.2-SNAPSHOT,
  2.13.0-SNAPSHOT).
- **Node:** `nodeTest` (unit/integration, always) **plus** a master-only TCK
  conformance lane (`stageTckSuites` + `npm run test:tck`, two suite versions).
- **Python:** a `pythonTest` Gradle task exists but is **not** wired into any
  workflow today — no tck phase.

A rigid produce→tck→upload triple would misfit this. Each artifact action
therefore encodes *its own* test/tck phase (or none), gated by `run-tck`.
This spec preserves today's exact test wiring — it does not add Python tests or
change which tests run.

## Design

### Layer 1 — foundation action

`.github/actions/build-foundation/action.yml` (composite; renamed from
`build-native`).

Steps: Setup Gradle → Setup GraalVM (Java 24, `graalvm-community`,
`github-token`) → Run Build (`./gradlew … -PskipNodeTests=true build
<version>`).

Inputs: `github-token` (required); `native-version` (default `''`, empty omits
`-PnativeVersion`). Version arg: `${{ inputs.native-version != '' &&
format('-PnativeVersion={0}', inputs.native-version) || '' }}`.

### Layer 2 — per-artifact actions

Each is one composite action named for the artifact; every `run` step declares
`shell: bash`; each applies the version-arg conditional. Common inputs:

- `native-version` (default `''`).
- `run-tck` (default `'false'`) — when `'true'`, run this artifact's
  master-only test/tck phase in addition to its always-on tests.
- `publish` (default `'none'`) — `'none' | 'artifact' | 'release'`, selecting
  the publish phase. `'artifact'` = `actions/upload-artifact@v7` (main.yml CI
  retention, incl. any staging/rename this artifact needs); `'release'` =
  `svenstaro/upload-release-action@v2` (release assets, per-OS names, tag).
- Publish-only inputs, consumed only when `publish != 'none'`:
  `github-token` / `repo-token`, `native-version`, `arch`, `script-name`,
  `distro-os`, `tag` as each artifact requires. (Exact per-artifact input set
  is finalized in the plan.)

**`.github/actions/cli/action.yml`** — produce: `native-cli:distro`; tck
(if `run-tck`): `native-cli-integration-tests:test` × two suite versions;
publish: distro zip (`artifact` stages `dw-cli-…` and uploads; `release`
uploads via svenstaro with `asset_name`).

**`.github/actions/python/action.yml`** — produce: pip deps (owns
`--break-system-packages`, gated by a `break-system-packages` input, default
`'false'`) + `native-lib:buildPythonWheel`; tck: none (preserves today);
publish: the wheel.

**`.github/actions/node/action.yml`** — produce: `setup-node` +
`native-lib:buildNodePackage` + `nodeTest` (always); tck (if `run-tck`):
`stageTckSuites` + `npm run test:tck` × two suite versions; publish: the `.tgz`
(`artifact` stages OS-qualified name; `release` via svenstaro).

**dwlib (raw `.so`/`.dll`/`.dylib` + `.h`)** is a 4th uploaded thing today,
separate from the wheel/tgz that embed it. Whether it becomes its own
`native-lib` artifact action or rides along with one binding is **deferred to
the implementation plan.**

**Future** `go`, `rust`: same shape, added without touching foundation or other
artifact actions.

### Workflow composition (target)

Checkout stays first (local `uses:` needs the workspace). Foundation second.

**`ci.yml`** (mulesoft matrix; produce only, no tck, no publish):
```
checkout
build-foundation   (github-token)
cli                (defaults: run-tck false, publish none)
python             (break-system-packages omitted → false)
node
```

**`main.yml`** (ubuntu-latest / windows-2022 / macos-latest):
```
checkout
build-foundation   (github-token)
cli                (run-tck: master?, publish: 'artifact', + arch/script-name/distro-os)
python             (break-system-packages: 'true', publish: 'artifact')
node               (run-tck: master?, publish: 'artifact', + arch/script-name)
```
`run-tck` is passed `${{ github.ref == 'refs/heads/master' }}` so the tck phase
stays master-only, matching today.

**`release.yml`** (tag builds):
```
checkout
Guess Extension Version   (sets NATIVE_VERSION, ARCH — must precede foundation)
build-foundation   (github-token, native-version: ${{ env.NATIVE_VERSION }})
cli                (native-version, publish: 'release', repo-token, tag, arch, script-name, distro-os)
python             (native-version, break-system-packages: 'true', publish: 'release', repo-token, tag)
node               (native-version, publish: 'release', repo-token, tag, arch, script-name)
```
Plus whatever the dwlib decision (planning) yields, and the shared header
upload.

### Behavioral equivalence

Pure restructuring — the effective commands, the artifacts produced, their
names, and which tests run per trigger must be **identical** to PR #150's
current green state:

- version arg: empty for ci/main, `${{ env.NATIVE_VERSION }}` for release.
- `--break-system-packages`: only on main/release python.
- master-only tck: CLI regression + Node TCK gated exactly as today; Python
  still has none.
- publish: ci none; main `upload-artifact` (same staged names); release
  svenstaro (same `asset_name`s, per-OS dwlib, header).

## Naming decisions

- Foundation: **`build-foundation`**.
- Artifact actions named **by artifact**: `cli`, `python`, `node` (referenced
  `uses: ./.github/actions/cli`). Not `produce-cli` — the action owns
  produce + tck + publish, selected by inputs.

## Risks / notes

- **Input surface grows** on each artifact action (publish/tck knobs). This is
  the deliberate tradeoff for one-call-per-artifact + one-file-per-artifact;
  accepted per the goal. Keep inputs documented in each `action.yml`.
- **Produce/publish now co-located** inside each artifact action (reverses the
  earlier separation proposal) — a conscious choice favoring fewer workflow
  touch-points over strict separation, reasonable for a small, stable set of
  publish mechanisms.
- **Local composite actions require checkout first** (unchanged). **`shell:
  bash` mandatory** on every composite `run` step.
- **`release.yml` ordering:** `Guess Extension Version` before
  `build-foundation` (populates `NATIVE_VERSION`/`ARCH`).
- **Job-scoped setup ordering:** foundation before all artifact actions.
- More step groups per job in the CI UI; negligible vs. native-image compile
  time.

## Testing / validation

No unit tests apply. Validation:
- YAML parse for every action file and all three workflows.
- Rendered commands per workflow equal PR #150's green state (version arg,
  break-system-packages, tck gating, publish mechanism + artifact names).
- No `build-native` reference remains; foundation is `build-foundation`.
- Each workflow composes `build-foundation → cli → python → node` (foundation
  first), one call per artifact.
- End-to-end: the `Build Native CLI` (`main.yml`) PR run green on all three
  legs — the live proof, same bar PR #150 cleared. `ci.yml`/`release.yml`
  validated by parse + command inspection (not triggered on PRs).
