# CI `build-native` Composite Action

**Date:** 2026-07-30

## Goal

Remove the duplicated build prefix across the three GitHub Actions workflows
(`ci.yml`, `main.yml`, `release.yml`) by extracting it into a single composite
action — one source of truth for how the DataWeave native artifacts (CLI
distro, Python wheel, Node package, native lib) are built and tested in CI.

## Context

The three workflows each repeat the same ~9-step build prefix verbatim:
setup Gradle, setup GraalVM, run build, create distro, install Python build
deps, build the Python wheel, setup Node, build the Node package, run Node
tests. Every fix to that prefix (recent examples: `--break-system-packages`
for macOS PEP 668, the `windows-2022`/`distro_os` matrix work) currently has to
be applied in up to three places.

The actual build *logic* already lives in Gradle tasks (`nativeCompile`,
`distro`, `buildPythonWheel`, `buildNodePackage`); the YAML only invokes them.
So the duplication being removed is the **CI orchestration** of those tasks,
not the build logic itself.

This is a follow-up to the runner-migration work
(`2026-07-29-github-runners-migration-design.md`) and is delivered as a
**separate branch + PR** (`ci-composite-action`, stacked on
`runners-migration`).

## Current state

Shared prefix present in all three workflows (identical except `release.yml`
threads `-PnativeVersion=<ver>` and `ci.yml`'s pip step omits
`--break-system-packages`):

1. Setup Gradle (`gradle/actions/setup-gradle@v3`)
2. Setup GraalVM (`graalvm/setup-graalvm@v1`, Java 24, `graalvm-community`)
3. Run Build (`./gradlew … -PskipNodeTests=true build`)
4. Create Distro (`native-cli:distro`)
5. Install Python build dependencies (`pip … setuptools wheel`)
6. Create Native Lib Python Wheel (`native-lib:buildPythonWheel`)
7. Setup Node.js (`actions/setup-node@v4`, node 18)
8. Create Native Lib Node Package (`native-lib:buildNodePackage`)
9. Run Node.js Tests (`native-lib:nodeTest`)

Divergent tails (stay in each workflow, unchanged):

- `ci.yml` — nothing after the prefix (build + test only).
- `main.yml` — master-only regression tests (currently **interleaved** between
  Run Build and Create Distro) + master-only Node TCK conformance (after Run
  Node.js Tests), then artifact staging + `upload-artifact` steps.
- `release.yml` — tag-version derivation (`NATIVE_VERSION`, `ARCH`) and
  `svenstaro/upload-release-action` release-asset uploads (incl. the per-OS
  `dwlib` steps).

## Design

### The composite action

Location: `.github/actions/build-native/action.yml`
(`runs.using: "composite"`).

It owns the 9-step prefix as one contiguous block, each step preserved as a
separate step (so per-target failure granularity stays visible in the CI UI).
`actions/checkout` is **not** in the action — it must run first in the calling
job so the action's own files are on disk.

**Inputs:**

```yaml
inputs:
  github-token:
    description: Token for graalvm/setup-graalvm component downloads.
    required: true
  native-version:
    description: Passed as -PnativeVersion to Gradle. Empty string = omit the flag.
    required: false
    default: ''
  break-system-packages:
    description: >-
      Add --break-system-packages to the pip install (needed on GitHub-hosted
      macOS per PEP 668; not needed on the self-hosted mulesoft runners).
    required: false
    default: 'false'
```

Composite actions cannot read `secrets` or the caller's matrix directly, which
is why `github-token` is an input rather than a secret reference.

**Conditional `-PnativeVersion`** — the four Gradle steps (build, distro,
wheel, node package) use:

```yaml
run: ./gradlew … ${{ inputs.native-version != '' && format('-PnativeVersion={0}', inputs.native-version) || '' }}
shell: bash
```

Empty input → bare command (today's `ci`/`main` behavior); non-empty →
`-PnativeVersion=<ver>` (today's `release` behavior).

**Conditional pip flag:**

```yaml
run: python3 -m pip install ${{ inputs.break-system-packages == 'true' && '--break-system-packages' || '' }} --upgrade setuptools wheel
shell: bash
```

Every `run` step in a composite action **must** declare `shell: bash`.

### Caller wiring

| Input | `ci.yml` | `main.yml` | `release.yml` |
|---|---|---|---|
| `github-token` | `${{ secrets.GITHUB_TOKEN }}` | same | same |
| `native-version` | `''` (omit) | `''` (omit) | `${{ env.NATIVE_VERSION }}` |
| `break-system-packages` | `'false'` | `'true'` | `'true'` |

Note: `main.yml` intentionally does **not** thread a native version — it relies
on the `NATIVE_VERSION: 100.100.100` workflow-env default today, so
`native-version: ''` preserves current behavior exactly.

Each job becomes: `actions/checkout` → `uses: ./.github/actions/build-native`
(with the inputs above) → the workflow's own divergent tail.

### Ordering change in `main.yml`

`main.yml`'s master-only regression steps are currently interleaved *inside*
the prefix (between Run Build and Create Distro). To let the action own one
contiguous block, the master-only regression + TCK steps **move to after** the
action call.

This is functionally identical: the regression tests only need the compiled
CLI that Run Build already produced, and Gradle's up-to-date checks mean
running them after the Node build does not recompile anything. Only the CI
step *list order* changes.

## Risks / notes

- **Local-path action requires checkout first.** `uses: ./.github/actions/…`
  resolves against the checked-out workspace, so `actions/checkout` must be the
  first step in every calling job (it already is).
- **`shell:` is mandatory** on composite-action `run` steps — omitting it is a
  load-bearing gotcha (the action fails to parse).
- Expression fallbacks (`… && X || ''`) are how the conditional flag/version
  args are injected without duplicating whole steps; verify the rendered
  command matches the pre-refactor command per workflow.
- `ci.yml` runs on `mulesoft-*` runners; the action runs on whatever runner the
  calling job selects, so mixed runner types are fine.

## Testing

No unit tests apply. Validation is:

- YAML parse for `action.yml` and all three workflows.
- Confirm the rendered Gradle commands per workflow match the pre-refactor
  commands: `ci`/`main` have no `-PnativeVersion`; `release` has
  `-PnativeVersion=<ver>` on build, distro, wheel, and node-package steps.
- Confirm the pip step renders with `--break-system-packages` for
  `main`/`release` and without it for `ci`.
- Confirm each workflow still has its divergent tail intact (ci: none;
  main: regression/TCK + upload-artifact; release: svenstaro uploads).
- End-to-end: the `Build Native CLI` (`main.yml`) run is green on all three
  legs after the refactor; `ci.yml` and `release.yml` parse and their step
  lists are unchanged in behavior.
