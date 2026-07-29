# GitHub Actions Runner Migration + macOS Artifacts

**Date:** 2026-07-29

## Goal

Update the runners used across the three GitHub Actions workflows, and add
macOS artifact production:

- **A) `ci.yml`** — keep the `mulesoft` runners, but make it a pure build+test
  workflow: remove all artifact output.
- **B) `main.yml` + `release.yml`** — replace the `mulesoft` runners with
  standard GitHub-hosted runners.
- **C) `main.yml` + `release.yml`** — add a macOS runner leg so a
  macOS-compatible artifact is produced.

## Current state

All three workflows use the same matrix:

```yaml
matrix:
  os: [ mulesoft-ubuntu, mulesoft-windows ]
  include:
    - os: mulesoft-ubuntu
      script_name: linux
    - os: mulesoft-windows
      script_name: windows
runs-on: ${{ matrix.os }}
```

- `ci.yml` — scheduled (Wednesdays 12:00 UTC). Builds, creates distro, builds
  Python wheel + Node package, runs Node tests. Then 3 upload-prep steps
  (*Derive platform tokens*, *Stage renamed CLI distro*, *Stage OS-qualified
  Node package*) and 4 `upload-artifact` steps (CLI distro, Python wheel, Node
  package, native shared library).
- `main.yml` — push/PR to master. Same build steps plus master-only regression
  + Node TCK conformance lanes, plus the same 3 prep + 4 upload-artifact steps.
- `release.yml` — on `v*` tags. Builds, then uploads assets to the GitHub
  Release via `svenstaro/upload-release-action`. The native shared library
  upload is split into **per-OS conditional** steps: Linux (`.so`), Windows
  (`.dll`), plus a shared header upload.

Artifact naming already derives arch from `uname -m` (`ARCH`), so adding a new
OS leg produces correctly-named artifacts automatically.

## Changes

### A) `ci.yml` — build + run only

- Matrix and `runs-on` **unchanged** (keep `mulesoft-ubuntu` + `mulesoft-windows`).
- **Remove** these steps (they exist only to produce/stage uploads):
  - *Derive platform tokens*
  - *Stage renamed CLI distro*
  - *Upload generated script*
  - *Upload Python wheel*
  - *Stage OS-qualified Node package*
  - *Upload Node package*
  - *Upload native shared library*
- **Keep** everything up to and including *Run Node.js Tests*.
- Result: `ci.yml` builds and runs tests, produces no artifacts.

### B) `main.yml` + `release.yml` — GitHub-hosted runners

Replace the matrix runner labels in both files:

- `mulesoft-ubuntu` → `ubuntu-latest`
- `mulesoft-windows` → `windows-latest`

`script_name` mappings (`linux`, `windows`) and all other steps stay the same.

### C) macOS leg on `main.yml` + `release.yml`

Add a third matrix entry to both files:

```yaml
matrix:
  os: [ ubuntu-latest, windows-latest, macos-latest ]
  include:
    - os: ubuntu-latest
      script_name: linux
    - os: windows-latest
      script_name: windows
    - os: macos-latest
      script_name: macos
```

- Chosen runner: **`macos-latest` (Apple Silicon, arm64)**. `uname -m` returns
  `arm64`, so all artifacts read `...-macos-arm64...` with no other change.
- **`main.yml`** — macOS is just another matrix leg; every existing step runs
  for it. The *Upload native shared library* step already lists `dwlib.dylib`
  in its `path`, so macOS is covered with no edit to that step.
- **`release.yml`** — add a new conditional step mirroring the existing
  Linux/Windows branches:

  ```yaml
  - name: Upload native shared library to release (macOS)
    if: runner.os == 'macOS'
    uses: svenstaro/upload-release-action@v2
    with:
      repo_token: ${{ secrets.GITHUB_TOKEN }}
      file: native-lib/python/src/dataweave/native/dwlib.dylib
      asset_name: dwlib-${{env.NATIVE_VERSION}}-${{ matrix.script_name }}-${{ env.ARCH }}.dylib
      tag: ${{ github.ref }}
      overwrite: true
  ```

  The shared *header* upload step is not OS-conditional and needs no change.

## Orchestration

A coordinator (main session) dispatches parallel subagents — one per workflow
file (`ci.yml`, `main.yml`, `release.yml`) since the edits are independent. All
work happens on a single new branch; a single PR is opened at the end.

## Risks / notes

- macOS `native-lib` native build uses `-J-Xmx6G`; `macos-latest` (arm64) has
  ~7GB RAM. Should fit but is the tightest leg — monitor the first run.
- The master-only regression + Node TCK steps in `main.yml` will also execute
  on the macOS leg for master pushes (expected, more coverage).
- macOS GitHub-hosted minutes bill at a higher multiplier than Linux/Windows.

## Testing

No unit tests apply. Validation is:
- YAML lint / parse sanity for all three files.
- Confirm no `mulesoft-*` labels remain in `main.yml` / `release.yml`, and that
  `ci.yml` still uses them.
- Confirm `ci.yml` has zero `upload-artifact` steps.
- Confirm the three-way matrix + macOS release branch exist.
