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
- `mulesoft-windows` → `windows-2022`

`script_name` mappings (`linux`, `windows`) and all other steps stay the same.

> **Windows label: `windows-2022`, not `windows-latest`.** The migration
> originally targeted `windows-latest`, but that image rolled forward to
> Visual Studio 18 (VS 2026, install path `…\Microsoft Visual Studio\18\…`),
> which the pinned `node-gyp` 11.5.0 cannot detect (`find VS unknown version
> "undefined"`) — the Node addon build fails. `windows-2022` ships the VS 2022
> / v17 toolchain that node-gyp 11 supports (the same toolchain class the
> self-hosted `mulesoft-windows` runner uses) and is still a standard
> GitHub-hosted runner, satisfying goal B.
>
> **Follow-up:** when `windows-2022` is eventually retired, the durable fix is
> bumping `node-gyp` to `^12.1.0` (which added VS 18/2026 support) and
> returning to `windows-latest`.

### C) macOS leg on `main.yml` + `release.yml`

Add a third matrix entry to both files:

```yaml
matrix:
  os: [ ubuntu-latest, windows-2022, macos-latest ]
  include:
    # script_name → artifact-name OS token (our naming convention).
    # distro_os   → Gradle distro classifier (native-cli getOsName():
    #               macOS is "osx", not "macos"), used for the source
    #               filename produced by native-cli:distro.
    - os: ubuntu-latest
      script_name: linux
      distro_os: linux
    - os: windows-2022
      script_name: windows
      distro_os: windows
    - os: macos-latest
      script_name: macos
      distro_os: osx
```

- Chosen runner: **`macos-latest` (Apple Silicon, arm64)**. `uname -m` returns
  `arm64`, so all artifacts read `...-macos-arm64...` with no other change.
- **`distro_os` field.** `native-cli:distro` names its zip with the Gradle
  `getOsName()` classifier, which is **`osx`** on macOS (not `macos`). The
  staging step that renames the distro to the `dw-cli-<ver>-<os>-<arch>.zip`
  convention (and `release.yml`'s upload `file:`) must reference the *source*
  file by its `distro_os` classifier, while the *convention* artifact name
  keeps `script_name` (`macos`). For linux/windows the two tokens coincide, so
  only macOS needs the distinction.
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
- `windows-2022` is pinned (see section B); it is not deprecated today but will
  eventually be retired — track the node-gyp `^12` follow-up before then.

## Deviations discovered during CI

These fixes were required to get all three legs green and are outside the
original A/B/C scope; recorded here so the as-shipped state is complete.

- **GraalVM buildtools `0.11.2 → 0.11.5`** (`build.gradle`). On `windows-latest`
  the toolcache (C:) and workspace (D:) live on different drives; buildtools
  0.11.2 called `Path.relativize` across drive roots and crashed
  `nativeCompile` (`'other' has different root`). 0.11.5 guards relativize to
  same-root paths.
- **`node-gyp` bumped to `^11`** (`native-lib/node/package.json` +
  regenerated lockfile). Intended to fix VS detection on `windows-latest`; it
  did **not** (11.5.0 still can't see VS 18), which is why the Windows leg was
  pinned to `windows-2022` instead. The bump is harmless and left in place.
- **`--break-system-packages`** added to the Python build-deps step in
  `main.yml` + `release.yml`. macOS system Python enforces PEP 668 and refuses
  the plain `pip install --upgrade setuptools wheel`.

## Testing

No unit tests apply. Validation is:
- YAML lint / parse sanity for all three files.
- Confirm no `mulesoft-*` labels remain in `main.yml` / `release.yml`, and that
  `ci.yml` still uses them.
- Confirm the Windows leg uses `windows-2022` (not `windows-latest`).
- Confirm `ci.yml` has zero `upload-artifact` steps.
- Confirm the three-way matrix + macOS release branch exist, and that the
  distro staging/upload references `matrix.distro_os` for the source filename.
- End-to-end: all three legs (`ubuntu-latest`, `windows-2022`, `macos-latest`)
  of the `Build Native CLI` workflow pass — confirmed green on commit `4bb7e46`.
