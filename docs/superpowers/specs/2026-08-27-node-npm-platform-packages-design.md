# Node npm Platform Packages Design

**Status:** Draft (pending review)
**Date:** 2026-08-27
**Scope:** Node packaging prework only. Does not publish to npm, does not add a
promote workflow, and does not change `release.yml` beyond the Node asset
filenames the existing composite action already uploads.

## Goal

Ship GitHub Release / CI Node artifacts that match the industry native-addon
layout (one install name, one optional package per OS/arch) and the PDK
publication identity (unscoped npm name, MuleSoft provenance in package
metadata). A later, separate promote workflow can `npm publish` those tarballs
without rebuilding.

Customer command after promote (out of this spec):

```bash
npm install dataweave-native
```

## Non-goals

- Publishing to npmjs (OIDC / Trusted Publisher / `workflow_dispatch` promote).
- Changing Python wheels, CLI zips, Homebrew, or Chocolatey.
- Asking users to install a platform-specific package by name.
- Compiling Graal/`dwlib` at `npm install` time.
- Keeping `@dataweave/native` as a published name (nothing is on npm yet;
  hard-cut the package `name` field). In-repo TypeScript import paths stay
  relative; docs and comments that mention `@dataweave/native` are updated to
  `dataweave-native`.

## Constraints (locked in brainstorming)

- Follow PDK: **unscoped** `dataweave-native`, not `@mulesoft/dataweave-native`.
- Follow native-addon convention: **four** npm packages at the same version.
- Platforms = current CI matrix: `linux-x64`, `win32-x64`, `darwin-arm64`
  (`macos-latest` is Apple Silicon). No `darwin-x64` in v1.
- Version inside every `package.json` equals the Gradle `nativeVersion`
  (`-PnativeVersion` on tag builds; default from `gradle.properties` otherwise).
  Stop hardcoding `0.0.1` in Node pack output.
- `nodeTest` / local gyp rebuild stay in-tree. Developers do not need the
  optional packages installed to run Vitest.
- Do not add npm publish steps to `release.yml`.

## Current state

- Single package `@dataweave/native@0.0.1`.
- `ffi.ts` loads `../build/Release/dwlib_addon.node` only.
- `findLibrary()` already probes `<pkg>/native/dwlib.*`.
- `buildNodePackage` runs `npm pack` once → `dataweave-native-0.0.1.tgz`.
- CI stages that file as `dataweave-node-0.0.1-<os>-<arch>.tgz` and uploads
  **one tgz per OS**, all with the **same** npm `name`+`version`. npm cannot
  accept more than one of those as the same package version.

## Package set

| npm `name` | Contents | `os` / `cpu` | Customer-facing |
|---|---|---|---|
| `dataweave-native` | compiled JS (`dist/`), types, **no** `dwlib`, **no** `.node` | none | yes — only public install name |
| `dataweave-native-linux-x64` | `dwlib_addon.node` + `dwlib.so` | `linux` / `x64` | no |
| `dataweave-native-win32-x64` | `dwlib_addon.node` + `dwlib.dll` | `win32` / `x64` | no |
| `dataweave-native-darwin-arm64` | `dwlib_addon.node` + `dwlib.dylib` | `darwin` / `arm64` | no |

Tokens follow npm’s `process.platform` / `process.arch` (`win32`, `darwin`,
`x64`, `arm64`), not CI’s `script_name` (`macos`) or `uname -m` (`x86_64`).

Meta `package.json` (same version on all four):

```json
{
  "name": "dataweave-native",
  "version": "<nativeVersion>",
  "author": "MuleSoft",
  "repository": {
    "type": "git",
    "url": "git+https://github.com/mulesoft/data-weave-cli.git"
  },
  "homepage": "https://dataweave.mulesoft.com/",
  "bugs": {
    "url": "https://github.com/mulesoft/data-weave-cli/issues"
  },
  "optionalDependencies": {
    "dataweave-native-linux-x64": "<nativeVersion>",
    "dataweave-native-win32-x64": "<nativeVersion>",
    "dataweave-native-darwin-arm64": "<nativeVersion>"
  }
}
```

Use the real public GitHub repo URL that this project releases from (adjust if
the canonical remote differs). Platform packages set `os`/`cpu`, omit
`optionalDependencies`, and do not export a JS API.

`files` for a platform package:

- `dwlib_addon.node`
- `dwlib.<so|dll|dylib>` (that platform only)
- `package.json` / `README.md` (one-liner: “optional native binary for
  `dataweave-native`”)

Do not ship `src/addon.c`, `binding.gyp`, or `gypfile: true` on published
platform packages. Install must not run `node-gyp`.

## Load order

`getAddon()` in `ffi.ts` tries, in order:

1. In-tree / unpacked-from-source: `<pkg>/build/Release/dwlib_addon.node`
   (today’s path; keeps `nodeTest` and `npm run build` working).
2. Same package (dev or accidental fat layout): `<pkg>/native/dwlib_addon.node`.
3. Optional dependency: `require("dataweave-native-<platform>-<arch>")` where
   the platform package’s `main` is `./dwlib_addon.node`.

If all fail, throw `DataWeaveError` (or the existing Error) that names the
expected optional package and the current `process.platform`/`process.arch`.

`findLibrary()` stays as today for `dwlib.*`, with one addition: if the addon
was loaded from an optional package, prefer `dwlib.*` next to that addon
(the platform package root). Env `DATAWEAVE_NATIVE_LIB` still wins.

Unsupported host (e.g. `darwin-x64`): optional deps are skipped by npm; loader
throws a clear “unsupported platform” message listing the three supported
pairs.

## Layout in the repo

Keep developing in `native-lib/node` (meta sources + gyp + tests).

Generate platform package directories at pack time under
`native-lib/node/build/npm/<name>/` (gitignored). Do not commit three copies of
binaries.

Suggested pack script (`native-lib/node/scripts/pack-packages.mjs` or Gradle
`doLast`):

1. Read `nativeVersion` (env `NATIVE_VERSION` or rewrite `package.json`
   `version` before pack; Gradle already has `nativeVersion`).
2. Set meta `name` to `dataweave-native` and `version` to that value; write
   `optionalDependencies` with the same version.
3. `npm pack` the meta package (after `tsc`; **exclude** `native/`,
   `build/Release/`, `binding.gyp` from the meta tarball via `files`).
4. On this OS only, create the matching platform package dir, copy
   `build/Release/dwlib_addon.node` and staged `native/dwlib.*`, write
   `package.json` (`main`: `./dwlib_addon.node`, `os`, `cpu`, `version`),
   `npm pack` that dir.

CI therefore still produces **one platform tgz per runner**. Meta tgz is
identical on every OS; upload it from **linux only** (or checksum-equal from
any one job) so the Release is not overwritten with three identical metas.

## Versioning

- Source `native-lib/node/package.json` `version` may remain a placeholder in
  git **or** be rewritten in `buildNodePackage` from `project.version` /
  `-PnativeVersion`. The **packed** tarball `package.json` must contain the
  release version.
- `npm pack` output names become `dataweave-native-<ver>.tgz` and
  `dataweave-native-<platform>-<arch>-<ver>.tgz`.
- GitHub Release / CI **asset** names (human convention, still used by a
  future promote job):

  ```
  dataweave-native-<ver>.tgz
  dataweave-native-linux-x64-<ver>.tgz
  dataweave-native-win32-x64-<ver>.tgz
  dataweave-native-darwin-arm64-<ver>.tgz
  ```

  This **replaces** `dataweave-node-0.0.1-<script_name>-<arch>.tgz` for Node
  only. CLI/Python/dwlib names from the 2026-07-28 convention spec stay.

- `.github/actions/node/action.yml` staging/upload paths must use
  `inputs.native-version` (release) or the Gradle default version (main), not
  `0.0.1`. Map `script_name`+`arch` to npm tokens:

  | CI `script_name` + `uname -m` | npm package / asset token |
  |---|---|
  | `linux` + `x86_64` | `linux-x64` |
  | `windows` + `x86_64` | `win32-x64` |
  | `macos` + `arm64` | `darwin-arm64` |

## Gradle

`buildNodePackage`:

- Still depends on `stageNodeNativeLib`.
- After gyp + `tsc`, run the pack script (not a single `npm pack` of the
  fat tree).
- Outputs: meta tgz + this-platform tgz under `native-lib/node/`.

`nodeTest` is unchanged: stage lib, `npm install`, gyp, tsc, vitest. It must
not require optional packages.

`clean` deletes `node/build/npm` and generated tgz names.

## CI / release action

`.github/actions/node/action.yml`:

- `publish: artifact` / `release`: upload the **platform** tgz from this
  runner using the asset names above.
- Upload the **meta** tgz only when `script-name == linux` (one copy).
- `file` / `asset_name` must not hardcode `dataweave-native-0.0.1.tgz`.

`release.yml` itself stays “call the node action”; behavior change is inside
the composite action.

## Tests

- Unit: `getAddon` / new `resolveAddonPath` tries in-tree path first; then
  optional package name for a mocked `process.platform`/`arch`; throws on
  unknown platform with the supported list.
- Unit: `findLibrary` still honors env and packaged `native/`; add case
  “library beside resolved addon path”.
- Integration / TCK: unchanged in-tree gyp path (step 1 of load order).
- Pack smoke (can be a small Node test or Gradle check): after a mocked pack
  of fixtures, meta `package.json` has the three `optionalDependencies` and
  does not include `dwlib.*`; platform `package.json` has `os`/`cpu` and
  `main` pointing at the `.node`.

Benchmark `wrapper.mjs` and READMEs: replace `@dataweave/native` with
`dataweave-native`. `DW_BENCH_NODE_PACKAGE` still points at an extracted
**meta** tree with in-tree `dist/` + `native/` (dev layout). Optional-package
install layout is not required for benchmarks in this spec.

## Docs

Update `native-lib/node/README.md`, `native-lib/README.md`, and install
snippets: `npm install dataweave-native` (future) and local
`npm install ./dataweave-native-<ver>.tgz` plus the matching platform tgz if
testing the published layout. Document the three supported platforms.

## Success criteria

- `npm pack` / `buildNodePackage` on one OS emits meta tgz + that OS’s
  platform tgz; meta tarball has no native binaries.
- Four distinct npm `name`s; same `version`; npm can accept all four at that
  version (no collision).
- `nodeTest` still passes without installing optional packages.
- CI/release attach the four assets (3 platforms + 1 meta) with versioned
  names.
- No npm publish in this change set.

## Out of scope (follow-up)

- Promote workflow, OIDC Trusted Publisher, exists-check against registry.
- `darwin-x64` / `linux-arm64` packages.
- Python version alignment (still `0.0.1` in wheel names until a Python
  prework).
- Deprecation alias `@dataweave/native`.
