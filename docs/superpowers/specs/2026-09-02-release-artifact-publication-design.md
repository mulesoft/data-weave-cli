# Release Artifact Publication Design

**Status:** Approved
**Date:** 2026-09-02
**Scope:** Move GitHub Release publication out of the cross-platform build jobs
in `.github/workflows/release.yml`. Preserve the existing Linux, Windows, and
macOS artifact matrix.

## Goal

Publish release assets from a MuleSoft-managed Ubuntu runner after all native
platform builds succeed, so the GitHub-hosted macOS runner never accesses the
GitHub Releases API and is not blocked by the organization IP allow list.

## Background

Release run `33542023719` built the macOS ARM64 native artifacts successfully,
then failed in the CLI composite action while `svenstaro/upload-release-action`
requested release `v1.0.37`. GitHub rejected the request because the
GitHub-hosted macOS runner IP is outside the `mulesoft` organization allow list.

The existing `mulesoft-ubuntu` and `mulesoft-windows` jobs completed. The
normal CI workflow already demonstrates artifact-only output from every matrix
job.

## Design

The existing tag-triggered release workflow remains a single workflow with two
phases:

1. A three-platform matrix builds native CLI, Python, Node, and native library
   outputs. Each job uses `publish: artifact`; none uploads to a GitHub Release.
2. A non-matrix `publish-release` job runs on `mulesoft-ubuntu`, requires the
   entire build matrix to succeed, downloads all produced artifacts, creates a
   GitHub Release when its tag has no release, and uploads the downloaded files.

The publishing job uses `GITHUB_TOKEN` and `permissions: contents: write`.
It runs only on a MuleSoft-managed runner, whose IP is accepted by the
organization allow list.

## Artifact Contract

All artifacts must have explicit, unique names that include the package version,
platform, and architecture where applicable. The artifact names must be stable
enough for the publishing job to download them without inspecting a prior job's
output.

The publication job uploads only the packaged distribution files, not the
artifact archive wrappers. It preserves the release asset names currently
published by each composite action:

- CLI: `dw-cli-<version>-<linux|windows|macos>-<arch>.zip`
- Node: `dataweave-native-<platform>-<version>.tgz`, plus the Linux-produced
  `dataweave-native-<version>.tgz` meta package
- Python: the existing platform-qualified built wheel filename
- Native library: platform-specific `dwlib-<version>-<platform>-<arch>` file,
  plus the shared header only once

The Python packaging already produces platform-qualified wheel filenames. The
publisher must retain and upload each wheel, so Linux, Windows, and macOS
customers receive their matching native-library build. The release publisher
must upload each distinct asset exactly once.

## Publish Semantics

For a tag `v<version>`, `publish-release` must:

1. Download all release artifacts produced by the build matrix.
2. Test whether the matching GitHub Release exists.
3. Create it with generated notes only when absent.
4. Upload every asset with replacement enabled, making reruns idempotent.

The composite actions expose only `none` and `artifact` publication modes. No
platform build job may invoke `svenstaro/upload-release-action`, `gh release`,
or any GitHub Releases API endpoint. A failed matrix job prevents
`publish-release` from starting, so a partial release is never published.

## Non-goals

- Changing the set of supported operating systems or architectures.
- Replacing the GitHub-hosted macOS runner.
- Publishing to Homebrew, npm, PyPI, or any other registry.
- Modifying the manual Homebrew promotion workflow.
- Publishing a release before every platform build completes.

## Testing

- Validate workflow syntax with the available YAML parser and `actionlint` when
  available.
- Inspect the generated workflow to verify build jobs use artifact publication
  only and the sole release API consumer runs on `mulesoft-ubuntu`.
- Trigger a test tag in a controlled repository or perform a release dry run if
  repository policy permits it; verify macOS build artifacts are downloaded and
  attached from the Ubuntu publisher.

## Success Criteria

- A tag build produces artifacts on Linux, Windows, and macOS without calling
  the GitHub Releases API from GitHub-hosted macOS.
- The `mulesoft-ubuntu` publisher creates the GitHub Release if it is missing.
- A rerun replaces assets on an existing release rather than failing.
- A failed build matrix prevents release creation or asset upload.
- Release assets retain their existing external filenames.
