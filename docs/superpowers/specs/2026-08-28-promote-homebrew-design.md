# Promote Homebrew CLI Design

**Status:** Draft (pending review)
**Date:** 2026-08-28
**Scope:** First promote target only — Homebrew tap for the `dw` CLI. Does not
publish npm or PyPI. Does not change `release.yml`.

## Goal

Add a **manual** GitHub Actions workflow that promotes an existing GitHub
Release’s macOS CLI zip into `mulesoft/homebrew-data-weave` by opening a
pull request that updates `formula/dw.rb` (`url`, `sha256`, `version`).

Customer path stays:

```bash
brew tap mulesoft/data-weave
brew install dw
```

## Non-goals

- `npm publish` / PyPI / Chocolatey.
- Rebuilding natives or changing `release.yml`.
- Pushing directly to the tap’s default branch.
- Linux or Windows Homebrew/Linuxbrew bottles.

## Constraints (locked)

- Trigger: `workflow_dispatch` only. Inputs: `tag` (required, `v*`), `dry_run`
  (boolean, default false).
- Source of truth: GitHub Release assets already attached by `release.yml`.
- Exists-check: if tap `formula/dw.rb` `version` already equals the tag
  (without the leading `v`), skip with success.
- Tap update is a **PR**, not a push to `master`.
- Auth: repo secret `HOMEBREW_TAP_TOKEN` with contents + pull-requests on
  `mulesoft/homebrew-data-weave`. Not npm OIDC.
- Fail if the expected macOS CLI asset is missing from the Release.

## Current tap

`formula/dw.rb` today:

- `url` → `mulesoft-labs/data-weave-cli` release asset `dw-1.0.36-macOS`
- `version` → `2.11.0-20251026` (does not match the url filename)

This repo’s release asset name (current convention):

```
dw-cli-<ver>-macos-<arch>.zip
```

`macos-latest` produces `arm64`. First promote retargets `url` at:

```
https://github.com/mulesoft/data-weave-cli/releases/download/<tag>/dw-cli-<ver>-macos-arm64.zip
```

If a future Intel macOS zip exists, that is a later formula `on_intel` /
`on_arm` split — out of scope.

## Workflow

File: `.github/workflows/promote-release.yml`

Job `homebrew` (`ubuntu-latest`):

1. Normalize `tag` → `version` (`v1.2.3` → `1.2.3`). Reject tags that do not
   match `v` + semver-ish (`[0-9].*`).
2. `gh release view <tag>` — must exist.
3. Confirm asset `dw-cli-${version}-macos-arm64.zip` is listed. Download it
   (or fetch bytes for sha only).
4. `sha256sum` the zip.
5. Checkout tap with `HOMEBREW_TAP_TOKEN`.
6. Parse current `version` from `formula/dw.rb`. If equal to `${version}`,
   exit 0 (“already promoted”).
7. Rewrite `url`, `sha256`, `version`. Keep `desc`, `homepage`, `install`.
   Update `homepage` to `https://github.com/mulesoft/data-weave-cli` if it
   still points at `mulesoft-labs`.
8. `dry_run`: print the new formula and stop.
9. Else: commit on `promote-dw-<version>` and open a PR to the tap default
   branch (`master`). PR body lists tag, asset URL, sha256.

Idempotent: a second run after the PR merged hits the exists-check. A second
run while the PR is open may fail on branch exists — recreate or reuse the
branch and force-update only that promote branch (not `master`).

## Formula shape after promote

```ruby
class Dw < Formula
  desc "DataWeave CLI"
  homepage "https://github.com/mulesoft/data-weave-cli"
  url "https://github.com/mulesoft/data-weave-cli/releases/download/v<ver>/dw-cli-<ver>-macos-arm64.zip"
  sha256 "<hex>"
  version "<ver>"

  def install
    prefix.install "bin"
    prefix.install "libs"
  end
end
```

`install` assumes the zip still contains `bin/` and `libs/` (current
`native-cli:distro` layout). If the zip layout changes, this job must fail
loudly rather than invent paths — optional sanity: unzip listing must include
`bin/dw` (or `bin/dw.exe` is N/A on macOS).

## Secrets / permissions

- `HOMEBREW_TAP_TOKEN`: PAT or GitHub App installation token, repo scope on
  the tap.
- Workflow `contents: read` on this repo (release download via `GITHUB_TOKEN`).
- Do not use `GITHUB_TOKEN` to push the tap (wrong repo).

## Testing

- Unit-testable helpers if we extract rewrite/parse into a small script
  (`scripts/promote/homebrew.mjs` or `.sh`): parse version from formula,
  render new formula, reject bad tags.
- CI of this repo does **not** run promote on PRs (dispatch only).
- Manual `dry_run` against a real tag after merge.

## Success criteria

- Dispatch with a tag that has the macOS zip opens a tap PR (or no-ops if
  already at that version).
- `dry_run` makes no tap changes.
- `release.yml` unchanged.

## Out of scope (follow-up)

- npm / PyPI promote jobs in the same workflow file (add later as extra jobs).
- Multi-arch Homebrew bottles.
- Auto-dispatch after `release.yml` (stay manual).
