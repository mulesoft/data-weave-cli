# Release Artifact Publication Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Publish complete tag-release assets from a MuleSoft-managed Ubuntu job after every platform build succeeds.

**Architecture:** The existing tag-triggered matrix continues to build Linux, Windows, and macOS assets but publishes each output as an Actions artifact. A final `publish-release` job on `mulesoft-ubuntu` downloads every artifact, creates the release if absent, then uses `gh release upload --clobber` to publish assets. This is a single workflow, so failed matrix work prevents the publisher from running.

**Tech Stack:** GitHub Actions, composite actions, GitHub CLI, bash, Python YAML parser.

**Spec:** `docs/superpowers/specs/2026-09-02-release-artifact-publication-design.md`

## Global Constraints

- Keep the current Linux, Windows, and macOS build matrix; macOS remains required to build ARM64 artifacts.
- No platform build job may access the GitHub Releases API.
- The sole release publishing job runs on `mulesoft-ubuntu`.
- The publisher must create the GitHub Release only if it does not already exist.
- Reruns must replace identically named assets.
- A failed matrix build must prevent publishing a partial release.
- Preserve all externally published asset filenames.
- Keep the Homebrew promotion workflow and branch out of scope.

---

### Task 1: Make composite action artifact outputs unique and release-neutral

**Files:**
- Modify: `.github/actions/cli/action.yml:16-72`
- Modify: `.github/actions/python/action.yml:28-74`
- Modify: `.github/actions/node/action.yml:17-111`
- Modify: `.github/actions/native-lib/action.yml:12-88`

**Interfaces:**
- Consumes: `publish` input values `none` and `artifact`.
- Produces when `publish: artifact`: one Actions artifact per release file, with a stable explicit `name`, while retaining the existing file paths and external filename inside each artifact.
- Removes `publish: release`, `repo-token`, and `tag` from all four composite actions because no workflow invokes them.

- [ ] **Step 1: Add failing workflow-contract coverage** by creating `scripts/release-artifacts.test.mjs`.

```javascript
import { readFileSync } from "node:fs";
import { test } from "node:test";
import assert from "node:assert/strict";

const files = [
  ".github/actions/cli/action.yml",
  ".github/actions/python/action.yml",
  ".github/actions/node/action.yml",
  ".github/actions/native-lib/action.yml",
];

test("artifact publishing names every uploaded release input", () => {
  for (const file of files) {
    const action = readFileSync(file, "utf8");
    assert.match(action, /uses: actions\/upload-artifact@v7\.0\.1/);
    assert.match(action, /\n\s+name: .+/);
  }
});

test("Python wheel artifact names retain the platform-qualified wheel filename", () => {
  const action = readFileSync(".github/actions/python/action.yml", "utf8");
  assert.match(action, /path: native-lib\/python\/dist\/dataweave_native-0\.0\.1-py3-\*\.whl/);
});
```

- [ ] **Step 2: Run the coverage to verify it fails.**

Run: `node --test scripts/release-artifacts.test.mjs`

Expected: FAIL because the CLI, Python, and Node artifact uploads lack explicit names.

- [ ] **Step 3: Update the CLI artifact upload** so it uses an explicit Actions artifact name:

```yaml
      with:
        name: cli-${{ inputs.native-version }}-${{ inputs.script-name }}-${{ inputs.arch }}
        path: native-cli/build/distributions/dw-cli-${{ inputs.native-version }}-${{ inputs.script-name }}-${{ inputs.arch }}.zip
        archive: false
```

Keep its existing staged filename unchanged.

- [ ] **Step 4: Name the Python artifact explicitly without changing its platform-qualified wheel filename.**

```yaml
      with:
        name: python-wheel-${{ inputs.native-version }}-${{ inputs.platform }}
        path: native-lib/python/dist/dataweave_native-0.0.1-py3-*.whl
        archive: false
```

Add an optional `platform` input to the Python action only to name the GitHub
Actions artifact. Do not modify `native-lib/python/setup.py`, its wheel tags,
or the wheel filename pattern; it already produces platform-specific wheel
filenames such as `manylinux2014_x86_64`, `win_amd64`, and `macosx_*_arm64`.

- [ ] **Step 5: Name both Node artifact uploads explicitly.**

```yaml
      with:
        name: node-platform-${{ inputs.native-version }}-${{ inputs.script-name }}-${{ inputs.arch }}
        path: ${{ steps.node-package.outputs.platform }}
        archive: false
```

```yaml
      with:
        name: node-meta-${{ inputs.native-version }}
        path: ${{ steps.node-package.outputs.meta }}
        archive: false
```

The `node-meta` upload remains Linux-only as it is today.

- [ ] **Step 6: Verify the native-lib artifact contract remains sufficient.** Its artifact already uses `dwlib-${{ inputs.native-version }}-${{ inputs.script-name }}-${{ inputs.arch }}`. Do not add duplicate uploads. Its file payload includes `dwlib.h` on every platform, which Task 2 handles by uploading only one matching header.

- [ ] **Step 7: Run coverage to verify it passes.**

Run: `node --test scripts/release-artifacts.test.mjs`

Expected: PASS.

- [ ] **Step 8: Commit the focused artifact contract.**

```bash
git add .github/actions/cli/action.yml .github/actions/python/action.yml .github/actions/node/action.yml .github/actions/native-lib/action.yml scripts/release-artifacts.test.mjs
git commit -m "refactor(release): publish build outputs as named artifacts"
```

---

### Task 2: Separate release creation and upload into an internal Ubuntu publisher

**Files:**
- Modify: `.github/workflows/release.yml:1-87`
- Modify: `scripts/release-artifacts.test.mjs`

**Interfaces:**
- Consumes: matrix artifacts named by Task 1.
- Produces: an existing or newly created GitHub Release for the pushed `v*` tag containing all external asset filenames.
- Publisher command: `gh release view "$TAG" || gh release create "$TAG" --generate-notes`, followed by `gh release upload "$TAG" ... --clobber`.

- [ ] **Step 1: Extend the contract test with the desired workflow behavior.**

```javascript
test("release publication happens only on internal Ubuntu after the matrix", () => {
  const workflow = readFileSync(".github/workflows/release.yml", "utf8");
  assert.match(workflow, /publish: 'artifact'/);
  assert.match(workflow, /publish-release:/);
  assert.match(workflow, /needs: RELEASE_EXTENSION/);
  assert.match(workflow, /runs-on: mulesoft-ubuntu/);
  assert.match(workflow, /permissions:\n\s+contents: write/);
  assert.match(workflow, /gh release view "\$TAG" \|\| gh release create "\$TAG" --generate-notes/);
  assert.match(workflow, /gh release upload "\$TAG"/);
  assert.doesNotMatch(workflow, /publish: 'release'/);
});
```

- [ ] **Step 2: Run the contract test to verify it fails.**

Run: `node --test scripts/release-artifacts.test.mjs`

Expected: FAIL because `release.yml` currently uses `publish: 'release'` within matrix jobs and has no publisher job.

- [ ] **Step 3: Grant release permission at workflow scope.** Immediately after the trigger declaration add:

```yaml
permissions:
  contents: write
```

- [ ] **Step 4: Change every matrix composite action invocation to `publish: 'artifact'`.**

Remove `repo-token` and `tag` inputs from the CLI, Python, Node, and native-lib action invocations. Pass the matrix platform token to Python only for its Actions artifact name:

```yaml
      - name: Python
        uses: ./.github/actions/python
        with:
          native-version: ${{ env.NATIVE_VERSION }}
          break-system-packages: 'true'
          platform: ${{ matrix.script_name }}-${{ env.ARCH }}
          publish: 'artifact'
```

Keep all build/test steps and matrix platform fields otherwise unchanged.

- [ ] **Step 5: Add the publisher job after the matrix job.**

```yaml
  publish-release:
    needs: RELEASE_EXTENSION
    runs-on: mulesoft-ubuntu
    steps:
      - name: Download release artifacts
        uses: actions/download-artifact@v8
        with:
          path: release-assets
          merge-multiple: true
      - name: Create release and upload assets
        env:
          GH_TOKEN: ${{ secrets.GITHUB_TOKEN }}
          TAG: ${{ github.ref_name }}
        run: |
          set -euo pipefail
          gh release view "$TAG" || gh release create "$TAG" --generate-notes
          gh release upload "$TAG" release-assets/* --clobber
```

`needs: RELEASE_EXTENSION` makes GitHub Actions require all matrix children to succeed before this job starts. `merge-multiple: true` makes all payload files available under one directory for a single `gh release upload` call.

- [ ] **Step 6: Add a duplicate-header guard before the upload command.** The three native library artifacts each contain `dwlib.h`, while the platform-qualified Python wheel filenames are already distinct and must all remain. Preserve exactly one header under its existing release name:

```bash
HEADER=$(find release-assets -type f -name 'dwlib.h' -print -quit)
if [ -z "$HEADER" ]; then
  echo "dwlib.h is missing"
  exit 1
fi
mv "$HEADER" "$RUNNER_TEMP/dwlib.h"
find release-assets -type f -name 'dwlib.h' -delete
mv "$RUNNER_TEMP/dwlib.h" "release-assets/dwlib-${TAG#v}.h"
```

This preserves the existing release filename `dwlib-<version>.h` and leaves all
`.so`, `.dll`, `.dylib`, `.zip`, `.tgz`, and platform-qualified `.whl` files
unchanged.

- [ ] **Step 7: Run the contract test to verify it passes.**

Run: `node --test scripts/release-artifacts.test.mjs`

Expected: PASS.

- [ ] **Step 8: Validate YAML and inspect the release workflow.**

Run:

```bash
python3 -c "import yaml; yaml.safe_load(open('.github/workflows/release.yml')); print('YAML valid')"
git diff -- .github/workflows/release.yml
```

Expected: valid YAML; all matrix action invocations use `publish: 'artifact'`; the only `gh release` commands are in `publish-release` on `mulesoft-ubuntu`.

- [ ] **Step 9: Commit the publisher job.**

```bash
git add .github/workflows/release.yml scripts/release-artifacts.test.mjs
git commit -m "fix(release): publish assets from internal Ubuntu"
```

---

### Task 3: Final verification and release workflow review

**Files:**
- Verify: `.github/workflows/release.yml`
- Verify: `.github/actions/cli/action.yml`
- Verify: `.github/actions/python/action.yml`
- Verify: `.github/actions/node/action.yml`
- Verify: `.github/actions/native-lib/action.yml`
- Verify: `scripts/release-artifacts.test.mjs`

**Interfaces:**
- Verifies the complete release pipeline contract defined in the spec.

- [ ] **Step 1: Run all release artifact contract coverage.**

Run: `node --test scripts/release-artifacts.test.mjs`

Expected: PASS with no failures.

- [ ] **Step 2: Validate every modified Actions YAML file.**

Run:

```bash
python3 - <<'PY'
import yaml

for path in [
    ".github/workflows/release.yml",
    ".github/actions/cli/action.yml",
    ".github/actions/python/action.yml",
    ".github/actions/node/action.yml",
    ".github/actions/native-lib/action.yml",
]:
    with open(path) as source:
        yaml.safe_load(source)
    print(f"YAML valid: {path}")
PY
```

Expected: every listed file prints `YAML valid`.

- [ ] **Step 3: Run `actionlint` if installed.**

Run:

```bash
if command -v actionlint >/dev/null 2>&1; then
  actionlint .github/workflows/release.yml
else
  echo "actionlint unavailable"
fi
```

Expected: no diagnostics if `actionlint` is installed; otherwise record that it is unavailable.

- [ ] **Step 4: Review release API placement and artifact filenames.**

Run:

```bash
rg -n "publish: 'release'|upload-release-action|gh release|runs-on: mulesoft-ubuntu|publish-release" .github/workflows/release.yml .github/actions
git diff --check origin/master...HEAD
```

Expected: no `publish: 'release'` in `release.yml`; `gh release` commands only in the `publish-release` job; no whitespace errors.

- [ ] **Step 5: Commit any verification-driven correction.** If all previous checks pass without source changes, do not create an empty commit. Otherwise:

```bash
git add <corrected-files>
git commit -m "fix(release): correct artifact publication"
```

---

## Spec Coverage

| Spec requirement | Plan task |
|---|---|
| Matrix builds artifacts only | 1, 2 |
| Publisher runs on MuleSoft Ubuntu after successful matrix | 2 |
| Creates release if absent | 2 |
| Reruns replace assets | 2 |
| No GitHub Release API call from macOS | 2, 3 |
| Existing release asset filenames retained | 1, 2, 3 |
| YAML and contract verification | 3 |
