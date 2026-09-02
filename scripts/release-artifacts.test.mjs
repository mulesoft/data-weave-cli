import { readFileSync } from "node:fs";
import assert from "node:assert/strict";
import { test } from "node:test";

const files = [
  ".github/actions/cli/action.yml",
  ".github/actions/python/action.yml",
  ".github/actions/node/action.yml",
  ".github/actions/native-lib/action.yml",
];

test("artifact publishing names every uploaded release input", () => {
  for (const file of files) {
    const action = readFileSync(file, "utf8");
    const uploads = action.matchAll(
      /uses: actions\/upload-artifact@v7\.0\.1\n\s+with:\n(?<options>(?:\s+.+\n?)+)/g,
    );
    for (const upload of uploads) {
      assert.match(upload.groups.options, /^\s+name: .+/m);
    }
  }
});

test("Python wheel artifact retains its platform-qualified wheel filename", () => {
  const action = readFileSync(".github/actions/python/action.yml", "utf8");
  assert.match(action, /path: native-lib\/python\/dist\/dataweave_native-0\.0\.1-py3-\*\.whl/);
});

test("composite actions do not expose direct release publishing", () => {
  for (const file of files) {
    const action = readFileSync(file, "utf8");
    assert.doesNotMatch(action, /'release'/);
    assert.doesNotMatch(action, /repo-token:/);
    assert.doesNotMatch(action, /tag:/);
    assert.doesNotMatch(action, /svenstaro\/upload-release-action/);
  }
});

test("release publication happens only on internal Ubuntu after the matrix", () => {
  const workflow = readFileSync(".github/workflows/release.yml", "utf8");
  assert.match(workflow, /publish: 'artifact'/);
  assert.match(workflow, /publish-release:/);
  assert.match(workflow, /needs: RELEASE_EXTENSION/);
  assert.match(workflow, /runs-on: mulesoft-ubuntu/);
  assert.match(workflow, /publish-release:\n(?:.|\n)*?permissions:\n\s+contents: write/);
  assert.match(workflow, /GH_REPO: \$\{\{ github\.repository \}\}/);
  assert.match(workflow, /gh release view "\$TAG" \|\| gh release create "\$TAG" --generate-notes/);
  assert.match(workflow, /gh release upload "\$TAG"/);
  assert.doesNotMatch(workflow, /publish: 'release'/);
});

test("publisher retains the versioned native library release filenames", () => {
  const workflow = readFileSync(".github/workflows/release.yml", "utf8");
  assert.doesNotMatch(workflow, /merge-multiple: true/);
  assert.match(workflow, /dwlib-\$\{VERSION\}-\$\{platform\}\.\$\{extension\}/);
  assert.match(workflow, /release-assets\/dwlib-\$\{VERSION\}-linux-x86_64/);
  assert.match(workflow, /shopt -s globstar nullglob/);
});
