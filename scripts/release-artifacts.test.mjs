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
    assert.match(action, /uses: actions\/upload-artifact@v7\.0\.1/);
    assert.match(action, /\n\s+name: .+/);
  }
});

test("Python wheel artifact retains its platform-qualified wheel filename", () => {
  const action = readFileSync(".github/actions/python/action.yml", "utf8");
  assert.match(action, /path: native-lib\/python\/dist\/dataweave_native-0\.0\.1-py3-\*\.whl/);
});

test("release publication happens only on internal Ubuntu after the matrix", () => {
  const workflow = readFileSync(".github/workflows/release.yml", "utf8");
  assert.match(workflow, /publish: 'artifact'/);
  assert.match(workflow, /publish-release:/);
  assert.match(workflow, /needs: RELEASE_EXTENSION/);
  assert.match(workflow, /runs-on: mulesoft-ubuntu/);
  assert.match(workflow, /publish-release:\n(?:.|\n)*?permissions:\n\s+contents: write/);
  assert.match(workflow, /gh release view "\$TAG" \|\| gh release create "\$TAG" --generate-notes/);
  assert.match(workflow, /gh release upload "\$TAG"/);
  assert.doesNotMatch(workflow, /publish: 'release'/);
});
