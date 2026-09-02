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
