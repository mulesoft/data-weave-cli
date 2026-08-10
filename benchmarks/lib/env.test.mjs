import { test } from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { createHash } from "node:crypto";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { gatherEnv } from "./env.mjs";

const tempDirs = [];

function makeTempDir() {
  const dir = mkdtempSync(join(tmpdir(), "dw-bench-env-test-"));
  tempDirs.push(dir);
  return dir;
}

test.after(() => {
  for (const dir of tempDirs) {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("gatherEnv returns all required fields", () => {
  const env = gatherEnv({ runner: "node-wrapper", runtimeVersion: "node vX" });
  for (const key of ["os", "cpu", "runtimeVersion", "weaveVersion", "commit", "dwlibBuildId"]) {
    assert.ok(env[key] !== undefined && env[key] !== "", `env.${key} must be set`);
  }
});

test("gatherEnv reads the pinned weaveVersion from gradle.properties", () => {
  const env = gatherEnv({ runner: "node-wrapper", runtimeVersion: "node vX" });
  // gradle.properties pins e.g. 2.12.0-YYYYMMDD; assert it looks like a weave version.
  assert.match(env.weaveVersion, /^\d+\.\d+\.\d+/);
});

test("gatherEnv attributes an explicitly selected native library", () => {
  const libraryPath = join(makeTempDir(), "dwlib.dylib");
  const libraryBytes = Buffer.from("external native library fixture");
  writeFileSync(libraryPath, libraryBytes);
  const expectedBuildId = "dwlib-" + createHash("sha256")
    .update(String(libraryBytes.length))
    .update(libraryBytes.subarray(0, 65536))
    .digest("hex")
    .slice(0, 8);

  const env = gatherEnv({
    runner: "node-wrapper",
    runtimeVersion: "node vX",
    dwlibPath: libraryPath,
  });

  assert.equal(env.dwlibBuildId, expectedBuildId);
});
