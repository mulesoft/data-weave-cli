import { test } from "node:test";
import assert from "node:assert/strict";
import { gatherEnv } from "./env.mjs";

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
