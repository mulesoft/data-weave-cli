import { test, after } from "node:test";
import assert from "node:assert/strict";
import { mkdirSync, writeFileSync, rmSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { loadWrapper, resolveDwlibPath, resolveWrapperPath } from "./wrapper.mjs";

const tempDirs = [];

function makeTempDir() {
  const dir = join(tmpdir(), `dw-bench-test-${Date.now()}-${Math.random().toString(36).slice(2)}`);
  mkdirSync(dir, { recursive: true });
  tempDirs.push(dir);
  return dir;
}

after(() => {
  for (const dir of tempDirs) {
    try {
      rmSync(dir, { recursive: true, force: true });
    } catch {}
  }
});

test("DW_BENCH_NODE_PACKAGE set to nonexistent dir throws", async () => {
  const orig = process.env.DW_BENCH_NODE_PACKAGE;
  process.env.DW_BENCH_NODE_PACKAGE = "/nonexistent/test/path";
  try {
    await assert.rejects(loadWrapper, /does not contain dist\/index\.js/);
  } finally {
    if (orig !== undefined) {
      process.env.DW_BENCH_NODE_PACKAGE = orig;
    } else {
      delete process.env.DW_BENCH_NODE_PACKAGE;
    }
  }
});

test("DW_BENCH_NODE_PACKAGE set to valid package dir loads", async () => {
  const orig = process.env.DW_BENCH_NODE_PACKAGE;
  const packageDir = makeTempDir();
  const distDir = join(packageDir, "dist");
  mkdirSync(distDir, { recursive: true });
  writeFileSync(join(distDir, "index.js"), "export function run() { return null; }");

  process.env.DW_BENCH_NODE_PACKAGE = packageDir;
  try {
    const api = await loadWrapper();
    assert.equal(typeof api.run, "function");
  } finally {
    if (orig !== undefined) {
      process.env.DW_BENCH_NODE_PACKAGE = orig;
    } else {
      delete process.env.DW_BENCH_NODE_PACKAGE;
    }
  }
});

test("DW_BENCH_NODE_PACKAGE resolves its wrapper and native library", () => {
  const orig = process.env.DW_BENCH_NODE_PACKAGE;
  const packageDir = makeTempDir();
  const distDir = join(packageDir, "dist");
  const nativeDir = join(packageDir, "native");
  mkdirSync(distDir, { recursive: true });
  mkdirSync(nativeDir, { recursive: true });
  writeFileSync(join(distDir, "index.js"), "export function run() { return null; }");
  writeFileSync(join(nativeDir, "dwlib.dylib"), "fixture native library");

  process.env.DW_BENCH_NODE_PACKAGE = packageDir;
  try {
    assert.equal(resolveWrapperPath(), join(distDir, "index.js"));
    assert.equal(resolveDwlibPath(), join(nativeDir, "dwlib.dylib"));
  } finally {
    if (orig !== undefined) {
      process.env.DW_BENCH_NODE_PACKAGE = orig;
    } else {
      delete process.env.DW_BENCH_NODE_PACKAGE;
    }
  }
});
