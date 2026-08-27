import { after, test } from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { npmPackInvocation, packPackages } from "./pack-packages.mjs";

const tempDirs = [];

function makeNodeDir() {
  const nodeDir = mkdtempSync(join(tmpdir(), "dw-node-pack-test-"));
  tempDirs.push(nodeDir);
  mkdirSync(join(nodeDir, "dist"), { recursive: true });
  mkdirSync(join(nodeDir, "build", "Release"), { recursive: true });
  mkdirSync(join(nodeDir, "native"), { recursive: true });
  writeFileSync(join(nodeDir, "dist", "index.js"), "export const dataweave = true;\n");
  writeFileSync(join(nodeDir, "build", "Release", "dwlib_addon.node"), "addon");
  writeFileSync(join(nodeDir, "native", "dwlib.dylib"), "native library");
  writeFileSync(join(nodeDir, "package.json"), JSON.stringify({
    name: "@dataweave/native",
    version: "0.0.1",
    main: "dist/index.js",
    files: [
      "dist/",
      "native/",
      "build/Release/dwlib_addon.node",
      "src/addon.c",
      "binding.gyp",
      "docs/",
    ],
    gypfile: true,
  }));
  return nodeDir;
}

function readJson(path) {
  return JSON.parse(readFileSync(path, "utf8"));
}

after(() => {
  for (const dir of tempDirs) {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("selects the npm pack invocation for each platform", () => {
  const stagingDir = "package staging";
  const windowsInvocation = npmPackInvocation("win32", stagingDir);
  assert.equal(windowsInvocation.command, "cmd.exe");
  assert.deepEqual(windowsInvocation.args.slice(0, 3), ["/d", "/s", "/c"]);
  assert.match(windowsInvocation.args[3], /^npm pack /);
  assert.deepEqual(npmPackInvocation("linux", stagingDir), {
    command: "npm",
    args: ["pack", stagingDir],
  });
});

test("packs meta and supported native package staging", async () => {
  const nodeDir = makeNodeDir();

  await packPackages({
    nodeDir,
    version: "1.2.3",
    platform: "darwin",
    arch: "arm64",
    runNpmPack: async () => {},
  });

  const meta = readJson(join(nodeDir, "build", "npm", "dataweave-native", "package.json"));
  assert.equal(meta.name, "dataweave-native");
  assert.equal(meta.version, "1.2.3");
  assert.equal(meta.optionalDependencies["dataweave-native-darwin-arm64"], "1.2.3");
  assert.equal(meta.gypfile, undefined);
  assert.deepEqual(meta.files, ["dist/", "docs/"]);

  const native = readJson(join(nodeDir, "build", "npm", "dataweave-native-darwin-arm64", "package.json"));
  assert.equal(native.name, "dataweave-native-darwin-arm64");
  assert.deepEqual(native.os, ["darwin"]);
  assert.deepEqual(native.cpu, ["arm64"]);
  assert.equal(native.main, "./dwlib_addon.node");
});

test("packs only the meta package for an unsupported platform and architecture", async () => {
  const nodeDir = makeNodeDir();
  const packedDirectories = [];

  await packPackages({
    nodeDir,
    version: "1.2.3",
    platform: "darwin",
    arch: "x64",
    runNpmPack: async (stagingDir) => packedDirectories.push(stagingDir),
  });

  assert.deepEqual(packedDirectories, [join(nodeDir, "build", "npm", "dataweave-native")]);
});

test("falls back to the package version when NATIVE_VERSION is empty", async () => {
  const nodeDir = makeNodeDir();
  const originalVersion = process.env.NATIVE_VERSION;
  process.env.NATIVE_VERSION = "  ";

  try {
    await packPackages({
      nodeDir,
      platform: "darwin",
      arch: "x64",
      runNpmPack: async () => {},
    });

    const meta = readJson(join(nodeDir, "build", "npm", "dataweave-native", "package.json"));
    assert.equal(meta.version, "0.0.1");
  } finally {
    if (originalVersion === undefined) {
      delete process.env.NATIVE_VERSION;
    } else {
      process.env.NATIVE_VERSION = originalVersion;
    }
  }
});
