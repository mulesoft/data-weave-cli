import { after, test } from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { gunzipSync } from "node:zlib";
import { npmPackInvocation, packPackages } from "./pack-packages.mjs";

const tempDirs = [];

function makeNodeDir() {
  const repoDir = mkdtempSync(join(tmpdir(), "dw-node-pack-test-"));
  const nodeDir = join(repoDir, "native-lib", "node");
  tempDirs.push(repoDir);
  mkdirSync(nodeDir, { recursive: true });
  mkdirSync(join(nodeDir, "dist"), { recursive: true });
  mkdirSync(join(nodeDir, "build", "Release"), { recursive: true });
  mkdirSync(join(nodeDir, "native"), { recursive: true });
  writeFileSync(join(nodeDir, "dist", "index.js"), "export const dataweave = true;\n");
  writeFileSync(join(nodeDir, "build", "Release", "dwlib_addon.node"), "addon");
  writeFileSync(join(nodeDir, "native", "dwlib.dylib"), "native library");
  writeFileSync(join(nodeDir, "README.md"), "# DataWeave Node.js Bindings\n");
  writeFileSync(join(repoDir, "LICENSE.txt"), "BSD 3-Clause License\n");
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

function readTarEntries(tarball) {
  const tar = gunzipSync(readFileSync(tarball));
  const entries = new Map();
  let offset = 0;

  while (offset + 512 <= tar.length && tar[offset] !== 0) {
    const name = tar.subarray(offset, offset + 100).toString("utf8").replace(/\0.*$/, "");
    const size = Number.parseInt(
      tar.subarray(offset + 124, offset + 136).toString("utf8").replace(/\0.*$/, "").trim(),
      8,
    );
    const contentStart = offset + 512;
    entries.set(name, tar.subarray(contentStart, contentStart + size).toString("utf8"));
    offset = contentStart + Math.ceil(size / 512) * 512;
  }

  return entries;
}

after(() => {
  for (const dir of tempDirs) {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("selects the npm pack invocation for each platform", () => {
  const stagingDir = "package staging";
  const windowsInvocation = npmPackInvocation("win32", stagingDir);
  assert.deepEqual(windowsInvocation, {
    command: "cmd.exe",
    args: ["/d", "/s", "/c", "npm", "pack", stagingDir],
  });
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
  });

  const meta = readJson(join(nodeDir, "build", "npm", "dataweave-native", "package.json"));
  assert.equal(meta.name, "dataweave-native");
  assert.equal(meta.version, "1.2.3");
  assert.equal(meta.license, "BSD-3-Clause");
  assert.equal(meta.optionalDependencies["dataweave-native-darwin-arm64"], "1.2.3");
  assert.equal(meta.gypfile, undefined);
  assert.deepEqual(meta.files, ["dist/", "docs/", "README.md", "LICENSE.txt"]);
  assert.equal(
    readFileSync(join(nodeDir, "build", "npm", "dataweave-native", "README.md"), "utf8"),
    "# DataWeave Node.js Bindings\n",
  );
  assert.equal(
    readFileSync(join(nodeDir, "build", "npm", "dataweave-native", "LICENSE.txt"), "utf8"),
    "BSD 3-Clause License\n",
  );

  const tarEntries = readTarEntries(join(nodeDir, "dataweave-native-1.2.3.tgz"));
  assert.equal(tarEntries.get("package/README.md"), "# DataWeave Node.js Bindings\n");
  assert.equal(tarEntries.get("package/LICENSE.txt"), "BSD 3-Clause License\n");
  const tarPackage = JSON.parse(tarEntries.get("package/package.json"));
  assert.equal(tarPackage.license, "BSD-3-Clause");
  assert.deepEqual(tarPackage.files, ["dist/", "docs/", "README.md", "LICENSE.txt"]);

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
