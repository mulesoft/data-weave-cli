import { execFile } from "node:child_process";
import { cp, mkdir, readFile, readdir, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);
const nativePackages = [
  { platform: "linux", arch: "x64", name: "dataweave-native-linux-x64" },
  { platform: "win32", arch: "x64", name: "dataweave-native-win32-x64" },
  { platform: "darwin", arch: "arm64", name: "dataweave-native-darwin-arm64" },
];

function nativePackageName(platform, arch) {
  return nativePackages.find((entry) => entry.platform === platform && entry.arch === arch)?.name;
}

export function npmPackInvocation(platform, stagingDir) {
  if (platform === "win32") {
    const quotedStagingDir = `"${stagingDir.replaceAll('"', '""')}"`;
    return { command: "cmd.exe", args: ["/d", "/s", "/c", `npm pack ${quotedStagingDir}`] };
  }
  return { command: "npm", args: ["pack", stagingDir] };
}

async function defaultRunNpmPack(stagingDir, nodeDir) {
  const { command, args } = npmPackInvocation(process.platform, stagingDir);
  await execFileAsync(command, args, { cwd: nodeDir });
}

async function copyIfPresent(source, destination) {
  try {
    await cp(source, destination, { recursive: true });
  } catch (error) {
    if (error.code !== "ENOENT") {
      throw error;
    }
  }
}

export async function packPackages({
  nodeDir,
  version,
  platform = process.platform,
  arch = process.arch,
  skipNative = false,
  runNpmPack = defaultRunNpmPack,
}) {
  const sourcePackage = JSON.parse(await readFile(join(nodeDir, "package.json"), "utf8"));
  const environmentVersion = process.env.NATIVE_VERSION;
  const packageVersion = version ?? ((environmentVersion && environmentVersion.trim()) || sourcePackage.version);
  const npmBuildDir = join(nodeDir, "build", "npm");
  const metaDir = join(npmBuildDir, "dataweave-native");
  const optionalDependencies = Object.fromEntries(
    nativePackages.map(({ name }) => [name, packageVersion]),
  );
  const metaPackage = {
    ...sourcePackage,
    name: "dataweave-native",
    version: packageVersion,
    files: ["dist/", "docs/"],
    optionalDependencies,
  };
  delete metaPackage.gypfile;

  await rm(npmBuildDir, { recursive: true, force: true });
  await mkdir(metaDir, { recursive: true });
  await writeFile(join(metaDir, "package.json"), `${JSON.stringify(metaPackage, null, 2)}\n`);
  await cp(join(nodeDir, "dist"), join(metaDir, "dist"), { recursive: true });
  await copyIfPresent(join(nodeDir, "docs"), join(metaDir, "docs"));
  await runNpmPack(metaDir, nodeDir);

  const nativeName = skipNative ? undefined : nativePackageName(platform, arch);
  if (nativeName === undefined) {
    return;
  }

  const nativeDir = join(npmBuildDir, nativeName);
  await mkdir(nativeDir, { recursive: true });
  await writeFile(join(nativeDir, "package.json"), `${JSON.stringify({
    name: nativeName,
    version: packageVersion,
    main: "./dwlib_addon.node",
    os: [platform],
    cpu: [arch],
    files: ["dwlib_addon.node", "dwlib.*"],
  }, null, 2)}\n`);
  await cp(join(nodeDir, "build", "Release", "dwlib_addon.node"), join(nativeDir, "dwlib_addon.node"));
  const nativeFiles = await readdir(join(nodeDir, "native"));
  const nativeLibrary = nativeFiles.find((file) => /^dwlib\.(dylib|so|dll)$/.test(file));
  if (nativeLibrary === undefined) {
    throw new Error("No native/dwlib shared library found");
  }
  await cp(join(nodeDir, "native", nativeLibrary), join(nativeDir, nativeLibrary));
  await runNpmPack(nativeDir, nodeDir);
}

export async function main() {
  const nodeDir = join(fileURLToPath(new URL(".", import.meta.url)), "..");
  await packPackages({ nodeDir });
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  await main();
}
