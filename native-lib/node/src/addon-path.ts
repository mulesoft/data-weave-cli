import { existsSync } from "node:fs";
import { join } from "node:path";

export const SUPPORTED_NATIVE_PACKAGES = [
  { platform: "linux", arch: "x64", name: "dataweave-native-linux-x64" },
  { platform: "win32", arch: "x64", name: "dataweave-native-win32-x64" },
  { platform: "darwin", arch: "arm64", name: "dataweave-native-darwin-arm64" },
] as const;

export function nativePackageName(platform: string, arch: string): string | undefined {
  return SUPPORTED_NATIVE_PACKAGES.find((p) => p.platform === platform && p.arch === arch)?.name;
}

export function resolveAddonPath(opts?: {
  packageRoot?: string;
  platform?: string;
  arch?: string;
  requireFn?: (id: string) => { filename?: string } | string;
}): string {
  const packageRoot = opts?.packageRoot ?? join(__dirname, "..");
  const platform = opts?.platform ?? process.platform;
  const arch = opts?.arch ?? process.arch;
  const inTree = join(packageRoot, "build", "Release", "dwlib_addon.node");
  if (existsSync(inTree)) return inTree;
  const packaged = join(packageRoot, "native", "dwlib_addon.node");
  if (existsSync(packaged)) return packaged;
  const name = nativePackageName(platform, arch);
  const supported = SUPPORTED_NATIVE_PACKAGES.map(
    (p) => `${p.platform}-${p.arch} (${p.name})`
  ).join(", ");
  if (!name) {
    throw new Error(
      `unsupported platform ${platform}-${arch} for dataweave-native. Supported: ${supported}.`
    );
  }
  const requireFn = opts?.requireFn ?? ((id: string) => require(id));
  try {
    const loaded = requireFn(name);
    if (typeof loaded === "string") return loaded;
    if (loaded && typeof loaded.filename === "string") return loaded.filename;
    const resolved = require.resolve(name);
    return resolved;
  } catch {
    throw new Error(
      `Could not load native addon. Install optional dependency ${name} (supported: ${supported}).`
    );
  }
}
