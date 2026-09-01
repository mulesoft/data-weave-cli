import { existsSync } from "node:fs";
import { join } from "node:path";

/**
 * Optional npm packages that ship a prebuilt `dwlib_addon.node` for one
 * `process.platform` / `process.arch` pair. These are the only hosts the
 * published `dataweave-native` meta package can load without a local gyp
 * rebuild. Keep in sync with `nativePackages` in `scripts/pack-packages.mjs`.
 */
export const SUPPORTED_NATIVE_PACKAGES = [
  { platform: "linux", arch: "x64", name: "dataweave-native-linux-x64" },
  { platform: "win32", arch: "x64", name: "dataweave-native-win32-x64" },
  { platform: "darwin", arch: "arm64", name: "dataweave-native-darwin-arm64" },
] as const;

/** Options for {@link resolveAddonPath}. All fields are optional and default to the running process. */
export interface ResolveAddonPathOptions {
  /** Package root that contains `build/Release` and `native/` (defaults to the parent of this module). */
  packageRoot?: string;
  /** Override of `process.platform` (tests). */
  platform?: string;
  /** Override of `process.arch` (tests). */
  arch?: string;
  /**
   * Resolves an optional platform package to a filesystem path.
   * Production uses `require.resolve` so this step does not `dlopen` the addon;
   * `ffi` loads the `.node` afterward. Tests pass a stub.
   */
  requireFn?: (id: string) => { filename?: string } | string;
}

/**
 * Returns the optional-dependency package name for a Node platform/arch pair,
 * or `undefined` when that pair is not in {@link SUPPORTED_NATIVE_PACKAGES}
 * (for example `darwin-x64` or `linux-arm64`).
 *
 * @param platform - `process.platform` token (`linux`, `win32`, `darwin`).
 * @param arch - `process.arch` token (`x64`, `arm64`).
 */
export function nativePackageName(platform: string, arch: string): string | undefined {
  return SUPPORTED_NATIVE_PACKAGES.find((p) => p.platform === platform && p.arch === arch)?.name;
}

/**
 * Locates the compiled N-API addon (`dwlib_addon.node`).
 *
 * Resolution is attempted in priority order:
 * 1. In-tree / source build — `<packageRoot>/build/Release/dwlib_addon.node`.
 * 2. Same-package layout — `<packageRoot>/native/dwlib_addon.node`.
 * 3. Optional dependency `dataweave-native-<platform>-<arch>` via `require.resolve`.
 *
 * @param opts - Overrides for tests or an explicit package root.
 * @returns The absolute path of the `.node` file (or the resolved package main).
 * @throws Error if the host is unsupported, the optional package is missing
 *   (`MODULE_NOT_FOUND`), or loading the addon fails for another reason.
 */
export function resolveAddonPath(opts?: ResolveAddonPathOptions): string {
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
  const requireFn = opts?.requireFn ?? ((id: string) => require.resolve(id));
  try {
    const loaded = requireFn(name);
    if (typeof loaded === "string") return loaded;
    if (loaded && typeof loaded.filename === "string") return loaded.filename;
    const resolved = require.resolve(name);
    return resolved;
  } catch (error: unknown) {
    if (typeof error === "object" && error !== null && "code" in error && error.code === "MODULE_NOT_FOUND") {
      throw new Error(
        `Could not load native addon. Install optional dependency ${name} (supported: ${supported}).`,
        { cause: error }
      );
    }
    const detail = error instanceof Error ? error.message : String(error);
    throw new Error(`Could not load native addon ${name}: ${detail}`, { cause: error });
  }
}
