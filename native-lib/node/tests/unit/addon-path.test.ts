import { describe, it, expect, vi, beforeEach } from "vitest";
import { existsSync } from "node:fs";
import { join } from "node:path";
import { nativePackageName, resolveAddonPath, SUPPORTED_NATIVE_PACKAGES } from "../../src/addon-path";

vi.mock("node:fs", () => ({ existsSync: vi.fn() }));
const mockedExistsSync = vi.mocked(existsSync);

describe("nativePackageName", () => {
  it("maps the three supported pairs", () => {
    expect(nativePackageName("linux", "x64")).toBe("dataweave-native-linux-x64");
    expect(nativePackageName("win32", "x64")).toBe("dataweave-native-win32-x64");
    expect(nativePackageName("darwin", "arm64")).toBe("dataweave-native-darwin-arm64");
  });

  it("returns undefined for darwin-x64 and linux-arm64", () => {
    expect(nativePackageName("darwin", "x64")).toBeUndefined();
    expect(nativePackageName("linux", "arm64")).toBeUndefined();
  });
});

describe("resolveAddonPath", () => {
  const root = "/pkg";

  beforeEach(() => {
    mockedExistsSync.mockReset();
  });

  it("prefers in-tree build/Release", () => {
    mockedExistsSync.mockImplementation((p) => String(p) === join(root, "build", "Release", "dwlib_addon.node"));
    expect(resolveAddonPath({ packageRoot: root })).toBe(join(root, "build", "Release", "dwlib_addon.node"));
  });

  it("falls back to packaged native/dwlib_addon.node", () => {
    mockedExistsSync.mockImplementation((p) => String(p) === join(root, "native", "dwlib_addon.node"));
    expect(resolveAddonPath({ packageRoot: root })).toBe(join(root, "native", "dwlib_addon.node"));
  });

  it("loads the optional package when local files are missing", () => {
    mockedExistsSync.mockReturnValue(false);
    const requireFn = vi.fn(() => ({ filename: "/opt/node_modules/dataweave-native-linux-x64/dwlib_addon.node" }));
    expect(
      resolveAddonPath({ packageRoot: root, platform: "linux", arch: "x64", requireFn })
    ).toBe("/opt/node_modules/dataweave-native-linux-x64/dwlib_addon.node");
    expect(requireFn).toHaveBeenCalledWith("dataweave-native-linux-x64");
  });

  it("throws listing supported platforms when the host is unsupported", () => {
    mockedExistsSync.mockReturnValue(false);
    expect(() => resolveAddonPath({ packageRoot: root, platform: "darwin", arch: "x64" })).toThrow(
      /unsupported platform/
    );
    expect(() => resolveAddonPath({ packageRoot: root, platform: "darwin", arch: "x64" })).toThrow(
      /dataweave-native-darwin-arm64/
    );
    expect(SUPPORTED_NATIVE_PACKAGES).toHaveLength(3);
  });

  it("throws naming the expected optional package when supported but missing", () => {
    mockedExistsSync.mockReturnValue(false);
    const requireFn = vi.fn(() => {
      throw Object.assign(new Error("Cannot find module"), { code: "MODULE_NOT_FOUND" });
    });
    expect(() =>
      resolveAddonPath({ packageRoot: root, platform: "linux", arch: "x64", requireFn })
    ).toThrow(/Install optional dependency dataweave-native-linux-x64/);
  });

  it("reports optional package load failures without claiming it is missing", () => {
    mockedExistsSync.mockReturnValue(false);
    const requireFn = vi.fn(() => {
      throw Object.assign(new Error("dlopen failed"), { code: "ERR_DLOPEN_FAILED" });
    });

    expect(() =>
      resolveAddonPath({ packageRoot: root, platform: "linux", arch: "x64", requireFn })
    ).toThrow(/Could not load native addon dataweave-native-linux-x64: dlopen failed/);
    expect(() =>
      resolveAddonPath({ packageRoot: root, platform: "linux", arch: "x64", requireFn })
    ).not.toThrow(/Install optional dependency/);
  });
});
