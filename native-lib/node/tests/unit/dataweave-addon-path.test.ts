import { beforeEach, describe, expect, it, vi } from "vitest";
import { existsSync } from "node:fs";
import { resolveAddonPath } from "../../src/addon-path";
import * as ffi from "../../src/ffi";
import { DataWeave } from "../../src/dataweave";

vi.mock("node:fs", () => ({ existsSync: vi.fn() }));
vi.mock("../../src/addon-path", () => ({ resolveAddonPath: vi.fn() }));
vi.mock("../../src/ffi", () => ({
  initialize: vi.fn(),
  cleanup: vi.fn(),
  runScript: vi.fn(),
  runScriptStreaming: vi.fn(),
  runScriptTransform: vi.fn(),
  runWithResolver: vi.fn(),
}));

const mockedExistsSync = vi.mocked(existsSync);
const mockedResolveAddonPath = vi.mocked(resolveAddonPath);
const mockedInitialize = vi.mocked(ffi.initialize);

describe("DataWeave addon path resolution", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockedResolveAddonPath.mockReturnValue("/opt/plat/dwlib_addon.node");
    mockedExistsSync.mockImplementation((path) => String(path) === "/opt/plat/dwlib.so");
  });

  it("uses platform-package addon and library artifacts together", () => {
    const dataWeave = new DataWeave();

    dataWeave.initialize();

    expect(mockedResolveAddonPath).toHaveBeenCalledOnce();
    expect(mockedInitialize).toHaveBeenCalledWith(
      "/opt/plat/dwlib.so",
      "/opt/plat/dwlib_addon.node"
    );
  });

  it("preserves an explicit library path while sharing the resolved addon path", () => {
    const dataWeave = new DataWeave({ libPath: "/custom/dwlib.so" });

    dataWeave.initialize();

    expect(mockedResolveAddonPath).toHaveBeenCalledOnce();
    expect(mockedInitialize).toHaveBeenCalledWith(
      "/custom/dwlib.so",
      "/opt/plat/dwlib_addon.node"
    );
  });
});
