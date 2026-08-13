import { describe, it, expect, vi, beforeEach } from "vitest";

// Pure-logic test of DataWeave.initialize()'s lifecycle/error handling, with
// the native addon mocked out entirely -- no dwlib required (see the "unit"
// project in vitest.config.ts). This covers a ref-count leak that is only
// observable in the sequencing of calls into ffi.ts, not in any externally
// visible native state, so a real end-to-end native failure isn't a
// practical way to assert on it (see task-4-report.md's fix report for why).
vi.mock("../../src/ffi", () => ({
  initialize: vi.fn(),
  createEngine: vi.fn(),
  createEngineWithResolver: vi.fn(),
  destroyEngine: vi.fn(),
  runScriptEngine: vi.fn(),
  runScriptStreamingEngine: vi.fn(),
  runScriptTransformEngine: vi.fn(),
  cleanup: vi.fn(),
}));

import * as ffi from "../../src/ffi";
import { DataWeave } from "../../src/dataweave";
import { DataWeaveError } from "../../src/errors";

describe("DataWeave.initialize() native ref-count safety", () => {
  beforeEach(() => {
    vi.mocked(ffi.initialize).mockReset();
    vi.mocked(ffi.createEngine).mockReset();
    vi.mocked(ffi.createEngineWithResolver).mockReset();
    vi.mocked(ffi.destroyEngine).mockReset();
    vi.mocked(ffi.cleanup).mockReset();
  });

  it("releases the native library ref-count if engine creation fails after ffi.initialize() succeeded", () => {
    vi.mocked(ffi.initialize).mockImplementation(() => {});
    vi.mocked(ffi.createEngineWithResolver).mockImplementation(() => {
      throw new Error("native engine creation boom");
    });

    const dw = new DataWeave({ libPath: "mock-lib-path", resolveModule: () => null });

    expect(() => dw.initialize()).toThrow(DataWeaveError);

    // ffi.initialize() already succeeded, incrementing the native library's
    // ref count. Since `initialized` never became true, cleanup()'s
    // early-return guard means nothing else would ever call ffi.cleanup() --
    // initialize()'s own catch block must have released it.
    expect(ffi.cleanup).toHaveBeenCalledTimes(1);
  });

  it("does not call ffi.cleanup() when ffi.initialize() itself is what fails", () => {
    vi.mocked(ffi.initialize).mockImplementation(() => {
      throw new Error("library not found");
    });

    const dw = new DataWeave({ libPath: "mock-lib-path" });

    expect(() => dw.initialize()).toThrow(DataWeaveError);

    // No ref count was ever acquired, so there is nothing to release.
    expect(ffi.cleanup).not.toHaveBeenCalled();
  });

  it("leaves engineHandle unset and the instance cleanly re-initializable after a failed attempt", () => {
    vi.mocked(ffi.initialize).mockImplementation(() => {});
    vi.mocked(ffi.createEngine)
      .mockImplementationOnce(() => {
        throw new Error("transient native failure");
      })
      .mockImplementationOnce(() => 42);

    const dw = new DataWeave({ libPath: "mock-lib-path" });

    expect(() => dw.initialize()).toThrow(DataWeaveError);
    expect(ffi.cleanup).toHaveBeenCalledTimes(1);

    // A later initialize() call (e.g. once the transient failure clears)
    // must succeed cleanly -- the failed attempt must not have left the
    // instance permanently "half-initialized" (this.initialized stuck true
    // without an engine handle, or vice versa).
    vi.mocked(ffi.cleanup).mockClear();
    dw.initialize();
    expect(ffi.createEngine).toHaveBeenCalledTimes(2);

    dw.cleanup();
    expect(ffi.destroyEngine).toHaveBeenCalledWith(42);
    expect(ffi.cleanup).toHaveBeenCalledTimes(1);
  });

  it("does not call ffi.cleanup() from initialize() on the successful path", () => {
    vi.mocked(ffi.initialize).mockImplementation(() => {});
    vi.mocked(ffi.createEngine).mockImplementation(() => 7);

    const dw = new DataWeave({ libPath: "mock-lib-path" });
    dw.initialize();

    expect(ffi.cleanup).not.toHaveBeenCalled();

    dw.cleanup();
    expect(ffi.destroyEngine).toHaveBeenCalledWith(7);
    expect(ffi.cleanup).toHaveBeenCalledTimes(1);
  });

  it("still clears `initialized` when ffi.cleanup() rejects, so the instance is re-initializable", async () => {
    vi.mocked(ffi.initialize).mockImplementation(() => {});
    vi.mocked(ffi.createEngine).mockImplementation(() => 7);
    vi.mocked(ffi.cleanup).mockRejectedValueOnce(new Error("native cleanup boom"));

    const dw = new DataWeave({ libPath: "mock-lib-path" });
    dw.initialize();

    await expect(dw.cleanup()).rejects.toThrow("native cleanup boom");

    // Even though ffi.cleanup() rejected, the engine handle was already
    // destroyed and nulled -- `initialized` must not stay stuck `true`, or a
    // later initialize() call becomes a permanent no-op (the early-return
    // guard `if (this.initialized) return;`) and the instance is stranded
    // with a null engineHandle.
    vi.mocked(ffi.initialize).mockClear();
    vi.mocked(ffi.createEngine).mockClear();
    vi.mocked(ffi.createEngine).mockImplementation(() => 9);

    dw.initialize();

    expect(ffi.initialize).toHaveBeenCalledTimes(1);
    expect(ffi.createEngine).toHaveBeenCalledTimes(1);
  });

  it("coalesces concurrent cleanup() calls into a single native teardown", async () => {
    vi.mocked(ffi.createEngine).mockReturnValue(1);
    let resolveNative!: () => void;
    vi.mocked(ffi.cleanup).mockReturnValue(
      new Promise<void>((resolve) => {
        resolveNative = resolve;
      })
    );

    const dw = new DataWeave("/fake/lib");
    dw.initialize();

    // Two overlapping cleanup() calls while ffi.cleanup() is still pending.
    const p1 = dw.cleanup();
    const p2 = dw.cleanup();
    resolveNative();
    await Promise.all([p1, p2]);

    // The native ref-count decrement (ffi.cleanup) and destroyEngine each run
    // exactly once, not once per caller -- this is the double-decrement fix.
    expect(ffi.cleanup).toHaveBeenCalledTimes(1);
    expect(ffi.destroyEngine).toHaveBeenCalledTimes(1);
  });
});
