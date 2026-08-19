import * as ffi from "./ffi";
import { resolveAddonPath } from "./addon-path";
import { findLibrary, buildInputsJson } from "./utils";
import { parseNativeResponse } from "./result";
import { createChunkReader } from "./reader";
import { streamFromNative } from "./stream";
import { DataWeaveError, DataWeaveScriptError } from "./errors";
import type { ExecutionResult, StreamingResult, Inputs, TransformOptions } from "./types";
import type { ModuleResolver } from "./resolver";

/**
 * Constructor options for {@link DataWeave}.
 */
export interface DataWeaveOptions {
  /**
   * Path to dwlib native library.
   * If not provided, uses default location.
   */
  libPath?: string;

  /**
   * Module resolver for external DataWeave modules.
   * Optional. If not provided, only built-in modules are available.
   *
   * MUST be synchronous (cannot return Promise).
   *
   * Each DataWeave instance owns an independent native engine, so multiple
   * instances with different resolvers coexist in one process with no
   * cross-talk. Streaming/transform still resolve only built-in modules for a
   * resolver-backed engine (custom modules fail closed); see external-modules.md.
   *
   * Security: the resolver runs with full process permissions and no
   * sandboxing (same trust model as the CLI resolving `.dwl` files from
   * disk) — only use resolvers pointed at trusted sources.
   */
  resolveModule?: ModuleResolver;
}

/**
 * A handle to the DataWeave native runtime for executing scripts.
 *
 * Wraps the native `dwlib` shared library via the FFI addon. Call
 * {@link DataWeave.initialize} once before running scripts and
 * {@link DataWeave.cleanup} when done. For most callers the module-level
 * {@link run} / {@link runStreaming} / {@link runTransform} functions — backed
 * by a lazily-initialized singleton — are more convenient than constructing an
 * instance directly.
 */
export class DataWeave {
  private readonly addonPath: string;
  private readonly libPath: string;
  private readonly resolveModule?: ModuleResolver;
  private state: "uninitialized" | "ready" | "cleaning-up" = "uninitialized";
  private engineHandle: number | null = null;
  private cleanupPromise: Promise<void> | null = null;

  /**
   * @param options - Configuration options or a legacy libPath string.
   *   When a string is provided, it is treated as {@link DataWeaveOptions.libPath}.
   */
  constructor(options?: DataWeaveOptions | string) {
    this.addonPath = resolveAddonPath();
    if (typeof options === "string") {
      // Legacy constructor signature: DataWeave(libPath)
      this.libPath = options;
      this.resolveModule = undefined;
    } else {
      this.libPath = options?.libPath ?? findLibrary(this.addonPath);
      this.resolveModule = options?.resolveModule;
    }
  }

  /**
   * Loads and initializes the native runtime. Idempotent — a no-op if already
   * initialized.
   *
   * @throws DataWeaveError if the native library fails to load or initialize.
   * @throws DataWeaveError if called while a `cleanup()` is still in progress
   *   — await the cleanup first.
   */
  initialize(): void {
    if (this.state === "ready") return;
    if (this.state === "cleaning-up") {
      throw new DataWeaveError(
        "Cannot initialize while cleanup is in progress; await cleanup() first."
      );
    }
    let libRefAcquired = false;
    try {
      ffi.initialize(this.libPath, this.addonPath);
      libRefAcquired = true;
      this.engineHandle = this.resolveModule
        ? ffi.createEngineWithResolver(this.resolveModule)
        : ffi.createEngine();
    } catch (e: unknown) {
      // If ffi.initialize() already succeeded but engine creation then threw,
      // we already hold an increment of the native library's ref-counted
      // handle. this.state stays "uninitialized" below (we're about to throw),
      // so cleanup()'s early-return guard (`if (this.state !== "ready") return;`)
      // means nothing else will ever call ffi.cleanup() for this instance --
      // release the ref-count ourselves here or it leaks for the process
      // lifetime.
      if (libRefAcquired) {
        ffi.cleanup();
      }
      this.engineHandle = null;
      throw new DataWeaveError(`Failed to initialize: ${e instanceof Error ? e.message : e}`);
    }
    this.state = "ready";
  }

  /**
   * Releases the native runtime. Idempotent — a no-op if not initialized. After
   * cleanup the instance can be re-initialized via {@link DataWeave.initialize}.
   *
   * Resolves once the underlying native isolate has actually finished tearing
   * down. If a streaming/transform operation on this or any other instance is
   * still in flight when the last reference is released, native teardown
   * waits for it to drain before resolving — awaiting this rather than
   * firing-and-forgetting avoids racing a subsequent {@link initialize} against
   * an isolate that is still tearing down.
   */
  async cleanup(): Promise<void> {
    // Coalesce first: doCleanup() flips `state` to "cleaning-up" synchronously
    // as its first statement, so by the time a second overlapping call runs,
    // `state` has already left "ready". If the not-ready guard below ran
    // first, that second caller would resolve immediately instead of
    // awaiting the first caller's in-flight native teardown -- contradicting
    // this method's contract of resolving only once the isolate has actually
    // finished tearing down (round-6 review, task-1 fix round 1). Checking
    // `cleanupPromise` first ensures every concurrent caller that overlaps
    // with an in-flight doCleanup() awaits that SAME promise, so the native
    // teardown (ffi.destroyEngine/ffi.cleanup) still happens exactly once.
    if (this.cleanupPromise) return this.cleanupPromise;
    // Not coalescing with an in-flight cleanup: nothing to do unless we're
    // "ready" (covers both never-initialized and already-settled cleanup).
    if (this.state !== "ready") return;
    this.cleanupPromise = this.doCleanup();
    try {
      await this.cleanupPromise;
    } finally {
      // Clear on both fulfilment and rejection so a later cleanup() (after a
      // re-initialize, or a retry of a rejected cleanup) can run again.
      this.cleanupPromise = null;
    }
  }

  private async doCleanup(): Promise<void> {
    // Transition BEFORE releasing the engine so run()/initialize() called
    // during the async teardown window are rejected deterministically rather
    // than seeing a stale "ready" state with a null engineHandle (round-6 #1/#3).
    this.state = "cleaning-up";
    try {
      if (this.engineHandle !== null) {
        ffi.destroyEngine(this.engineHandle);
        this.engineHandle = null;
      }
      await ffi.cleanup();
    } finally {
      this.state = "uninitialized";
    }
  }

  /**
   * Executes a script and returns its result in a single (non-streaming) call.
   *
   * @param script - The DataWeave script to run.
   * @param inputs - Named inputs made available to the script (e.g. `payload`).
   * @param opts - When `raiseOnError` is set, an unsuccessful result is thrown
   *   as a {@link DataWeaveScriptError} instead of being returned.
   * @returns The {@link ExecutionResult} carrying the output payload or error.
   * @throws DataWeaveError if the runtime is not initialized.
   * @throws DataWeaveScriptError if the script fails and `opts.raiseOnError` is set.
   */
  run(script: string, inputs?: Inputs, opts?: { raiseOnError?: boolean }): ExecutionResult {
    this.ensureReady();
    const inputsJson = buildInputsJson(inputs ?? {});

    const raw = ffi.runScriptEngine(this.engineHandle!, script, inputsJson);

    const result = parseNativeResponse(raw);

    if (opts?.raiseOnError && !result.success) {
      throw new DataWeaveScriptError(result);
    }
    return result;
  }

  /**
   * Executes a script and streams its output as it is produced.
   *
   * Yields output chunks in order; the generator's return value is the terminal
   * {@link StreamingResult} with the final status and content metadata.
   *
   * @param script - The DataWeave script to run.
   * @param inputs - Named inputs made available to the script.
   * @returns An async generator of output chunks, returning the streaming metadata.
   * @throws DataWeaveError if the runtime is not initialized.
   */
  async *runStreaming(script: string, inputs?: Inputs): AsyncGenerator<Buffer, StreamingResult, undefined> {
    this.ensureReady();
    const inputsJson = buildInputsJson(inputs ?? {});
    return yield* streamFromNative((chunkCb) =>
      ffi.runScriptStreamingEngine(this.engineHandle!, script, inputsJson, chunkCb)
    );
  }

  /**
   * Executes a script over a streamed primary input, streaming the output.
   *
   * The `input` chunks are fed to the script as the primary input (named
   * `opts.inputName`, default `payload`); output chunks are yielded as they are
   * produced and the generator returns the terminal {@link StreamingResult}.
   * Sync iterables are consumed on demand; async iterables are pre-buffered (see
   * {@link createChunkReader}).
   *
   * @param script - The DataWeave script to run.
   * @param input - A sync or async iterable of byte chunks for the primary input.
   * @param opts - Primary-input framing (`inputName`, `mimeType`, `charset`) and
   *   any additional named `inputs`.
   * @returns An async generator of output chunks, returning the streaming metadata.
   * @throws DataWeaveError if the runtime is not initialized.
   */
  async *runTransform(
    script: string,
    input: AsyncIterable<Buffer | Uint8Array> | Iterable<Buffer | Uint8Array>,
    opts?: TransformOptions
  ): AsyncGenerator<Buffer, StreamingResult, undefined> {
    this.ensureReady();

    const inputName = opts?.inputName ?? "payload";
    const inputMimeType = opts?.mimeType ?? "application/json";
    const inputCharset = opts?.charset ?? null;
    const extraInputs = opts?.inputs ?? {};
    const inputsJson = Object.keys(extraInputs).length > 0 ? buildInputsJson(extraInputs) : "{}";

    const readCb = await createChunkReader(input);

    return yield* streamFromNative((writeCb) =>
      ffi.runScriptTransformEngine(
        this.engineHandle!,
        script,
        inputsJson,
        inputName,
        inputMimeType,
        inputCharset,
        readCb,
        writeCb
      )
    );
  }

  private ensureReady(): void {
    if (this.state === "ready") return;
    if (this.state === "cleaning-up") {
      throw new DataWeaveError(
        "DataWeave runtime is cleaning up; await cleanup() before running again."
      );
    }
    throw new DataWeaveError("DataWeave runtime not initialized. Call initialize() first.");
  }
}

// Module-level convenience API with lazy singleton
let globalInstance: DataWeave | null = null;
// Guards against beforeExit and exit both driving cleanup for the same
// shutdown. Belt-and-suspenders on top of cleanup()'s own idempotency.
let cleanupStarted = false;
// Process exit hooks are registered exactly once for the lifetime of the
// module, NOT per singleton. Re-creating the singleton after cleanup() must
// not attach a second pair of listeners (that accumulates until Node emits
// MaxListenersExceededWarning). The listeners tolerate a null globalInstance:
// cleanup() no-ops when there is nothing to release, and cleanupStarted
// coalesces beforeExit/exit for a given shutdown. Unlike cleanupStarted, this
// guard is never reset — that is the whole point.
let exitHooksRegistered = false;

/**
 * Registers the process-wide exit-cleanup hooks exactly once for this
 * module. Subsequent calls (e.g. from a revived singleton after cleanup())
 * are no-ops: the hooks registered on first use are reused for the rest of
 * the process's lifetime, which is safe because they tolerate a null
 * `globalInstance` and `cleanupStarted` coalesces beforeExit/exit for a
 * given shutdown.
 *
 * Two hooks are registered, covering complementary cases:
 * - `beforeExit` fires when the event loop is about to drain naturally and
 *   CAN run async work (Node keeps the loop alive until it settles), so it
 *   drains any in-flight streaming/transform operation gracefully. This is
 *   the common case.
 * - `exit` runs strictly synchronously and is only a best-effort fallback for
 *   the paths that skip `beforeExit` — `process.exit()`, an uncaught
 *   exception, and normal process termination. Because it is synchronous it
 *   can only run the fast cleanup path, so an in-flight async operation may be
 *   abandoned. Node does NOT emit `exit` (nor `beforeExit`) for termination
 *   signals such as SIGTERM/SIGINT/SIGKILL, nor for every fatal failure mode,
 *   so this is not a guarantee: callers that require graceful shutdown must
 *   register and await their own handlers for the catchable signals (e.g.
 *   `process.on("SIGTERM", async () => { await cleanup(); process.exit(0); })`);
 *   SIGKILL cannot be caught, so no in-process cleanup can run for it.
 * The `cleanupStarted` guard ensures only one of the two hooks actually
 * runs cleanup for a given shutdown.
 */
function registerExitHooksOnce(): void {
  if (exitHooksRegistered) return;
  exitHooksRegistered = true;
  process.on("beforeExit", async () => {
    if (cleanupStarted) return;
    cleanupStarted = true;
    await cleanup(); // beforeExit can await: drains in-flight ops
  });
  process.on("exit", () => {
    if (cleanupStarted) return; // beforeExit already handled it
    cleanup(); // fallback: best-effort sync fast path
  });
}

/**
 * Returns the process-wide {@link DataWeave} singleton, creating and
 * initializing it on first use (or after a prior {@link cleanup}).
 *
 * The exit-cleanup hooks are registered exactly once for the process via
 * {@link registerExitHooksOnce}, not once per singleton: a singleton revived
 * after cleanup() reuses the same pair of listeners rather than adding new
 * ones, which would otherwise accumulate a pair per init/cleanup cycle until
 * Node emits `MaxListenersExceededWarning`. Reuse is safe because the
 * listeners tolerate a null `globalInstance` and `cleanupStarted` coalesces
 * beforeExit/exit for a given shutdown.
 */
function getGlobalInstance(): DataWeave {
  if (!globalInstance) {
    globalInstance = new DataWeave();
    globalInstance.initialize();
    registerExitHooksOnce();
  }
  return globalInstance;
}

/**
 * Executes a script on the shared {@link DataWeave} singleton.
 * @see DataWeave.run
 */
export function run(script: string, inputs?: Inputs, opts?: { raiseOnError?: boolean }): ExecutionResult {
  return getGlobalInstance().run(script, inputs, opts);
}

/**
 * Streams a script's output on the shared {@link DataWeave} singleton.
 * @see DataWeave.runStreaming
 */
export function runStreaming(
  script: string,
  inputs?: Inputs
): AsyncGenerator<Buffer, StreamingResult, undefined> {
  return getGlobalInstance().runStreaming(script, inputs);
}

/**
 * Streams a transform over a streamed input on the shared {@link DataWeave} singleton.
 * @see DataWeave.runTransform
 */
export function runTransform(
  script: string,
  input: AsyncIterable<Buffer | Uint8Array> | Iterable<Buffer | Uint8Array>,
  opts?: TransformOptions
): AsyncGenerator<Buffer, StreamingResult, undefined> {
  return getGlobalInstance().runTransform(script, input, opts);
}

/**
 * Releases the shared {@link DataWeave} singleton, if one was created. A fresh
 * singleton is created lazily on the next convenience-API call.
 */
export async function cleanup(): Promise<void> {
  if (globalInstance) {
    const instance = globalInstance;
    globalInstance = null;
    await instance.cleanup();
    // Reset the guard only after the drain has fully completed, so a
    // revived singleton (created by a later getGlobalInstance() call)
    // gets its own live hooks for the next real exit. This must stay
    // last: resetting earlier could let a concurrent `exit` firing on
    // this same shutdown re-enter cleanup while the async drain above
    // is still in flight.
    cleanupStarted = false;
  }
}
