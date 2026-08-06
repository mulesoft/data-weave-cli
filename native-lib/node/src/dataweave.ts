import * as ffi from "./ffi";
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
   * Note: the native layer installs at most one resolver per process
   * lifetime, bound to the thread (main thread or `worker_threads` Worker)
   * that registers it first. If you construct multiple `DataWeave` instances
   * with different `resolveModule` callbacks in the same process, later
   * instances silently reuse the first resolver instead of their own; if a
   * later instance is constructed on a *different* thread, its resolver is
   * not invoked at all and custom module paths resolve as "not found" (see
   * docs/external-modules.md#multiple-resolvers-in-one-process).
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
  private readonly libPath: string;
  private readonly resolveModule?: ModuleResolver;
  private initialized = false;

  /**
   * @param options - Configuration options or a legacy libPath string.
   *   When a string is provided, it is treated as {@link DataWeaveOptions.libPath}.
   */
  constructor(options?: DataWeaveOptions | string) {
    if (typeof options === "string") {
      // Legacy constructor signature: DataWeave(libPath)
      this.libPath = options;
      this.resolveModule = undefined;
    } else {
      this.libPath = options?.libPath ?? findLibrary();
      this.resolveModule = options?.resolveModule;
    }
  }

  /**
   * Loads and initializes the native runtime. Idempotent — a no-op if already
   * initialized.
   *
   * @throws DataWeaveError if the native library fails to load or initialize.
   */
  initialize(): void {
    if (this.initialized) return;
    try {
      ffi.initialize(this.libPath);
    } catch (e: unknown) {
      throw new DataWeaveError(`Failed to initialize: ${e instanceof Error ? e.message : e}`);
    }
    this.initialized = true;
  }

  /**
   * Releases the native runtime. Idempotent — a no-op if not initialized. After
   * cleanup the instance can be re-initialized via {@link DataWeave.initialize}.
   */
  cleanup(): void {
    if (!this.initialized) return;
    ffi.cleanup();
    this.initialized = false;
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
    this.ensureInitialized();
    const inputsJson = buildInputsJson(inputs ?? {});

    let raw: string;
    if (this.resolveModule) {
      // Use resolver-aware entrypoint
      raw = ffi.runWithResolver(script, inputsJson, "application/json", this.resolveModule);
    } else {
      // Use standard entrypoint (backward compatible)
      raw = ffi.runScript(script, inputsJson);
    }

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
    this.ensureInitialized();
    const inputsJson = buildInputsJson(inputs ?? {});
    return yield* streamFromNative((chunkCb) => ffi.runScriptStreaming(script, inputsJson, chunkCb));
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
    this.ensureInitialized();

    const inputName = opts?.inputName ?? "payload";
    const inputMimeType = opts?.mimeType ?? "application/json";
    const inputCharset = opts?.charset ?? null;
    const extraInputs = opts?.inputs ?? {};
    const inputsJson = Object.keys(extraInputs).length > 0 ? buildInputsJson(extraInputs) : "{}";

    const readCb = await createChunkReader(input);

    return yield* streamFromNative((writeCb) =>
      ffi.runScriptTransform(script, inputsJson, inputName, inputMimeType, inputCharset, readCb, writeCb)
    );
  }

  private ensureInitialized(): void {
    if (!this.initialized) {
      throw new DataWeaveError("DataWeave runtime not initialized. Call initialize() first.");
    }
  }
}

// Module-level convenience API with lazy singleton
let globalInstance: DataWeave | null = null;

/**
 * Returns the process-wide {@link DataWeave} singleton, creating and
 * initializing it (and registering a process-exit cleanup hook) on first use.
 */
function getGlobalInstance(): DataWeave {
  if (!globalInstance) {
    globalInstance = new DataWeave();
    globalInstance.initialize();
    process.on("exit", () => cleanup());
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
export function cleanup(): void {
  if (globalInstance) {
    globalInstance.cleanup();
    globalInstance = null;
  }
}