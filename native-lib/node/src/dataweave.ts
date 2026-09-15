import * as ffi from "./ffi";
import { resolveAddonPath } from "./addon-path";
import { findLibrary, buildInputsJson } from "./utils";
import { parseNativeResponse } from "./result";
import { createChunkReader } from "./reader";
import { interruptNativeStreamIfParked, nativeStreamParked, streamFromNative } from "./stream";
import { DataWeaveError, DataWeaveScriptError } from "./errors";
import type { NativeStreamingOperation } from "./ffi";
import type { ExecutionResult, StreamingResult, Inputs, TransformOptions } from "./types";
import type { ModuleResolver } from "./resolver";

interface EngineOperationToken {
  readonly handle: number;
  readonly generation: number;
}

interface ActiveStreamState {
  completionSettled: boolean;
  closeFinalized: boolean;
}

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
 * Wraps the native `dwlib` shared library via the FFI addon. Construct an
 * instance, call {@link DataWeave.initialize} before running scripts, and await
 * {@link DataWeave.cleanup} when done.
 */
export class DataWeave {
  private readonly addonPath: string;
  private readonly libPath: string;
  private readonly resolveModule?: ModuleResolver;
  private state: "uninitialized" | "ready" | "cleaning-up" = "uninitialized";
  private engineHandle: number | null = null;
  private engineGeneration = 0;
  private readonly activeStreams = new Set<NativeStreamingOperation>();
  private readonly activeStreamStates = new Map<NativeStreamingOperation, ActiveStreamState>();
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
      const engineHandle = this.resolveModule
        ? ffi.createEngineWithResolver(this.resolveModule)
        : ffi.createEngine();
      this.engineHandle = engineHandle;
    } catch (e: unknown) {
      // If ffi.initialize() already succeeded but engine creation then threw, we
      // already hold an increment of the native library's ref-counted handle and
      // must release it (ffi.cleanup()), or it leaks for the process lifetime.
      // ffi.cleanup() is async, so model the rollback as PENDING state instead of
      // firing-and-forgetting it (review #7 #3): (1) an un-awaited rejection must
      // not become an unhandledRejection, and (2) a concurrent initialize()/run()
      // must not race a fresh graal_create_isolate against the in-flight release.
      // Reuse the same cleanupPromise/"cleaning-up" machinery cleanup() uses:
      // hold state "cleaning-up" until the release settles (so initialize()'s own
      // "cleaning-up" guard rejects a concurrent retry deterministically, and a
      // concurrent cleanup() coalesces onto this same promise), then return to
      // "uninitialized". The synchronous throw to THIS caller is preserved.
      this.engineHandle = null;
      if (libRefAcquired) {
        this.state = "cleaning-up";
        // ffi.cleanup() can fail synchronously (throw) as well as asynchronously
        // (reject a returned promise). Calling it inside a try/catch -- rather
        // than eagerly as the argument to Promise.resolve(ffi.cleanup()) -- lets
        // a synchronous throw be caught and normalized into a rejected promise
        // BEFORE cleanupPromise is assigned, so it still flows through the same
        // .finally() state reset instead of escaping here and stranding this
        // instance in "cleaning-up" forever (review #8 #2). Existing callers
        // still observe ffi.cleanup() invoked synchronously, in the same tick as
        // this catch block, exactly as before this fix.
        let releaseResult: Promise<void> | void;
        try {
          releaseResult = ffi.cleanup();
        } catch (cleanupError) {
          releaseResult = Promise.reject(cleanupError);
        }
        this.cleanupPromise = Promise.resolve(releaseResult).finally(() => {
          this.state = "uninitialized";
          this.cleanupPromise = null;
        });
        // Never let an un-awaited rollback surface as an unhandledRejection. A
        // caller that awaits cleanup() (which coalesces onto cleanupPromise)
        // still observes the rejection; this handler only covers the un-awaited
        // path.
        this.cleanupPromise.catch(() => {});
      }
      throw new DataWeaveError(`Failed to initialize: ${e instanceof Error ? e.message : e}`);
    }
    this.state = "ready";
    this.engineGeneration++;
  }

  /**
   * Releases the native runtime. Idempotent — a no-op if not initialized. After
   * cleanup the instance can be re-initialized via {@link DataWeave.initialize}.
   *
   * Resolution depends on whether this call releases the FINAL shared native
   * reference in the process. When it does, it first drains any in-flight
   * streaming/transform operation on this or any other instance, then attempts
   * isolate teardown and resolves once that attempt completes. The promise thus
   * guarantees logical release and that teardown was attempted — not
   * necessarily physical reclamation of the isolate: an ordinary teardown
   * failure retains the live isolate and is retried where safe (at a later
   * initialization or async op-completion drain), and an unrecoverable
   * teardown-plus-detach double failure intentionally leaks the isolate until
   * process exit (with a diagnostic on stderr). Awaiting this rather than
   * firing-and-forgetting lets the drain complete before a subsequent
   * {@link initialize}. When other initialized instances remain, it resolves as
   * soon as this instance's engine is released, leaving the shared isolate live
   * for them.
   */
  async cleanup(): Promise<void> {
    // Coalesce first: doCleanup() flips `state` to "cleaning-up" synchronously
    // as its first statement, so by the time a second overlapping call runs,
    // `state` has already left "ready". If the not-ready guard below ran
    // first, that second caller would resolve immediately instead of
    // awaiting the first caller's in-flight native teardown -- contradicting
    // this method's contract of resolving only once the in-flight native
    // teardown attempt has completed (round-6 review, task-1 fix round 1).
    // Checking
    // `cleanupPromise` first ensures every concurrent caller that overlaps
    // with an in-flight doCleanup() awaits that SAME promise, so the native
    // teardown (ffi.destroyEngine/ffi.cleanup) still happens exactly once.
    if (this.cleanupPromise) return this.cleanupPromise;
    // A lifecycle failure leaves the instance in "cleaning-up" with no live
    // cleanupPromise so a later call can retry without admitting new work.
    if (this.state === "uninitialized") return;
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
    const activeStreams = [...this.activeStreams];
    let lifecycleError: { readonly hasError: false } | { readonly hasError: true; readonly error: unknown } = {
      hasError: false,
    };
    for (const operation of activeStreams) {
      try {
        operation.cancel();
      } catch (error) {
        if (!lifecycleError.hasError) lifecycleError = { hasError: true, error };
      }
    }
    // A failed synchronous cancellation cannot guarantee that completion will
    // ever settle. Abort before waiting or destroying the engine and keep the
    // instance in cleaning-up state so a later cleanup() can retry.
    if (lifecycleError.hasError) throw lifecycleError.error;

    if (activeStreams.length > 0) {
      await Promise.allSettled(activeStreams.map((operation) => operation.completion));
    }

    for (const operation of activeStreams) {
      const streamState = this.activeStreamStates.get(operation);
      if (!streamState || streamState.closeFinalized) continue;
      try {
        operation.close();
      } catch (error) {
        if (!lifecycleError.hasError) lifecycleError = { hasError: true, error };
      }
    }
    if (lifecycleError.hasError) throw lifecycleError.error;

    let destroyError: { readonly hasError: false } | { readonly hasError: true; readonly error: unknown } = {
      hasError: false,
    };
    try {
      if (this.engineHandle !== null) {
        try {
          ffi.destroyEngine(this.engineHandle);
        } catch (e) {
          // Round-14 (#6): a throwing destroyEngine() (e.g. wrong-thread
          // destruction) must NOT skip ffi.cleanup() -- that would strand this
          // env's native init reference and block isolate teardown. Capture the
          // primary error, clear the handle so a retry does not double-destroy,
          // and fall through to release the reference below.
          destroyError = { hasError: true, error: e };
        } finally {
          this.engineHandle = null;
        }
      }
      await ffi.cleanup();
    } finally {
      this.state = "uninitialized";
    }
    // Surface the primary destruction error after the reference was released. If
    // ffi.cleanup() itself rejected, its error already propagated from the await
    // (the more actionable reference-release failure wins; the destroy error is
    // then suppressed).
    if (destroyError.hasError) throw destroyError.error;
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
    const token = this.captureOperationToken();
    const inputsJson = buildInputsJson(inputs ?? {});

    this.assertCurrentOperation(token);
    const raw = ffi.runScriptEngine(token.handle, script, inputsJson);

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
  runStreaming(script: string, inputs?: Inputs): AsyncGenerator<Buffer, StreamingResult, undefined> {
    const token = this.captureOperationToken();
    return this.runStreamingInternal(token, script, inputs);
  }

  private runStreamingInternal(
    token: EngineOperationToken,
    script: string,
    inputs?: Inputs
  ): AsyncGenerator<Buffer, StreamingResult, undefined> {
    return streamFromNative(
      (chunkCb) => {
        this.assertCurrentOperation(token);
        const inputsJson = buildInputsJson(inputs ?? {});
        this.assertCurrentOperation(token);
        return ffi.runScriptStreamingEngine(token.handle, script, inputsJson, chunkCb);
      },
      (operation) => { this.registerActiveStream(operation); },
      (operation) => { this.markActiveStreamClosed(operation); }
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
  runTransform(
    script: string,
    input: AsyncIterable<Buffer | Uint8Array> | Iterable<Buffer | Uint8Array>,
    opts?: TransformOptions
  ): AsyncGenerator<Buffer, StreamingResult, undefined> {
    const token = this.captureOperationToken();
    return this.runTransformInternal(token, script, input, opts);
  }

  private runTransformInternal(
    token: EngineOperationToken,
    script: string,
    input: AsyncIterable<Buffer | Uint8Array> | Iterable<Buffer | Uint8Array>,
    opts?: TransformOptions
  ): AsyncGenerator<Buffer, StreamingResult, undefined> {
    type QueuedRequest<T> = {
      readonly kind: "next" | "control";
      readonly run: (onParked: () => void) => Promise<T>;
      readonly resolve: (result: T) => void;
      readonly reject: (error: unknown) => void;
    };

    let closed = false;
    let controlPending = false;
    let stream: AsyncGenerator<Buffer, StreamingResult, undefined> | null = null;
    let setupPromise: Promise<AsyncGenerator<Buffer, StreamingResult, undefined> | null>;
    let admissionState: "pending" | "abandoned" | "admitted" = "pending";
    let abandonSetup = () => { admissionState = "abandoned"; };
    const requestQueue: Array<QueuedRequest<unknown>> = [];
    let requestRunning = false;

    const enqueue = <T>(
      kind: QueuedRequest<T>["kind"],
      request: QueuedRequest<T>["run"]
    ): Promise<T> => {
      const result = new Promise<T>((resolve, reject) => {
        requestQueue.push({ kind, run: request, resolve, reject } as QueuedRequest<unknown>);
      });
      drainRequests();
      return result;
    };

    function drainRequests(): void {
      if (requestRunning) return;
      const request = requestQueue.shift();
      if (!request) return;
      requestRunning = true;
      let result: Promise<unknown>;
      try {
        result = request.run(() => {
          if (controlPending) interruptForControl();
        });
      } catch (error) {
        result = Promise.reject(error);
      }
      result.then(request.resolve, request.reject).finally(() => {
        requestRunning = false;
        drainRequests();
      });
    }

    function interruptAdmittedPullForControl(): void {
      const controlIndex = requestQueue.findIndex((request) => request.kind === "control");
      if (controlIndex > 0 && requestQueue.slice(0, controlIndex).some((request) => request.kind === "next")) return;
      if (stream) interruptNativeStreamIfParked(stream);
    }

    function interruptForControl(): void {
      // Setup has no native pull to preserve: settle every queued pre-control
      // next() as done immediately. Admitted streams instead retain FIFO until
      // the last earlier pull is genuinely parked.
      if (admissionState !== "admitted") {
        abandonSetup();
        return;
      }
      interruptAdmittedPullForControl();
    }

    const setup = (): Promise<AsyncGenerator<Buffer, StreamingResult, undefined> | null> =>
      setupPromise ??= new Promise((resolve, reject) => {
        abandonSetup = () => {
          admissionState = "abandoned";
          resolve(null);
        };
        try {
          if (controlPending) {
            abandonSetup();
            return;
          }
          this.assertCurrentOperation(token);

          const inputName = opts?.inputName ?? "payload";
          const inputMimeType = opts?.mimeType ?? "application/json";
          const inputCharset = opts?.charset ?? null;
          const extraInputs = opts?.inputs ?? {};
          const inputsJson = Object.keys(extraInputs).length > 0 ? buildInputsJson(extraInputs) : "{}";
          createChunkReader(input).then(
            (readCb) => {
              if (admissionState === "abandoned" || controlPending) return;
              try {
                this.assertCurrentOperation(token);
                stream = streamFromNative(
                  (writeCb) => {
                    this.assertCurrentOperation(token);
                    return ffi.runScriptTransformEngine(
                      token.handle,
                      script,
                      inputsJson,
                      inputName,
                      inputMimeType,
                      inputCharset,
                      readCb,
                      writeCb
                    );
                  },
                  (operation) => {
                    admissionState = "admitted";
                    this.registerActiveStream(operation);
                  },
                  (operation) => { this.markActiveStreamClosed(operation); }
                );
                resolve(stream);
              } catch (error) {
                reject(error);
              }
            },
            (error) => {
              if (admissionState !== "abandoned") reject(error);
            }
          );
        } catch (error) {
          reject(error);
        }
      });

    return {
      next: (...args: [] | [undefined]) => enqueue("next", async (onParked) => {
        if (closed) {
          return { done: true, value: undefined } as unknown as IteratorReturnResult<StreamingResult>;
        }
        let activeStream: AsyncGenerator<Buffer, StreamingResult, undefined> | null;
        try {
          activeStream = await setup();
        } catch (error) {
          closed = true;
          throw error;
        }
        if (!activeStream || admissionState === "abandoned") {
          return { done: true, value: undefined } as unknown as IteratorReturnResult<StreamingResult>;
        }
        const nextPromise = activeStream.next(...args);
        nativeStreamParked(activeStream).then((parked) => {
          if (parked) onParked();
        });
        const result = await nextPromise;
        if (result.done) closed = true;
        return result;
      }),
      return: (value) => {
        const isPrimaryControl = !closed && !controlPending;
        if (isPrimaryControl) controlPending = true;
        const result = enqueue("control", async (_onParked) => {
          try {
            if (stream && !closed) return await stream.return(value);
            return {
              done: true,
              value: await value,
            } as IteratorReturnResult<StreamingResult>;
          } finally {
            closed = true;
          }
        });
        if (isPrimaryControl) interruptForControl();
        return result;
      },
      throw: (error?: unknown) => {
        const isPrimaryControl = !closed && !controlPending;
        if (isPrimaryControl) controlPending = true;
        const result = enqueue("control", async (_onParked) => {
          try {
            if (stream && !closed) return await stream.throw(error);
            throw error;
          } finally {
            closed = true;
          }
        });
        if (isPrimaryControl) interruptForControl();
        return result;
      },
      [Symbol.asyncIterator]() {
        return this;
      },
    };
  }

  private registerActiveStream(operation: NativeStreamingOperation): void {
    const state: ActiveStreamState = { completionSettled: false, closeFinalized: false };
    this.activeStreams.add(operation);
    this.activeStreamStates.set(operation, state);
    operation.completion.then(
      () => {
        state.completionSettled = true;
        this.releaseActiveStream(operation, state);
      },
      () => {
        state.completionSettled = true;
        this.releaseActiveStream(operation, state);
      }
    );
  }

  private markActiveStreamClosed(operation: NativeStreamingOperation): void {
    const state = this.activeStreamStates.get(operation);
    if (!state) return;
    state.closeFinalized = true;
    this.releaseActiveStream(operation, state);
  }

  private releaseActiveStream(
    operation: NativeStreamingOperation,
    state: ActiveStreamState
  ): void {
    if (!state.completionSettled || !state.closeFinalized) return;
    this.activeStreams.delete(operation);
    this.activeStreamStates.delete(operation);
  }

  private captureOperationToken(): EngineOperationToken {
    this.ensureReady();
    return { handle: this.engineHandle!, generation: this.engineGeneration };
  }

  private assertCurrentOperation(token: EngineOperationToken): void {
    if (
      this.state !== "ready" ||
      this.engineHandle !== token.handle ||
      this.engineGeneration !== token.generation
    ) {
      throw new DataWeaveError("DataWeave operation belongs to a stale engine generation.");
    }
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
