import * as ffi from "./ffi";
import { findLibrary, buildInputsJson } from "./utils";
import { parseNativeResponse, parseStreamingResult } from "./result";
import type {
  ExecutionResult,
  StreamingResult,
  Inputs,
  TransformOptions,
} from "./types";

export type {
  ExecutionResult,
  StreamingResult,
  Inputs,
  InputValue,
  InputEntry,
  TransformOptions,
} from "./types";

export class DataWeaveError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "DataWeaveError";
  }
}

export class DataWeaveScriptError extends DataWeaveError {
  result: ExecutionResult;
  constructor(result: ExecutionResult) {
    super(result.error ?? "Script execution failed");
    this.name = "DataWeaveScriptError";
    this.result = result;
  }
}

export class DataWeave {
  private libPath: string;
  private initialized = false;

  constructor(libPath?: string) {
    this.libPath = libPath ?? findLibrary();
  }

  initialize(): void {
    if (this.initialized) return;
    try {
      ffi.initialize(this.libPath);
    } catch (e: unknown) {
      throw new DataWeaveError(`Failed to initialize: ${e instanceof Error ? e.message : e}`);
    }
    this.initialized = true;
  }

  cleanup(): void {
    if (!this.initialized) return;
    ffi.cleanup();
    this.initialized = false;
  }

  run(script: string, inputs?: Inputs, opts?: { raiseOnError?: boolean }): ExecutionResult {
    this.ensureInitialized();
    const inputsJson = buildInputsJson(inputs ?? {});
    const raw = ffi.runScript(script, inputsJson);
    const result = parseNativeResponse(raw);

    if (opts?.raiseOnError && !result.success) {
      throw new DataWeaveScriptError(result);
    }
    return result;
  }

  async *runStreaming(script: string, inputs?: Inputs): AsyncGenerator<Buffer, StreamingResult, undefined> {
    this.ensureInitialized();
    const inputsJson = buildInputsJson(inputs ?? {});

    const chunks: Buffer[] = [];
    const pendingResolves: Array<() => void> = [];
    let done = false;
    let metaRaw: string | null = null;

    const chunkCb = (chunk: Buffer) => {
      chunks.push(chunk);
      // Resolve one waiting consumer if any
      const resolve = pendingResolves.shift();
      if (resolve) {
        resolve();
      }
    };

    const metaPromise = ffi.runScriptStreaming(script, inputsJson, chunkCb).then((raw) => {
      metaRaw = raw;
      done = true;
      // Wake all waiting consumers
      while (pendingResolves.length > 0) {
        const resolve = pendingResolves.shift();
        if (resolve) resolve();
      }
    });

    while (true) {
      if (chunks.length > 0) {
        yield chunks.shift()!;
        continue;
      }
      if (done) break;
      await new Promise<void>((resolve) => { pendingResolves.push(resolve); });
    }

    // Drain remaining chunks
    while (chunks.length > 0) {
      yield chunks.shift()!;
    }

    await metaPromise;
    return parseStreamingResult(metaRaw ?? "");
  }

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

    const isAsync = Symbol.asyncIterator in (input as object);

    let readCb: (bufSize: number) => Buffer | null;

    if (isAsync) {
      // Async iterables must be pre-buffered because the native read callback
      // is invoked synchronously on the JS main thread and cannot await.
      const inputBuffers: (Buffer | null)[] = [];
      const asyncIter = (input as AsyncIterable<Buffer | Uint8Array>)[Symbol.asyncIterator]();
      try {
        while (true) {
          const { value, done: d } = await asyncIter.next();
          if (d) break;
          inputBuffers.push(Buffer.isBuffer(value) ? value : Buffer.from(value));
        }
      } catch { /* input error = EOF */ }

      let bufIdx = 0;
      let currentBuf: Buffer | null = null;
      let readOffset = 0;

      readCb = (bufSize: number): Buffer | null => {
        while (true) {
          if (currentBuf && readOffset < currentBuf.length) {
            const n = Math.min(currentBuf.length - readOffset, bufSize);
            const slice = currentBuf.subarray(readOffset, readOffset + n);
            readOffset += n;
            if (readOffset >= currentBuf.length) {
              currentBuf = null;
              readOffset = 0;
            }
            return Buffer.from(slice);
          }
          if (bufIdx < inputBuffers.length) {
            currentBuf = inputBuffers[bufIdx];
            inputBuffers[bufIdx] = null; // Release memory as we consume
            bufIdx++;
            readOffset = 0;
            continue;
          }
          return null;
        }
      };
    } else {
      // Sync iterables are consumed on-demand — constant memory, no pre-buffering.
      const syncIter = (input as Iterable<Buffer | Uint8Array>)[Symbol.iterator]();
      let currentBuf: Buffer | null = null;
      let readOffset = 0;
      let iterDone = false;

      readCb = (bufSize: number): Buffer | null => {
        while (true) {
          if (currentBuf && readOffset < currentBuf.length) {
            const n = Math.min(currentBuf.length - readOffset, bufSize);
            const slice = currentBuf.subarray(readOffset, readOffset + n);
            readOffset += n;
            if (readOffset >= currentBuf.length) {
              currentBuf = null;
              readOffset = 0;
            }
            return Buffer.from(slice);
          }
          if (iterDone) return null;
          const { value, done: d } = syncIter.next();
          if (d) {
            iterDone = true;
            return null;
          }
          currentBuf = Buffer.isBuffer(value) ? value : Buffer.from(value);
          readOffset = 0;
        }
      };
    }

    const chunks: Buffer[] = [];
    const pendingResolves: Array<() => void> = [];
    let done = false;
    let metaRaw: string | null = null;

    const writeCb = (chunk: Buffer) => {
      chunks.push(chunk);
      // Resolve one waiting consumer if any
      const resolve = pendingResolves.shift();
      if (resolve) {
        resolve();
      }
    };

    const metaPromise = ffi.runScriptTransform(
      script, inputsJson, inputName, inputMimeType, inputCharset, readCb, writeCb
    ).then((raw) => {
      metaRaw = raw;
      done = true;
      // Wake all waiting consumers
      while (pendingResolves.length > 0) {
        const resolve = pendingResolves.shift();
        if (resolve) resolve();
      }
    });

    while (true) {
      if (chunks.length > 0) {
        yield chunks.shift()!;
        continue;
      }
      if (done) break;
      await new Promise<void>((resolve) => { pendingResolves.push(resolve); });
    }

    while (chunks.length > 0) {
      yield chunks.shift()!;
    }

    await metaPromise;
    return parseStreamingResult(metaRaw ?? "");
  }

  private ensureInitialized(): void {
    if (!this.initialized) {
      throw new DataWeaveError("DataWeave runtime not initialized. Call initialize() first.");
    }
  }
}

// Module-level convenience API with lazy singleton
let globalInstance: DataWeave | null = null;

function getGlobalInstance(): DataWeave {
  if (!globalInstance) {
    globalInstance = new DataWeave();
    globalInstance.initialize();
    process.on("exit", () => cleanup());
  }
  return globalInstance;
}

export function run(script: string, inputs?: Inputs, opts?: { raiseOnError?: boolean }): ExecutionResult {
  return getGlobalInstance().run(script, inputs, opts);
}

export function runStreaming(
  script: string,
  inputs?: Inputs
): AsyncGenerator<Buffer, StreamingResult, undefined> {
  return getGlobalInstance().runStreaming(script, inputs);
}

export function runTransform(
  script: string,
  input: AsyncIterable<Buffer | Uint8Array> | Iterable<Buffer | Uint8Array>,
  opts?: TransformOptions
): AsyncGenerator<Buffer, StreamingResult, undefined> {
  return getGlobalInstance().runTransform(script, input, opts);
}

export function cleanup(): void {
  if (globalInstance) {
    globalInstance.cleanup();
    globalInstance = null;
  }
}
