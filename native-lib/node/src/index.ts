import * as ffi from "./ffi";
import { findLibrary, buildInputsJson } from "./utils";
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

function parseNativeResponse(raw: string): ExecutionResult {
  if (!raw) {
    return makeResult(false, null, "Native returned empty response", false, null, null);
  }

  let parsed: Record<string, unknown>;
  try {
    parsed = JSON.parse(raw);
  } catch (e) {
    return makeResult(false, null, `Failed to parse native JSON response: ${e}`, false, null, null);
  }

  const success = Boolean(parsed.success);
  if (!success) {
    return makeResult(false, null, (parsed.error as string) ?? null, false, null, null);
  }

  return makeResult(
    true,
    (parsed.result as string) ?? null,
    null,
    Boolean(parsed.binary),
    (parsed.mimeType as string) ?? null,
    (parsed.charset as string) ?? null
  );
}

function makeResult(
  success: boolean,
  result: string | null,
  error: string | null,
  binary: boolean,
  mimeType: string | null,
  charset: string | null
): ExecutionResult {
  return {
    success,
    result,
    error,
    binary,
    mimeType,
    charset,
    getBytes() {
      if (!this.success || this.result === null) return null;
      return Buffer.from(this.result, "base64");
    },
    getString() {
      if (!this.success || this.result === null) return null;
      if (this.binary) return this.result;
      const bytes = Buffer.from(this.result, "base64");
      return bytes.toString((this.charset as BufferEncoding) ?? "utf-8");
    },
  };
}

function parseStreamingResult(raw: string): StreamingResult {
  let meta: Record<string, unknown>;
  try {
    meta = raw ? JSON.parse(raw) : { success: false, error: "Empty response" };
  } catch {
    return { success: false, error: "Failed to parse metadata", mimeType: null, charset: null, binary: false };
  }

  const success = Boolean(meta.success);
  if (!success) {
    return { success: false, error: (meta.error as string) ?? null, mimeType: null, charset: null, binary: false };
  }
  return {
    success: true,
    error: null,
    mimeType: (meta.mimeType as string) ?? null,
    charset: (meta.charset as string) ?? null,
    binary: Boolean(meta.binary),
  };
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
    let resolveChunk: (() => void) | null = null;
    let done = false;
    let metaRaw: string | null = null;

    const chunkCb = (chunk: Buffer) => {
      chunks.push(chunk);
      if (resolveChunk) {
        resolveChunk();
        resolveChunk = null;
      }
    };

    const metaPromise = ffi.runScriptStreaming(script, inputsJson, chunkCb).then((raw) => {
      metaRaw = raw;
      done = true;
      if (resolveChunk) {
        resolveChunk();
        resolveChunk = null;
      }
    });

    while (true) {
      if (chunks.length > 0) {
        yield chunks.shift()!;
        continue;
      }
      if (done) break;
      await new Promise<void>((resolve) => { resolveChunk = resolve; });
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

    // Pre-buffer all input chunks (the read callback is synchronous from native side)
    const inputBuffers: Buffer[] = [];
    let inputDone = false;

    const iter = Symbol.asyncIterator in (input as object)
      ? (input as AsyncIterable<Buffer | Uint8Array>)[Symbol.asyncIterator]()
      : (input as Iterable<Buffer | Uint8Array>)[Symbol.iterator]();

    // Eagerly read all input (needed because read callback is sync)
    const feedPromise = (async () => {
      try {
        while (true) {
          const { value, done: d } = await (iter as AsyncIterator<Buffer | Uint8Array>).next();
          if (d) break;
          inputBuffers.push(Buffer.isBuffer(value) ? value : Buffer.from(value));
        }
      } catch { /* input error = EOF */ }
      inputDone = true;
    })();

    // Wait for at least some input to be available
    await feedPromise;

    let readOffset = 0;
    let currentBuf: Buffer | null = null;
    let bufIdx = 0;

    const readCb = (bufSize: number): Buffer | null => {
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
          currentBuf = inputBuffers[bufIdx++];
          readOffset = 0;
          continue;
        }
        return null; // EOF
      }
    };

    const chunks: Buffer[] = [];
    let resolveChunk: (() => void) | null = null;
    let done = false;
    let metaRaw: string | null = null;

    const writeCb = (chunk: Buffer) => {
      chunks.push(chunk);
      if (resolveChunk) {
        resolveChunk();
        resolveChunk = null;
      }
    };

    const metaPromise = ffi.runScriptTransform(
      script, inputsJson, inputName, inputMimeType, inputCharset, readCb, writeCb
    ).then((raw) => {
      metaRaw = raw;
      done = true;
      if (resolveChunk) {
        resolveChunk();
        resolveChunk = null;
      }
    });

    while (true) {
      if (chunks.length > 0) {
        yield chunks.shift()!;
        continue;
      }
      if (done) break;
      await new Promise<void>((resolve) => { resolveChunk = resolve; });
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
