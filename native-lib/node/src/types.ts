/**
 * The outcome of a non-streaming script execution.
 *
 * The payload is carried in `result` as a base64-encoded string; use the
 * {@link ExecutionResult.getBytes} / {@link ExecutionResult.getString}
 * accessors to decode it rather than reading `result` directly.
 */
export interface ExecutionResult {
  /** Whether the script executed successfully. */
  success: boolean;
  /** The base64-encoded output payload, or `null` when there is none. */
  result: string | null;
  /** The error message when `success` is `false`, otherwise `null`. */
  error: string | null;
  /** Whether the payload is binary rather than text. */
  binary: boolean;
  /** The output MIME type, if reported by the runtime. */
  mimeType: string | null;
  /** The output charset used to decode text, if reported by the runtime. */
  charset: string | null;
  /** Decodes and returns the payload as raw bytes, or `null` if unsuccessful/empty. */
  getBytes(): Buffer | null;
  /** Decodes and returns the payload as a string (using `charset`, default UTF-8), or `null` if unsuccessful/empty. */
  getString(): string | null;
}

/**
 * The terminal metadata of a streaming execution. The output bytes are
 * delivered separately via the stream's chunk callbacks; this object carries
 * only the final status and content metadata.
 */
export interface StreamingResult {
  /** Whether the stream completed successfully. */
  success: boolean;
  /** The error message when `success` is `false`, otherwise `null`. */
  error: string | null;
  /** The output MIME type, if reported by the runtime. */
  mimeType: string | null;
  /** The output charset, if reported by the runtime. */
  charset: string | null;
  /** Whether the streamed payload is binary rather than text. */
  binary: boolean;
}

/**
 * An explicit input value with its content and format metadata. Use this shape
 * (rather than a bare primitive) when you need to control the MIME type,
 * charset, or reader properties for an input.
 */
export interface InputValue {
  /** The input content — a string, or a {@link Buffer} for binary data. */
  content: string | Buffer;
  /** The MIME type of the content (e.g. `application/json`, `text/csv`). */
  mimeType: string;
  /** The charset used to encode string content; defaults to `utf-8`. */
  charset?: string;
  /** Optional reader/format properties forwarded to the DataWeave data format. */
  properties?: Record<string, string | number | boolean>;
}

/**
 * A single named input. Either an explicit {@link InputValue}, or a plain value
 * (string, number, boolean, `null`, or object/array) whose MIME type is
 * inferred during normalization.
 */
export type InputEntry = InputValue | string | number | boolean | null | object;

/** A map of input names (e.g. `payload`) to their values, made available to the script. */
export type Inputs = Record<string, InputEntry>;

/** Options controlling how the primary streaming input of `runTransform` is interpreted. */
export interface TransformOptions {
  /** The name the primary input is bound to in the script; defaults to `payload`. */
  inputName?: string;
  /** The MIME type of the primary input; defaults to `application/json`. */
  mimeType?: string;
  /** The charset of the primary input, if it is text. */
  charset?: string;
  /** Additional named inputs made available alongside the primary streaming input. */
  inputs?: Inputs;
}
