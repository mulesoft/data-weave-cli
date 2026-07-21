import type { ExecutionResult, StreamingResult } from "./types";

/** Normalizes a charset name for comparison: lowercased, non-alphanumerics stripped. */
function normalizeCharset(charset: string): string {
  return charset.toLowerCase().replace(/[^a-z0-9]/g, "");
}

/**
 * Decodes payload bytes to a string using a DataWeave/IANA charset name.
 *
 * The native runtime reports charsets using IANA-style names (e.g. `UTF-16`,
 * `US-ASCII`, `ISO-8859-1`) that Node's `Buffer.toString` does not accept —
 * passing them through directly throws `ERR_UNKNOWN_ENCODING`. This maps the
 * common cases to a valid Node {@link BufferEncoding} and handles UTF-16
 * byte-order (Node only decodes little-endian): a leading BOM, or a `UTF-16BE`
 * label, is honored by stripping the BOM and byte-swapping big-endian input.
 * Unrecognized charsets fall back to UTF-8 so decoding degrades gracefully
 * instead of throwing.
 *
 * @param bytes - The decoded payload bytes.
 * @param charset - The charset name reported by the runtime, or `null`.
 * @returns The decoded string.
 */
export function decodeBytes(bytes: Buffer, charset: string | null): string {
  if (!charset) return bytes.toString("utf-8");

  switch (normalizeCharset(charset)) {
    case "utf16":
    case "utf16le":
    case "utf16be":
    case "ucs2":
    case "unicode":
      return decodeUtf16(bytes, normalizeCharset(charset) === "utf16be");
    case "usascii":
    case "ascii":
      return bytes.toString("ascii");
    case "latin1":
    case "iso88591":
    case "cp1252":
    case "windows1252":
      return bytes.toString("latin1");
    case "base64":
      return bytes.toString("base64");
    case "hex":
      return bytes.toString("hex");
    case "utf8":
    default:
      return bytes.toString("utf-8");
  }
}

/**
 * Decodes UTF-16 bytes to a string, honoring a BOM if present (which overrides
 * `labelIsBE`) and byte-swapping big-endian input, since Node only has a
 * little-endian UTF-16 decoder.
 */
function decodeUtf16(bytes: Buffer, labelIsBE: boolean): string {
  let big = labelIsBE;
  let start = 0;
  if (bytes.length >= 2) {
    if (bytes[0] === 0xfe && bytes[1] === 0xff) { big = true; start = 2; }       // BE BOM
    else if (bytes[0] === 0xff && bytes[1] === 0xfe) { big = false; start = 2; } // LE BOM
  }
  let body = bytes.subarray(start);
  if (big) {
    body = Buffer.from(body); // copy so swap16 doesn't mutate the caller's bytes
    if (body.length % 2 === 0) body.swap16();
  }
  return body.toString("utf16le");
}

/**
 * Parses the JSON envelope returned by the native `run_script` FFI call into an
 * {@link ExecutionResult}.
 *
 * The native layer returns a JSON object of the shape
 * `{ success, result, error, binary, mimeType, charset }`, where `result` is a
 * base64-encoded payload. Any failure to obtain that envelope — an empty
 * string, malformed JSON, or `success: false` — is surfaced as an
 * unsuccessful result rather than a thrown error, so callers can branch on
 * `result.success` uniformly.
 *
 * @param raw - The raw JSON string produced by the native call.
 * @returns A successful result carrying the decoded payload, or an
 *   unsuccessful result whose `error` describes what went wrong.
 */
export function parseNativeResponse(raw: string): ExecutionResult {
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

/**
 * Builds an {@link ExecutionResult}, attaching the `getBytes` / `getString`
 * accessors that decode the base64 `result` payload on demand.
 *
 * `getBytes` returns the raw decoded bytes as a {@link Buffer}; `getString`
 * returns text, decoding binary payloads as-is and non-binary payloads using
 * `charset` (defaulting to UTF-8). Both accessors return `null` when the result
 * is unsuccessful or carries no payload.
 *
 * @param success - Whether the script executed successfully.
 * @param result - The base64-encoded payload, or `null` when there is none.
 * @param error - An error message when unsuccessful, otherwise `null`.
 * @param binary - Whether the payload is binary rather than text.
 * @param mimeType - The output MIME type, if reported by the runtime.
 * @param charset - The output charset used to decode text, if reported.
 * @returns The assembled result object with lazy decoding accessors.
 */
export function makeResult(
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
      return decodeBytes(bytes, this.charset);
    },
  };
}

/**
 * Parses the trailing metadata envelope emitted by the streaming FFI calls
 * (`run_script_streaming` / `run_script_transform`) into a
 * {@link StreamingResult}.
 *
 * Unlike {@link parseNativeResponse}, the payload itself is delivered
 * out-of-band through the chunk callbacks; this envelope carries only the
 * final status and content metadata (`mimeType`, `charset`, `binary`). An empty
 * string, malformed JSON, or `success: false` all yield an unsuccessful result.
 *
 * @param raw - The raw JSON metadata string produced once streaming completes.
 * @returns The streaming outcome and content metadata.
 */
export function parseStreamingResult(raw: string): StreamingResult {
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