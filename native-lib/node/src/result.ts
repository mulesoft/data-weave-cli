import type { ExecutionResult, StreamingResult } from "./types";

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
      return bytes.toString((this.charset as BufferEncoding) ?? "utf-8");
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