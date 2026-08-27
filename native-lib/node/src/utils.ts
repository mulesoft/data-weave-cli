import { existsSync } from "node:fs";
import { join, dirname } from "node:path";
import type { InputEntry, Inputs } from "./types";

/** Environment variable holding an explicit absolute path to the `dwlib` shared library. */
const ENV_NATIVE_LIB = "DATAWEAVE_NATIVE_LIB";

/** Platform-specific shared-library extensions, in the order they are probed (macOS, Linux, Windows). */
const LIB_EXTENSIONS = [".dylib", ".so", ".dll"];

/** Returns the candidate `dwlib` file names for every supported platform (`dwlib.dylib`, `dwlib.so`, `dwlib.dll`). */
function libNames(): string[] {
  return LIB_EXTENSIONS.map((ext) => `dwlib${ext}`);
}

/**
 * Locates the DataWeave native shared library (`dwlib.*`) on disk.
 *
 * Resolution is attempted in priority order:
 * 1. The {@link ENV_NATIVE_LIB} environment variable, if it points at an existing file.
 * 2. Next to the resolved native addon, when provided.
 * 3. The packaged location — `<pkg>/native/dwlib.*` relative to this module.
 * 4. A dev-build fallback — walking up to 10 parent directories looking for
 *    `build/native/nativeCompile/dwlib.*`.
 * 5. The current working directory.
 *
 * @returns The absolute path to the located library.
 * @throws Error if no library can be found in any of the above locations.
 */
export function findLibrary(addonPath?: string): string {
  const envValue = (process.env[ENV_NATIVE_LIB] ?? "").trim();
  if (envValue && existsSync(envValue)) {
    return envValue;
  }

  if (addonPath) {
    const addonDir = dirname(addonPath);
    for (const name of libNames()) {
      const p = join(addonDir, name);
      if (existsSync(p)) return p;
    }
  }

  const thisDir = __dirname;

  // Packaged: <pkg>/dist/utils.js → <pkg>/native/dwlib.*
  const nativeDir = join(thisDir, "..", "native");
  for (const name of libNames()) {
    const p = join(nativeDir, name);
    if (existsSync(p)) return p;
  }

  // Dev fallback: walk up to find build/native/nativeCompile/dwlib.*
  let dir = thisDir;
  for (let i = 0; i < 10; i++) {
    const buildDir = join(dir, "build", "native", "nativeCompile");
    if (existsSync(buildDir)) {
      for (const name of libNames()) {
        const p = join(buildDir, name);
        if (existsSync(p)) return p;
      }
    }
    const parent = dirname(dir);
    if (parent === dir) break;
    dir = parent;
  }

  // CWD fallback
  for (const name of libNames()) {
    if (existsSync(name)) return join(process.cwd(), name);
  }

  throw new Error(
    `Could not find DataWeave native library (dwlib). ` +
      `Set ${ENV_NATIVE_LIB} to an absolute path or install a package that bundles the native library.`
  );
}

/**
 * Normalizes a single input value into the base64-encoded envelope the native
 * layer expects: `{ content, mimeType, charset, properties? }`.
 *
 * The shape is inferred from the value:
 * - `null` / `undefined` → the JSON literal `null` as `application/json`.
 * - An explicit {@link InputValue} object (has both `content` and `mimeType`) →
 *   its content is base64-encoded (Buffers directly, strings via `charset`),
 *   preserving any `charset` and `properties`.
 * - A string → `text/plain`.
 * - A number, boolean, or any other object/array → JSON-serialized as
 *   `application/json`, falling back to `String(value)` / `text/plain` if it
 *   cannot be serialized.
 *
 * @param value - The raw input entry to normalize.
 * @param mimeType - Optional MIME type override; when omitted a type is inferred
 *   from the value.
 * @returns The native-ready envelope with base64-encoded `content`.
 */
export function normalizeInputValue(value: InputEntry, mimeType?: string): Record<string, unknown> {
  if (value === null || value === undefined) {
    const content = Buffer.from("null", "utf-8").toString("base64");
    return { content, mimeType: mimeType ?? "application/json", charset: "utf-8" };
  }

  if (typeof value === "object" && !Array.isArray(value) && !Buffer.isBuffer(value)) {
    const obj = value as Record<string, unknown>;
    if ("content" in obj && "mimeType" in obj) {
      const rawContent = obj.content;
      const charset = (obj.charset as string) ?? "utf-8";
      let encodedContent: string;
      if (Buffer.isBuffer(rawContent)) {
        encodedContent = rawContent.toString("base64");
      } else {
        encodedContent = Buffer.from(String(rawContent), charset as BufferEncoding).toString("base64");
      }
      const normalized: Record<string, unknown> = {
        content: encodedContent,
        mimeType: obj.mimeType,
      };
      if (obj.charset) normalized.charset = obj.charset;
      if (obj.properties) normalized.properties = obj.properties;
      return normalized;
    }
  }

  let content: string;
  let defaultMime: string;

  if (typeof value === "string") {
    content = value;
    defaultMime = "text/plain";
  } else if (typeof value === "number" || typeof value === "boolean") {
    content = JSON.stringify(value);
    defaultMime = "application/json";
  } else {
    try {
      content = JSON.stringify(value);
      defaultMime = "application/json";
    } catch {
      content = String(value);
      defaultMime = "text/plain";
    }
  }

  const charset = "utf-8";
  const encodedContent = Buffer.from(content, charset).toString("base64");
  return { content: encodedContent, mimeType: mimeType ?? defaultMime, charset };
}

/**
 * Normalizes every entry in an {@link Inputs} map via {@link normalizeInputValue}
 * and serializes the result to the JSON string passed to the native FFI calls.
 *
 * @param inputs - The named inputs to make available to the script (e.g. `payload`).
 * @returns A JSON string mapping each input name to its base64-encoded envelope.
 */
export function buildInputsJson(inputs: Inputs): string {
  const normalized: Record<string, unknown> = {};
  for (const [key, val] of Object.entries(inputs)) {
    normalized[key] = normalizeInputValue(val);
  }
  return JSON.stringify(normalized);
}
