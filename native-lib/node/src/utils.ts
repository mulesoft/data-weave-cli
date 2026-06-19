import { existsSync } from "node:fs";
import { join, dirname } from "node:path";
import type { InputEntry, Inputs } from "./types";

const ENV_NATIVE_LIB = "DATAWEAVE_NATIVE_LIB";

const LIB_EXTENSIONS = [".dylib", ".so", ".dll"];

function libNames(): string[] {
  return LIB_EXTENSIONS.map((ext) => `dwlib${ext}`);
}

export function findLibrary(): string {
  const envValue = (process.env[ENV_NATIVE_LIB] ?? "").trim();
  if (envValue && existsSync(envValue)) {
    return envValue;
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

export function buildInputsJson(inputs: Inputs): string {
  const normalized: Record<string, unknown> = {};
  for (const [key, val] of Object.entries(inputs)) {
    normalized[key] = normalizeInputValue(val);
  }
  return JSON.stringify(normalized);
}
