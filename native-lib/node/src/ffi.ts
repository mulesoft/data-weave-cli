import { join } from "node:path";
import type { ModuleResolver } from "./resolver";

interface NativeAddon {
  initialize(libPath: string): void;
  runScript(script: string, inputsJson: string): string;
  runScriptStreaming(script: string, inputsJson: string, chunkCb: (chunk: Buffer) => void): Promise<string>;
  runScriptTransform(
    script: string,
    inputsJson: string,
    inputName: string,
    inputMimeType: string,
    inputCharset: string | null,
    readCb: (bufSize: number) => Buffer | null,
    writeCb: (chunk: Buffer) => void
  ): Promise<string>;
  runWithResolver(
    script: string,
    inputsJson: string,
    mimeType: string,
    resolverCallback: ModuleResolver,
    isolate: null
  ): string;
  cleanup(): void;
}

let addon: NativeAddon | null = null;

function getAddon(): NativeAddon {
  if (!addon) {
    const addonPath = join(__dirname, "..", "build", "Release", "dwlib_addon.node");
    addon = require(addonPath) as NativeAddon;
  }
  return addon;
}

export function initialize(libPath: string): void {
  getAddon().initialize(libPath);
}

export function runScript(script: string, inputsJson: string): string {
  return getAddon().runScript(script, inputsJson);
}

export function runScriptStreaming(
  script: string,
  inputsJson: string,
  chunkCb: (chunk: Buffer) => void
): Promise<string> {
  return getAddon().runScriptStreaming(script, inputsJson, chunkCb);
}

export function runScriptTransform(
  script: string,
  inputsJson: string,
  inputName: string,
  inputMimeType: string,
  inputCharset: string | null,
  readCb: (bufSize: number) => Buffer | null,
  writeCb: (chunk: Buffer) => void
): Promise<string> {
  return getAddon().runScriptTransform(script, inputsJson, inputName, inputMimeType, inputCharset, readCb, writeCb);
}

export function runWithResolver(
  script: string,
  inputsJson: string,
  mimeType: string,
  resolverCallback: ModuleResolver
): string {
  return getAddon().runWithResolver(script, inputsJson, mimeType, resolverCallback, null);
}

export function cleanup(): void {
  getAddon().cleanup();
}
