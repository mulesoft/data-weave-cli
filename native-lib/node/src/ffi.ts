import { resolveAddonPath } from "./addon-path";
import type { ModuleResolver } from "./resolver";

interface NativeAddon {
  initialize(libPath: string): void;
  runScript(script: string, inputsJson: string): string;
  createEngine(): number;
  createEngineWithResolver(resolver: ModuleResolver): number;
  destroyEngine(handle: number): void;
  runScriptEngine(handle: number, script: string, inputsJson: string): string;
  runScriptStreamingEngine(
    handle: number,
    script: string,
    inputsJson: string,
    chunkCb: (chunk: Buffer) => void
  ): Promise<string>;
  runScriptTransformEngine(
    handle: number,
    script: string,
    inputsJson: string,
    inputName: string,
    inputMimeType: string,
    inputCharset: string | null,
    readCb: (bufSize: number) => Buffer | null,
    writeCb: (chunk: Buffer) => void
  ): Promise<string>;
  cleanup(): Promise<void>;
}

let addon: NativeAddon | null = null;

function getAddon(addonPath?: string): NativeAddon {
  if (!addon) {
    addon = require(addonPath ?? resolveAddonPath()) as NativeAddon;
  }
  return addon;
}

export function initialize(libPath: string, addonPath?: string): void {
  getAddon(addonPath).initialize(libPath);
}

export function runScript(script: string, inputsJson: string): string {
  return getAddon().runScript(script, inputsJson);
}

export function createEngine(): number {
  return getAddon().createEngine();
}

export function createEngineWithResolver(resolver: ModuleResolver): number {
  return getAddon().createEngineWithResolver(resolver);
}

export function destroyEngine(handle: number): void {
  getAddon().destroyEngine(handle);
}

export function runScriptEngine(handle: number, script: string, inputsJson: string): string {
  return getAddon().runScriptEngine(handle, script, inputsJson);
}

export function runScriptStreamingEngine(
  handle: number,
  script: string,
  inputsJson: string,
  chunkCb: (chunk: Buffer) => void
): Promise<string> {
  return getAddon().runScriptStreamingEngine(handle, script, inputsJson, chunkCb);
}

export function runScriptTransformEngine(
  handle: number,
  script: string,
  inputsJson: string,
  inputName: string,
  inputMimeType: string,
  inputCharset: string | null,
  readCb: (bufSize: number) => Buffer | null,
  writeCb: (chunk: Buffer) => void
): Promise<string> {
  return getAddon().runScriptTransformEngine(
    handle,
    script,
    inputsJson,
    inputName,
    inputMimeType,
    inputCharset,
    readCb,
    writeCb
  );
}

export function cleanup(): Promise<void> {
  return getAddon().cleanup();
}
