export { DataWeave, run, runStreaming, runTransform, cleanup } from "./dataweave";
export { DataWeaveError, DataWeaveScriptError } from "./errors";

export type {
  ExecutionResult,
  StreamingResult,
  Inputs,
  InputValue,
  InputEntry,
  TransformOptions,
} from "./types";