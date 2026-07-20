import type { ExecutionResult } from "./types";

/** Base error for all failures raised by the DataWeave binding. */
export class DataWeaveError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "DataWeaveError";
  }
}

/**
 * Raised when a script executes but reports a failure, when the caller opted in
 * via `raiseOnError`. Carries the full {@link ExecutionResult} for inspection.
 */
export class DataWeaveScriptError extends DataWeaveError {
  result: ExecutionResult;
  constructor(result: ExecutionResult) {
    super(result.error ?? "Script execution failed");
    this.name = "DataWeaveScriptError";
    this.result = result;
  }
}