export interface ExecutionResult {
  success: boolean;
  result: string | null;
  error: string | null;
  binary: boolean;
  mimeType: string | null;
  charset: string | null;
  getBytes(): Buffer | null;
  getString(): string | null;
}

export interface StreamingResult {
  success: boolean;
  error: string | null;
  mimeType: string | null;
  charset: string | null;
  binary: boolean;
}

export interface InputValue {
  content: string | Buffer;
  mimeType: string;
  charset?: string;
  properties?: Record<string, string | number | boolean>;
}

export type InputEntry = InputValue | string | number | boolean | null | object;

export type Inputs = Record<string, InputEntry>;

export interface TransformOptions {
  inputName?: string;
  mimeType?: string;
  charset?: string;
  inputs?: Inputs;
}

export interface StreamOutput {
  stream: AsyncGenerator<Buffer, void, undefined>;
  metadata: Promise<StreamingResult>;
}
