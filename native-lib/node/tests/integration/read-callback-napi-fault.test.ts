import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { spawnSync } from "node:child_process";
import { join } from "node:path";
import { findLibrary } from "../../src/utils";

interface NativeStreamingOperation {
  readonly completion: Promise<string>;
  acknowledge(sequence: bigint, bytes: number): void;
  close(): void;
}

type ReadCallbackFault =
  | "create-size"
  | "get-global"
  | "is-buffer"
  | "get-buffer-info"
  | "diagnostic-generic";

interface TestAddon {
  initialize(libPath: string): void;
  createEngine(): number;
  destroyEngine(handle: number): void;
  runScriptEngine(handle: number, script: string, inputsJson: string): string;
  runScriptTransformEngine(
    handle: number,
    script: string,
    inputsJson: string,
    inputName: string,
    inputMimeType: string,
    inputCharset: string | null,
    readCb: (bufSize: number) => Buffer | null,
    writeCb: (chunk: Buffer, sequence: bigint) => void
  ): NativeStreamingOperation;
  cleanup(): Promise<void>;
  __test_failNextReadCallback(stage: ReadCallbackFault): void;
  __test_failNextReadExceptionClear(): void;
}

const addon = require("../../build/Release/dwlib_addon.node") as TestAddon;
const SCRIPT = "output application/json deferred=true\n---\npayload";
const ADDON_PATH = join(__dirname, "..", "..", "build", "Release", "dwlib_addon.node");
const SECRET_MARKER = "tenant-secret-read-callback-7f3c";
const SENSITIVE_DIAGNOSTIC_FIXTURE = String.raw`
const addon = require(process.argv[1]);
const libPath = process.argv[2];
const marker = process.argv[3];
const script = "output application/json deferred=true\n---\npayload";

async function main() {
  addon.initialize(libPath);
  const handle = addon.createEngine();
  try {
    let readCallbackCalls = 0;
    let operation;
    operation = addon.runScriptTransformEngine(
      handle, script, "{}", "payload", "application/json", "UTF-8",
      () => {
        readCallbackCalls++;
        throw new Error("sensitive input: " + marker);
      },
      (chunk, sequence) => operation.acknowledge(sequence, chunk.length)
    );
    const metadata = JSON.parse(await operation.completion);
    operation.close();
    process.stdout.write(JSON.stringify({
      success: metadata.success,
      hasError: Boolean(metadata.error),
      readCallbackCalls,
    }));
  } finally {
    addon.destroyEngine(handle);
    await addon.cleanup();
  }
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
`;
const THROWING_DIAGNOSTIC_FIXTURE = String.raw`
const addon = require(process.argv[1]);
const libPath = process.argv[2];
const script = "output application/json deferred=true\n---\npayload";

function start(handle, readCb) {
  const output = [];
  let operation;
  operation = addon.runScriptTransformEngine(
    handle, script, "{}", "payload", "application/json", "UTF-8", readCb,
    (chunk, sequence) => {
      output.push(chunk);
      operation.acknowledge(sequence, chunk.length);
    }
  );
  return { operation, output };
}

async function main() {
  addon.initialize(libPath);
  const handle = addon.createEngine();
  try {
    let readCallbackCalls = 0;
    let messageGetterCalls = 0;
    const thrown = new Proxy({}, {
      get(_target, property) {
        if (property === "message") {
          messageGetterCalls++;
          throw new Error("diagnostic message getter exploded");
        }
        return undefined;
      },
    });
    const failed = start(handle, () => {
      readCallbackCalls++;
      throw thrown;
    });
    const failedMetadata = JSON.parse(await failed.operation.completion);
    failed.operation.close();

    const healthy = JSON.parse(addon.runScriptEngine(
      handle, "output application/json --- 6 * 7", "{}"
    ));

    process.stdout.write(JSON.stringify({
      failedSuccess: failedMetadata.success,
      failedHasError: Boolean(failedMetadata.error),
      failedError: failedMetadata.error,
      readCallbackCalls,
      messageGetterCalls,
      healthySuccess: healthy.success,
      healthyResult: healthy.result,
    }));
  } finally {
    addon.destroyEngine(handle);
    await addon.cleanup();
  }
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
`;
const UNCLEARABLE_ORIGINAL_EXCEPTION_FIXTURE = String.raw`
const addon = require(process.argv[1]);
const libPath = process.argv[2];
const script = "output application/json deferred=true\n---\npayload";

addon.initialize(libPath);
const handle = addon.createEngine();
addon.__test_failNextReadExceptionClear();
let operation;
operation = addon.runScriptTransformEngine(
  handle, script, "{}", "payload", "application/json", "UTF-8",
  () => { throw new Error("original read callback exception"); },
  (chunk, sequence) => operation.acknowledge(sequence, chunk.length)
);
operation.completion.then(() => {
  process.stderr.write("unexpected completion\n");
  process.exitCode = 90;
});
setTimeout(() => {
  process.stderr.write("unexpected timeout\n");
  process.exitCode = 91;
}, 5_000).unref();
`;
const GENERIC_DIAGNOSTIC_STATUS_FIXTURE = String.raw`
const addon = require(process.argv[1]);
const libPath = process.argv[2];
const script = "output application/json deferred=true\n---\npayload";

async function main() {
  addon.initialize(libPath);
  const handle = addon.createEngine();
  try {
    const thrown = new Proxy({}, {
      get(_target, property) {
        if (property === "message") throw new Error("generic diagnostic status getter exploded");
        return undefined;
      },
    });
    addon.__test_failNextReadCallback("diagnostic-generic");
    let operation;
    operation = addon.runScriptTransformEngine(
      handle, script, "{}", "payload", "application/json", "UTF-8",
      () => { throw thrown; },
      (chunk, sequence) => operation.acknowledge(sequence, chunk.length)
    );
    const metadata = JSON.parse(await operation.completion);
    operation.close();
    process.stdout.write(JSON.stringify({ success: metadata.success, hasError: Boolean(metadata.error) }));
  } finally {
    addon.destroyEngine(handle);
    await addon.cleanup();
  }
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
`;

function runTransform(handle: number): {
  operation: NativeStreamingOperation;
  output: Buffer[];
} {
  const input = Buffer.from("[1, 2, 3]");
  const output: Buffer[] = [];
  let offset = 0;
  let operation!: NativeStreamingOperation;
  operation = addon.runScriptTransformEngine(
    handle,
    SCRIPT,
    "{}",
    "payload",
    "application/json",
    "UTF-8",
    (bufSize) => {
      if (offset >= input.length) return null;
      const chunk = input.subarray(offset, Math.min(offset + bufSize, input.length));
      offset += chunk.length;
      return chunk;
    },
    (chunk, sequence) => {
      output.push(chunk);
      operation.acknowledge(sequence, chunk.length);
    }
  );
  return { operation, output };
}

beforeAll(() => addon.initialize(findLibrary()));
afterAll(async () => addon.cleanup());

describe.sequential("transform read callback N-API failures", () => {
  it("suppresses read callback exception details unless explicitly enabled", () => {
    const runFixture = (debug: boolean) => {
      const env = { ...process.env };
      delete env.DATAWEAVE_READ_CALLBACK_DEBUG;
      if (debug) env.DATAWEAVE_READ_CALLBACK_DEBUG = "1";
      return spawnSync(
        process.execPath,
        [
          "--force-node-api-uncaught-exceptions-policy",
          "-e",
          SENSITIVE_DIAGNOSTIC_FIXTURE,
          ADDON_PATH,
          findLibrary(),
          SECRET_MARKER,
        ],
        { encoding: "utf-8", timeout: 30_000, env }
      );
    };

    const suppressed = runFixture(false);
    expect(suppressed.error, suppressed.error?.message).toBeUndefined();
    expect(suppressed.signal, suppressed.stderr).toBeNull();
    expect(suppressed.status, suppressed.stderr).toBe(0);
    expect(suppressed.stderr).toContain(
      "Read callback threw an exception (details suppressed; set " +
      "DATAWEAVE_READ_CALLBACK_DEBUG=1"
    );
    expect(suppressed.stderr).not.toContain(SECRET_MARKER);
    expect(suppressed.stderr).not.toContain("sensitive input");
    expect(suppressed.stderr).not.toContain("Stack:");
    expect(JSON.parse(suppressed.stdout)).toEqual({
      success: false,
      hasError: true,
      readCallbackCalls: 1,
    });

    const detailed = runFixture(true);
    expect(detailed.error, detailed.error?.message).toBeUndefined();
    expect(detailed.signal, detailed.stderr).toBeNull();
    expect(detailed.status, detailed.stderr).toBe(0);
    expect(detailed.stderr).toContain("Read callback threw exception:");
    expect(detailed.stderr).toContain(SECRET_MARKER);
    expect(detailed.stderr).toContain("Stack:");
    expect(JSON.parse(detailed.stdout)).toEqual({
      success: false,
      hasError: true,
      readCallbackCalls: 1,
    });
  });

  it("contains exceptions thrown while reading callback diagnostics", () => {
    const child = spawnSync(
      process.execPath,
      [
        "--force-node-api-uncaught-exceptions-policy",
        "-e",
        THROWING_DIAGNOSTIC_FIXTURE,
        ADDON_PATH,
        findLibrary(),
      ],
      {
        encoding: "utf-8",
        timeout: 30_000,
        env: { ...process.env, DATAWEAVE_READ_CALLBACK_DEBUG: "1" },
      }
    );

    expect(child.error, child.error?.message).toBeUndefined();
    expect(child.signal, child.stderr).toBeNull();
    expect(child.status, child.stderr).toBe(0);
    expect(child.stderr).toContain("(Unable to extract exception details)");
    expect(JSON.parse(child.stdout)).toEqual({
      failedSuccess: false,
      failedHasError: true,
      failedError: "Input read callback signalled an error (returned -1)",
      readCallbackCalls: 1,
      messageGetterCalls: 1,
      healthySuccess: true,
      healthyResult: "NDI=",
    });
  });

  it("fails closed when the original read callback exception cannot be cleared", () => {
    const child = spawnSync(
      process.execPath,
      [
        "--force-node-api-uncaught-exceptions-policy",
        "-e",
        UNCLEARABLE_ORIGINAL_EXCEPTION_FIXTURE,
        ADDON_PATH,
        findLibrary(),
      ],
      {
        encoding: "utf-8",
        timeout: 30_000,
        env: { ...process.env, DATAWEAVE_TEST_HOOKS: "1" },
      }
    );

    expect(child.error, child.error?.message).toBeUndefined();
    expect(child.status === 0 && child.signal === null, child.stderr).toBe(false);
    expect(child.signal, child.stderr).not.toBe("SIGSEGV");
    expect(child.stderr).toContain(
      "Failed to clear the original read callback exception"
    );
    expect(child.stderr).not.toContain("unexpected completion");
    expect(child.stderr).not.toContain("unexpected timeout");
  });

  it("clears a pending diagnostic exception reported with a generic status", () => {
    const child = spawnSync(
      process.execPath,
      [
        "--force-node-api-uncaught-exceptions-policy",
        "-e",
        GENERIC_DIAGNOSTIC_STATUS_FIXTURE,
        ADDON_PATH,
        findLibrary(),
      ],
      {
        encoding: "utf-8",
        timeout: 30_000,
        env: {
          ...process.env,
          DATAWEAVE_READ_CALLBACK_DEBUG: "1",
          DATAWEAVE_TEST_HOOKS: "1",
        },
      }
    );

    expect(child.error, child.error?.message).toBeUndefined();
    expect(child.signal, child.stderr).toBeNull();
    expect(child.status, child.stderr).toBe(0);
    expect(child.stderr).toContain("(Unable to extract exception details)");
    expect(child.stderr).not.toContain("generic diagnostic status getter exploded");
    expect(JSON.parse(child.stdout)).toEqual({ success: false, hasError: true });
  });

  it.each([
    "create-size",
    "get-global",
    "is-buffer",
    "get-buffer-info",
  ] as const)("fails closed at %s and consumes the one-shot fault", async (fault) => {
    const handle = addon.createEngine();
    try {
      addon.__test_failNextReadCallback(fault);
      const failed = runTransform(handle);
      const failedMetadata = JSON.parse(await failed.operation.completion);
      failed.operation.close();

      expect(failedMetadata.success).toBe(false);
      expect(failedMetadata.error).toBeTruthy();

      const healthy = runTransform(handle);
      const healthyMetadata = JSON.parse(await healthy.operation.completion);
      healthy.operation.close();

      expect(healthyMetadata.success).toBe(true);
      expect(Buffer.concat(healthy.output).toString("utf-8")).toContain("1");
    } finally {
      addon.destroyEngine(handle);
    }
  });
});
