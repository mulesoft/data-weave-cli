import { spawnSync } from "node:child_process";
import { existsSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, it } from "vitest";

const FIXTURE = join(__dirname, "fixtures", "resolver-reentrancy.cjs");
const DIST_ENTRY = join(__dirname, "..", "..", "dist", "index.js");
const ADDON_PATH = join(__dirname, "..", "..", "build", "Release", "dwlib_addon.node");
const FATAL_GRAAL_ERROR = /Fatal error|Must either be at a safepoint or in native mode/i;

function runFixture(mode: "facade" | "raw" | "raw-streaming" | "raw-transform") {
  const child = spawnSync(process.execPath, [FIXTURE, mode], {
    encoding: "utf-8",
    timeout: 30_000,
  });

  expect(child.error, child.error?.message).toBeUndefined();
  expect(child.signal, child.stderr).toBeNull();
  expect(child.status, child.stderr).toBe(0);
  expect(child.stderr).not.toMatch(FATAL_GRAAL_ERROR);

  return JSON.parse(child.stdout.trim()) as Record<string, unknown>;
}

describe("resolver callback reentrancy guard", () => {
  it("maps nested public DataWeave calls to DataWeaveError and preserves the outer run", () => {
    expect(existsSync(DIST_ENTRY), `built entry missing at ${DIST_ENTRY} - run \`npm run build:ts\``).toBe(true);

    expect(runFixture("facade")).toEqual({
      nestedErrorName: "DataWeaveError",
      outerResult: "42",
    });
  });

  it("exposes a stable raw-addon error code for nested native admission", () => {
    expect(existsSync(ADDON_PATH), `native addon missing at ${ADDON_PATH} - run \`npm run build:addon\``).toBe(true);

    expect(runFixture("raw")).toEqual({
      nestedErrorCode: "ERR_DATAWEAVE_CALLBACK_REENTRANCY",
      outerResult: "42",
    });
  });

  it("guards the streaming output callback and preserves the outer operation", () => {
    expect(runFixture("raw-streaming")).toEqual({
      nestedErrorCode: "ERR_DATAWEAVE_CALLBACK_REENTRANCY",
      outerSuccess: true,
      outerResult: [1, 2, 3],
    });
  });

  it("guards the transform output callback and preserves the outer operation", () => {
    expect(runFixture("raw-transform")).toEqual({
      nestedErrorCode: "ERR_DATAWEAVE_CALLBACK_REENTRANCY",
      outerSuccess: true,
      outerResult: [2, 4, 6],
    });
  });

  it("contains exceptions from hostile resolver diagnostic accessors", () => {
    const child = spawnSync(process.execPath, [FIXTURE, "hostile-diagnostic"], {
      encoding: "utf-8",
      timeout: 30_000,
      env: {
        ...process.env,
        DATAWEAVE_RESOLVER_DEBUG: "1",
      },
    });

    expect(child.error, child.error?.message).toBeUndefined();
    expect(child.signal, child.stderr).toBeNull();
    expect(child.status, child.stderr).toBe(0);
    expect(child.stderr).toContain("(Unable to extract exception details)");
    expect(child.stderr).not.toContain("resolver diagnostic getter exploded");
    expect(JSON.parse(child.stdout.trim())).toEqual({
      failedSuccess: false,
      failedHasError: true,
      messageGetterRan: true,
      healthySuccess: true,
      healthyResult: "42",
    });
  });

  it("fails closed when the original resolver callback exception cannot be cleared", () => {
    const child = spawnSync(
      process.execPath,
      [FIXTURE, "unclearable-original-exception"],
      {
        encoding: "utf-8",
        timeout: 30_000,
        env: { ...process.env, DATAWEAVE_TEST_HOOKS: "1" },
      }
    );

    expect(child.error, child.error?.message).toBeUndefined();
    expect(child.status === 0 && child.signal === null, child.stderr).toBe(false);
    expect(child.signal, child.stderr).not.toBe("SIGSEGV");
    expect(child.stderr).toContain("resolver callback reached");
    expect(child.stderr).toContain(
      "Failed to clear the original resolver callback exception"
    );
    expect(child.stderr).not.toContain("unexpected completion");
    expect(child.stderr).not.toContain("unexpected timeout");
  });
});
