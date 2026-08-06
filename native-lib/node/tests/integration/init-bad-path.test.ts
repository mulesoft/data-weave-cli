// Asserts that DataWeave.initialize() with a bad/nonexistent dwlib path throws
// a DataWeaveError. This is only reliably observable as the FIRST native
// initialization in a process (the runtime is loaded process-globally and
// ref-counted — see the NOTE in edge-cases.test.ts), so it is verified in a
// dedicated child process rather than in-lane, making it order- and
// pool-configuration-independent. (W-23517830, follow-up from W-23517404.)
import { describe, it, expect } from "vitest";
import { execFileSync } from "node:child_process";
import { join } from "node:path";
import { existsSync } from "node:fs";

const FIXTURE = join(__dirname, "fixtures", "bad-lib-init.cjs");
const DIST_ENTRY = join(__dirname, "..", "..", "dist", "index.js");

describe("bad library path initialization (isolated process)", () => {
  it("initialize() with a nonexistent library path throws a DataWeaveError", () => {
    // The fixture requires the built dist/ entry; nodeTest runs tsc first, but
    // guard so a source-only run fails with a clear message instead of a
    // confusing child error.
    expect(existsSync(DIST_ENTRY), `built entry missing at ${DIST_ENTRY} — run \`npm run build:ts\``).toBe(true);

    // execFileSync throws on a non-zero exit, so a "no throw" / wrong-error /
    // native-crash outcome in the child fails this test. A timeout is also
    // required: execFileSync blocks synchronously with no way for Vitest to
    // interrupt it, so a native deadlock in the child would otherwise hang
    // the whole suite instead of failing this one test.
    const stdout = execFileSync(process.execPath, [FIXTURE, "/no/such/dwlib-xyz.dylib"], {
      encoding: "utf-8",
      timeout: 30_000,
    });

    expect(stdout).toContain("OK:DataWeaveError");
    expect(stdout).toContain("Failed to");
  });
});