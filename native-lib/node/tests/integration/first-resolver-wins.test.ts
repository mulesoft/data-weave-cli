// Verifies the process-wide "first resolver wins" behavior documented in
// docs/external-modules.md#multiple-resolvers-in-one-process and
// ScriptRuntime.setResolver(): once a DataWeave instance's resolver is
// installed on the native engine singleton, a second instance constructed
// with a *different* resolver in the same process never has its resolver
// installed. That's only observable when the second instance's resolver is
// the second one ever installed for the whole process, so — like
// init-bad-path.test.ts — this runs in a dedicated child process rather than
// in-lane, making it order- and pool-configuration-independent.
import { describe, it, expect } from "vitest";
import { execFileSync } from "node:child_process";
import { join } from "node:path";
import { existsSync } from "node:fs";

const FIXTURE = join(__dirname, "fixtures", "first-resolver-wins.cjs");
const DIST_ENTRY = join(__dirname, "..", "..", "dist", "index.js");

describe("first-resolver-wins (isolated process)", () => {
  it("a second DataWeave instance's resolver is silently ignored in favor of the first", () => {
    expect(existsSync(DIST_ENTRY), `built entry missing at ${DIST_ENTRY} — run \`npm run build:ts\``).toBe(true);

    // execFileSync throws on a non-zero exit, so a "wrong resolver won" /
    // native-crash outcome in the child fails this test.
    const stdout = execFileSync(process.execPath, [FIXTURE], {
      encoding: "utf-8",
    });

    expect(stdout).toContain("OK:first-resolver-wins");
  });
});
