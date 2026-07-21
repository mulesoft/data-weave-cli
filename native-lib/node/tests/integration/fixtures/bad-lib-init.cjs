// Child-process fixture for the bad-library-path initialization test.
//
// Runs in a FRESH process (spawned by init-bad-path.test.ts) so that no prior
// good initialization has loaded dwlib — the only condition under which a
// bad-path initialize() reliably throws (the native runtime is loaded
// process-globally / ref-counted; see the NOTE in edge-cases.test.ts).
//
// Contract with the parent:
//   - Requires the built CommonJS entry at ../../../dist/index.js.
//   - On the expected DataWeaveError, prints "OK:<name>" and exits 0.
//   - On any other outcome (no throw, wrong error type), prints "FAIL:..." and
//     exits 1. A native crash would surface as a non-zero signal exit, which
//     the parent also treats as failure.
const path = require("node:path");

const { DataWeave, DataWeaveError } = require(path.join(__dirname, "..", "..", "..", "dist", "index.js"));

const BAD_PATH = process.argv[2] || "/no/such/dwlib-does-not-exist.dylib";

try {
  const dw = new DataWeave(BAD_PATH);
  dw.initialize();
  // Should be unreachable: a bad path in a fresh process must not initialize.
  console.log("FAIL:no-throw");
  process.exit(1);
} catch (e) {
  if (e instanceof DataWeaveError) {
    console.log("OK:" + e.name + ":" + String(e.message));
    process.exit(0);
  }
  console.log("FAIL:wrong-error:" + (e && e.constructor && e.constructor.name));
  process.exit(1);
}