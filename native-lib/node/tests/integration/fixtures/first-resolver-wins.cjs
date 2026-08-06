// Child-process fixture for the first-resolver-wins regression test.
//
// Runs in a FRESH process (spawned by first-resolver-wins.test.ts) so the
// process-wide ScriptRuntime singleton in the native layer starts with no
// resolver installed (see ScriptRuntime.setResolver(): once any DataWeave
// instance's resolver is installed, every later instance's resolver is
// silently ignored — a warning is logged and the first resolver keeps being
// used). That behavior is only observable on the FIRST resolver installation
// of a process, so this fixture -- not an in-lane vitest test -- is the only
// reliable way to exercise it.
//
// Contract with the parent:
//   - Requires the built CommonJS entry at ../../../dist/index.js.
//   - Constructs dw1 with a resolver for 'first.dwl', initializes it, and
//     runs a script that imports 'first.dwl' to force-install dw1's resolver
//     on the singleton engine.
//   - Constructs dw2 with a *different* resolver for 'second.dwl', and runs a
//     script that imports 'second.dwl'. Per the singleton semantics, dw2's
//     resolver is never installed, so this import must fail to resolve.
//   - Prints "OK:first-resolver-wins" and exits 0 when both expectations hold
//     (dw1's import succeeds, dw2's import fails). Prints "FAIL:<reason>" and
//     exits 1 otherwise. A native crash surfaces as a non-zero signal exit,
//     which the parent also treats as failure.
const path = require("node:path");

const { DataWeave, modulesFromMap } = require(path.join(__dirname, "..", "..", "..", "dist", "index.js"));

const dw1 = new DataWeave({
  resolveModule: modulesFromMap({
    "first.dwl": '%dw 2.0\nfun greet(n: String) = "Hello " ++ n',
  }),
});
dw1.initialize();

const firstResult = dw1.run(`
  %dw 2.0
  import first
  output application/json
  ---
  first::greet("World")
`);

if (!firstResult.success) {
  console.log("FAIL:first-resolver-did-not-resolve:" + firstResult.error);
  process.exit(1);
}

const dw2 = new DataWeave({
  resolveModule: modulesFromMap({
    "second.dwl": '%dw 2.0\nfun shout(n: String) = n ++ "!"',
  }),
});
dw2.initialize();

const secondResult = dw2.run(`
  %dw 2.0
  import second
  output application/json
  ---
  second::shout("hi")
`);

if (secondResult.success) {
  console.log("FAIL:second-resolver-unexpectedly-won");
  process.exit(1);
}

console.log("OK:first-resolver-wins");
process.exit(0);
