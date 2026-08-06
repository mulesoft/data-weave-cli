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
//   - Constructs dw1 with a resolver for 'first.dwl' and dw2 with a
//     *different* resolver for 'second.dwl', then initializes both.
//   - Runs a script through dw1 that imports 'first.dwl' to force-install
//     dw1's resolver on the singleton engine (must succeed).
//   - Runs a script through dw2 that imports 'second.dwl'. Per the singleton
//     semantics, dw2's resolver is never installed, so this import must fail.
//   - Runs a THIRD script, through dw2, that imports 'first.dwl' again and
//     asserts it still returns "Hello World". This is the check that actually
//     distinguishes "the first resolver remains active" from "custom
//     resolution broke entirely after the first call" — the second script
//     alone would fail identically under either explanation.
//   - Always calls cleanup() on both instances via try/finally, so teardown
//     is exercised even on failure, then exits naturally (no process.exit()).
//   - Prints "OK:first-resolver-wins" when all three expectations hold, or
//     "FAIL:<reason>" (with a non-zero exitCode) otherwise. A native crash
//     surfaces as a non-zero signal exit, which the parent also treats as
//     failure.
const path = require("node:path");

const { DataWeave, modulesFromMap } = require(path.join(__dirname, "..", "..", "..", "dist", "index.js"));

const dw1 = new DataWeave({
  resolveModule: modulesFromMap({
    "first.dwl": '%dw 2.0\nfun greet(n: String) = "Hello " ++ n',
  }),
});

const dw2 = new DataWeave({
  resolveModule: modulesFromMap({
    "second.dwl": '%dw 2.0\nfun shout(n: String) = n ++ "!"',
  }),
});

let failure = null;

try {
  dw1.initialize();
  dw2.initialize();

  const firstResult = dw1.run(`
    %dw 2.0
    import first
    output application/json
    ---
    first::greet("World")
  `);

  if (!firstResult.success) {
    failure = "first-resolver-did-not-resolve:" + firstResult.error;
  } else {
    const secondResult = dw2.run(`
      %dw 2.0
      import second
      output application/json
      ---
      second::shout("hi")
    `);

    if (secondResult.success) {
      failure = "second-resolver-unexpectedly-won";
    } else {
      // Prove the first resolver is still ACTIVE on dw2 (not merely that
      // dw2's own resolver lost). A resolver that died entirely after the
      // first call would also make second.dwl fail above -- this second
      // check on dw2 is what actually distinguishes "first resolver wins"
      // from "custom resolution stopped working after the first run".
      const stillFirstResult = dw2.run(`
        %dw 2.0
        import first
        output application/json
        ---
        first::greet("World")
      `);

      if (!stillFirstResult.success || JSON.parse(stillFirstResult.getString()) !== "Hello World") {
        failure = "first-resolver-no-longer-active-on-dw2:" + (stillFirstResult.error || stillFirstResult.getString());
      }
    }
  }
} finally {
  dw1.cleanup();
  dw2.cleanup();
}

if (failure) {
  console.log("FAIL:" + failure);
  process.exitCode = 1;
} else {
  console.log("OK:first-resolver-wins");
}
