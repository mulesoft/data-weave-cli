// DataWeave conformance harness: replays the runtime's own TCK corpus against
// the dataweave-native binding, in-process via run().
//
// The corpus is staged by Gradle (`stageTckSuites` → tests/tck/suites/<suite>/)
// and is gitignored. When it is absent (e.g. a source-only run without the
// Gradle download), this file registers no tests and the tck lane passes via
// vitest's passWithNoTests — the corpus is required only where it is staged
// (locally on demand, and master CI).
import { describe, it, expect } from "vitest";
import { readFileSync, readdirSync, existsSync, statSync } from "node:fs";
import { join } from "node:path";
import { DataWeave, modulesFromDirectory } from "../../src/index";
import { hasAdjacentDwlModule, parseCase, MAIN_TRANSFORM, type TckScenario } from "./case-loader";
import { compareOutput } from "./compare";
import {
  ACCEPTED_BASELINE_MISMATCHES,
  EXPECTED_EXECUTION_FAILURES,
  IGNORED_CASES,
  REENABLED_CASES,
  STRUCTURAL_MODULE_CASES,
  isIgnored,
  isExpectedExecutionFailure,
  isExpectedExecutionFailureCase,
  ignoreReason,
  validateIgnorePolicy,
  validateInventoryPolicy,
  validateReconciledPolicy,
  validateStructuralModulePolicy,
} from "./ignore-list";

const SUITES_DIR = join(__dirname, "suites");
const FIXTURES_DIR = join(__dirname, "fixtures");
const REQUIRE_CORPUS = process.env.DATAWEAVE_TCK_REQUIRE_CORPUS === "1";

/** A discovered case: its directory and the scenarios parsed from it. */
interface DiscoveredCase {
  caseIdentifier: string;
  dir: string;
  scenarios: TckScenario[];
}

function listDirs(parent: string): string[] {
  return readdirSync(parent).filter((n) => {
    try {
      return statSync(join(parent, n)).isDirectory();
    } catch {
      return false;
    }
  });
}

/** Walks every staged suite and returns the runnable cases (skips logged by the caller). */
function discoverCases(): {
  cases: DiscoveredCase[];
  skipped: number;
  structuralModuleCases: Set<string>;
} {
  const cases: DiscoveredCase[] = [];
  const structuralModuleCases = new Set<string>();
  let skipped = 0;
  for (const suite of listDirs(SUITES_DIR)) {
    const suiteDir = join(SUITES_DIR, suite);
    for (const caseName of listDirs(suiteDir)) {
      const dir = join(suiteDir, caseName);
      const fileNames = readdirSync(dir);
      const caseIdentifier = `${suite}/${caseName}`;
      const parsed = parseCase(suite, caseName, fileNames);
      if (parsed.kind === "skipped") {
        skipped++;
        if (hasAdjacentDwlModule(fileNames)) structuralModuleCases.add(caseIdentifier);
        continue;
      }
      cases.push({ caseIdentifier, dir, scenarios: parsed.scenarios });
    }
  }
  return { cases, skipped, structuralModuleCases };
}

/**
 * Registers a stand-in for the TCK suite when the corpus is missing/empty.
 * In the dedicated CI job (DATAWEAVE_TCK_REQUIRE_CORPUS=1) this must be loud —
 * a silent skip there would let the conformance lane go green with zero
 * cases. Local dev without the flag keeps the quiet skip.
 */
function registerMissingCorpus(reason: string) {
  if (REQUIRE_CORPUS) {
    describe("TCK conformance", () => {
      it("TCK corpus must be staged", () => {
        throw new Error(`TCK corpus ${reason} but DATAWEAVE_TCK_REQUIRE_CORPUS=1 — stage it with stageTckSuites`);
      });
    });
  } else {
    describe.skip(`TCK conformance (corpus ${reason} — run stageTckSuites)`, () => {
      it("skipped", () => {});
    });
  }
}

if (!existsSync(SUITES_DIR)) {
  // Corpus not staged — nothing to run in this lane. `npm run test:tck` on a
  // checkout without the Gradle download is a no-op (passWithNoTests), unless
  // the dedicated CI job opted into DATAWEAVE_TCK_REQUIRE_CORPUS=1.
  registerMissingCorpus("not staged");
} else {
  const { cases, skipped, structuralModuleCases } = discoverCases();
  if (cases.length === 0) {
    // Corpus directory exists but discovery found nothing runnable — same
    // silent-green risk as the missing-directory case above.
    registerMissingCorpus("empty");
  } else {
    const runnableCases = new Set(cases.map((item) => item.caseIdentifier));
    const runnableScenarios = new Set(cases.flatMap((item) => item.scenarios.map((scenario) => scenario.name)));
    const policyErrors = [
      ...validateInventoryPolicy(cases.length, skipped),
      ...validateIgnorePolicy(IGNORED_CASES, runnableCases),
      ...validateReconciledPolicy(
        IGNORED_CASES,
        ACCEPTED_BASELINE_MISMATCHES,
        REENABLED_CASES,
        runnableScenarios,
        EXPECTED_EXECUTION_FAILURES,
      ),
      ...validateStructuralModulePolicy(STRUCTURAL_MODULE_CASES, structuralModuleCases),
    ];
    if (policyErrors.length > 0) {
      throw new Error(`Invalid TCK policy:\n${policyErrors.join("\n")}`);
    }

    // One shared runtime for the whole lane. Modules imported by a handful of
    // TCK cases (org::mule::weave::v2::libs::lib) live only in the private
    // data-weave runtime repo's test resources, not in any published
    // artifact/TCK zip — resolve them from a committed fixture instead.
    const dw = new DataWeave({ resolveModule: modulesFromDirectory(FIXTURES_DIR) });

    describe("TCK conformance", () => {
      // eslint-disable-next-line no-console
      console.log(
        `TCK: ${cases.length} runnable cases, ${skipped} structurally skipped, `
        + `${structuralModuleCases.size} structural module cases, ${Object.keys(IGNORED_CASES).length} exclusions, `
        + `${Object.keys(ACCEPTED_BASELINE_MISMATCHES).length} expected output-mismatch failures, `
        + `${Object.keys(EXPECTED_EXECUTION_FAILURES).length} expected execution failures`
      );
      dw.initialize();

      for (const c of cases) {
        // Cases with an expected execution failure must run — the harness
        // asserts result.success === false plus a stable error discriminator
        // for them, so they are never skipped even though they're also
        // recorded in the legacy ignore registry for suite-routing purposes.
        const ignored = isIgnored(c.caseIdentifier) && !isExpectedExecutionFailureCase(c.caseIdentifier);
        for (const scenario of c.scenarios) {
          const expectedFailure = ACCEPTED_BASELINE_MISMATCHES[scenario.name];
          const execFail = isExpectedExecutionFailure(scenario.name);
          const testFn = ignored ? it.skip : it;
          const label = ignored
            ? `${scenario.name} [skip: ${ignoreReason(c.caseIdentifier)}]`
            : execFail
              // Reuse the "[xfail:" marker (not "[exec-xfail:") so the TCK
              // accounting reporter's `scenarioIdentifier`/xfailed detection
              // (tests/tck/reporter.ts), which only recognizes the literal
              // "skip:"/"xfail:" prefixes, still classifies these correctly.
              ? `${scenario.name} [xfail: execution failure: ${execFail.errorMatch}]`
              : expectedFailure
                ? `${scenario.name} [xfail: ${expectedFailure}]`
                : scenario.name;
          testFn(label, () => {
            const script = readFileSync(join(c.dir, MAIN_TRANSFORM), "utf-8");

            const inputs = Object.fromEntries(
              scenario.inputs.map((i) => [
                i.name,
                { content: readFileSync(join(c.dir, i.fileName)), mimeType: i.mimeType },
              ])
            );

            const result = dw.run(script, inputs);

            if (execFail) {
              expect(
                result.success,
                `${scenario.name}: expected execution failure but it succeeded — remove it from EXPECTED_EXECUTION_FAILURES`
              ).toBe(false);
              expect(
                result.error ?? "",
                `${scenario.name}: execution failed but error changed — update the errorMatch discriminator`
              ).toContain(execFail.errorMatch);
              return;
            }

            expect(result.success, `script failed: ${result.error}`).toBe(true);

            const actual = result.getBytes()!;
            const expected = readFileSync(join(c.dir, scenario.outputFileName));
            const encodingFile = join(c.dir, "encoding");
            const charset = existsSync(encodingFile)
              ? readFileSync(encodingFile, "utf-8").trim()
              : null;
            const cmp = compareOutput(scenario.outputExtension, actual, expected, charset);
            if (expectedFailure) {
              expect(
                cmp.match,
                `expected baseline mismatch for ${scenario.name} ([xfail: ${expectedFailure}]) but output matched — remove it from ACCEPTED_BASELINE_MISMATCHES`
              ).toBe(false);
            } else {
              expect(cmp.match, cmp.detail).toBe(true);
            }
          });
        }
      }
    });
  }
}
