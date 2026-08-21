// DataWeave conformance harness: replays the runtime's own TCK corpus against
// the @dataweave/native binding, in-process via run().
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
  IGNORED_CASES,
  STRUCTURAL_MODULE_CASES,
  isIgnored,
  ignoreReason,
  validateIgnorePolicy,
  validateInventoryPolicy,
  validateStructuralModulePolicy,
} from "./ignore-list";

const SUITES_DIR = join(__dirname, "suites");
const FIXTURES_DIR = join(__dirname, "fixtures");

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

if (!existsSync(SUITES_DIR)) {
  // Corpus not staged — nothing to run in this lane. `npm run test:tck` on a
  // checkout without the Gradle download is a no-op (passWithNoTests).
  describe.skip("TCK conformance (corpus not staged — run stageTckSuites)", () => {
    it("skipped", () => {});
  });
} else {
  const { cases, skipped, structuralModuleCases } = discoverCases();
  const runnableCases = new Set(cases.map((item) => item.caseIdentifier));
  const policyErrors = [
    ...validateInventoryPolicy(cases.length, skipped),
    ...validateIgnorePolicy(IGNORED_CASES, runnableCases),
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
      + `${structuralModuleCases.size} structural module cases, ${Object.keys(IGNORED_CASES).length} exclusions`
    );
    dw.initialize();

    for (const c of cases) {
      const ignored = isIgnored(c.caseIdentifier);
      for (const scenario of c.scenarios) {
        const testFn = ignored ? it.skip : it;
        const label = ignored ? `${scenario.name} [skip: ${ignoreReason(c.caseIdentifier)}]` : scenario.name;
        testFn(label, () => {
          const script = readFileSync(join(c.dir, MAIN_TRANSFORM), "utf-8");

          const inputs = Object.fromEntries(
            scenario.inputs.map((i) => [
              i.name,
              { content: readFileSync(join(c.dir, i.fileName)), mimeType: i.mimeType },
            ])
          );

          const result = dw.run(script, inputs);
          expect(result.success, `script failed: ${result.error}`).toBe(true);

          const actual = result.getBytes()!;
          const expected = readFileSync(join(c.dir, scenario.outputFileName));
          const encodingFile = join(c.dir, "encoding");
          const charset = existsSync(encodingFile)
            ? readFileSync(encodingFile, "utf-8").trim()
            : null;
          const cmp = compareOutput(scenario.outputExtension, actual, expected, charset);
          expect(cmp.match, cmp.detail).toBe(true);
        });
      }
    }
  });
}
