import type { Reporter } from "vitest/reporters";
import type { TestCase, TestModule, Vitest } from "vitest/node";
import { accountTckResults, validateTckAccounting, type TckResult } from "./accounting";

const TCK_FILE = "/tests/tck/tck.test.ts";

function isTckCase(testCase: TestCase): boolean {
  return testCase.module.moduleId.replaceAll("\\", "/").endsWith(TCK_FILE);
}

function scenarioIdentifier(name: string): string {
  return name.split(/ \[(?:skip|xfail):/, 1)[0];
}

export function matchesTestNamePattern(name: string, pattern: RegExp | undefined): boolean {
  if (!pattern) return true;
  pattern.lastIndex = 0;
  return pattern.test(name);
}

export class TckAccountingReporter implements Reporter {
  private readonly selected = new Set<string>();
  private readonly results: TckResult[] = [];
  private testNamePattern?: RegExp;

  onInit(vitest: Vitest): void {
    this.testNamePattern = vitest.config.testNamePattern;
  }

  onTestRunStart(): void {
    this.selected.clear();
    this.results.length = 0;
  }

  onTestModuleCollected(testModule: TestModule): void {
    if (!testModule.moduleId.replaceAll("\\", "/").endsWith(TCK_FILE)) return;
    for (const testCase of testModule.children.allTests()) {
      const identifier = scenarioIdentifier(testCase.name);
      const isPolicySkip = testCase.options.mode === "skip" && testCase.name.includes(" [skip:");
      if (isPolicySkip && matchesTestNamePattern(testCase.fullName, this.testNamePattern)) {
        this.selected.add(identifier);
      }
    }
  }

  onTestCaseReady(testCase: TestCase): void {
    if (isTckCase(testCase) && matchesTestNamePattern(testCase.fullName, this.testNamePattern)) {
      this.selected.add(scenarioIdentifier(testCase.name));
    }
  }

  onTestCaseResult(testCase: TestCase): void {
    if (!isTckCase(testCase)) return;
    const identifier = scenarioIdentifier(testCase.name);
    if (!this.selected.has(identifier) && testCase.result().state === "skipped") return;
    const state = testCase.result().state;
    if (state === "pending") return;
    this.results.push({
      identifier,
      outcome: state === "passed" && testCase.name.includes(" [xfail:") ? "xfailed" : state,
    });
  }

  onTestRunEnd(): void {
    if (this.selected.size === 0) return;
    const summary = accountTckResults(this.selected, this.results);
    console.log(
      `TCK totals: selected=${summary.selected}, passed=${summary.passed}, failed=${summary.failed}, `
      + `skipped=${summary.skipped}, xfailed=${summary.xfailed}, `
      + `accounted=${summary.accounted}, unaccounted=${summary.unaccounted}`
    );
    const errors = validateTckAccounting(this.selected, this.results);
    if (errors.length > 0) {
      throw new Error(`Invalid TCK result accounting:\n${errors.join("\n")}`);
    }
  }
}

export default TckAccountingReporter;
