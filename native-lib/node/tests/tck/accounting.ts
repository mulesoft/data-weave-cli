export type TckOutcome = "passed" | "failed" | "skipped";

export interface TckResult {
  identifier: string;
  outcome: TckOutcome;
}

export interface TckSummary {
  selected: number;
  passed: number;
  failed: number;
  skipped: number;
  accounted: number;
  unaccounted: number;
}

export function accountTckResults(
  selected: ReadonlySet<string>,
  results: readonly TckResult[]
): TckSummary {
  const passed = results.filter((result) => result.outcome === "passed").length;
  const failed = results.filter((result) => result.outcome === "failed").length;
  const skipped = results.filter((result) => result.outcome === "skipped").length;
  const accounted = passed + failed + skipped;
  return {
    selected: selected.size,
    passed,
    failed,
    skipped,
    accounted,
    unaccounted: selected.size - accounted,
  };
}

export function validateTckAccounting(
  selected: ReadonlySet<string>,
  results: readonly TckResult[]
): string[] {
  const counts = new Map<string, number>();
  for (const result of results) {
    counts.set(result.identifier, (counts.get(result.identifier) ?? 0) + 1);
  }

  const errors = [...counts]
    .filter(([, count]) => count > 1)
    .map(([identifier]) => `${identifier}: duplicate result`);
  errors.push(...[...selected]
    .filter((identifier) => !counts.has(identifier))
    .map((identifier) => `${identifier}: missing result`));
  errors.push(...[...counts.keys()]
    .filter((identifier) => !selected.has(identifier))
    .map((identifier) => `${identifier}: result was not selected`));
  return errors.sort();
}
