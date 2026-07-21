// Semantic comparison of actual vs expected TCK output, dispatched by the
// expected file's extension. Ported from the CLI's AssertionHelper: comparison
// is format-aware, NOT byte equality — e.g. JSON is compared structurally so
// key order and insignificant whitespace don't cause false failures.
//
// Pure and unit-testable: takes strings/Buffers, returns a match result.

/** Outcome of comparing one scenario's actual output against the expected. */
export interface CompareResult {
  match: boolean;
  /** Human-readable reason when `match` is false. */
  detail?: string;
}

const ok: CompareResult = { match: true };
const fail = (detail: string): CompareResult => ({ match: false, detail });

/**
 * Compares actual output bytes against expected output bytes using the strategy
 * for `extension`:
 * - json: structural deep-equal after JSON.parse (order-insensitive for objects)
 * - xml: whitespace-normalized string compare (tags/text, insignificant space collapsed)
 * - csv / txt / dwl / yaml / yml / urlencoded / properties: whitespace-normalized string
 * - bin: exact byte compare
 *
 * @param extension - The expected output file's extension (no dot), case-insensitive.
 * @param actual - The produced output bytes.
 * @param expected - The expected output bytes.
 * @returns Whether they match, with a detail message on mismatch.
 */
export function compareOutput(extension: string, actual: Buffer, expected: Buffer): CompareResult {
  const ext = extension.replace(/^\./, "").toLowerCase();

  if (ext === "bin") {
    return actual.equals(expected) ? ok : fail(`binary mismatch: ${actual.length} vs ${expected.length} bytes`);
  }

  const a = actual.toString("utf-8");
  const e = expected.toString("utf-8");

  switch (ext) {
    case "json":
      return compareJson(a, e);
    case "xml":
      return compareNormalizedString(a, e, collapseWhitespace);
    case "dwl":
      return compareNormalizedString(a, e, stripAllWhitespace);
    case "csv":
    case "txt":
    case "yaml":
    case "yml":
    case "urlencoded":
    case "properties":
      return compareNormalizedString(a, e, normalizeEol);
    default:
      // Unknown extension: fall back to a trimmed EOL-normalized string compare.
      return compareNormalizedString(a, e, normalizeEol);
  }
}

function compareJson(actual: string, expected: string): CompareResult {
  let a: unknown;
  let e: unknown;
  try {
    a = JSON.parse(actual);
  } catch (err) {
    return fail(`actual is not valid JSON: ${err}`);
  }
  try {
    e = JSON.parse(expected);
  } catch (err) {
    return fail(`expected is not valid JSON: ${err}`);
  }
  return deepEqual(a, e) ? ok : fail(`JSON mismatch:\n  actual:   ${actual.trim()}\n  expected: ${expected.trim()}`);
}

function compareNormalizedString(
  actual: string,
  expected: string,
  normalize: (s: string) => string
): CompareResult {
  const a = normalize(actual);
  const e = normalize(expected);
  return a === e ? ok : fail(`text mismatch:\n  actual:   ${JSON.stringify(a)}\n  expected: ${JSON.stringify(e)}`);
}

/** Normalizes line endings and trims — the mildest normalization (csv/txt/yaml). */
function normalizeEol(s: string): string {
  return s.replace(/\r\n/g, "\n").trim();
}

/** Collapses all runs of whitespace to nothing — used for XML (structure-insensitive to layout). */
function collapseWhitespace(s: string): string {
  return s.replace(/\s+/g, "");
}

/** Strips every whitespace character — used for generated DWL comparison. */
function stripAllWhitespace(s: string): string {
  return s.replace(/\s+/g, "");
}

/** Structural deep equality for JSON values (objects compared key-insensitively to order). */
export function deepEqual(a: unknown, b: unknown): boolean {
  if (a === b) return true;
  if (a === null || b === null) return a === b;
  if (typeof a !== typeof b) return false;

  if (Array.isArray(a) || Array.isArray(b)) {
    if (!Array.isArray(a) || !Array.isArray(b) || a.length !== b.length) return false;
    return a.every((v, i) => deepEqual(v, b[i]));
  }

  if (typeof a === "object" && typeof b === "object") {
    const ao = a as Record<string, unknown>;
    const bo = b as Record<string, unknown>;
    const ak = Object.keys(ao);
    const bk = Object.keys(bo);
    if (ak.length !== bk.length) return false;
    return ak.every((k) => Object.prototype.hasOwnProperty.call(bo, k) && deepEqual(ao[k], bo[k]));
  }

  return false;
}