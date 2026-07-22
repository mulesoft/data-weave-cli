// Normalizes a TCK transform so its output format matches the expected file.
//
// The CLI rewrites the transform's AST (via the runtime CodeGenerator) to force
// the output directive to the format implied by the expected file's extension.
// We can't run that AST rewriter from Node, so we do a deliberately small
// text-level normalization that covers the common cases and lean on the ignore
// list for the rest (e.g. `do`-block headers, which a regex can't safely
// rewrite). Empirically this runs ~91% of supported runtime cases.
//
// Rule: split on the document body separator — a `---` at column 0 — into
// header + body. If the header has no `output` directive, append one for the
// target MIME. Matching only column-0 `---` avoids mis-splitting on the
// indented `---` that appears inside a `do { … }` block (whose document
// separator, if any, is still at column 0). Transforms that pin a *conflicting*
// output format are a separate case handled via the ignore list, not here,
// since replacing a directive needs the AST rewrite the CLI uses.

/**
 * Ensures the transform declares an `output` directive for `mime`.
 *
 * If the header (everything before the first column-0 `---`) already has an
 * `output` line, the script is returned unchanged. Otherwise `output <mime>` is
 * appended to the header. A transform with no column-0 `---` is treated as a
 * bare body and given a fresh `output <mime>\n---\n` header.
 *
 * @param src - The raw transform.dwl contents.
 * @param mime - The MIME type implied by the expected output file's extension.
 * @returns The transform with a guaranteed output directive.
 */
export function ensureOutputDirective(src: string, mime: string): string {
  const lines = src.split(/\r?\n/);
  // The document body separator is a `---` at column 0. An indented `---`
  // belongs to a do-block and must not be treated as the header/body split.
  const sep = lines.findIndex((l) => /^---\s*$/.test(l));

  if (sep < 0) {
    // No document separator: the whole file is a body. Give it a header.
    return `output ${mime}\n---\n${src}`;
  }

  const header = lines.slice(0, sep);
  const body = lines.slice(sep + 1);

  if (header.some((l) => /^\s*output\s+/.test(l))) {
    return src; // already has an output directive
  }

  return [...header, `output ${mime}`].join("\n") + "\n---\n" + body.join("\n");
}