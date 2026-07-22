// Normalizes a TCK transform so its output format matches the expected file.
//
// The CLI rewrites the transform's AST (via the runtime CodeGenerator) to force
// the output directive to the format implied by the expected file's extension.
// We can't run that AST rewriter from Node, so we do a deliberately small
// text-level normalization that covers the common cases and lean on the ignore
// list for the rest.
//
// Rule: split on the document body separator — a `---` at column 0 — into
// header + body (matching only column-0 avoids mis-splitting on the indented
// `---` inside a `do { … }` block), then:
//   - no header `output` directive → append `output <mime>`;
//   - header pins a plain `mime/type` of a DIFFERENT family than the target →
//     replace just the mime token (keeping trailing options), matching what the
//     CLI's AST rewriter does for the expected extension;
//   - otherwise leave the directive alone. "Otherwise" deliberately covers
//     same-family directives, `multipart/*` subtypes (so a pinned
//     `multipart/mixed` with its `boundary=` option isn't clobbered), and
//     non-mime output selectors like `output :Type json`.

import { familyForMime } from "./formats";

/**
 * Ensures the transform's `output` directive targets `mime`.
 *
 * @param src - The raw transform.dwl contents.
 * @param mime - The MIME type implied by the expected output file's extension.
 * @returns The transform with an output directive for the target format.
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

  const outputIdx = header.findIndex((l) => /^\s*output\s+/.test(l));
  if (outputIdx < 0) {
    return [...header, `output ${mime}`].join("\n") + "\n---\n" + body.join("\n");
  }

  // Replace the mime token only when it is a plain `mime/type` of a different
  // family than the target. Leave `:Type` selectors and same-/multipart-family
  // directives untouched to avoid corrupting a directive that was correct.
  const token = header[outputIdx].match(/^\s*output\s+(\S+)/)?.[1] ?? "";
  const targetFamily = familyForMime(mime);
  if (token.includes("/") && familyForMime(token) !== targetFamily && familyForMime(token) !== "multipart") {
    header[outputIdx] = header[outputIdx].replace(/^(\s*output\s+)\S+/, `$1${mime}`);
  }

  return header.join("\n") + "\n---\n" + body.join("\n");
}