// Extension ↔ MIME mapping for the TCK harness.
//
// The DataWeave binding selects a data format purely from the MIME type: input
// format comes from each input's `mimeType`, output format from the script's
// `output` directive. TCK cases encode the format in the file extension
// (`in0.json`, `out.xml`), so the harness maps extension → MIME.
//
// Only formats compiled into this dwlib are listed. The runtime reports its
// supported set as: application/dw, application/json, application/xml,
// application/csv, application/octet-stream, text/plain,
// application/x-www-form-urlencoded, multipart/form-data, text/x-java-properties.
// Notably YAML is NOT compiled into this build, so yaml/yml cases are
// unsupported and must be skipped by the loader.

/** Maps a lowercased file extension (no dot) to the DataWeave MIME type, or undefined if unsupported. */
export const EXTENSION_TO_MIME: Readonly<Record<string, string>> = {
  json: "application/json",
  xml: "application/xml",
  csv: "application/csv",
  txt: "text/plain",
  dwl: "application/dw",
  bin: "application/octet-stream",
  properties: "text/x-java-properties",
  urlencoded: "application/x-www-form-urlencoded",
  multipart: "multipart/form-data",
};

/** Returns the MIME type for a file extension (case-insensitive, leading dot optional), or undefined. */
export function mimeForExtension(ext: string): string | undefined {
  return EXTENSION_TO_MIME[ext.replace(/^\./, "").toLowerCase()];
}

/** Whether the given file extension maps to a format this dwlib supports. */
export function isSupportedExtension(ext: string): boolean {
  return mimeForExtension(ext) !== undefined;
}

/**
 * Maps a MIME type to a coarse format family used to compare two output
 * directives. Any `multipart/*` subtype collapses to `multipart` (so a pinned
 * `multipart/mixed` isn't confused with `multipart/form-data`); known MIME
 * types map to their extension key; anything else returns the MIME unchanged.
 *
 * @param mime - A MIME type (e.g. from an `output` directive or an expected file).
 * @returns A family token for equality comparison.
 */
export function familyForMime(mime: string): string {
  const m = mime.toLowerCase();
  if (m.startsWith("multipart/")) return "multipart";
  for (const [ext, knownMime] of Object.entries(EXTENSION_TO_MIME)) {
    if (knownMime === m) return ext;
  }
  return m;
}