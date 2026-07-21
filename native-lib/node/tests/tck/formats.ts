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