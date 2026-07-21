// Parses a TCK case directory into runnable scenarios, applying the same
// structural skip filters as the CLI's TCKCliTest.
//
// TCK case layout (one directory per case, defined by the runtime's
// FolderBasedTest):
//   transform.dwl      the transform (fixed main file name)
//   inN.<ext>          inputs; basename (in0, in1…) is the variable name,
//                      extension selects the reader format
//   out.<ext>          expected output; one scenario per out.* file
//   config.properties  optional per-case config
//
// The parsing here is pure — it operates on a provided list of file names, not
// the filesystem — so it is unit-testable without real TCK fixtures.
import { isSupportedExtension, mimeForExtension } from "./formats";

/** Fixed name of the transform script in a TCK case (per FolderBasedTest). */
export const MAIN_TRANSFORM = "transform.dwl";

const INPUT_PATTERN = /^in[0-9]+\.[a-zA-Z]+$/;
const OUTPUT_PATTERN = /^out\.[a-zA-Z]+$/;
const INPUT_CONFIG_PATTERN = /^in[0-9]+-config\.properties$/;
const OUTPUT_CONFIG_PATTERN = /^out[0-9]*-config\.properties$/;

/** Returns the extension (without dot, lowercased) of a file name, or "" if none. */
export function extensionOf(name: string): string {
  const i = name.lastIndexOf(".");
  return i < 0 ? "" : name.slice(i + 1).toLowerCase();
}

/** An input file within a case: the DataWeave variable name and its source file. */
export interface TckInput {
  /** Variable name the input binds to (the file's base name, e.g. `in0`). */
  name: string;
  /** The input file name (e.g. `in0.json`). */
  fileName: string;
  /** MIME type derived from the extension. */
  mimeType: string;
}

/** One runnable scenario: the transform plus its inputs and a single expected output. */
export interface TckScenario {
  /** Scenario id: `<case>-<outputFileName>`. */
  name: string;
  inputs: TckInput[];
  /** Expected-output file name (e.g. `out.json`). */
  outputFileName: string;
  /** Comparison format, from the output extension. */
  outputMime: string;
  outputExtension: string;
}

/** The result of inspecting one case directory. */
export type CaseParseResult =
  | { kind: "scenarios"; scenarios: TckScenario[] }
  | { kind: "skipped"; reason: string };

/**
 * Parses a case directory (given its file listing) into scenarios, or a skip
 * decision. Mirrors the CLI's structural filters: a case is skipped unless it
 * has exactly one transform.dwl, and is skipped outright if it carries
 * per-input/output config properties, groovy/java cases, a bare
 * config.properties, or a _wip marker. Scenarios whose input or output
 * extension maps to an unsupported format (e.g. yaml) are dropped; if none
 * remain the case is skipped.
 *
 * @param caseName - The directory (case) name, used as the scenario prefix.
 * @param fileNames - The file names directly inside the case directory.
 * @returns Either the runnable scenarios or a reason the case was skipped.
 */
export function parseCase(caseName: string, fileNames: string[]): CaseParseResult {
  if (caseName.endsWith("_wip") || caseName.endsWith("wip")) {
    return { kind: "skipped", reason: "work-in-progress (_wip)" };
  }

  if (fileNames.some((n) => INPUT_CONFIG_PATTERN.test(n) || OUTPUT_CONFIG_PATTERN.test(n))) {
    return { kind: "skipped", reason: "per-input/output config.properties not supported" };
  }
  if (fileNames.some((n) => n === "config.properties")) {
    return { kind: "skipped", reason: "config.properties not supported" };
  }
  if (fileNames.some((n) => n.endsWith(".groovy"))) {
    return { kind: "skipped", reason: "java/groovy case not supported" };
  }

  // Exactly one transform: a .dwl that is not an inN.dwl / out.dwl.
  const dwlFiles = fileNames.filter(
    (n) => extensionOf(n) === "dwl" && !INPUT_PATTERN.test(n) && !OUTPUT_PATTERN.test(n)
  );
  if (dwlFiles.length !== 1) {
    return { kind: "skipped", reason: `expected exactly one transform dwl, found ${dwlFiles.length}` };
  }
  if (!fileNames.includes(MAIN_TRANSFORM)) {
    return { kind: "skipped", reason: `transform is not named ${MAIN_TRANSFORM}` };
  }

  const inputFiles = fileNames.filter((n) => INPUT_PATTERN.test(n)).sort();
  const outputFiles = fileNames.filter((n) => OUTPUT_PATTERN.test(n)).sort();
  if (outputFiles.length === 0) {
    return { kind: "skipped", reason: "no expected output (out.*) file" };
  }

  // Drop scenarios that reference an unsupported input or output format.
  const unsupportedInput = inputFiles.find((n) => !isSupportedExtension(extensionOf(n)));
  if (unsupportedInput) {
    return { kind: "skipped", reason: `unsupported input format: ${unsupportedInput}` };
  }

  const inputs: TckInput[] = inputFiles.map((fileName) => ({
    name: fileName.slice(0, fileName.lastIndexOf(".")),
    fileName,
    mimeType: mimeOrThrow(fileName),
  }));

  const scenarios: TckScenario[] = outputFiles
    .filter((out) => isSupportedExtension(extensionOf(out)))
    .map((outputFileName) => {
      const outputExtension = extensionOf(outputFileName);
      return {
        name: `${caseName}-${outputFileName}`,
        inputs,
        outputFileName,
        outputExtension,
        outputMime: mimeOrThrow(outputFileName),
      };
    });

  if (scenarios.length === 0) {
    return { kind: "skipped", reason: "no scenarios with a supported output format" };
  }
  return { kind: "scenarios", scenarios };
}

function mimeOrThrow(fileName: string): string {
  // Guarded by isSupportedExtension at call sites; this keeps types non-null.
  const mime = mimeForExtension(extensionOf(fileName));
  if (!mime) throw new Error(`no MIME mapping for ${fileName}`);
  return mime;
}