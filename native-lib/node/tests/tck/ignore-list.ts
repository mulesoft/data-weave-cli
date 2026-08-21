// Cases the TCK harness skips, keyed by full scenario directory name with a documented reason.
//
// Seeded empirically against the tck@zip corpus (2.13.0-SNAPSHOT) after
// removing the transform normalizer. Keys are full "<scenario>-out.<ext>"
// directory names matching scenario ids under the new naming (Task 3).
// The set overlaps heavily with the CLI's TCKCliTest.ignoreTests() — the
// same runtime and harness limitations apply to the binding.
//
// Reasons:
//   unresolved-module — needs a DW library/resource not compiled into dwlib
//                       (org::mule::weave::v2::libs::*, dw::Client, imports,
//                       readUrl/classpath resources).
//   java              — uses the Java module / java:: types (application/java).
//   dw::Runtime       — needs dw::Runtime run/orElseTry behavior not in dwlib.
//   dwl-output-format — application/dw output formatting (quoting, @(…) metadata)
//                       differs from the expected DWL fixture.
//   multipart         — multipart writing edge cases (empty parts, binary parts,
//                       boundary nondeterminism).
//   nondeterministic  — output embeds a timestamp or varies by environment (DST).
//   slow              — passes but risks exceeding the 30s test timeout on CI.
//   coercion/runtime  — runtime coercion/streaming behavior, also CLI-ignored.
//   xml               — attribute selector runtime behavior or namespace differences.

export const SUPPORTED_CATEGORIES = [
  "unavailable-module-or-resource",
  "unavailable-java-module",
  "runtime-baseline-mismatch",
  "environment-sensitive",
  "slow",
] as const;

export type IgnoreCategory = typeof SUPPORTED_CATEGORIES[number];

export interface IgnoreEntry {
  caseIdentifier: string;
  category: IgnoreCategory | string;
  reason: string;
}

export type ExpectedFailurePolicy = Readonly<Record<string, string>>;

const EXPECTED_RUNNABLE_CASES = 729;
const EXPECTED_STRUCTURAL_SKIPS = 193;

const LEGACY_IGNORED_CASES: Readonly<Record<string, { reason: string }>> = {
  // unresolved-module — library/resource not present in dwlib
  "dw-binary-out.dwl": { reason: "unresolved-module: readUrl/classpath resource" },
  "is-empty-using-empty-stream-out.json": { reason: "unresolved-module: dw::Client streaming" },
  "module-singleton-out.json": { reason: "unresolved-module: lib not in dwlib" },
  "private_scope_directives-out.xml": { reason: "unresolved-module: resource not in dwlib" },
  "read-binary-files-out.bin": { reason: "unresolved-module: readUrl/classpath resource" },
  "read_lines-out.json": { reason: "unresolved-module: readUrl/classpath resource" },
  "sql_date_mapping-out.json": { reason: "unresolved-module: java/sql date mapping" },
  "streaming_binary_inside_value-out.json": { reason: "unresolved-module: dw::Client streaming" },
  "try-out.json": { reason: "unresolved-module: resource not in dwlib" },
  "try-handle-array-value-with-failures-out.json": { reason: "unresolved-module: resource not in dwlib" },
  "try-handle-attribute-delegate-with-failures-out.json": { reason: "unresolved-module: resource not in dwlib" },
  "try-handle-attributes-value-with-failures-out.json": { reason: "unresolved-module: resource not in dwlib" },
  "try-handle-binary-value-with-failures-out.json": { reason: "unresolved-module: resource not in dwlib" },
  "try-handle-delegate-value-with-failures-out.json": { reason: "unresolved-module: resource not in dwlib" },
  "try-handle-key-value-pair-value-with-failures-out.json": { reason: "unresolved-module: resource not in dwlib" },
  "try-handle-materialized-object-with-failures-out.json": { reason: "unresolved-module: resource not in dwlib" },
  "try-handle-name-value-pair-value-with-failures-out.json": { reason: "unresolved-module: resource not in dwlib" },
  "try-handle-schema-property-value-with-failures-out.json": { reason: "unresolved-module: resource not in dwlib" },
  "try-handle-schema-value-with-failures-out.json": { reason: "unresolved-module: resource not in dwlib" },
  "underflow-out.json": { reason: "unresolved-module: resource not in dwlib" },
  "urlEncodeDecode-out.json": { reason: "unresolved-module: resource not in dwlib" },

  // java — Java module / java:: interop
  "java-big-decimal-out.xml": { reason: "java: java::lang interop" },
  "java-field-ref-out.json": { reason: "java: java module" },
  "java-interop-enum-out.json": { reason: "java: java module" },
  "java-interop-function-call-out.json": { reason: "java: java module" },
  "java_epoch_bridge-out.json": { reason: "java: java module epoch bridge" },
  "runtime_run_null_java-out.json": { reason: "java: java module null handling" },
  "write-function-with-null-out.xml": { reason: "java: application/java format not in dwlib" },

  // dw::Runtime module cases — the run/orElseTry/dataFormatsDescriptors behavior
  // isn't available in this dwlib (not a directive issue).
  "runtime_dataFormatsDescriptors-out.json": { reason: "dw::Runtime: internal data-format descriptors" },
  "runtime_orElseTry-out.json": { reason: "dw::Runtime: run/orElseTry behavior not in dwlib" },
  "runtime_run-out.json": { reason: "dw::Runtime: run behavior not in dwlib (also slow)" },
  "runtime_run_coercionException-out.json": { reason: "dw::Runtime: run behavior not in dwlib" },
  "runtime_run_fibo-out.json": { reason: "dw::Runtime: run behavior not in dwlib" },
  "try-recursive-call-out.json": { reason: "dw::Runtime: run behavior not in dwlib" },

  // dwl-output-format — application/dw formatting differs from expected DWL
  "coerciones_toString-out.json": { reason: "dwl-output-format: application/dw formatting differs" },

  // multipart — multipart writing edge cases (boundary nondeterminism, binary parts, empty parts)
  "multipart-binary-out.multipart": { reason: "multipart: boundary nondeterminism + binary part encoding" },
  "multipart-class-cast-issue-out.multipart": { reason: "multipart: boundary nondeterminism" },
  "multipart-empty-part-out.multipart": { reason: "multipart: boundary nondeterminism + empty part handling" },
  "multipart-mixed-message-out.multipart": { reason: "multipart: empty parts / structural" },
  "multipart-write-binary-out.json": { reason: "multipart: binary part write" },
  "multipart-write-message-out.multipart": { reason: "multipart: empty parts / structural" },
  "multipart-write-subtype-override-out.multipart": { reason: "multipart: subtype override" },

  // slow — passes but risks exceeding the 30s test timeout on CI
  "big_intersection-out.json": { reason: "slow: 500-way intersection type exceeds the test timeout" },

  // nondeterministic — output embeds a timestamp or varies by environment (DST)
  "dates_atBeginningOfDay-out.json": { reason: "nondeterministic: DST timezone offset varies by environment" },
  "dates_atBeginningOfMonth-out.json": { reason: "nondeterministic: DST timezone offset varies by environment" },
  "dates_atBeginningOfWeek-out.json": { reason: "nondeterministic: DST timezone offset varies by environment" },
  "dates_atBeginningOfYear-out.json": { reason: "nondeterministic: DST timezone offset varies by environment" },
  "properties-passthrough-out.properties": { reason: "nondeterministic: properties output embeds a timestamp comment" },
  "properties-writer-out.properties": { reason: "nondeterministic: properties output embeds a timestamp comment" },

  // coercion/runtime behavior (also CLI-ignored)
  "access_raw_value-out.json": { reason: "coercion/runtime: Cannot coerce Null to String" },
  "csv-invalid-utf8-out.csv": { reason: "coercion/runtime: csv invalid utf8 handling" },
  "read-concat-out.json": { reason: "coercion/runtime: Cannot coerce Null to String" },
  "update-op-out.dwl": { reason: "coercion/runtime: Cannot coerce Null to Number" },

  // xml — attribute selector runtime behavior or serialization differences
  "multi_attribute_selector_after_empty_filter_slot-out.json": { reason: "xml: attribute selector runtime behavior" },
  "repeated_attribute_selector_map_slot_permutations-out.json": { reason: "xml: attribute selector runtime behavior" },
  "xml-escaped-data-out.xml": { reason: "xml: escaping differs from fixture" },
  "xml-streaming-selectors-out.xml": { reason: "xml: streaming selector serialization" },
  "xml-value-selector-out.xml": { reason: "xml: namespace scoping in fixture" },
  "xml_empty_namespace-out.xml": { reason: "xml: empty namespace serialization" },
};

const CORE_MODULE_CASES = new Set([
  "read-binary-files-out.bin",
  "multipart-binary-out.multipart",
  "multipart-class-cast-issue-out.multipart",
  "multipart-empty-part-out.multipart",
  "multipart-mixed-message-out.multipart",
  "multipart-write-binary-out.json",
  "multipart-write-message-out.multipart",
  "multipart-write-subtype-override-out.multipart",
  "properties-passthrough-out.properties",
  "csv-invalid-utf8-out.csv",
  "xml-escaped-data-out.xml",
  "xml-streaming-selectors-out.xml",
  "xml-value-selector-out.xml",
  "xml_empty_namespace-out.xml",
]);

function categoryFor(reason: string): IgnoreCategory {
  if (reason.startsWith("unresolved-module:")) return "unavailable-module-or-resource";
  if (reason.startsWith("java:")) return "unavailable-java-module";
  if (reason.startsWith("nondeterministic:")) return "environment-sensitive";
  if (reason.startsWith("slow:")) return "slow";
  return "runtime-baseline-mismatch";
}

const LEGACY_POLICY: Readonly<Record<string, IgnoreEntry>> = Object.fromEntries(
  Object.entries(LEGACY_IGNORED_CASES).map(([caseName, entry]) => {
    const suite = CORE_MODULE_CASES.has(caseName) ? "core-modules" : "runtime";
    const caseIdentifier = `${suite}/${caseName}`;
    return [caseIdentifier, {
      caseIdentifier,
      category: categoryFor(entry.reason),
      reason: entry.reason,
    }];
  })
);

export const ACCEPTED_BASELINE_MISMATCHES: ExpectedFailurePolicy = {
  "core-modules/csv-invalid-utf8-out.csv:out.csv": "runtime emits a replacement character where the fixture expects an empty CSV value",
  "core-modules/multipart-binary-out.multipart:out.multipart": "multipart writer output differs from the baseline fixture",
  "core-modules/multipart-class-cast-issue-out.multipart:out.multipart": "multipart writer output differs from the baseline fixture",
  "core-modules/multipart-empty-part-out.multipart:out.multipart": "multipart writer output differs from the baseline fixture",
  "core-modules/multipart-mixed-message-out.multipart:out.multipart": "multipart writer output differs from the baseline fixture",
  "core-modules/multipart-write-message-out.multipart:out.multipart": "multipart writer output differs from the baseline fixture",
  "core-modules/multipart-write-subtype-override-out.multipart:out.multipart": "multipart writer output differs from the baseline fixture",
  "core-modules/properties-passthrough-out.properties:out.properties": "properties writer output differs from the baseline fixture",
  "core-modules/xml-escaped-data-out.xml:out.xml": "XML character escaping differs from the baseline fixture",
  "core-modules/xml-streaming-selectors-out.xml:out.xml": "streaming XML serialization differs from the baseline fixture",
  "core-modules/xml-value-selector-out.xml:out.xml": "XML namespace scoping differs from the baseline fixture",
  "core-modules/xml_empty_namespace-out.xml:out.xml": "empty XML namespace serialization differs from the baseline fixture",
  "runtime/access_raw_value-out.json:out.json": "runtime coercion output differs from the baseline fixture",
  "runtime/coerciones_toString-out.json:out.json": "locale-sensitive runtime output differs from the baseline fixture",
  "runtime/properties-writer-out.properties:out.properties": "properties writer output differs from the baseline fixture",
  "runtime/read-concat-out.json:out.json": "runtime coercion output differs from the baseline fixture",
  "runtime/runtime_dataFormatsDescriptors-out.json:out.json": "dw::Runtime output differs from the baseline fixture",
  "runtime/runtime_orElseTry-out.json:out.json": "source-location runtime output differs from the baseline fixture",
  "runtime/runtime_run-out.json:out.json": "dw::Runtime output differs from the baseline fixture",
  "runtime/try-recursive-call-out.json:out.json": "source-location runtime output differs from the baseline fixture",
  "runtime/update-op-out.dwl:out.dwl": "runtime coercion output differs from the baseline fixture",
};

export const REENABLED_CASES = [
  "runtime/big_intersection-out.json",
  "runtime/dates_atBeginningOfDay-out.json",
  "runtime/dates_atBeginningOfMonth-out.json",
  "runtime/dates_atBeginningOfWeek-out.json",
  "runtime/dates_atBeginningOfYear-out.json",
  "runtime/multi_attribute_selector_after_empty_filter_slot-out.json",
  "runtime/repeated_attribute_selector_map_slot_permutations-out.json",
] as const;

export const CAPABILITY_EXCLUSIONS = Object.fromEntries(
  Object.entries(LEGACY_POLICY).filter(([identifier]) =>
    !Object.keys(ACCEPTED_BASELINE_MISMATCHES).some((scenario) => scenario.startsWith(`${identifier}:`))
      && !REENABLED_CASES.includes(identifier as typeof REENABLED_CASES[number])
  )
);

export const IGNORED_CASES = CAPABILITY_EXCLUSIONS;

export const STRUCTURAL_MODULE_CASES = new Set([
  "runtime/implicit_type_parameters-out.json",
  "runtime/import_mapping-out.json",
  "runtime/import_mapping_with_functions-out.json",
  "runtime/import_mapping_with_implicit_input-out.json",
  "runtime/import_namespace-out.xml",
  "runtime/infinit_list-out.json",
  "runtime/interceptor_functions-out.json",
  "runtime/lazy_metadata_definition-out.json",
  "runtime/location-out.json",
  "runtime/locationString-out.json",
  "runtime/logwith_function-out.json",
  "runtime/read-function-by-id-out.json",
  "runtime/read-function-out.json",
  "runtime/runtime_evalUrl-out.json",
  "runtime/runtime_runUrl-out.json",
  "runtime/type_selector_materialize-out.json",
  "runtime/weave_multiple_namespace-out.dwl",
]);

export function validateIgnorePolicy(
  entries: Readonly<Record<string, IgnoreEntry>>,
  runnableCases?: ReadonlySet<string>
): string[] {
  const errors: string[] = [];
  for (const [identifier, entry] of Object.entries(entries)) {
    if (!entry.caseIdentifier) errors.push(`${identifier}: missing case identity`);
    else if (entry.caseIdentifier !== identifier) errors.push(`${identifier}: case identity must match registry key`);
    if (!SUPPORTED_CATEGORIES.includes(entry.category as IgnoreCategory)) {
      errors.push(`${identifier}: unsupported category ${entry.category}`);
    }
    if (!entry.reason.trim()) errors.push(`${identifier}: missing reason`);
    if (runnableCases && !runnableCases.has(identifier)) {
      errors.push(`${identifier}: not a discovered runnable case`);
    }
  }
  return errors;
}

export function validateInventoryPolicy(runnableCases: number, structuralSkips: number): string[] {
  const errors: string[] = [];
  if (runnableCases !== EXPECTED_RUNNABLE_CASES) {
    errors.push(`expected ${EXPECTED_RUNNABLE_CASES} runnable cases, discovered ${runnableCases}`);
  }
  if (structuralSkips !== EXPECTED_STRUCTURAL_SKIPS) {
    errors.push(`expected ${EXPECTED_STRUCTURAL_SKIPS} structurally skipped cases, discovered ${structuralSkips}`);
  }
  return errors;
}

export function validateReconciledPolicy(
  exclusions: Readonly<Record<string, IgnoreEntry>>,
  expectedFailures: ExpectedFailurePolicy,
  reenabledCases: readonly string[] = [],
  runnableScenarios?: ReadonlySet<string>,
): string[] {
  const errors: string[] = [];
  const reenabledCounts = new Map<string, number>();
  for (const identifier of reenabledCases) {
    reenabledCounts.set(identifier, (reenabledCounts.get(identifier) ?? 0) + 1);
  }
  for (const identifier of reenabledCounts.keys()) {
    if (Object.prototype.hasOwnProperty.call(exclusions, identifier)) {
      errors.push(`${identifier}: case is both skipped and re-enabled`);
    }
    if (runnableScenarios && ![...runnableScenarios].some((scenario) => scenario.startsWith(`${identifier}:`))) {
      errors.push(`${identifier}: not a discovered runnable case`);
    }
  }
  errors.push(...[...reenabledCounts]
    .filter(([, count]) => count > 1)
    .map(([identifier]) => `${identifier}: duplicate re-enabled case`));
  for (const [identifier, reason] of Object.entries(expectedFailures)) {
    if (!reason.trim()) errors.push(`${identifier}: missing expected-failure reason`);
    if (runnableScenarios && !runnableScenarios.has(identifier)) {
      errors.push(`${identifier}: not a discovered runnable scenario`);
    }
    const caseIdentifier = identifier.slice(0, identifier.lastIndexOf(":"));
    if (Object.prototype.hasOwnProperty.call(exclusions, caseIdentifier)) {
      errors.push(`${identifier}: case is both skipped and expected to fail`);
    }
  }
  return errors.sort();
}

export function validateStructuralModulePolicy(
  entries: ReadonlySet<string>,
  structuralModuleCases: ReadonlySet<string>
): string[] {
  const errors = [...entries]
    .filter((identifier) => !structuralModuleCases.has(identifier))
    .sort()
    .map((identifier) => `${identifier}: not a structural module case`);
  errors.push(...[...structuralModuleCases]
    .filter((identifier) => !entries.has(identifier))
    .sort()
    .map((identifier) => `${identifier}: structural module case is not registered`));
  return errors;
}

/** Whether a case is on the ignore list. */
export function isIgnored(caseIdentifier: string): boolean {
  return Object.prototype.hasOwnProperty.call(IGNORED_CASES, caseIdentifier);
}

/** The documented skip reason for a case, or undefined if not ignored. */
export function ignoreReason(caseIdentifier: string): string | undefined {
  return IGNORED_CASES[caseIdentifier]?.reason;
}
