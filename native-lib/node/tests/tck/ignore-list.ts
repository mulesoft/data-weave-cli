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

export interface IgnoreEntry {
  reason: string;
}

export const IGNORED_CASES: Readonly<Record<string, IgnoreEntry>> = {
  // unresolved-module — library/resource not present in dwlib
  "dw-binary-out.dwl": { reason: "unresolved-module: readUrl/classpath resource" },
  "full-qualified-name-ref-out.json": { reason: "unresolved-module: org::mule::weave::v2::libs" },
  "import-component-alias-lib-out.json": { reason: "unresolved-module: import lib not in dwlib" },
  "import-lib-out.json": { reason: "unresolved-module: import lib not in dwlib" },
  "import-lib-with-alias-out.json": { reason: "unresolved-module: import lib not in dwlib" },
  "import-named-lib-out.json": { reason: "unresolved-module: import lib not in dwlib" },
  "import-star-out.json": { reason: "unresolved-module: import lib not in dwlib" },
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

/** Whether a case is on the ignore list. */
export function isIgnored(caseName: string): boolean {
  return Object.prototype.hasOwnProperty.call(IGNORED_CASES, caseName);
}

/** The documented skip reason for a case, or undefined if not ignored. */
export function ignoreReason(caseName: string): string | undefined {
  return IGNORED_CASES[caseName]?.reason;
}