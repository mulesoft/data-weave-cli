// Cases the TCK harness skips, keyed by case name with a documented reason.
//
// Seeded empirically: each name below was observed to fail when replayed
// through this dwlib build (weaveTestSuiteVersion in gradle.properties) across
// the runtime and core-modules suites, grouped by root cause. The set overlaps
// heavily with the CLI's TCKCliTest.ignoreTests() — the same runtime and
// harness limitations apply to the binding.
//
// Reasons:
//   unresolved-module — needs a DW library/resource not compiled into dwlib
//                       (org::mule::weave::v2::libs::*, dw::Client, imports,
//                       readUrl/classpath resources).
//   java              — uses the Java module / java:: types (application/java).
//   dw::Runtime       — needs dw::Runtime run/orElseTry behavior not in dwlib.
//   dwl-output-format — application/dw output formatting (quoting, @(…) metadata)
//                       differs from the expected DWL fixture.
//   multipart         — multipart writing edge cases (empty parts, binary parts)
//                       not supported / not comparable here.
//   nondeterministic  — output embeds a timestamp or otherwise varies per run.
//   coercion/runtime  — runtime coercion/streaming behavior, also CLI-ignored.
//
// (do-block and output-format-mismatch cases are no longer skipped — see
// transform.ts: the normalizer splits on a column-0 `---` and replaces a
// conflicting output mime, recovering them.)

export interface IgnoreEntry {
  reason: string;
}

export const IGNORED_CASES: Readonly<Record<string, IgnoreEntry>> = {
  // unresolved-module — library/resource not present in dwlib
  "dw-binary": { reason: "unresolved-module: readUrl/classpath resource" },
  "full-qualified-name-ref": { reason: "unresolved-module: org::mule::weave::v2::libs" },
  "import-component-alias-lib": { reason: "unresolved-module: import lib not in dwlib" },
  "import-lib": { reason: "unresolved-module: import lib not in dwlib" },
  "import-lib-with-alias": { reason: "unresolved-module: import lib not in dwlib" },
  "import-named-lib": { reason: "unresolved-module: import lib not in dwlib" },
  "import-star": { reason: "unresolved-module: import lib not in dwlib" },
  "is-empty-using-empty-stream": { reason: "unresolved-module: dw::Client" },
  "module-singleton": { reason: "unresolved-module: lib not in dwlib" },
  "read_lines": { reason: "unresolved-module: readUrl/classpath resource" },
  "read-binary-files": { reason: "unresolved-module: readUrl/classpath resource" },
  "sql_date_mapping": { reason: "unresolved-module: java/sql date mapping" },
  "streaming_binary_inside_value": { reason: "unresolved-module: dw::Client" },
  try: { reason: "unresolved-module: resource not in dwlib" },
  underflow: { reason: "unresolved-module: resource not in dwlib" },
  urlEncodeDecode: { reason: "unresolved-module: resource not in dwlib" },
  "try-handle-attribute-delegate-with-failures": { reason: "unresolved-module: resource not in dwlib" },
  "try-handle-materialized-object-with-failures": { reason: "unresolved-module: resource not in dwlib" },
  "private_scope_directives": { reason: "unresolved-module: resource not in dwlib" },
  "try-handle-array-value-with-failures": { reason: "unresolved-module: resource not in dwlib" },
  "try-handle-attributes-value-with-failures": { reason: "unresolved-module: resource not in dwlib" },
  "try-handle-binary-value-with-failures": { reason: "unresolved-module: resource not in dwlib" },
  "try-handle-delegate-value-with-failures": { reason: "unresolved-module: resource not in dwlib" },
  "try-handle-key-value-pair-value-with-failures": { reason: "unresolved-module: resource not in dwlib" },
  "try-handle-name-value-pair-value-with-failures": { reason: "unresolved-module: resource not in dwlib" },
  "try-handle-schema-property-value-with-failures": { reason: "unresolved-module: resource not in dwlib" },
  "try-handle-schema-value-with-failures": { reason: "unresolved-module: resource not in dwlib" },

  // java — Java module / java:: interop
  "java-big-decimal": { reason: "java: java::lang interop" },
  "java-field-ref": { reason: "java: java module" },
  "java-interop-enum": { reason: "java: java module" },
  "java-interop-function-call": { reason: "java: java module" },
  "write-function-with-null": { reason: "java: java module" },
  "runtime_run_null_java": { reason: "java: java module" },

  // Previously-skipped cases that now run after ensureOutputDirective was
  // strengthened: splitting on a column-0 `---` recovered the do-block cases
  // (do-1, do-block, csv-streaming, range-selector, …), and replacing a
  // conflicting output mime recovered format-mismatch cases (recursive_mapObject,
  // xml-root-with-text, groupby-complex's application/java, tree-filter*,
  // bad-inline, string_interpolation_selection, overload-functions,
  // dynamic_attribute_name). Remaining try-handle-*/private_scope_directives
  // fail on unresolved-module and are grouped above.

  // dw::Runtime module cases — the run/orElseTry/dataFormatsDescriptors behavior
  // isn't available in this dwlib (not a directive issue).
  "runtime_orElseTry": { reason: "dw::Runtime: run/orElseTry behavior not in dwlib" },
  "runtime_run": { reason: "dw::Runtime: run behavior not in dwlib" },
  "runtime_run_coercionException": { reason: "dw::Runtime: run behavior not in dwlib" },
  "runtime_run_fibo": { reason: "dw::Runtime: run behavior not in dwlib" },
  "try-recursive-call": { reason: "dw::Runtime: run behavior not in dwlib" },
  "runtime_dataFormatsDescriptors": { reason: "dw::Runtime: internal data-format descriptors" },

  // dwl-output-format — application/dw formatting differs from expected DWL
  "coerciones_toString": { reason: "dwl-output-format: application/dw formatting differs" },

  // multipart — multipart writing edge cases
  "multipart-binary": { reason: "multipart: binary part comparison unsupported" },
  "multipart-class-cast-issue": { reason: "multipart: writing edge case" },
  "multipart-empty-part": { reason: "multipart: empty part handling" },
  "multipart-mixed-message": { reason: "multipart: empty parts / structural" },
  "multipart-write-binary": { reason: "multipart: binary part write" },
  "multipart-write-message": { reason: "multipart: empty parts / structural" },
  "multipart-write-subtype-override": { reason: "multipart: subtype override" },

  // nondeterministic — output embeds a timestamp
  "properties-writer": { reason: "nondeterministic: properties output embeds a timestamp comment" },
  "properties-passthrough": { reason: "nondeterministic: properties output embeds a timestamp comment" },

  // coercion/runtime behavior (also CLI-ignored)
  "access_raw_value": { reason: "coercion/runtime: Cannot coerce Null to String" },
  "read-concat": { reason: "coercion/runtime: Cannot coerce Null to String" },
  "csv-invalid-utf8": { reason: "coercion/runtime: csv invalid utf8 handling" },

  // residual xml cases with irreconcilable serialization or namespace scoping
  "xml-escaped-data": { reason: "xml: escaping differs from fixture" },
  "xml-value-selector": { reason: "xml: namespace scoping in fixture" },
  "xml-streaming-selectors": { reason: "xml: streaming selector serialization" },
  "xml_empty_namespace": { reason: "xml: empty namespace serialization" },
};

/** Whether a case is on the ignore list. */
export function isIgnored(caseName: string): boolean {
  return Object.prototype.hasOwnProperty.call(IGNORED_CASES, caseName);
}

/** The documented skip reason for a case, or undefined if not ignored. */
export function ignoreReason(caseName: string): string | undefined {
  return IGNORED_CASES[caseName]?.reason;
}