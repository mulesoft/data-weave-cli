"""Auditable exclusions for TCK cases Python cannot execute deterministically."""

from dataclasses import dataclass
from typing import Dict, Iterable, List, Mapping, Optional


UNSUPPORTED_DW_MODULE_RESOLUTION = "unsupported-dw-module-resolution"
UNAVAILABLE_JAVA_MODULE = "unavailable-java-module"
UNAVAILABLE_CLASSPATH_TEST_RESOURCE = "unavailable-classpath-test-resource"

SUPPORTED_CATEGORIES = frozenset(
    (
        UNSUPPORTED_DW_MODULE_RESOLUTION,
        UNAVAILABLE_JAVA_MODULE,
        UNAVAILABLE_CLASSPATH_TEST_RESOURCE,
    )
)


# These cases are transform-shape structural skips because each bundles its
# imported module beside transform.dwl. They are reported separately and are
# deliberately not active exclusions, so they cannot hide runnable failures.
STRUCTURAL_MODULE_CASES = frozenset(
    (
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
    )
)


@dataclass(frozen=True)
class Exclusion:
    case_identifier: str
    category: str
    reason: str


def _exclusion(case_identifier: str, category: str, reason: str) -> Exclusion:
    return Exclusion(case_identifier, category, reason)


# Each entry has a full case identifier and direct runtime evidence. Categories
# describe only an observed, unsupported limitation; they never match patterns.
EXCLUDED_CASES: Dict[str, Exclusion] = {
    "runtime/import-component-alias-lib-out.json": _exclusion(
        "runtime/import-component-alias-lib-out.json",
        UNSUPPORTED_DW_MODULE_RESOLUTION,
        "imports a test-only DW module; the Python binding has no module resolver",
    ),
    "runtime/import-lib-out.json": _exclusion(
        "runtime/import-lib-out.json",
        UNSUPPORTED_DW_MODULE_RESOLUTION,
        "imports a test-only DW module; the Python binding has no module resolver",
    ),
    "runtime/import-lib-with-alias-out.json": _exclusion(
        "runtime/import-lib-with-alias-out.json",
        UNSUPPORTED_DW_MODULE_RESOLUTION,
        "imports a test-only DW module; the Python binding has no module resolver",
    ),
    "runtime/import-named-lib-out.json": _exclusion(
        "runtime/import-named-lib-out.json",
        UNSUPPORTED_DW_MODULE_RESOLUTION,
        "imports a test-only DW module; the Python binding has no module resolver",
    ),
    "runtime/import-star-out.json": _exclusion(
        "runtime/import-star-out.json",
        UNSUPPORTED_DW_MODULE_RESOLUTION,
        "imports a test-only DW module; the Python binding has no module resolver",
    ),
    "runtime/module-singleton-out.json": _exclusion(
        "runtime/module-singleton-out.json",
        UNSUPPORTED_DW_MODULE_RESOLUTION,
        "imports a test-only DW module; the Python binding has no module resolver",
    ),
    "runtime/is-empty-using-empty-stream-out.json": _exclusion(
        "runtime/is-empty-using-empty-stream-out.json",
        UNSUPPORTED_DW_MODULE_RESOLUTION,
        "imports dw::Client, which is not resolved by the Python binding",
    ),
    "runtime/streaming_binary_inside_value-out.json": _exclusion(
        "runtime/streaming_binary_inside_value-out.json",
        UNSUPPORTED_DW_MODULE_RESOLUTION,
        "imports dw::Natives, which is not resolved by the Python binding",
    ),
    "runtime/try-handle-array-value-with-failures-out.json": _exclusion(
        "runtime/try-handle-array-value-with-failures-out.json",
        UNSUPPORTED_DW_MODULE_RESOLUTION,
        "imports dw::Natives, which is not resolved by the Python binding",
    ),
    "runtime/try-handle-attribute-delegate-with-failures-out.json": _exclusion(
        "runtime/try-handle-attribute-delegate-with-failures-out.json",
        UNSUPPORTED_DW_MODULE_RESOLUTION,
        "imports dw::Natives, which is not resolved by the Python binding",
    ),
    "runtime/try-handle-attributes-value-with-failures-out.json": _exclusion(
        "runtime/try-handle-attributes-value-with-failures-out.json",
        UNSUPPORTED_DW_MODULE_RESOLUTION,
        "imports dw::Natives, which is not resolved by the Python binding",
    ),
    "runtime/try-handle-binary-value-with-failures-out.json": _exclusion(
        "runtime/try-handle-binary-value-with-failures-out.json",
        UNSUPPORTED_DW_MODULE_RESOLUTION,
        "imports dw::Natives, which is not resolved by the Python binding",
    ),
    "runtime/try-handle-delegate-value-with-failures-out.json": _exclusion(
        "runtime/try-handle-delegate-value-with-failures-out.json",
        UNSUPPORTED_DW_MODULE_RESOLUTION,
        "imports dw::Natives, which is not resolved by the Python binding",
    ),
    "runtime/try-handle-key-value-pair-value-with-failures-out.json": _exclusion(
        "runtime/try-handle-key-value-pair-value-with-failures-out.json",
        UNSUPPORTED_DW_MODULE_RESOLUTION,
        "imports dw::Natives, which is not resolved by the Python binding",
    ),
    "runtime/try-handle-materialized-object-with-failures-out.json": _exclusion(
        "runtime/try-handle-materialized-object-with-failures-out.json",
        UNSUPPORTED_DW_MODULE_RESOLUTION,
        "imports dw::Natives, which is not resolved by the Python binding",
    ),
    "runtime/try-handle-name-value-pair-value-with-failures-out.json": _exclusion(
        "runtime/try-handle-name-value-pair-value-with-failures-out.json",
        UNSUPPORTED_DW_MODULE_RESOLUTION,
        "imports dw::Natives, which is not resolved by the Python binding",
    ),
    "runtime/try-handle-schema-property-value-with-failures-out.json": _exclusion(
        "runtime/try-handle-schema-property-value-with-failures-out.json",
        UNSUPPORTED_DW_MODULE_RESOLUTION,
        "imports dw::Natives, which is not resolved by the Python binding",
    ),
    "runtime/try-handle-schema-value-with-failures-out.json": _exclusion(
        "runtime/try-handle-schema-value-with-failures-out.json",
        UNSUPPORTED_DW_MODULE_RESOLUTION,
        "imports dw::Natives, which is not resolved by the Python binding",
    ),
    "core-modules/multipart-write-binary-out.json": _exclusion(
        "core-modules/multipart-write-binary-out.json",
        UNSUPPORTED_DW_MODULE_RESOLUTION,
        "cannot resolve dw::core::Assertions before exercising multipart binary output",
    ),
    "core-modules/read-binary-files-out.bin": _exclusion(
        "core-modules/read-binary-files-out.bin",
        UNSUPPORTED_DW_MODULE_RESOLUTION,
        "cannot resolve dw::core::Assertions before reading the binary fixture",
    ),
    "runtime/full-qualified-name-ref-out.json": _exclusion(
        "runtime/full-qualified-name-ref-out.json",
        UNSUPPORTED_DW_MODULE_RESOLUTION,
        "cannot resolve org::mule::weave::v2::libs::lib test modules",
    ),
    "runtime/private_scope_directives-out.xml": _exclusion(
        "runtime/private_scope_directives-out.xml",
        UNSUPPORTED_DW_MODULE_RESOLUTION,
        "cannot resolve dw::Module",
    ),
    "runtime/try-out.json": _exclusion(
        "runtime/try-out.json",
        UNSUPPORTED_DW_MODULE_RESOLUTION,
        "cannot resolve dw::core::Assertions",
    ),
    "runtime/urlEncodeDecode-out.json": _exclusion(
        "runtime/urlEncodeDecode-out.json",
        UNSUPPORTED_DW_MODULE_RESOLUTION,
        "cannot resolve dw::core::Assertions",
    ),
    "runtime/java-big-decimal-out.xml": _exclusion(
        "runtime/java-big-decimal-out.xml",
        UNAVAILABLE_JAVA_MODULE,
        "cannot resolve java::lang::String::valueOf",
    ),
    "runtime/java-field-ref-out.json": _exclusion(
        "runtime/java-field-ref-out.json",
        UNAVAILABLE_JAVA_MODULE,
        "cannot resolve test POJO Constants or java::lang::String",
    ),
    "runtime/java-interop-enum-out.json": _exclusion(
        "runtime/java-interop-enum-out.json",
        UNAVAILABLE_JAVA_MODULE,
        "cannot resolve test POJO GenderEnum or java::lang::String",
    ),
    "runtime/java-interop-function-call-out.json": _exclusion(
        "runtime/java-interop-function-call-out.json",
        UNAVAILABLE_JAVA_MODULE,
        "cannot resolve test POJO MyCompanyUtils or java::lang::String",
    ),
    "runtime/java_epoch_bridge-out.json": _exclusion(
        "runtime/java_epoch_bridge-out.json",
        UNAVAILABLE_JAVA_MODULE,
        "cannot resolve java::time::Instant members",
    ),
    "runtime/runtime_run_coercionException-out.json": _exclusion(
        "runtime/runtime_run_coercionException-out.json",
        UNAVAILABLE_JAVA_MODULE,
        "dw::Runtime.run cannot resolve application/java and returns UnknownContentTypeException",
    ),
    "runtime/runtime_run_fibo-out.json": _exclusion(
        "runtime/runtime_run_fibo-out.json",
        UNAVAILABLE_JAVA_MODULE,
        "dw::Runtime.run cannot resolve application/java and returns UnknownContentTypeException",
    ),
    "runtime/runtime_run_null_java-out.json": _exclusion(
        "runtime/runtime_run_null_java-out.json",
        UNAVAILABLE_JAVA_MODULE,
        "application/java returns UnknownContentTypeException",
    ),
    "runtime/sql_date_mapping-out.json": _exclusion(
        "runtime/sql_date_mapping-out.json",
        UNAVAILABLE_JAVA_MODULE,
        "cannot resolve Java test class org::mule::weave::v2::pojo::SqlDateTest",
    ),
    "runtime/underflow-out.json": _exclusion(
        "runtime/underflow-out.json",
        UNAVAILABLE_JAVA_MODULE,
        "cannot resolve java::lang::Long::{MIN_VALUE,MAX_VALUE}",
    ),
    "runtime/write-function-with-null-out.xml": _exclusion(
        "runtime/write-function-with-null-out.xml",
        UNAVAILABLE_JAVA_MODULE,
        "write(null, application/java) reports unknown content type",
    ),
    "runtime/dw-binary-out.dwl": _exclusion(
        "runtime/dw-binary-out.dwl",
        UNAVAILABLE_CLASSPATH_TEST_RESOURCE,
        "readUrl cannot find classpath://dw-binary/in0.bin",
    ),
    "runtime/read_lines-out.json": _exclusion(
        "runtime/read_lines-out.json",
        UNAVAILABLE_CLASSPATH_TEST_RESOURCE,
        "readUrl cannot find classpath://read_lines/test.txt",
    ),
}


def exclusion_for(case_identifier: str) -> Optional[Exclusion]:
    return EXCLUDED_CASES.get(case_identifier)


def validate_exclusions(
    entries: Mapping[str, object], scenarios: Optional[Iterable[object]] = None
) -> List[str]:
    errors = []
    for identifier, entry in entries.items():
        case_identifier = _entry_field(entry, "case_identifier")
        category = _entry_field(entry, "category")
        reason = _entry_field(entry, "reason")
        if not case_identifier:
            errors.append(f"{identifier}: missing case identity")
        elif case_identifier != identifier:
            errors.append(f"{identifier}: case identity must match registry key")
        if not category:
            errors.append(f"{identifier}: missing category")
        elif category not in SUPPORTED_CATEGORIES:
            errors.append(f"{identifier}: unsupported category {category}")
        if not reason or not reason.strip():
            errors.append(f"{identifier}: missing reason")
    if scenarios is not None:
        discovered = {
            scenario.identifier.rsplit(":", 1)[0]
            for scenario in scenarios
        }
        for identifier in entries:
            if identifier not in discovered:
                errors.append(f"{identifier}: not a discovered runnable case")
    return errors


def validate_structural_module_cases(
    entries: Iterable[str], structural_case_identifiers: Iterable[str]
) -> List[str]:
    registry = set(entries)
    structural_skips = set(structural_case_identifiers)
    errors = [
        f"{identifier}: not a structural skip"
        for identifier in sorted(registry)
        if identifier not in structural_skips
    ]
    errors.extend(
        f"{identifier}: structural module case is not registered"
        for identifier in sorted(structural_skips)
        if identifier not in registry
    )
    return errors


def _entry_field(entry: object, field: str) -> Optional[str]:
    if isinstance(entry, Exclusion):
        return getattr(entry, field)
    return entry.get(field)
