"""Auditable exclusions for TCK cases Python cannot execute."""

from dataclasses import dataclass
from typing import Dict, Iterable, List, Mapping, Optional


MODULE_RESOLUTION_NOT_SUPPORTED = "module-resolution-not-supported"


@dataclass(frozen=True)
class Exclusion:
    category: str
    reason: str


# The binding exposes no module resolver. These cases import test-only DW
# modules, so they cannot be executed until the Python API gains that feature.
_MODULE_CASES = (
    "runtime/implicit_type_parameters-out.json",
    "runtime/import-component-alias-lib-out.json",
    "runtime/import-lib-out.json",
    "runtime/import-lib-with-alias-out.json",
    "runtime/import-named-lib-out.json",
    "runtime/import-star-out.json",
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
    "runtime/module-singleton-out.json",
    "runtime/type_selector_materialize-out.json",
    "runtime/weave_multiple_namespace-out.dwl",
)

EXCLUDED_CASES: Dict[str, Exclusion] = {
    case: Exclusion(
        MODULE_RESOLUTION_NOT_SUPPORTED,
        "imports a test-only DW module; Python binding has no module resolver",
    )
    for case in _MODULE_CASES
}


def exclusion_for(case_identifier: str) -> Optional[Exclusion]:
    return EXCLUDED_CASES.get(case_identifier)


def validate_exclusions(
    entries: Mapping[str, object], scenarios: Optional[Iterable[object]] = None
) -> List[str]:
    errors = []
    for identifier, entry in entries.items():
        category = entry.category if isinstance(entry, Exclusion) else entry.get("category")
        reason = entry.reason if isinstance(entry, Exclusion) else entry.get("reason")
        if not category:
            errors.append(f"{identifier}: missing category")
        if not reason:
            errors.append(f"{identifier}: missing reason")
    if scenarios is not None:
        transforms = {
            scenario.identifier.rsplit(":", 1)[0]: scenario.transform
            for scenario in scenarios
        }
        for identifier, entry in entries.items():
            category = entry.category if isinstance(entry, Exclusion) else entry.get("category")
            if (
                category == MODULE_RESOLUTION_NOT_SUPPORTED
                and identifier in transforms
                and not _imports_module(transforms[identifier])
            ):
                errors.append(
                    f"{identifier}: module-resolution-not-supported requires a DW import"
                )
    return errors


def _imports_module(transform: str) -> bool:
    return any(line.lstrip().startswith("import ") for line in transform.splitlines())
