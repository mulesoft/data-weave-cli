"""Filesystem loader for the Gradle-staged DataWeave TCK corpus."""

from dataclasses import dataclass
from pathlib import Path
import re
from typing import Dict, List, Optional

from dataweave import InputValue


EXTENSION_TO_MIME = {
    "bin": "application/octet-stream",
    "csv": "application/csv",
    "dwl": "application/dw",
    "json": "application/json",
    "multipart": "multipart/form-data",
    "properties": "text/x-java-properties",
    "txt": "text/plain",
    "urlencoded": "application/x-www-form-urlencoded",
    "xml": "application/xml",
}

INPUT_PATTERN = re.compile(r"^in[0-9]+\.[a-zA-Z]+$")
OUTPUT_PATTERN = re.compile(r"^out\.[a-zA-Z]+$")
INPUT_CONFIG_PATTERN = re.compile(r"^in[0-9]+-config\.properties$")
OUTPUT_CONFIG_PATTERN = re.compile(r"^out[0-9]*-config\.properties$")


@dataclass(frozen=True)
class TckScenario:
    identifier: str
    transform: str
    inputs: Dict[str, InputValue]
    expected: bytes
    output_extension: str
    charset: Optional[str]


@dataclass(frozen=True)
class DiscoveredCase:
    identifier: str
    scenarios: List[TckScenario]


@dataclass(frozen=True)
class Discovery:
    cases: List[DiscoveredCase]
    structural_skips: int
    structural_case_identifiers: List[str]
    structural_module_case_identifiers: List[str]


def extension_of(name: str) -> str:
    return Path(name).suffix.removeprefix(".").lower()


def discover_cases(suites_dir: Path) -> Discovery:
    cases: List[DiscoveredCase] = []
    structural_skips = 0
    structural_case_identifiers: List[str] = []
    structural_module_case_identifiers: List[str] = []
    if not suites_dir.exists():
        return Discovery(
            cases,
            structural_skips,
            structural_case_identifiers,
            structural_module_case_identifiers,
        )

    for suite_dir in sorted(path for path in suites_dir.iterdir() if path.is_dir()):
        for case_dir in sorted(path for path in suite_dir.iterdir() if path.is_dir()):
            if not case_dir.exists():
                structural_skips += 1
                structural_case_identifiers.append(f"{suite_dir.name}/{case_dir.name}")
                continue
            scenarios = _load_case(suite_dir.name, case_dir)
            if scenarios is None:
                structural_skips += 1
                identifier = f"{suite_dir.name}/{case_dir.name}"
                structural_case_identifiers.append(identifier)
                if _has_adjacent_dwl_module(case_dir):
                    structural_module_case_identifiers.append(identifier)
            else:
                cases.append(DiscoveredCase(f"{suite_dir.name}/{case_dir.name}", scenarios))
    return Discovery(
        cases,
        structural_skips,
        structural_case_identifiers,
        structural_module_case_identifiers,
    )


def _has_adjacent_dwl_module(case_dir: Path) -> bool:
    return any(
        path.is_file()
        and extension_of(path.name) == "dwl"
        and not INPUT_PATTERN.fullmatch(path.name)
        and not OUTPUT_PATTERN.fullmatch(path.name)
        and path.name != "transform.dwl"
        for path in case_dir.iterdir()
    )


def _load_case(suite_name: str, case_dir: Path) -> Optional[List[TckScenario]]:
    files = {path.name: path for path in case_dir.iterdir() if path.is_file()}
    case_name = case_dir.name
    if case_name.endswith("_wip") or case_name.endswith("wip"):
        return None
    if "config.properties" in files or any(
        INPUT_CONFIG_PATTERN.fullmatch(name) or OUTPUT_CONFIG_PATTERN.fullmatch(name)
        for name in files
    ):
        return None
    if any(extension_of(name) == "groovy" for name in files):
        return None

    transforms = [
        path for name, path in files.items()
        if extension_of(name) == "dwl"
        and not INPUT_PATTERN.fullmatch(name)
        and not OUTPUT_PATTERN.fullmatch(name)
    ]
    if len(transforms) != 1 or "transform.dwl" not in files:
        return None

    input_paths = sorted(
        (path for name, path in files.items() if INPUT_PATTERN.fullmatch(name)),
        key=lambda path: path.name,
    )
    if any(extension_of(path.name) not in EXTENSION_TO_MIME for path in input_paths):
        return None
    output_paths = sorted(
        (
            path
            for name, path in files.items()
            if OUTPUT_PATTERN.fullmatch(name) and extension_of(name) in EXTENSION_TO_MIME
        ),
        key=lambda path: path.name,
    )
    if not output_paths:
        return None

    inputs = {
        path.stem: InputValue(path.read_bytes(), mime_type=EXTENSION_TO_MIME[extension_of(path.name)])
        for path in input_paths
    }
    transform = files["transform.dwl"].read_text(encoding="utf-8")
    encoding = files.get("encoding")
    charset = encoding.read_text(encoding="utf-8").strip() if encoding else None
    case_id = f"{suite_name}/{case_name}"
    return [
        TckScenario(
            identifier=f"{case_id}:{path.name}",
            transform=transform,
            inputs=inputs,
            expected=path.read_bytes(),
            output_extension=extension_of(path.name),
            charset=charset,
        )
        for path in output_paths
    ]
