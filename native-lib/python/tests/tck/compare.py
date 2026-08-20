"""Output comparators for the formats emitted by the staged TCK corpus."""

import json
import xml.etree.ElementTree as ElementTree
from dataclasses import dataclass
from typing import Any, Optional


@dataclass(frozen=True)
class CompareResult:
    match: bool
    detail: str = ""


def compare_output(
    extension: str, actual: bytes, expected: bytes, charset: Optional[str] = None
) -> CompareResult:
    extension = extension.removeprefix(".").lower()
    if extension == "bin":
        return _result(actual == expected, "binary mismatch")
    if extension not in {
        "json", "xml", "csv", "txt", "dwl", "properties", "urlencoded", "multipart"
    }:
        return CompareResult(False, f"unknown output extension: {extension}")

    encoding = _python_encoding(charset)
    try:
        actual_text = actual.decode(encoding)
        expected_text = expected.decode(encoding)
    except UnicodeDecodeError as error:
        return CompareResult(False, f"cannot decode output as {encoding}: {error}")
    if extension == "json":
        return _compare_json(actual_text, expected_text)
    if extension == "xml":
        return _compare_xml(actual_text, expected_text)
    if extension == "dwl":
        return _result(_strip_whitespace(actual_text) == _strip_whitespace(expected_text), "DWL mismatch")
    return _result(_normalize_text(actual_text) == _normalize_text(expected_text), "text mismatch")


def _compare_json(actual: str, expected: str) -> CompareResult:
    try:
        actual_value = json.loads(actual)
    except json.JSONDecodeError as error:
        return CompareResult(False, f"actual is not valid JSON: {error}")
    try:
        expected_value = json.loads(expected)
    except json.JSONDecodeError as error:
        return CompareResult(False, f"expected is not valid JSON: {error}")
    return _result(_json_equal(actual_value, expected_value), "JSON mismatch")


def _compare_xml(actual: str, expected: str) -> CompareResult:
    try:
        actual_value = _xml_value(ElementTree.fromstring(actual))
    except ElementTree.ParseError as error:
        return CompareResult(False, f"actual is not valid XML: {error}")
    try:
        expected_value = _xml_value(ElementTree.fromstring(expected))
    except ElementTree.ParseError as error:
        return CompareResult(False, f"expected is not valid XML: {error}")
    return _result(actual_value == expected_value, "XML mismatch")


def _xml_value(element: ElementTree.Element) -> Any:
    return (
        element.tag,
        tuple(sorted(element.attrib.items())),
        (element.text or "").strip(),
        tuple(_xml_value(child) for child in element),
    )


def _json_equal(actual: Any, expected: Any) -> bool:
    if isinstance(actual, str) and isinstance(expected, str):
        return _normalize_eol(actual) == _normalize_eol(expected)
    if isinstance(actual, list) and isinstance(expected, list):
        return len(actual) == len(expected) and all(
            _json_equal(left, right) for left, right in zip(actual, expected)
        )
    if isinstance(actual, dict) and isinstance(expected, dict):
        return actual.keys() == expected.keys() and all(
            _json_equal(actual[key], expected[key]) for key in actual
        )
    return actual == expected


def _python_encoding(charset: Optional[str]) -> str:
    return (charset or "utf-8").replace("UTF-16", "utf-16")


def _normalize_eol(value: str) -> str:
    return value.replace("\r\n", "\n")


def _normalize_text(value: str) -> str:
    return _normalize_eol(value).strip()


def _strip_whitespace(value: str) -> str:
    return "".join(value.split())


def _result(match: bool, detail: str) -> CompareResult:
    return CompareResult(match, "" if match else detail)
