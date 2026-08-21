import base64
import json
from typing import Any, Dict, Optional

from .models import DataWeaveError, ExecutionResult, InputValue, StreamingResult


def parse_native_encoded_response(raw: str) -> ExecutionResult:
    if raw is None:
        return ExecutionResult(False, None, "Native returned null", False, None, None)

    if raw == "":
        return ExecutionResult(False, None, "Native returned empty response", False, None, None)

    try:
        parsed = json.loads(raw)
    except Exception as e:
        return ExecutionResult(False, None, f"Failed to parse native JSON response: {e}", False, None, None)

    if not isinstance(parsed, dict):
        return ExecutionResult(False, None, "Native response JSON is not an object", False, None, None)

    success = bool(parsed.get("success", False))
    if not success:
        return ExecutionResult(False, None, parsed.get("error"), False, None, None)

    return ExecutionResult(
        success=True,
        result=parsed.get("result"),
        error=None,
        binary=bool(parsed.get("binary", False)),
        mime_type=parsed.get("mimeType"),
        charset=parsed.get("charset"),
    )


def parse_streaming_result(meta: dict) -> StreamingResult:
    success = meta.get("success", False)
    if not success:
        return StreamingResult(
            success=False,
            error=meta.get("error"),
            mime_type=None,
            charset=None,
            binary=False,
        )
    return StreamingResult(
        success=True,
        error=None,
        mime_type=meta.get("mimeType"),
        charset=meta.get("charset"),
        binary=meta.get("binary", False),
    )


def normalize_input_value(value: Any, mime_type: Optional[str] = None) -> Dict[str, Any]:
    if isinstance(value, dict):
        allowed_keys = {"content", "mimeType", "charset", "properties"}
        extra_keys = set(value.keys()) - allowed_keys
        if extra_keys:
            raise DataWeaveError(
                "Explicit input dict contains unsupported keys: " + ", ".join(sorted(extra_keys))
            )

        if "content" in value or "mimeType" in value:
            if "content" not in value or "mimeType" not in value:
                raise DataWeaveError(
                    "Explicit input dict must include both 'content' and 'mimeType'"
                )

            raw_content = value.get("content")
            charset = value.get("charset") or "utf-8"
            if isinstance(raw_content, bytes):
                encoded_content = base64.b64encode(raw_content).decode("ascii")
            else:
                encoded_content = base64.b64encode(str(raw_content).encode(charset)).decode("ascii")

            normalized: Dict[str, Any] = {
                "content": encoded_content,
                "mimeType": value.get("mimeType"),
            }
            if "charset" in value:
                normalized["charset"] = value.get("charset")
            if "properties" in value:
                normalized["properties"] = value.get("properties")
            return normalized

    if isinstance(value, InputValue):
        out: Dict[str, Any] = {
            "content": value.encode_content(),
            "mimeType": value.mime_type or mime_type,
        }
        if value.charset is not None:
            out["charset"] = value.charset
        if value.properties is not None:
            out["properties"] = value.properties
        return out

    if isinstance(value, str):
        content = value
        default_mime = "text/plain"
    elif isinstance(value, (int, float, bool)):
        content = json.dumps(value)
        default_mime = "application/json"
    elif value is None:
        content = "null"
        default_mime = "application/json"
    else:
        try:
            content = json.dumps(value)
            default_mime = "application/json"
        except (TypeError, ValueError):
            content = str(value)
            default_mime = "text/plain"

    charset = "utf-8"
    encoded_content = base64.b64encode(content.encode(charset)).decode("ascii")

    return {
        "content": encoded_content,
        "mimeType": mime_type or default_mime,
        "charset": charset,
    }
