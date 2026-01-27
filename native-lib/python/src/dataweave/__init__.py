"""
DataWeave Python Module

A simple Python wrapper for executing DataWeave scripts via the native library.
This module abstracts all GraalVM and native library complexity, providing a
clean Python API for executing DataWeave scripts with or without inputs.

Basic Usage:
    import dataweave

    result = dataweave.run_script("2 + 2")
    print(result.get_string())
"""

import base64
import ctypes
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional, Union


class DataWeaveError(Exception):
    pass


class DataWeaveLibraryNotFoundError(Exception):
    pass


_ENV_NATIVE_LIB = "DATAWEAVE_NATIVE_LIB"


@dataclass
class InputValue:
    content: Union[str, bytes]
    mimeType: Optional[str] = None
    charset: Optional[str] = None
    properties: Optional[Dict[str, Union[str, int, bool]]] = None

    def encode_content(self) -> str:
        if isinstance(self.content, bytes):
            raw = self.content
        else:
            raw = self.content.encode(self.charset or "utf-8")
        return base64.b64encode(raw).decode("ascii")


@dataclass
class ExecutionResult:
    success: bool
    result: Optional[str]
    error: Optional[str]
    binary: bool
    mimeType: Optional[str]
    charset: Optional[str]

    def get_bytes(self) -> Optional[bytes]:
        if not self.success or self.result is None:
            return None
        return base64.b64decode(self.result)

    def get_string(self) -> Optional[str]:
        if not self.success or self.result is None:
            return None
        if self.binary:
            return self.result
        return self.get_bytes().decode(self.charset or "utf-8")


def _parse_native_encoded_response(raw: str) -> ExecutionResult:
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
        mimeType=parsed.get("mimeType"),
        charset=parsed.get("charset"),
    )


def _candidate_library_paths() -> list[Path]:
    paths: list[Path] = []

    env_value = (__import__("os").environ.get(_ENV_NATIVE_LIB) or "").strip()
    if env_value:
        paths.append(Path(env_value))

    pkg_dir = Path(__file__).resolve().parent
    native_dir = pkg_dir / "native"
    paths.append(native_dir / "dwlib.dylib")
    paths.append(native_dir / "dwlib.so")
    paths.append(native_dir / "dwlib.dll")

    # Dev fallback: if this package is being used from the data-weave-cli repo
    # tree, locate native-lib/build/native/nativeCompile.
    for parent in pkg_dir.parents:
        build_dir = parent / "build" / "native" / "nativeCompile"
        if build_dir.name == "nativeCompile" and build_dir.parent.name == "native" and build_dir.parent.parent.name == "build":
            paths.append(build_dir / "dwlib.dylib")
            paths.append(build_dir / "dwlib.so")
            paths.append(build_dir / "dwlib.dll")
            break

    # CWD fallback
    paths.append(Path("dwlib.dylib"))
    paths.append(Path("dwlib.so"))
    paths.append(Path("dwlib.dll"))

    return paths


def _find_library() -> str:
    for p in _candidate_library_paths():
        if p.exists() and p.is_file():
            return str(p)

    raise DataWeaveLibraryNotFoundError(
        "Could not find DataWeave native library (dwlib). "
        f"Set {_ENV_NATIVE_LIB} to an absolute path or install a wheel that bundles the native library."
    )


def _normalize_input_value(value: Any, mime_type: Optional[str] = None) -> Dict[str, Any]:
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
            "mimeType": value.mimeType or mime_type,
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
        "properties": None,
    }


class DataWeave:
    def __init__(self, lib_path: Optional[str] = None):
        self._lib_path = lib_path or _find_library()
        self._lib = None
        self._isolate = None
        self._thread = None
        self._initialized = False

    def _load_library(self):
        try:
            self._lib = ctypes.CDLL(self._lib_path)
        except OSError as e:
            raise DataWeaveError(f"Failed to load library from {self._lib_path}: {e}")

    def _setup_graal_structures(self):
        class graal_isolate_t(ctypes.Structure):
            pass

        class graal_isolatethread_t(ctypes.Structure):
            pass

        self._graal_isolate_t_ptr = ctypes.POINTER(graal_isolate_t)
        self._graal_isolatethread_t_ptr = ctypes.POINTER(graal_isolatethread_t)

    def _create_isolate(self):
        self._lib.graal_create_isolate.argtypes = [
            ctypes.c_void_p,
            ctypes.POINTER(self._graal_isolate_t_ptr),
            ctypes.POINTER(self._graal_isolatethread_t_ptr),
        ]
        self._lib.graal_create_isolate.restype = ctypes.c_int

        self._isolate = self._graal_isolate_t_ptr()
        self._thread = self._graal_isolatethread_t_ptr()

        result = self._lib.graal_create_isolate(None, ctypes.byref(self._isolate), ctypes.byref(self._thread))
        if result != 0:
            raise DataWeaveError(f"Failed to create GraalVM isolate. Error code: {result}")

    def _setup_functions(self):
        if not hasattr(self._lib, "run_script"):
            raise DataWeaveError("Native library does not export run_script")

        self._lib.run_script.argtypes = [
            self._graal_isolatethread_t_ptr,
            ctypes.c_char_p,
            ctypes.c_char_p,
        ]
        self._lib.run_script.restype = ctypes.c_void_p

        if hasattr(self._lib, "free_cstring"):
            self._lib.free_cstring.argtypes = [self._graal_isolatethread_t_ptr, ctypes.c_void_p]
            self._lib.free_cstring.restype = None

    def _decode_and_free(self, ptr: Optional[int]) -> str:
        if not ptr:
            return ""

        try:
            result_bytes = ctypes.string_at(ptr)
            return result_bytes.decode("utf-8")
        finally:
            if self._lib is not None and hasattr(self._lib, "free_cstring"):
                self._lib.free_cstring(self._thread, ptr)

    def initialize(self):
        if self._initialized:
            return

        self._load_library()
        self._setup_graal_structures()
        self._create_isolate()
        self._setup_functions()
        self._initialized = True

    def cleanup(self):
        if not self._initialized:
            return

        if hasattr(self._lib, "graal_detach_thread") and self._thread:
            try:
                self._lib.graal_detach_thread.argtypes = [self._graal_isolatethread_t_ptr]
                self._lib.graal_detach_thread.restype = ctypes.c_int
                self._lib.graal_detach_thread(self._thread)
            except Exception:
                pass

        self._initialized = False
        self._thread = None
        self._isolate = None
        self._lib = None

    def run(self, script: str, inputs: Optional[Dict[str, Any]] = None) -> ExecutionResult:
        if not self._initialized:
            raise DataWeaveError("DataWeave runtime not initialized. Call initialize() first.")

        if inputs is None:
            inputs = {}

        normalized_inputs = {key: _normalize_input_value(val) for key, val in inputs.items()}
        inputs_json = json.dumps(normalized_inputs)

        try:
            result_ptr = self._lib.run_script(
                self._thread,
                script.encode("utf-8"),
                inputs_json.encode("utf-8"),
            )
            raw = self._decode_and_free(result_ptr)
            return _parse_native_encoded_response(raw)
        except Exception as e:
            raise DataWeaveError(f"Failed to execute script: {e}")

    def __enter__(self):
        self.initialize()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cleanup()
        return False


_global_instance: Optional[DataWeave] = None


def _get_global_instance() -> DataWeave:
    global _global_instance
    if _global_instance is None:
        _global_instance = DataWeave()
        _global_instance.initialize()
    return _global_instance


def run_script(script: str, inputs: Optional[Dict[str, Any]] = None) -> ExecutionResult:
    return _get_global_instance().run(script, inputs)


def cleanup():
    global _global_instance
    if _global_instance is not None:
        _global_instance.cleanup()
        _global_instance = None


__all__ = [
    "DataWeaveError",
    "DataWeaveLibraryNotFoundError",
    "ExecutionResult",
    "InputValue",
    "run_script",
    "cleanup",
]
