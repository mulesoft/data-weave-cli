"""Public facade for the DataWeave Python native binding."""

import ctypes

from .encoding import normalize_input_value as _normalize_input_value
from .encoding import parse_native_encoded_response as _parse_native_encoded_response
from .encoding import parse_streaming_result as _parse_streaming_result
from .models import (
    READ_CALLBACK,
    RESOLVE_MODULE_CALLBACK,
    WRITE_CALLBACK,
    DataWeaveError,
    DataWeaveLibraryNotFoundError,
    DataWeaveScriptError,
    ExecutionResult,
    InputValue,
    ReadCallback,
    Stream,
    StreamingResult,
    WriteCallback,
)
from .native import candidate_library_paths as _candidate_library_paths
from .native import find_library as _find_library
from .resolver import (
    ModuleResolver,
    compose_resolvers,
    modules_from_directory,
    modules_from_jars,
    modules_from_map,
)
from .runtime import DataWeave


__all__ = [
    "DataWeave", "DataWeaveError", "DataWeaveLibraryNotFoundError", "DataWeaveScriptError",
    "ExecutionResult", "InputValue", "ReadCallback", "Stream", "StreamingResult", "WriteCallback",
    "READ_CALLBACK", "RESOLVE_MODULE_CALLBACK", "WRITE_CALLBACK", "ModuleResolver", "compose_resolvers",
    "modules_from_directory", "modules_from_jars", "modules_from_map",
]
