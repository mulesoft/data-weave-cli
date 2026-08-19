import ctypes
import os
from pathlib import Path
from typing import Optional

from .models import DataWeaveError, DataWeaveLibraryNotFoundError, READ_CALLBACK, WRITE_CALLBACK


_ENV_NATIVE_LIB = "DATAWEAVE_NATIVE_LIB"


class graal_isolate_t(ctypes.Structure):
    pass


class graal_isolatethread_t(ctypes.Structure):
    pass


GraalIsolatePointer = ctypes.POINTER(graal_isolate_t)
GraalIsolateThreadPointer = ctypes.POINTER(graal_isolatethread_t)


def candidate_library_paths() -> list[Path]:
    paths: list[Path] = []
    env_value = (os.environ.get(_ENV_NATIVE_LIB) or "").strip()
    if env_value:
        paths.append(Path(env_value))

    pkg_dir = Path(__file__).resolve().parent
    native_dir = pkg_dir / "native"
    paths.extend(native_dir / name for name in ("dwlib.dylib", "dwlib.so", "dwlib.dll"))

    for parent in pkg_dir.parents:
        build_dir = parent / "build" / "native" / "nativeCompile"
        if build_dir.exists():
            paths.extend(build_dir / name for name in ("dwlib.dylib", "dwlib.so", "dwlib.dll"))
            break

    paths.extend(Path(name) for name in ("dwlib.dylib", "dwlib.so", "dwlib.dll"))
    return paths


def find_library() -> str:
    for path in candidate_library_paths():
        if path.exists() and path.is_file():
            return str(path)
    raise DataWeaveLibraryNotFoundError(
        "Could not find DataWeave native library (dwlib). "
        f"Set {_ENV_NATIVE_LIB} to an absolute path or install a wheel that bundles the native library."
    )


class NativeRuntime:
    """Owns the native library handle, isolate lifecycle, and ctypes ABI."""

    def __init__(self, lib_path: Optional[str] = None):
        self.lib_path = lib_path or find_library()
        self.lib = None
        self.isolate = None
        self.thread = None
        self.initialized = False
        self.has_callback_streaming = False
        self.has_callback_input_output = False

    def initialize(self) -> None:
        if self.initialized:
            return
        try:
            self.lib = ctypes.CDLL(self.lib_path)
        except OSError as error:
            raise DataWeaveError(f"Failed to load library from {self.lib_path}: {error}")
        isolate_created = False
        try:
            self._create_isolate()
            isolate_created = True
            self._setup_functions()
            self.initialized = True
        except Exception:
            if isolate_created:
                self._tear_down_isolate(suppress_errors=True)
            self._reset()
            raise

    def _create_isolate(self) -> None:
        self._require_export("graal_create_isolate")
        self.lib.graal_create_isolate.argtypes = [
            ctypes.c_void_p,
            ctypes.POINTER(GraalIsolatePointer),
            ctypes.POINTER(GraalIsolateThreadPointer),
        ]
        self.lib.graal_create_isolate.restype = ctypes.c_int
        self.isolate = GraalIsolatePointer()
        self.thread = GraalIsolateThreadPointer()
        try:
            result = self.lib.graal_create_isolate(None, ctypes.byref(self.isolate), ctypes.byref(self.thread))
        except Exception as error:
            raise DataWeaveError(f"Failed to create GraalVM isolate: {error}") from error
        if result != 0:
            raise DataWeaveError(f"Failed to create GraalVM isolate. Error code: {result}")

    def _setup_functions(self) -> None:
        self._require_export("run_script")
        self._require_export("free_cstring")
        self._require_export("graal_tear_down_isolate")
        self.lib.run_script.argtypes = [GraalIsolateThreadPointer, ctypes.c_char_p, ctypes.c_char_p]
        self.lib.run_script.restype = ctypes.c_void_p
        self.lib.free_cstring.argtypes = [GraalIsolateThreadPointer, ctypes.c_void_p]
        self.lib.free_cstring.restype = None
        self.lib.graal_tear_down_isolate.argtypes = [GraalIsolateThreadPointer]
        self.lib.graal_tear_down_isolate.restype = ctypes.c_int
        if hasattr(self.lib, "run_script_callback"):
            self._require_streaming_lifecycle_exports("run_script_callback")
            self.lib.run_script_callback.argtypes = [GraalIsolateThreadPointer, ctypes.c_char_p, ctypes.c_char_p, WRITE_CALLBACK, ctypes.c_void_p]
            self.lib.run_script_callback.restype = ctypes.c_void_p
            self.has_callback_streaming = True
        if hasattr(self.lib, "run_script_input_output_callback"):
            self._require_streaming_lifecycle_exports("run_script_input_output_callback")
            self.lib.run_script_input_output_callback.argtypes = [GraalIsolateThreadPointer, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p, READ_CALLBACK, WRITE_CALLBACK, ctypes.c_void_p]
            self.lib.run_script_input_output_callback.restype = ctypes.c_void_p
            self.has_callback_input_output = True

    def _require_export(self, name: str) -> None:
        if not hasattr(self.lib, name):
            raise DataWeaveError(f"Native library does not export {name}")

    def _require_streaming_lifecycle_exports(self, callback_name: str) -> None:
        for name in ("free_cstring", "graal_attach_thread", "graal_detach_thread"):
            if not hasattr(self.lib, name):
                raise DataWeaveError(f"{callback_name} requires native export {name}")
        self.lib.graal_attach_thread.argtypes = [GraalIsolatePointer, ctypes.POINTER(GraalIsolateThreadPointer)]
        self.lib.graal_attach_thread.restype = ctypes.c_int
        self.lib.graal_detach_thread.argtypes = [GraalIsolateThreadPointer]
        self.lib.graal_detach_thread.restype = ctypes.c_int

    def attach_thread(self):
        worker_thread = GraalIsolateThreadPointer()
        try:
            result = self.lib.graal_attach_thread(self.isolate, ctypes.byref(worker_thread))
        except Exception as error:
            raise DataWeaveError(f"Failed to attach worker thread to isolate: {error}") from error
        if result != 0:
            raise DataWeaveError(f"Failed to attach worker thread to isolate (code {result})")
        return worker_thread

    def detach_thread(self, thread) -> None:
        try:
            result = self.lib.graal_detach_thread(thread)
        except Exception as error:
            raise DataWeaveError(f"Failed to detach worker thread from isolate: {error}") from error
        if result != 0:
            raise DataWeaveError(f"Failed to detach worker thread from isolate. Error code: {result}")

    def decode_and_free(self, ptr, thread=None) -> str:
        if not ptr:
            return ""
        primary_error = None
        try:
            return ctypes.string_at(ptr).decode("utf-8")
        except Exception as error:
            primary_error = error
            raise
        finally:
            try:
                if self.lib is not None:
                    self.lib.free_cstring(thread or self.thread, ptr)
            except Exception:
                if primary_error is None:
                    raise

    def run_script(self, thread, script: bytes, inputs: bytes):
        return self.lib.run_script(thread, script, inputs)

    def run_script_callback(self, thread, script: bytes, inputs: bytes, write_callback):
        return self.lib.run_script_callback(thread, script, inputs, write_callback, None)

    def run_script_input_output_callback(self, thread, script: bytes, inputs: bytes, input_name: bytes, input_mime_type: bytes, input_charset: Optional[bytes], read_callback, write_callback):
        return self.lib.run_script_input_output_callback(
            thread, script, inputs, input_name, input_mime_type, input_charset, read_callback, write_callback, None,
        )

    def cleanup(self) -> None:
        if not self.initialized:
            return
        try:
            self._tear_down_isolate()
        finally:
            self._reset()

    def _tear_down_isolate(self, suppress_errors: bool = False) -> None:
        if self.thread is None:
            return
        try:
            result = self.lib.graal_tear_down_isolate(self.thread)
            if result != 0:
                raise DataWeaveError(f"Failed to tear down GraalVM isolate. Error code: {result}")
        except DataWeaveError:
            if not suppress_errors:
                raise
        except Exception as error:
            if not suppress_errors:
                raise DataWeaveError(f"Failed to tear down GraalVM isolate: {error}") from error

    def _reset(self) -> None:
        self.initialized = False
        self.thread = None
        self.isolate = None
        self.lib = None
        self.has_callback_streaming = False
        self.has_callback_input_output = False
