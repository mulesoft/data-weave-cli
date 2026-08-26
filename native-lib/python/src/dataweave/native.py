import ctypes
from contextlib import contextmanager
import os
from pathlib import Path
import sys
from threading import current_thread, get_ident, Lock
import traceback
from typing import Optional

from .models import DataWeaveError, DataWeaveLibraryNotFoundError, READ_CALLBACK, RESOLVE_MODULE_CALLBACK, WRITE_CALLBACK
from .resolver import ModuleResolver


_ENV_NATIVE_LIB = "DATAWEAVE_NATIVE_LIB"


class graal_isolate_t(ctypes.Structure):
    pass


class graal_isolatethread_t(ctypes.Structure):
    pass


GraalIsolatePointer = ctypes.POINTER(graal_isolate_t)
GraalIsolateThreadPointer = ctypes.POINTER(graal_isolatethread_t)


# ── Process-wide shared isolate (one per process, N handle-addressed engines) ──
# All mutations happen under _isolate_lock. Invariant: _isolate_ref_count equals
# the number of live engines across all DataWeave instances, and _isolate is not
# None iff the count > 0.
_isolate_lock = Lock()
_lib = None
_lib_path = None
_isolate = None
_isolate_thread = None          # the main attached IsolateThread (GraalIsolateThreadPointer)
_isolate_owner_thread = None    # the Python threading.Thread that created the isolate
_isolate_ref_count = 0


def _bind_abi(lib) -> None:
    """Binds argtypes/restypes for the engine ABI and lifecycle exports (once)."""
    for name in ("graal_create_isolate", "graal_attach_thread", "graal_detach_thread",
                 "graal_tear_down_isolate", "free_cstring",
                 "create_engine", "create_engine_with_resolver", "destroy_engine",
                 "run_script_engine", "run_script_callback_engine",
                 "run_script_input_output_callback_engine"):
        if not hasattr(lib, name):
            raise DataWeaveError(f"Native library does not export {name}")

    lib.graal_create_isolate.argtypes = [
        ctypes.c_void_p,
        ctypes.POINTER(GraalIsolatePointer),
        ctypes.POINTER(GraalIsolateThreadPointer),
    ]
    lib.graal_create_isolate.restype = ctypes.c_int
    lib.graal_attach_thread.argtypes = [GraalIsolatePointer, ctypes.POINTER(GraalIsolateThreadPointer)]
    lib.graal_attach_thread.restype = ctypes.c_int
    lib.graal_detach_thread.argtypes = [GraalIsolateThreadPointer]
    lib.graal_detach_thread.restype = ctypes.c_int
    lib.graal_tear_down_isolate.argtypes = [GraalIsolateThreadPointer]
    lib.graal_tear_down_isolate.restype = ctypes.c_int
    lib.free_cstring.argtypes = [GraalIsolateThreadPointer, ctypes.c_void_p]
    lib.free_cstring.restype = None

    lib.create_engine.argtypes = [GraalIsolateThreadPointer]
    lib.create_engine.restype = ctypes.c_int64
    lib.create_engine_with_resolver.argtypes = [
        GraalIsolateThreadPointer, RESOLVE_MODULE_CALLBACK, ctypes.c_void_p,
    ]
    lib.create_engine_with_resolver.restype = ctypes.c_int64
    lib.destroy_engine.argtypes = [GraalIsolateThreadPointer, ctypes.c_int64]
    lib.destroy_engine.restype = None
    lib.run_script_engine.argtypes = [
        GraalIsolateThreadPointer, ctypes.c_int64, ctypes.c_char_p, ctypes.c_char_p,
    ]
    lib.run_script_engine.restype = ctypes.c_void_p
    lib.run_script_callback_engine.argtypes = [
        GraalIsolateThreadPointer, ctypes.c_int64, ctypes.c_char_p, ctypes.c_char_p,
        WRITE_CALLBACK, ctypes.c_void_p,
    ]
    lib.run_script_callback_engine.restype = ctypes.c_void_p
    lib.run_script_input_output_callback_engine.argtypes = [
        GraalIsolateThreadPointer, ctypes.c_int64, ctypes.c_char_p, ctypes.c_char_p,
        ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p,
        READ_CALLBACK, WRITE_CALLBACK, ctypes.c_void_p,
    ]
    lib.run_script_input_output_callback_engine.restype = ctypes.c_void_p


def _acquire_isolate(lib_path: str):
    """Returns (lib, isolate, thread, owner_thread), creating the shared isolate on
    the first reference. Increments the refcount only on success."""
    global _lib, _lib_path, _isolate, _isolate_thread, _isolate_owner_thread, _isolate_ref_count
    with _isolate_lock:
        if _isolate is None:
            try:
                lib = ctypes.CDLL(lib_path)
            except OSError as error:
                raise DataWeaveError(f"Failed to load library from {lib_path}: {error}")
            _bind_abi(lib)
            isolate = GraalIsolatePointer()
            thread = GraalIsolateThreadPointer()
            try:
                result = lib.graal_create_isolate(None, ctypes.byref(isolate), ctypes.byref(thread))
            except Exception as error:
                raise DataWeaveError(f"Failed to create GraalVM isolate: {error}") from error
            if result != 0:
                raise DataWeaveError(f"Failed to create GraalVM isolate. Error code: {result}")
            _lib = lib
            _lib_path = lib_path
            _isolate = isolate
            _isolate_thread = thread
            _isolate_owner_thread = current_thread()
        _isolate_ref_count += 1
        return _lib, _isolate, _isolate_thread, _isolate_owner_thread


def _release_isolate() -> None:
    """Decrements the refcount; tears the isolate down and nulls globals on 0."""
    global _lib, _lib_path, _isolate, _isolate_thread, _isolate_owner_thread, _isolate_ref_count
    with _isolate_lock:
        if _isolate_ref_count == 0:
            return
        _isolate_ref_count -= 1
        if _isolate_ref_count > 0:
            return
        # Last release: tear down from the owner thread if we are on it, else a
        # fresh attached thread. Then clear globals regardless.
        lib, isolate, main_thread, owner = _lib, _isolate, _isolate_thread, _isolate_owner_thread
        try:
            if current_thread() is owner:
                _tear_down(lib, main_thread)
            else:
                worker = GraalIsolateThreadPointer()
                if lib.graal_attach_thread(isolate, ctypes.byref(worker)) != 0:
                    raise DataWeaveError("Failed to attach thread for isolate teardown")
                _tear_down(lib, worker)
        except BaseException:
            print(
                "DataWeave: GraalVM isolate teardown failed; the isolate reference "
                "has been cleared and a fresh isolate will be created on the next "
                "initialize().",
                file=sys.stderr,
            )
            raise
        finally:
            _lib = _lib_path = _isolate = _isolate_thread = _isolate_owner_thread = None


def _tear_down(lib, thread) -> None:
    if thread is None:
        return
    result = lib.graal_tear_down_isolate(thread)
    if result != 0:
        raise DataWeaveError(f"Failed to tear down GraalVM isolate. Error code: {result}")


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
        self._owner_thread = None
        self.handle = 0
        self.initialized = False
        # Every engine supports every API now (single unified ABI).
        self.has_callback_streaming = True
        self.has_callback_input_output = True
        self.has_module_resolver = True
        self._resolver = None
        self._resolver_callback = None
        self._resolver_token = 0
        self._resolver_buffers = []
        self._resolver_active = False
        self._resolver_active_ident = None
        self._resolver_lock = Lock()
        self._execution_owner = None

    def initialize(self) -> None:
        if self.initialized:
            return
        self.lib, self.isolate, self.thread, self._owner_thread = _acquire_isolate(self.lib_path)
        try:
            self.handle = self._create_engine()
        except Exception:
            # Roll back the ref we just took so a failed init leaks nothing.
            self.lib = self.isolate = self.thread = self._owner_thread = None
            _release_isolate()
            raise
        self.initialized = True

    def _create_engine(self) -> int:
        # Engines without a resolver are created here; the resolver variant is
        # installed by install_resolver() (Task 4) before this is called.
        with self._current_thread_attachment(self.thread) as thread:
            try:
                handle = self.lib.create_engine(thread)
            except Exception as error:
                raise DataWeaveError(f"Failed to create DataWeave engine: {error}") from error
        if not handle:
            raise DataWeaveError("Native create_engine returned a null handle")
        return handle

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

    def run_engine_and_decode(self, script: bytes, inputs: bytes) -> str:
        with self._serialized_native_operation():
            with self._current_thread_attachment(self.thread) as thread:
                with self._resolver_scope():
                    return self.decode_and_free(
                        self.lib.run_script_engine(thread, self.handle, script, inputs),
                        thread,
                    )

    def run_callback_engine_and_decode(self, thread, script: bytes, inputs: bytes, write_callback) -> str:
        with self._serialized_native_operation():
            with self._current_thread_attachment(thread) as current:
                return self.decode_and_free(
                    self.lib.run_script_callback_engine(
                        current, self.handle, script, inputs, write_callback, None
                    ),
                    current,
                )

    def run_input_output_callback_engine_and_decode(
        self, thread, script: bytes, inputs: bytes, input_name: bytes,
        input_mime_type: bytes, input_charset: Optional[bytes], read_callback, write_callback,
    ) -> str:
        with self._serialized_native_operation():
            with self._current_thread_attachment(thread) as current:
                return self.decode_and_free(
                    self.lib.run_script_input_output_callback_engine(
                        current, self.handle, script, inputs, input_name,
                        input_mime_type, input_charset, read_callback, write_callback, None,
                    ),
                    current,
                )

    @contextmanager
    def _resolver_scope(self):
        yield

    def run_script_with_resolver(self, thread, script: bytes, inputs: bytes, resolver: ModuleResolver):
        with self._serialized_native_operation():
            return self._run_script_with_resolver(thread, script, inputs, resolver)

    def run_script_with_resolver_and_decode(self, thread, script: bytes, inputs: bytes, resolver: ModuleResolver) -> str:
        with self._serialized_native_operation():
            with self._current_thread_attachment(thread) as current_thread:
                return self.decode_and_free(
                    self._run_script_with_resolver(current_thread, script, inputs, resolver),
                    current_thread,
                )

    def _run_script_with_resolver(self, thread, script: bytes, inputs: bytes, resolver: ModuleResolver):
        if not self.has_module_resolver:
            raise DataWeaveError(
                "Native library does not support module resolver API "
                "(run_script_with_resolver not found)."
            )
        if self._module_resolver is None:
            self._module_resolver = resolver
            self._module_resolver_callback = self._create_module_resolver_callback(resolver)
        elif self._module_resolver is not resolver:
            raise DataWeaveError("Native runtime already has a different module resolver")

        self._resolver_buffers.clear()
        self._resolver_active = True
        try:
            return self.lib.run_script_with_resolver(
                thread, script, inputs, self._module_resolver_callback
            )
        finally:
            self._resolver_active = False
            self._resolver_buffers.clear()

    def _create_module_resolver_callback(self, resolver: ModuleResolver):
        def resolve(_thread, module_path):
            try:
                if not self._resolver_active:
                    return None
                path = module_path.decode("utf-8")
                if path.startswith("/"):
                    path = path[1:]
                source = resolver(path)
                if not isinstance(source, str):
                    return None
                buffer = ctypes.create_string_buffer(source.encode("utf-8"))
                self._resolver_buffers.append(buffer)
                return ctypes.addressof(buffer)
            except BaseException:
                try:
                    if os.environ.get("DATAWEAVE_RESOLVER_DEBUG") == "1":
                        traceback.print_exc()
                    else:
                        print("DataWeave module resolver callback failed.", file=sys.stderr)
                except BaseException:
                    pass
                return None

        return RESOLVE_MODULE_CALLBACK(resolve)

    def cleanup(self) -> None:
        with self._serialized_native_operation():
            if not self.initialized:
                return
            self.initialized = False
            try:
                if self.handle:
                    with self._current_thread_attachment(self.thread) as thread:
                        self.lib.destroy_engine(thread, self.handle)
            finally:
                # Release the isolate ref even if destroy_engine throws, so a
                # throwing destroy cannot strand the isolate.
                self.lib = self.isolate = self.thread = self._owner_thread = None
                self._resolver = None
                self._resolver_callback = None
                self._resolver_buffers = []
                self._resolver_active = False
                self._resolver_active_ident = None
                _release_isolate()

    @contextmanager
    def _serialized_native_operation(self):
        owner = get_ident()
        if getattr(self, "_execution_owner", None) == owner:
            raise DataWeaveError("Reentrant DataWeave execution is not supported.")
        if not hasattr(self, "_resolver_lock"):
            self._resolver_lock = Lock()
        with self._resolver_lock:
            self._execution_owner = owner
            try:
                yield
            finally:
                self._execution_owner = None

    @contextmanager
    def _current_thread_attachment(self, thread):
        owner = getattr(self, "_owner_thread", current_thread())
        if current_thread() is owner or thread is not self.thread:
            yield thread
            return

        attached_thread = self.attach_thread()
        primary_error = None
        try:
            yield attached_thread
        except BaseException as error:
            primary_error = error
            raise
        finally:
            try:
                self.detach_thread(attached_thread)
            except Exception:
                if primary_error is None:
                    raise

