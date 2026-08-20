import base64
import ctypes
from dataclasses import dataclass
from typing import Callable, Dict, Generator, Optional, Union


class DataWeaveError(Exception):
    pass


class DataWeaveScriptError(DataWeaveError):
    """Raised when a DataWeave script fails (compile or runtime error).

    Carries the full result object so callers can inspect details.
    """

    def __init__(self, result):
        self.result = result
        super().__init__(result.error or "Script execution failed")


class DataWeaveLibraryNotFoundError(Exception):
    pass


# ctypes callback signatures matching NativeCallbacks.WriteCallback / ReadCallback.
# Buffer parameters use c_void_p (not c_char_p) because ctypes gives c_char_p
# special treatment that prevents writing into the buffer.
# int (*WriteCallback)(void *ctx, const char *buffer, int length)
WRITE_CALLBACK = ctypes.CFUNCTYPE(ctypes.c_int, ctypes.c_void_p, ctypes.c_void_p, ctypes.c_int)
# int (*ReadCallback)(void *ctx, char *buffer, int bufferSize)
READ_CALLBACK = ctypes.CFUNCTYPE(ctypes.c_int, ctypes.c_void_p, ctypes.c_void_p, ctypes.c_int)


WriteCallback = Callable[[bytes], int]
ReadCallback = Callable[[int], bytes]


@dataclass
class InputValue:
    content: Union[str, bytes]
    mime_type: Optional[str] = None
    charset: Optional[str] = None
    properties: Optional[Dict[str, Union[str, int, bool]]] = None

    def encode_content(self) -> str:
        if isinstance(self.content, bytes):
            raw = self.content
        else:
            raw = self.content.encode(self.charset or "utf-8")
        return base64.b64encode(raw).decode("ascii")


@dataclass(repr=False)
class ExecutionResult:
    success: bool
    result: Optional[str]
    error: Optional[str]
    binary: bool
    mime_type: Optional[str]
    charset: Optional[str]

    def __repr__(self):
        if not self.success:
            return f"ExecutionResult(success=False, error={self.error!r})"
        preview = (self.result[:50] + "...") if self.result and len(self.result) > 50 else self.result
        return f"ExecutionResult(success=True, mime_type={self.mime_type!r}, charset={self.charset!r}, result={preview!r})"

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


@dataclass
class StreamingResult:
    """Metadata returned after a streaming execution completes."""
    success: bool
    error: Optional[str]
    mime_type: Optional[str]
    charset: Optional[str]
    binary: bool


class Stream:
    """Wrapper around a streaming generator that captures metadata.

    Iterate to consume output chunks. After iteration completes,
    access ``.metadata`` for the :class:`StreamingResult`.
    """

    def __init__(self, gen: Generator[bytes, None, StreamingResult]):
        self._gen = gen
        self._metadata: Optional[StreamingResult] = None

    def __iter__(self):
        return self

    def __next__(self) -> bytes:
        try:
            return next(self._gen)
        except StopIteration as e:
            self._metadata = e.value
            raise

    @property
    def metadata(self) -> Optional[StreamingResult]:
        return self._metadata

    def close(self) -> None:
        """Stop consuming output and request bounded worker cleanup."""
        on_close = getattr(self, "_on_close", None)
        if on_close is not None:
            on_close()
        try:
            self._gen.close()
        except Exception:
            # Stream cancellation is best-effort; native calls cannot be forcibly
            # interrupted from Python and cleanup must not escape finalization.
            pass

    def __enter__(self):
        return self

    def __exit__(self, _exc_type, _exc_val, _exc_tb):
        self.close()
        return False

    def __del__(self):
        self.close()

    _close = close
