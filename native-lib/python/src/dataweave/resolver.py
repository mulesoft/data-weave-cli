"""Synchronous external DataWeave module resolver factories."""

from collections.abc import Callable, Mapping, Sequence
import os
from pathlib import Path
from typing import Optional, Union
from zipfile import BadZipFile, ZipFile


ModuleResolver = Callable[[str], Optional[str]]


def modules_from_map(modules: Mapping[str, str]) -> ModuleResolver:
    """Create a resolver backed by an immutable snapshot of module sources."""
    copied_modules = dict(modules)

    def resolve(module_path: str) -> Optional[str]:
        if module_path in copied_modules:
            return copied_modules.get(module_path)
        return None

    return resolve


def modules_from_directory(base_dir: Union[str, Path]) -> ModuleResolver:
    """Create a resolver that reads modules beneath a directory."""
    lexical_root = Path(base_dir).absolute()
    canonical_root = lexical_root.resolve(strict=True)

    if not canonical_root.is_dir():
        raise NotADirectoryError(f"Module root is not a directory: {base_dir}")

    def resolve(module_path: str) -> Optional[str]:
        requested_path = Path(module_path)
        if requested_path.is_absolute():
            return None

        candidate = Path(os.path.abspath(lexical_root / requested_path))
        try:
            candidate.relative_to(lexical_root)
        except ValueError:
            return None

        try:
            canonical_candidate = candidate.resolve(strict=True)
        except FileNotFoundError:
            return None
        except OSError as error:
            raise OSError(
                f"Failed to resolve DataWeave module {module_path!r}: {error}"
            ) from error

        try:
            canonical_candidate.relative_to(canonical_root)
        except ValueError:
            return None

        try:
            return canonical_candidate.read_text(encoding="utf-8")
        except UnicodeDecodeError as error:
            raise UnicodeDecodeError(
                error.encoding,
                error.object,
                error.start,
                error.end,
                f"{error.reason} while reading DataWeave module {module_path!r}",
            ) from error
        except OSError as error:
            raise OSError(
                f"Failed to read DataWeave module {module_path!r}: {error}"
            ) from error

    return resolve


def modules_from_jars(
    jar_paths: Sequence[Union[str, Path]],
) -> ModuleResolver:
    """Create a resolver from DataWeave modules stored in JAR files."""
    modules = {}
    for jar_path in jar_paths:
        try:
            with ZipFile(jar_path) as jar:
                for entry in jar.infolist():
                    if entry.is_dir() or not entry.filename.endswith(".dwl"):
                        continue
                    modules[entry.filename] = jar.read(entry).decode("utf-8")
        except (
            OSError,
            BadZipFile,
            UnicodeDecodeError,
            RuntimeError,
            NotImplementedError,
        ) as error:
            raise ValueError(
                f"Failed to load DataWeave modules from JAR {jar_path!s}: {error}"
            ) from error

    return modules_from_map(modules)


def compose_resolvers(*resolvers: ModuleResolver) -> ModuleResolver:
    """Create a resolver that returns the first resolved module source."""

    def resolve(module_path: str) -> Optional[str]:
        for resolver in resolvers:
            source = resolver(module_path)
            if source is not None:
                return source
        return None

    return resolve
