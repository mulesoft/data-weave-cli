from zipfile import ZipFile

import pytest

import dataweave.resolver as resolver_module
from dataweave import (
    compose_resolvers,
    modules_from_directory,
    modules_from_jars,
    modules_from_map,
)


@pytest.mark.unit
def test_modules_from_map_copies_input_and_matches_exact_paths():
    modules = {"org/test/lib.dwl": "original"}
    resolver = modules_from_map(modules)
    modules["org/test/lib.dwl"] = "changed"

    assert resolver("org/test/lib.dwl") == "original"
    assert resolver("/org/test/lib.dwl") is None
    assert resolver("org/test/missing.dwl") is None


@pytest.mark.unit
def test_compose_resolvers_uses_first_match_and_falls_back():
    calls = []

    def first(path):
        calls.append(("first", path))
        return None

    def second(path):
        calls.append(("second", path))
        return "source"

    def unused(path):
        raise AssertionError(f"unexpected lookup: {path}")

    resolver = compose_resolvers(first, second, unused)

    assert resolver("lib.dwl") == "source"
    assert calls == [("first", "lib.dwl"), ("second", "lib.dwl")]


@pytest.mark.unit
def test_compose_resolvers_propagates_resolver_errors():
    def failing(_path):
        raise PermissionError("denied")

    with pytest.raises(PermissionError, match="denied"):
        compose_resolvers(failing)("lib.dwl")


@pytest.mark.unit
def test_modules_from_directory_reads_nested_utf8_module(tmp_path):
    module = tmp_path / "org" / "test" / "lib.dwl"
    module.parent.mkdir(parents=True)
    module.write_text("fun answer() = 42", encoding="utf-8")

    resolver = modules_from_directory(tmp_path)

    assert resolver("org/test/lib.dwl") == "fun answer() = 42"
    assert resolver("org/test/missing.dwl") is None


@pytest.mark.unit
def test_modules_from_directory_keeps_root_after_chdir(tmp_path, monkeypatch):
    base = tmp_path / "modules"
    base.mkdir()
    (base / "lib.dwl").write_text("source", encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    resolver = modules_from_directory("modules")
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    monkeypatch.chdir(elsewhere)

    assert resolver("lib.dwl") == "source"


@pytest.mark.unit
def test_modules_from_directory_rejects_lexical_escape(tmp_path):
    base = tmp_path / "modules"
    base.mkdir()
    (tmp_path / "secret.dwl").write_text("secret", encoding="utf-8")

    assert modules_from_directory(base)("../secret.dwl") is None


@pytest.mark.unit
def test_modules_from_directory_rejects_absolute_path(tmp_path):
    base = tmp_path / "modules"
    base.mkdir()
    secret = tmp_path / "secret.dwl"
    secret.write_text("secret", encoding="utf-8")

    assert modules_from_directory(base)(str(secret)) is None


@pytest.mark.unit
def test_modules_from_directory_rejects_symlink_escape(tmp_path):
    base = tmp_path / "modules"
    base.mkdir()
    secret = tmp_path / "secret.dwl"
    secret.write_text("secret", encoding="utf-8")
    link = base / "link.dwl"
    try:
        link.symlink_to(secret)
    except (NotImplementedError, OSError):
        pytest.skip("symlink creation is unavailable")

    assert modules_from_directory(base)("link.dwl") is None


@pytest.mark.unit
def test_modules_from_directory_fails_for_missing_root(tmp_path):
    missing = tmp_path / "missing"

    with pytest.raises(FileNotFoundError, match="missing"):
        modules_from_directory(missing)


@pytest.mark.unit
def test_modules_from_directory_names_invalid_utf8_module(tmp_path):
    module = tmp_path / "invalid.dwl"
    module.write_bytes(b"\xff")

    with pytest.raises(Exception, match="invalid[.]dwl"):
        modules_from_directory(tmp_path)("invalid.dwl")


@pytest.mark.unit
def test_modules_from_jars_loads_dwl_entries_and_later_jars_win(tmp_path):
    first = tmp_path / "first.jar"
    second = tmp_path / "second.jar"
    with ZipFile(first, "w") as jar:
        jar.writestr("org/test/lib.dwl", "first")
        jar.writestr("ignored.txt", "ignored")
    with ZipFile(second, "w") as jar:
        jar.writestr("org/test/lib.dwl", "second")
        jar.writestr("org/test/other.dwl", "other")

    resolver = modules_from_jars([first, second])

    assert resolver("org/test/lib.dwl") == "second"
    assert resolver("org/test/other.dwl") == "other"
    assert resolver("ignored.txt") is None


@pytest.mark.unit
def test_modules_from_jars_names_malformed_archive(tmp_path):
    malformed = tmp_path / "malformed.jar"
    malformed.write_text("not a zip archive", encoding="utf-8")

    with pytest.raises(Exception, match="malformed[.]jar"):
        modules_from_jars([malformed])


@pytest.mark.unit
@pytest.mark.parametrize("error", [RuntimeError("read failed"), NotImplementedError("unsupported")])
def test_modules_from_jars_names_archive_when_entry_read_fails(
    monkeypatch, tmp_path, error
):
    archive = tmp_path / "modules.jar"

    class Entry:
        filename = "org/test/lib.dwl"

        @staticmethod
        def is_dir():
            return False

    class FailingZipFile:
        def __init__(self, path):
            assert path == archive

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        @staticmethod
        def infolist():
            return [Entry()]

        @staticmethod
        def read(_entry):
            raise error

    monkeypatch.setattr(resolver_module, "ZipFile", FailingZipFile)

    with pytest.raises(ValueError, match="modules[.]jar") as raised:
        modules_from_jars([archive])

    assert raised.value.__cause__ is error
