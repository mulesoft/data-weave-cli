from zipfile import ZipFile

import pytest

import dataweave


IMPORT_LIB_SCRIPT = """%dw 2.0
import org::test::lib
output application/json
---
lib::answer()
"""


@pytest.mark.integration
def test_run_resolves_module_from_map():
    resolver = dataweave.modules_from_map({
        "org/test/lib.dwl": "%dw 2.0\nfun answer() = 42",
    })

    with dataweave.DataWeave(resolve_module=resolver) as dw:
        result = dw.run(IMPORT_LIB_SCRIPT)

    assert result.success is True
    assert result.get_string() == "42"


@pytest.mark.integration
def test_missing_module_returns_unsuccessful_result():
    with dataweave.DataWeave(
        resolve_module=dataweave.modules_from_map({}),
    ) as dw:
        result = dw.run(IMPORT_LIB_SCRIPT)

    assert result.success is False
    assert "resolve" in (result.error or "").lower()


@pytest.mark.integration
def test_raise_on_error_promotes_missing_module_result():
    with dataweave.DataWeave(
        resolve_module=dataweave.modules_from_map({}),
    ) as dw:
        with pytest.raises(dataweave.DataWeaveScriptError) as error:
            dw.run(IMPORT_LIB_SCRIPT, raise_on_error=True)

    assert error.value.result.success is False
    assert "resolve" in (error.value.result.error or "").lower()


def _write_transitive_modules(module_root):
    module_dir = module_root / "org" / "test"
    module_dir.mkdir(parents=True)
    (module_dir / "base.dwl").write_text(
        "%dw 2.0\nfun value() = 40",
        encoding="utf-8",
    )
    (module_dir / "lib.dwl").write_text(
        "%dw 2.0\nimport org::test::base\nfun answer() = base::value() + 2",
        encoding="utf-8",
    )


@pytest.mark.integration
def test_directory_resolver_supports_transitive_imports(tmp_path):
    _write_transitive_modules(tmp_path)

    with dataweave.DataWeave(
        resolve_module=dataweave.modules_from_directory(tmp_path),
    ) as dw:
        result = dw.run(IMPORT_LIB_SCRIPT)

    assert result.success is True
    assert result.get_string() == "42"


@pytest.mark.integration
def test_jar_resolver_supports_transitive_imports(tmp_path):
    module_root = tmp_path / "modules"
    _write_transitive_modules(module_root)
    jar_path = tmp_path / "modules.jar"
    with ZipFile(jar_path, "w") as jar:
        jar.write(module_root / "org" / "test" / "base.dwl", "org/test/base.dwl")
        jar.write(module_root / "org" / "test" / "lib.dwl", "org/test/lib.dwl")

    with dataweave.DataWeave(
        resolve_module=dataweave.modules_from_jars([jar_path]),
    ) as dw:
        result = dw.run(IMPORT_LIB_SCRIPT)

    assert result.success is True
    assert result.get_string() == "42"


@pytest.mark.integration
def test_repeated_runs_reuse_the_instances_resolver():
    resolver = dataweave.modules_from_map({
        "org/test/lib.dwl": "%dw 2.0\nfun answer() = 42",
    })

    with dataweave.DataWeave(resolve_module=resolver) as dw:
        first = dw.run(IMPORT_LIB_SCRIPT)
        second = dw.run(IMPORT_LIB_SCRIPT)

    assert first.success is True
    assert first.get_string() == "42"
    assert second.success is True
    assert second.get_string() == "42"


@pytest.mark.integration
def test_cleanup_of_one_instance_preserves_another_instances_resolver():
    first = dataweave.DataWeave(resolve_module=dataweave.modules_from_map({
        "org/test/lib.dwl": "%dw 2.0\nfun answer() = 41",
    }))
    second = dataweave.DataWeave(resolve_module=dataweave.modules_from_map({
        "org/test/lib.dwl": "%dw 2.0\nfun answer() = 42",
    }))
    first_initialized = False
    second_initialized = False
    try:
        first.initialize()
        first_initialized = True
        second.initialize()
        second_initialized = True

        first_result = first.run(IMPORT_LIB_SCRIPT)
        second_result = second.run(IMPORT_LIB_SCRIPT)
        assert first_result.success is True
        assert first_result.get_string() == "41"
        assert second_result.success is True
        assert second_result.get_string() == "42"

        first.cleanup()
        first_initialized = False

        result = second.run(IMPORT_LIB_SCRIPT)
        assert result.success is True
        assert result.get_string() == "42"
    finally:
        try:
            if first_initialized:
                first.cleanup()
        finally:
            if second_initialized:
                second.cleanup()


@pytest.mark.integration
def test_streaming_builtin_import_does_not_invoke_unsupported_external_resolver(
    collect_stream,
):
    calls = []

    def resolver(module_path):
        calls.append(module_path)
        return None

    script = """%dw 2.0
import fromBase64 from dw::core::Binaries
output application/json
---
sizeOf(fromBase64("aGk="))
"""
    with dataweave.DataWeave(resolve_module=resolver) as dw:
        output, metadata = collect_stream(dw.run_streaming(script))

    assert metadata.success is True
    assert output.decode(metadata.charset or "utf-8") == "2"
    assert calls == []
