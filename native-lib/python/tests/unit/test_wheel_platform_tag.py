import importlib.util
from pathlib import Path

import setuptools


def load_setup_module(monkeypatch):
    monkeypatch.setattr(setuptools, "setup", lambda **kwargs: None)
    setup_path = Path(__file__).parents[2] / "setup.py"
    spec = importlib.util.spec_from_file_location("dataweave_setup", setup_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_arm64_macos_wheel_uses_macos_11_compatibility_tag(monkeypatch):
    setup_module = load_setup_module(monkeypatch)
    monkeypatch.setattr(setup_module.platform, "system", lambda: "Darwin")
    monkeypatch.setattr(setup_module.platform, "machine", lambda: "arm64")
    monkeypatch.setattr(setup_module.platform, "mac_ver", lambda: ("26.0", (), ""))

    assert setup_module.get_platform_tag() == "macosx_11_0_arm64"


def test_x86_64_macos_wheel_matches_native_library_deployment_target(monkeypatch):
    setup_module = load_setup_module(monkeypatch)
    monkeypatch.setattr(setup_module.platform, "system", lambda: "Darwin")
    monkeypatch.setattr(setup_module.platform, "machine", lambda: "x86_64")

    assert setup_module.get_platform_tag() == "macosx_11_0_x86_64"
