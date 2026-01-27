"""
Custom setup.py to build platform-specific wheels for dataweave-native.

Since this package bundles a native shared library (dwlib), the wheel must
be tagged with the correct platform (e.g., macosx_11_0_arm64, manylinux, etc.)
rather than the generic 'any' platform.

This is achieved by overriding the bdist_wheel command to set platform-specific
tags based on the current build environment.
"""

import platform
import struct
import sys

from setuptools import setup

try:
    from wheel.bdist_wheel import bdist_wheel as _bdist_wheel
except ImportError:
    _bdist_wheel = None


def get_platform_tag():
    """
    Determine the platform tag for the wheel based on the current system.
    """
    system = platform.system().lower()
    machine = platform.machine().lower()

    # Normalize machine architecture names
    if machine in ("x86_64", "amd64"):
        machine = "x86_64"
    elif machine in ("arm64", "aarch64"):
        machine = "arm64"
    elif machine in ("i386", "i686"):
        machine = "i686"

    if system == "darwin":
        # macOS: use macosx_11_0 as minimum for universal compatibility
        # Adjust based on actual deployment target if needed
        mac_ver = platform.mac_ver()[0]
        if mac_ver:
            parts = mac_ver.split(".")
            major = int(parts[0])
            minor = int(parts[1]) if len(parts) > 1 else 0
            # Use at least 11.0 for arm64, 10.9 for x86_64
            if machine == "arm64":
                major = max(major, 11)
                minor = 0
            else:
                major = max(major, 10)
                minor = max(minor, 9) if major == 10 else 0
        else:
            major, minor = (11, 0) if machine == "arm64" else (10, 9)

        return f"macosx_{major}_{minor}_{machine}"

    elif system == "linux":
        # Linux: use manylinux2014 for broad compatibility
        # manylinux2014 supports glibc 2.17+
        return f"manylinux2014_{machine}"

    elif system == "windows":
        # Windows: win_amd64 or win32
        bits = struct.calcsize("P") * 8
        if bits == 64:
            return "win_amd64"
        else:
            return "win32"

    else:
        # Fallback: use the platform module's platform tag
        return None


if _bdist_wheel is not None:

    class bdist_wheel(_bdist_wheel):
        """
        Custom bdist_wheel that forces platform-specific tags for native library wheels.
        """

        def finalize_options(self):
            super().finalize_options()
            # Mark as platform-specific (not pure Python)
            self.root_is_pure = False

        def get_tag(self):
            # Get the default tags
            python, abi, plat = super().get_tag()

            # Override with platform-specific tag
            platform_tag = get_platform_tag()
            if platform_tag:
                plat = platform_tag

            # Use py3 and none for Python/ABI since we don't have compiled Python extensions
            return "py3", "none", plat

else:
    bdist_wheel = None


cmdclass = {}
if bdist_wheel is not None:
    cmdclass["bdist_wheel"] = bdist_wheel

setup(cmdclass=cmdclass)
