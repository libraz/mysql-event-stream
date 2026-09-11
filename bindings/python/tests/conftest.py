"""Test configuration for mysql-event-stream Python binding tests."""

import os
import sys
from pathlib import Path

import pytest

# Names the library can carry when it is shipped inside a distribution.
_BUNDLED_LIB_NAMES = ("libmes.dylib", "libmes.so", "mes.dll")


def _build_dir_lib() -> Path:
    """Return the path the development build writes libmes to."""
    project_root = Path(__file__).parent.parent.parent.parent
    lib_name = "libmes.dylib" if sys.platform == "darwin" else "libmes.so"
    return project_root / "build" / "core" / lib_name


def _bundled_lib() -> Path | None:
    """Return the library shipped next to the installed package, if present."""
    import mysql_event_stream

    pkg_dir = Path(mysql_event_stream.__file__).parent
    for name in _BUNDLED_LIB_NAMES:
        candidate = pkg_dir / name
        if candidate.exists():
            return candidate
    return None


def _find_lib_path() -> str:
    """Resolve libmes for the tests that exercise the native library.

    ``MES_LIB_PATH`` takes precedence, then the development build directory,
    then the library bundled with an installed distribution. The build
    directory is preferred over the bundled copy so a development run always
    exercises the library it just built.

    A library that cannot be resolved leaves nothing for the native tests to
    verify, so it aborts them instead of skipping them.
    """
    env_path = os.environ.get("MES_LIB_PATH")
    if env_path:
        if not Path(env_path).exists():
            pytest.fail(
                f"MES_LIB_PATH is set to {env_path}, which does not exist.",
                pytrace=False,
            )
        return env_path

    build_path = _build_dir_lib()
    if build_path.exists():
        return str(build_path)

    bundled = _bundled_lib()
    if bundled is not None:
        return str(bundled)

    pytest.fail(
        f"libmes not found at {build_path} and not bundled with the installed "
        "package. Build it with 'cmake -B build -DCMAKE_BUILD_TYPE=Release && "
        "cmake --build build --parallel', or set MES_LIB_PATH to an existing library.",
        pytrace=False,
    )


@pytest.fixture()
def lib_path() -> str:
    """Provide the path to the libmes shared library."""
    return _find_lib_path()
