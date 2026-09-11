"""Tests for how the binding resolves the native library it loads."""

from __future__ import annotations

import ctypes
import os
from pathlib import Path

import pytest

import mysql_event_stream
from mysql_event_stream._ffi import (
    _BUILD_DIRS,
    _find_library,
    _platform_lib_name,
    _resolve_library_path,
    _source_tree_root,
    get_library,
    load_library,
)


def _image_address(lib: ctypes.CDLL) -> int:
    """Return the address a symbol resolves to inside a loaded library.

    Two handles on one image resolve a symbol to the same address. Two images
    loaded from different files do not, and they share none of the C ABI's
    process-wide state -- the log callback above all -- so comparing addresses
    is what distinguishes them; comparing the handles themselves does not.
    """
    address = ctypes.cast(lib.mes_set_log_callback, ctypes.c_void_p).value
    assert address is not None
    return address


def _distribution_tree(root: Path) -> Path:
    """Create a package directory shaped like an installed distribution."""
    pkg_dir = root / "site-packages" / "mysql_event_stream"
    pkg_dir.mkdir(parents=True)
    (pkg_dir / _platform_lib_name()).touch()
    return pkg_dir


def _checkout_tree(root: Path, *, staged: bool) -> Path:
    """Create a package directory shaped like this repository's checkout.

    Args:
        root: Directory standing in for the repository root.
        staged: Whether a copy of the library sits next to the package, as
            building a wheel leaves behind.
    """
    pkg_dir = root / "bindings" / "python" / "src" / "mysql_event_stream"
    pkg_dir.mkdir(parents=True)
    (root / "bindings" / "python" / "pyproject.toml").touch()
    if staged:
        (pkg_dir / _platform_lib_name()).touch()
    return pkg_dir


def _built_library(root: Path, build_dir: str) -> Path:
    """Create a development build's library inside a source tree."""
    path = root / build_dir / "core" / _platform_lib_name()
    path.parent.mkdir(parents=True)
    path.touch()
    return path


class TestTheEnvironmentDecidesWhichCopyWins:
    """The staged copy is the distribution's library, not the checkout's."""

    def test_an_installed_distribution_uses_the_library_it_ships(self, tmp_path: Path) -> None:
        pkg_dir = _distribution_tree(tmp_path)
        assert _source_tree_root(pkg_dir) is None
        assert _resolve_library_path(pkg_dir, _platform_lib_name()) == (
            pkg_dir / _platform_lib_name()
        )

    def test_a_source_checkout_uses_the_build_rather_than_the_staged_copy(
        self, tmp_path: Path
    ) -> None:
        pkg_dir = _checkout_tree(tmp_path, staged=True)
        built = _built_library(tmp_path, "build")
        assert _source_tree_root(pkg_dir) == tmp_path
        assert _resolve_library_path(pkg_dir, _platform_lib_name()) == built

    def test_a_source_checkout_prefers_the_client_enabled_build(self, tmp_path: Path) -> None:
        pkg_dir = _checkout_tree(tmp_path, staged=True)
        client_build = _built_library(tmp_path, "build-client")
        _built_library(tmp_path, "build")
        assert _resolve_library_path(pkg_dir, _platform_lib_name()) == client_build

    def test_a_source_checkout_never_falls_back_to_the_staged_copy(self, tmp_path: Path) -> None:
        pkg_dir = _checkout_tree(tmp_path, staged=True)
        assert _resolve_library_path(pkg_dir, _platform_lib_name()) is None, (
            "a copy staged for wheel building was resolved from a source checkout, "
            "where rebuilding the core does not refresh it"
        )


class TestAnExplicitOverride:
    """MES_LIB_PATH names the image the caller wants; nothing may substitute."""

    def test_is_honoured(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        override = tmp_path / _platform_lib_name()
        override.touch()
        monkeypatch.setenv("MES_LIB_PATH", str(override))
        assert _find_library() == str(override)

    def test_is_an_error_when_missing_rather_than_a_silent_fallback(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("MES_LIB_PATH", str(tmp_path / "absent" / _platform_lib_name()))
        with pytest.raises(OSError, match="does not exist"):
            _find_library()


def test_the_resolved_library_belongs_to_the_environment_running_the_suite() -> None:
    """The suite must exercise the library its own environment owns.

    Run from a wheel there is no build tree, so the only library that can
    resolve is the one the wheel ships; asserting it resolves and loads is what
    keeps a wheel whose library is missing or unloadable from passing its test
    command with skipped tests. Run from a checkout the build directory wins,
    so the run cannot silently exercise a superseded staged copy instead.
    """
    override = os.environ.get("MES_LIB_PATH")
    pkg_dir = Path(mysql_event_stream.__file__).parent
    source_root = _source_tree_root(pkg_dir)
    resolved = Path(_find_library())

    if override:
        assert resolved == Path(override)
    elif source_root is not None:
        assert resolved.parent.parent.name in _BUILD_DIRS, (
            f"resolved {resolved}, which is not a development build of this checkout"
        )
        assert resolved.parent.parent.parent == source_root
        assert resolved != pkg_dir / _platform_lib_name()
    else:
        assert resolved.parent == pkg_dir, (
            f"resolved {resolved}, which is not the library the installed package ships"
        )

    version = load_library(str(resolved)).mes_version()
    assert version, "the resolved library reports no version"


def test_the_default_resolver_and_the_suite_reach_one_image(lib_path: str) -> None:
    """One process must not end up with two images of libmes loaded.

    Anything reaching the library through the package's default must land on
    the same image the tests were handed, or state installed through one -- a
    log handler, for instance -- is invisible to the other.
    """
    assert _image_address(get_library()) == _image_address(load_library(lib_path)), (
        "the library the package resolves by default and the one the suite was handed "
        "are different images, whose process-wide state is not shared"
    )
