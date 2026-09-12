"""Tests for how the binding resolves the native library it loads."""

from __future__ import annotations

import ctypes
import ctypes.util
import os
import tomllib
from pathlib import Path

import pytest

import mysql_event_stream
from mysql_event_stream import _ffi
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


def _distribution_tree(root: Path, *, shipped: bool = True) -> Path:
    """Create a package directory shaped like an installed distribution.

    Args:
        root: Directory standing in for the installation prefix.
        shipped: Whether the distribution carries its own copy of the library.
            A distribution built without one is what the provenance check exists
            to catch, so it has to be constructible.
    """
    pkg_dir = root / "site-packages" / "mysql_event_stream"
    pkg_dir.mkdir(parents=True)
    if shipped:
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


def _library_ships_with_the_package(pkg_dir: Path, resolved: Path) -> bool:
    """Report whether a resolved library lies inside the package directory.

    Paths are compared as they were resolved, without following symlinks. What
    is being asked is whether the resolver built this path out of the package's
    own directory, and that is legible in the path itself. A virtualenv that
    links the package into place moves both sides of the comparison together, so
    following links would change no answer there while making the check depend
    on the filesystem layout underneath the installation.

    The check is therefore about location, not identity: it distinguishes a
    library the distribution carries from one the loader found elsewhere, and it
    would not notice a wrong library sitting at the right path.
    """
    return resolved.is_relative_to(pkg_dir)


def _resolution_in_an_installed_context(
    monkeypatch: pytest.MonkeyPatch, pkg_dir: Path, system_library: str
) -> Path:
    """Run the real resolver as an installed package, above a given system library.

    The resolver reads its own location from the module's ``__file__`` and
    reaches the system through ``ctypes.util.find_library``, so redirecting the
    two puts the actual search in a constructed environment instead of restating
    what it would have decided.

    Args:
        monkeypatch: Fixture used to place the module and the loader.
        pkg_dir: Directory the package is to be imported from.
        system_library: Path the loader reports for a system-wide libmes.

    Returns:
        The library the resolver settles on.
    """
    monkeypatch.delenv("MES_LIB_PATH", raising=False)
    monkeypatch.setattr(_ffi, "__file__", str(pkg_dir / "_ffi.py"))
    monkeypatch.setattr(ctypes.util, "find_library", lambda _name: system_library)
    return Path(_find_library())


def _system_library(root: Path) -> Path:
    """Create a libmes that belongs to no distribution."""
    path = root / "usr" / "lib" / _platform_lib_name()
    path.parent.mkdir(parents=True)
    path.touch()
    return path


class TestAnInstalledPackageAnswersForTheLibraryItLoads:
    """Outside a checkout, only a library the distribution carries counts.

    A distribution that failed to ship its library still imports, because the
    resolver falls back to the loader's search path -- deliberately, since a
    system-wide libmes is a legitimate way to install this binding. What must
    not happen is a wheel passing its own gate that way: the suite would then be
    exercising a library the wheel does not contain, and would report nothing
    about the one it does.

    In a source checkout the development build satisfies this on its own, so the
    property costs an ordinary run nothing; the branch it guards is only ever
    reached from an installed package, which is why it is driven here rather
    than left to whichever environment happens to run the suite.
    """

    def test_a_library_it_carries_is_accepted(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        pkg_dir = _distribution_tree(tmp_path)
        resolved = _resolution_in_an_installed_context(
            monkeypatch, pkg_dir, str(_system_library(tmp_path))
        )
        assert resolved == pkg_dir / _platform_lib_name()
        assert _library_ships_with_the_package(pkg_dir, resolved)

    def test_a_library_found_elsewhere_on_the_system_is_rejected(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        pkg_dir = _distribution_tree(tmp_path, shipped=False)
        system_library = _system_library(tmp_path)
        resolved = _resolution_in_an_installed_context(monkeypatch, pkg_dir, str(system_library))
        assert resolved == system_library, (
            "the fallback to a system-wide library was not reached, so what the "
            "provenance check rejects was never constructed"
        )
        assert not _library_ships_with_the_package(pkg_dir, resolved), (
            "a distribution carrying no library of its own satisfied the provenance "
            "check with one the loader found elsewhere"
        )


def test_the_wheel_gate_does_not_hand_itself_a_library() -> None:
    """An explicit override must stay the caller's act, not the gate's.

    ``MES_LIB_PATH`` is returned ahead of everything else, which is correct: a
    caller naming a file has said which image it wants, and substituting another
    would load a second one. That exemption is only sound while it is somebody's
    deliberate instruction. A wheel build that set the variable for its own test
    command would suspend the provenance check for every run that matters, and
    the suite would go on passing, so the build configuration is held to not
    naming it at all -- in the test command, the environment, or anything else
    under the wheel builder's tables.

    The configuration in an environment variable exported around the build is
    out of this file's reach; what is covered is the configuration the
    repository carries.
    """
    manifest = Path(__file__).parent.parent / "pyproject.toml"
    wheel_build = tomllib.loads(manifest.read_text(encoding="utf-8"))["tool"]["cibuildwheel"]
    # repr flattens every nested table and list in one step, and unlike a
    # targeted key lookup it does not have to anticipate where the name lands.
    assert "MES_LIB_PATH" not in repr(wheel_build), (
        "the wheel build names MES_LIB_PATH, which would let its test command run "
        "against a library the wheel does not ship"
    )
