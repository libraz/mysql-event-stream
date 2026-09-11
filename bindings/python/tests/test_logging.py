"""Tests for the native log-callback exposure."""

from __future__ import annotations

import os
import subprocess
import sys
import threading
from pathlib import Path
from unittest.mock import MagicMock

import pytest

import mysql_event_stream.logging as logmod
from mysql_event_stream import CdcEngine, LogLevel, ParseError, set_log_callback


def _oversized_event_header() -> bytes:
    event = bytearray(23)
    event[4] = 19  # TABLE_MAP_EVENT
    event[5:9] = (1).to_bytes(4, "little")
    event[9:13] = (1024 * 1024 * 1024).to_bytes(4, "little")
    return bytes(event)


class TestLogLevel:
    def test_values(self) -> None:
        assert LogLevel.ERROR == 0
        assert LogLevel.WARN == 1
        assert LogLevel.INFO == 2
        assert LogLevel.DEBUG == 3

    def test_is_int(self) -> None:
        assert isinstance(LogLevel.WARN, int)


class TestSetLogCallback:
    def teardown_method(self) -> None:
        # Always detach so a registered trampoline never leaks into later tests.
        set_log_callback(None)

    def test_install_uses_process_lifetime_trampoline(self) -> None:
        def handler(level: LogLevel, message: str) -> None:
            pass

        stable = logmod._stable_callback
        set_log_callback(handler, LogLevel.DEBUG)
        assert logmod._stable_callback is stable
        assert logmod._active_handler is handler

    def test_detach_clears_handler_but_retains_trampoline(self) -> None:
        stable = logmod._stable_callback
        set_log_callback(lambda level, message: None)
        assert logmod._active_handler is not None
        set_log_callback(None)
        assert logmod._active_handler is None
        assert logmod._stable_callback is stable

    def test_explicit_library_path_is_forwarded(self, monkeypatch: pytest.MonkeyPatch) -> None:
        lib = MagicMock()
        seen: list[str | None] = []

        def get_specific(path: str | None = None) -> MagicMock:
            seen.append(path)
            return lib

        monkeypatch.setattr(logmod, "get_library", get_specific)
        set_log_callback(lambda level, message: None, lib_path="/tmp/libmes-test.dylib")
        assert seen == ["/tmp/libmes-test.dylib"]
        lib.mes_set_log_callback.assert_called_once()

    def test_reinstall_replaces_handler_not_trampoline(self) -> None:
        def first_handler(level: LogLevel, message: str) -> None:
            pass

        def second_handler(level: LogLevel, message: str) -> None:
            pass

        stable = logmod._stable_callback
        set_log_callback(first_handler)
        set_log_callback(second_handler)
        assert logmod._active_handler is second_handler
        assert logmod._stable_callback is stable

    def test_replaced_handler_receives_no_later_delivery(self) -> None:
        old_messages: list[str] = []
        new_messages: list[str] = []
        set_log_callback(lambda level, message: old_messages.append(message))
        logmod._stable_callback(LogLevel.WARN, b"old", None)

        set_log_callback(lambda level, message: new_messages.append(message))
        logmod._stable_callback(LogLevel.WARN, b"new", None)

        assert old_messages == ["old"]
        assert new_messages == ["new"]

    def test_native_engine_error_reaches_python_handler(self) -> None:
        messages: list[tuple[LogLevel, str]] = []
        set_log_callback(lambda level, message: messages.append((level, message)), LogLevel.DEBUG)

        with CdcEngine() as engine, pytest.raises(ParseError):
            engine.feed(_oversized_event_header())

        assert any(
            level == LogLevel.ERROR and "event=parse_error reason=event_too_large" in message
            for level, message in messages
        )

    def test_concurrent_replace_and_unset_stress(self) -> None:
        stop = threading.Event()
        errors: list[BaseException] = []

        def emit() -> None:
            try:
                while not stop.is_set():
                    logmod._stable_callback(LogLevel.WARN, b"stress", None)
            except BaseException as exc:  # pragma: no cover - diagnostic guard
                errors.append(exc)

        workers = [threading.Thread(target=emit) for _ in range(4)]
        for worker in workers:
            worker.start()
        try:
            for _ in range(200):
                set_log_callback(lambda level, message: None)
                set_log_callback(None)
        finally:
            stop.set()
            for worker in workers:
                worker.join(timeout=2)

        assert not errors
        assert all(not worker.is_alive() for worker in workers)


# Installs a handler, drives one real native log message through it and then
# leaves both the handler and the engine in place, the way application code
# that never unwinds its logging does. The verification hook is registered
# before the package is imported, so it runs after the package's own hook
# (atexit is LIFO) and observes the state a native reader thread would find.
_EXIT_WITH_HANDLER_INSTALLED = """
import atexit

received = []


def verify_detached_at_exit():
    import mysql_event_stream.logging as logmod

    assert logmod._active_handler is None, "handler still installed at interpreter exit"
    delivered = len(received)
    logmod._stable_callback(1, b"after shutdown", None)
    assert len(received) == delivered, "trampoline still dispatched into Python"


atexit.register(verify_detached_at_exit)

import mysql_event_stream as mes

mes.set_log_callback(lambda level, message: received.append(message), mes.LogLevel.DEBUG)

event = bytearray(23)
event[4] = 19  # TABLE_MAP_EVENT
event[5:9] = (1).to_bytes(4, "little")
event[9:13] = (1024 * 1024 * 1024).to_bytes(4, "little")

engine = mes.CdcEngine()
try:
    engine.feed(bytes(event))
except mes.ParseError:
    pass

assert received, "the native callback never reached Python"
"""


class TestShutdownDetach:
    def teardown_method(self) -> None:
        set_log_callback(None)

    def test_detach_clears_every_configured_library(self, monkeypatch: pytest.MonkeyPatch) -> None:
        first = MagicMock()
        second = MagicMock()
        libraries = {"/tmp/libmes-first.dylib": first, "/tmp/libmes-second.dylib": second}
        monkeypatch.setattr(logmod, "get_library", lambda path=None: libraries[path])
        monkeypatch.setattr(logmod, "_configured_libraries", [])

        for path in libraries:
            set_log_callback(lambda level, message: None, lib_path=path)

        logmod._detach_all()

        for lib in libraries.values():
            native_callback, _level, _userdata = lib.mes_set_log_callback.call_args[0]
            assert native_callback is logmod._null_callback
            assert not native_callback  # a NULL function pointer
        assert logmod._active_handler is None
        assert logmod._configured_libraries == []

    def test_exit_with_handler_still_installed_is_clean(self, lib_path: str) -> None:
        env = dict(os.environ)
        env["MES_LIB_PATH"] = lib_path
        package_root = str(Path(logmod.__file__).parent.parent)
        env["PYTHONPATH"] = os.pathsep.join(
            [package_root, *([env["PYTHONPATH"]] if env.get("PYTHONPATH") else [])]
        )

        result = subprocess.run(
            [sys.executable, "-c", _EXIT_WITH_HANDLER_INSTALLED],
            capture_output=True,
            text=True,
            timeout=120,
            env=env,
            check=False,
        )

        assert result.returncode == 0, result.stderr
        assert result.stderr == ""
