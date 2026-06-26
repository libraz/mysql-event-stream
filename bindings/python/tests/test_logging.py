"""Tests for the native log-callback exposure."""

from __future__ import annotations

import mysql_event_stream.logging as logmod
from mysql_event_stream import LogLevel, set_log_callback


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

    def test_install_retains_trampoline(self) -> None:
        def handler(level: LogLevel, message: str) -> None:
            pass

        set_log_callback(handler, LogLevel.DEBUG)
        # The ctypes trampoline must be retained module-side to survive GC while
        # the C core holds the pointer.
        assert logmod._active_callback is not None

    def test_detach_clears_trampoline(self) -> None:
        set_log_callback(lambda level, message: None)
        assert logmod._active_callback is not None
        set_log_callback(None)
        assert logmod._active_callback is None

    def test_reinstall_replaces_trampoline(self) -> None:
        set_log_callback(lambda level, message: None)
        first = logmod._active_callback
        set_log_callback(lambda level, message: None)
        assert logmod._active_callback is not None
        assert logmod._active_callback is not first
