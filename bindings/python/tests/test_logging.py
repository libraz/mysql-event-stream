"""Tests for the native log-callback exposure."""

from __future__ import annotations

import threading
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
