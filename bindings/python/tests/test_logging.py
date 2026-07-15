"""Tests for the native log-callback exposure."""

from __future__ import annotations

import threading

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
