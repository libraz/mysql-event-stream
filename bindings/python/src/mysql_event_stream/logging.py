"""Structured-log callback exposure for the native CDC library.

The C core emits structured diagnostics (decode warnings, truncation notices,
authentication traces, etc.) through a single process-wide log callback. This
module exposes that callback to Python so applications can surface or route the
diagnostics that are otherwise invisible from the bindings.
"""

from __future__ import annotations

import threading
from collections.abc import Callable
from enum import IntEnum

from ._ffi import (
    MES_LOG_CALLBACK,
    MES_LOG_DEBUG,
    MES_LOG_ERROR,
    MES_LOG_INFO,
    MES_LOG_WARN,
    get_library,
)

__all__ = ["LogLevel", "set_log_callback"]


class LogLevel(IntEnum):
    """Severity levels for native log messages (mirrors ``mes_log_level_t``)."""

    ERROR = MES_LOG_ERROR
    WARN = MES_LOG_WARN
    INFO = MES_LOG_INFO
    DEBUG = MES_LOG_DEBUG


# StructuredLog instances in the C++ core retain immutable callback snapshots,
# so a callback pointer may be invoked after a later set/unset call. Keep one
# CFUNCTYPE trampoline alive for the entire module lifetime and swap only the
# Python handler behind it. The RLock is held through handler invocation: a
# completed replacement/unset therefore guarantees no old handler remains in
# flight, while still allowing a handler to reconfigure logging recursively.
_callback_lock = threading.RLock()
_active_handler: Callable[[LogLevel, str], None] | None = None


def _dispatch(c_level: int, message: bytes | None, _userdata: object) -> None:
    with _callback_lock:
        if _active_handler is None:
            return
        try:
            text = message.decode("utf-8", "replace") if message else ""
            _active_handler(LogLevel(c_level), text)
        except Exception:  # noqa: BLE001 - a logging handler must not crash the core
            pass


_stable_callback = MES_LOG_CALLBACK(_dispatch)


def set_log_callback(
    callback: Callable[[LogLevel, str], None] | None,
    level: LogLevel = LogLevel.WARN,
) -> None:
    """Install (or clear) a process-wide handler for native log messages.

    The handler is global to the loaded library, not per-engine or per-client,
    matching the C ABI. Messages whose severity is less verbose than or equal to
    ``level`` are delivered; more verbose messages are suppressed (e.g.
    ``LogLevel.WARN`` delivers ERROR and WARN only).

    Args:
        callback: Called as ``callback(level, message)`` for each log record.
            Pass ``None`` to remove the current handler.
        level: Maximum verbosity to deliver. Ignored when ``callback`` is None.

    Note:
        Exceptions raised inside ``callback`` are swallowed: a logging handler
        must never disrupt the C core's stream processing.
    """
    global _active_handler

    lib = get_library()
    with _callback_lock:
        _active_handler = callback
        native_callback = _stable_callback if callback is not None else MES_LOG_CALLBACK(0)
        lib.mes_set_log_callback(native_callback, int(level), None)
