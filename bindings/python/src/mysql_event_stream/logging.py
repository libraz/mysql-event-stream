"""Structured-log callback exposure for the native CDC library.

The C core emits structured diagnostics (decode warnings, truncation notices,
authentication traces, etc.) through a single process-wide log callback. This
module exposes that callback to Python so applications can surface or route the
diagnostics that are otherwise invisible from the bindings.
"""

from __future__ import annotations

from collections.abc import Callable
from enum import IntEnum
from typing import Any

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


# The native library stores the callback pointer process-wide, so the ctypes
# trampoline must outlive this call. Keep a module-level reference; without it
# the closure would be garbage-collected and the C side would call freed
# memory. Reassigned on each set_log_callback() call; cleared when disabled.
# Typed as Any: it holds a ctypes CFUNCTYPE instance, whose factory is not a
# valid static type annotation.
_active_callback: Any = None


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
    global _active_callback

    lib = get_library()

    if callback is None:
        # Detach: install a null pointer and drop our retained trampoline.
        lib.mes_set_log_callback(MES_LOG_CALLBACK(0), int(level), None)
        _active_callback = None
        return

    def _trampoline(c_level: int, message: bytes | None, _userdata: object) -> None:
        try:
            text = message.decode("utf-8", "replace") if message else ""
            callback(LogLevel(c_level), text)
        except Exception:  # noqa: BLE001 - a logging handler must not crash the core
            pass

    trampoline = MES_LOG_CALLBACK(_trampoline)
    # Retain BEFORE handing the pointer to C so a GC pause cannot free it.
    _active_callback = trampoline
    lib.mes_set_log_callback(trampoline, int(level), None)
