"""Structured-log callback exposure for the native CDC library.

The C core emits structured diagnostics (decode warnings, truncation notices,
authentication traces, etc.) through a single process-wide log callback. This
module exposes that callback to Python so applications can surface or route the
diagnostics that are otherwise invisible from the bindings.
"""

from __future__ import annotations

import atexit
import contextlib
import ctypes
import threading
from collections.abc import Callable
from enum import IntEnum

from ._contract import LOG_LEVEL_MAX, LOG_LEVEL_MIN
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

# Every library instance the trampoline has been installed in. set_log_callback
# accepts an explicit lib_path, so more than one loaded libmes can be holding
# the pointer and all of them have to be cleared at interpreter shutdown.
_configured_libraries: list[ctypes.CDLL] = []


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
_null_callback = MES_LOG_CALLBACK(0)


def _detach_all() -> None:
    """Withdraw the trampoline from every library it was installed in.

    Registered with ``atexit``, so it runs while the interpreter can still
    execute Python code. The core may call the log callback from its own reader
    thread, which is a native thread that keeps running through interpreter
    finalization; a trampoline entered after finalization has begun cannot
    safely run Python, so the pointer is withdrawn before that window opens.
    This mirrors the cleanup hook the Node addon registers for the same
    process-wide callback.
    """
    global _active_handler

    with _callback_lock:
        _active_handler = None
        for lib in _configured_libraries:
            # Nothing could be reported this late in exit anyway.
            with contextlib.suppress(Exception):
                lib.mes_set_log_callback(_null_callback, int(LogLevel.ERROR), None)
        _configured_libraries.clear()


atexit.register(_detach_all)


def _validate_log_level(level: int) -> None:
    """Reject a severity the C ABI does not define.

    A level outside the documented range reaches the core as a threshold no
    record ever matches, which looks identical to never installing a handler.
    """
    if isinstance(level, bool) or not isinstance(level, int):
        raise TypeError("level must be an integer")
    if level < LOG_LEVEL_MIN or level > LOG_LEVEL_MAX:
        raise ValueError(
            f"level must be an integer between {LOG_LEVEL_MIN} and {LOG_LEVEL_MAX}, got {level}"
        )


def set_log_callback(
    callback: Callable[[LogLevel, str], None] | None,
    level: LogLevel = LogLevel.WARN,
    *,
    lib_path: str | None = None,
) -> None:
    """Install (or clear) a process-wide handler for native log messages.

    The handler is global to the loaded library, not per-engine or per-client,
    matching the C ABI. Messages whose severity is less verbose than or equal to
    ``level`` are delivered; more verbose messages are suppressed (e.g.
    ``LogLevel.WARN`` delivers ERROR and WARN only).

    Args:
        callback: Called as ``callback(level, message)`` for each log record.
            Pass ``None`` to remove the current handler.
        level: Maximum verbosity to deliver. Unused when ``callback`` is None,
            but still range-checked so a typo can never disable delivery
            silently.
        lib_path: Optional path to the libmes instance whose process-wide
            callback should be configured. Defaults to the standard loader.

    Raises:
        TypeError: If ``level`` is not an integer.
        ValueError: If ``level`` is not one of the four ``LogLevel`` values.

    Note:
        Exceptions raised inside ``callback`` are swallowed: a logging handler
        must never disrupt the C core's stream processing. The callback can run
        on the native reader thread; do not call ``BinlogClient.stop()``,
        ``close()``, ``poll()``, or any other client/engine operation from it.

        A handler left installed is detached automatically at interpreter
        shutdown, so an application does not have to unwind logging itself.
    """
    global _active_handler

    _validate_log_level(level)
    lib = get_library(lib_path)
    with _callback_lock:
        _active_handler = callback
        native_callback = _stable_callback if callback is not None else _null_callback
        lib.mes_set_log_callback(native_callback, int(level), None)
        if callback is not None and not any(lib is known for known in _configured_libraries):
            _configured_libraries.append(lib)
