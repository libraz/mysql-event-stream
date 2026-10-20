"""Entry-point validation for the options of the cross-binding contract.

``core/contracts/bindings.json`` declares a runtime type for every shared
option, and this module is the single place that enforces it. Every entry point
that accepts options -- :class:`~mysql_event_stream.stream.CdcStream`'s
constructor and ``configure()``, :class:`~mysql_event_stream.client.BinlogClient`'s
constructor, :meth:`~mysql_event_stream.engine.CdcEngine.enable_metadata` --
runs these checks, so a value one of them refuses can never be accepted by
another and reinterpreted by the ctypes layer.
"""

from __future__ import annotations

import functools
import inspect
from collections.abc import Callable, Mapping
from typing import ParamSpec, TypeVar

from ._contract import (
    MUTUALLY_EXCLUSIVE_OPTIONS,
    OPTION_RANGES,
    REQUIRED_TOGETHER_OPTIONS,
)
from .types import mes_validation_error

#: Options whose value must be a string.
_STRING_OPTIONS = frozenset({"host", "user", "password", "ssl_ca", "ssl_cert", "ssl_key"})

#: Options whose value must be a string or ``None``.
_OPTIONAL_STRING_OPTIONS = frozenset({"start_gtid", "start_binlog_file", "lib_path"})

#: Integer options that may be ``None`` to mean unset.
_OPTIONAL_INT_OPTIONS = frozenset({"start_binlog_position"})

#: Optional-string options that, once supplied, must not be empty. A file/offset
#: start naming no file has no position a server could start from.
_NON_EMPTY_STRING_OPTIONS = frozenset({"start_binlog_file"})

#: Options whose value must be a list of strings.
_STRING_LIST_OPTIONS = frozenset({"include_databases", "include_tables", "exclude_tables"})

#: Options whose value must be a bool.
_BOOL_OPTIONS = frozenset({"allow_public_key_retrieval"})

#: Options whose value must be callable or ``None``.
_CALLBACK_OPTIONS = frozenset({"on_metadata_error"})

#: Queue byte budget a ``max_queue_bytes`` of 0 resolves to (MES_DEFAULT_QUEUE_BYTES).
_DEFAULT_QUEUE_BYTES = 48 * 1024 * 1024

#: Event-size ceiling a ``max_event_size`` of 0, or one above it, resolves to.
_MAX_EVENT_SIZE_CAP = 1024 * 1024 * 1024

#: Smallest event-size ceiling the core accepts: a header plus a checksum.
_MIN_EVENT_SIZE = 19 + 4

#: What the client queue charges beside one event: its packet-prefix byte and
#: the one-MiB reserve for the checkpoint held with it.
_QUEUED_EVENT_OVERHEAD = 1 + 1024 * 1024

_P = ParamSpec("_P")
_R = TypeVar("_R")


def validate_option(key: str, value: object) -> None:
    """Validate one configuration value against the cross-binding contract.

    Args:
        key: Public option name.
        value: Value exactly as the caller supplied it.

    Raises:
        TypeError: If ``key`` is not a recognized option, or ``value`` does not
            match the type the contract declares for it.
        ValueError: If an integer option falls outside its accepted range, or an
            option that has to name something is supplied empty.
    """
    if key in OPTION_RANGES:
        if value is None and key in _OPTIONAL_INT_OPTIONS:
            return
        minimum, maximum = OPTION_RANGES[key]
        if isinstance(value, bool) or not isinstance(value, int):
            raise mes_validation_error(TypeError(f"{key} must be an integer"))
        if value < minimum or (maximum is not None and value > maximum):
            upper = "unbounded" if maximum is None else str(maximum)
            raise mes_validation_error(ValueError(f"{key} must be between {minimum} and {upper}"))
        return
    if key in _STRING_OPTIONS:
        if not isinstance(value, str):
            raise mes_validation_error(TypeError(f"{key} must be a string"))
        return
    if key in _OPTIONAL_STRING_OPTIONS:
        if value is not None and not isinstance(value, str):
            raise mes_validation_error(TypeError(f"{key} must be a string or None"))
        if value == "" and key in _NON_EMPTY_STRING_OPTIONS:
            raise mes_validation_error(ValueError(f"{key} must name a binlog file"))
        return
    if key in _STRING_LIST_OPTIONS:
        if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
            raise mes_validation_error(TypeError(f"{key} must be a list of strings"))
        return
    if key in _BOOL_OPTIONS:
        if not isinstance(value, bool):
            raise mes_validation_error(TypeError(f"{key} must be a bool"))
        return
    if key in _CALLBACK_OPTIONS:
        if value is not None and not callable(value):
            raise mes_validation_error(TypeError(f"{key} must be callable or None"))
        return
    raise mes_validation_error(TypeError(f"Unknown config key: {key!r}"))


def refuse_unknown_options(func: Callable[_P, _R]) -> Callable[_P, _R]:
    """Refuse a keyword the wrapped entry point does not declare, with a code.

    Python's own refusal of an unexpected keyword is a ``TypeError`` without
    ``code``; this raises the same coded refusal ``validate_option`` raises for
    an unknown key. A key that is an option of some other entry point is still
    unknown here. The wrapped signature is kept for introspection.

    Args:
        func: Entry point taking its options as keyword-only parameters.

    Returns:
        ``func`` wrapped so an undeclared keyword raises before it runs.
    """
    declared = frozenset(inspect.signature(func).parameters)

    @functools.wraps(func)
    def wrapper(*args: _P.args, **kwargs: _P.kwargs) -> _R:
        for key in kwargs:
            if key not in declared:
                raise mes_validation_error(TypeError(f"Unknown config key: {key!r}"))
        return func(*args, **kwargs)

    return wrapper


def _is_supplied(options: Mapping[str, object], key: str) -> bool:
    """Report whether a configuration supplied ``key`` at all.

    An absent key and ``None`` both mean unset.
    """
    return options.get(key) is not None


def _validate_required_together(options: Mapping[str, object]) -> None:
    """Reject a configuration supplying one option of a pair without the other.

    The constraint is over a pair, so no per-key check can express it.

    Args:
        options: Every option the configuration carries, unset keys included.

    Raises:
        ValueError: If exactly one option of a pair is supplied.
    """
    for pair in REQUIRED_TOGETHER_OPTIONS:
        unset = [key for key in pair if not _is_supplied(options, key)]
        if len(unset) in (0, len(pair)):
            continue
        raise mes_validation_error(
            ValueError(f"{pair[0]} and {pair[1]} are required together, and {unset[0]} is unset")
        )


def _validate_mutually_exclusive(options: Mapping[str, object]) -> None:
    """Reject a configuration supplying both options of an exclusive pair.

    Another whole-configuration check: the competing option is a different key,
    so no per-key validator can see it. Runs after the pair rule, which is what
    lets one option stand for a start mode its companion is bound to.

    Args:
        options: Every option the configuration carries, unset keys included.

    Raises:
        ValueError: If both options of an exclusive pair are supplied.
    """
    for pair in MUTUALLY_EXCLUSIVE_OPTIONS:
        if not all(_is_supplied(options, key) for key in pair):
            continue
        raise mes_validation_error(ValueError(f"{pair[0]} and {pair[1]} cannot be combined"))


def _validate_queue_admission(options: Mapping[str, object]) -> None:
    """Reject a queue byte budget too small to admit one maximum-sized event.

    The native client refuses such a configuration only when streaming starts,
    so it is checked over the resolved pair here, where it is accepted.

    Args:
        options: Every option the configuration carries, unset keys included.

    Raises:
        ValueError: If the resolved ``max_queue_bytes`` is below the resolved
            ``max_event_size`` plus the queue's per-event overhead.
    """
    if "max_event_size" not in options or "max_queue_bytes" not in options:
        return
    event_size = options["max_event_size"]
    queue_bytes = options["max_queue_bytes"]
    if not isinstance(event_size, int) or not isinstance(queue_bytes, int):
        return
    if event_size == 0 or event_size > _MAX_EVENT_SIZE_CAP:
        event_size = _MAX_EVENT_SIZE_CAP
    event_size = max(event_size, _MIN_EVENT_SIZE)
    queue_bytes = queue_bytes or _DEFAULT_QUEUE_BYTES
    required = event_size + _QUEUED_EVENT_OVERHEAD
    if queue_bytes < required:
        raise mes_validation_error(
            ValueError(
                f"max_queue_bytes resolves to {queue_bytes}, below the {required} bytes "
                f"one max_event_size event of {event_size} bytes needs"
            )
        )


def validate_options(
    options: Mapping[str, object], base: Mapping[str, object] | None = None
) -> None:
    """Validate every supplied option at the entry point that received it.

    Args:
        options: Public option names mapped to the values supplied for them.
        base: Configuration ``options`` overrides, when it is a partial update.
            The checks that span two options hold over the configuration the
            update produces, not over the keys the update happens to name.

    Raises:
        TypeError: If any key is unrecognized or any value has the wrong type.
        ValueError: If an integer option falls outside its accepted range, if
            an option that has to name something is supplied empty, if one
            option of a required-together pair is supplied alone, if two
            options naming competing start modes are supplied together, or if
            ``max_queue_bytes`` cannot admit one ``max_event_size`` event.
    """
    for key, value in options.items():
        validate_option(key, value)
    effective = {**(base or {}), **options}
    _validate_required_together(effective)
    _validate_mutually_exclusive(effective)
    _validate_queue_admission(effective)
