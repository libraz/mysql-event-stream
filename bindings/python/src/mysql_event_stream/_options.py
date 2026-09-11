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

from collections.abc import Mapping

from ._contract import (
    CONDITIONAL_OPTION_MINIMUMS,
    OPTION_RANGES,
    REQUIRED_TOGETHER_OPTIONS,
    UNSET_OPTION_VALUES,
)

#: Options whose value must be a string.
_STRING_OPTIONS = frozenset({"host", "user", "password", "ssl_ca", "ssl_cert", "ssl_key"})

#: Options whose value must be a string or ``None``.
_OPTIONAL_STRING_OPTIONS = frozenset({"start_gtid", "start_binlog_file", "lib_path"})

#: Options whose value must be a list of strings.
_STRING_LIST_OPTIONS = frozenset({"include_databases", "include_tables", "exclude_tables"})

#: Options whose value must be a bool.
_BOOL_OPTIONS = frozenset({"allow_public_key_retrieval"})

#: Options whose value must be callable or ``None``.
_CALLBACK_OPTIONS = frozenset({"on_metadata_error"})


def validate_option(key: str, value: object) -> None:
    """Validate one configuration value against the cross-binding contract.

    Args:
        key: Public option name.
        value: Value exactly as the caller supplied it.

    Raises:
        TypeError: If ``key`` is not a recognized option, or ``value`` does not
            match the type the contract declares for it.
        ValueError: If an integer option falls outside its accepted range.
    """
    if key in OPTION_RANGES:
        minimum, maximum = OPTION_RANGES[key]
        if isinstance(value, bool) or not isinstance(value, int):
            raise TypeError(f"{key} must be an integer")
        if value < minimum or (maximum is not None and value > maximum):
            upper = "unbounded" if maximum is None else str(maximum)
            raise ValueError(f"{key} must be between {minimum} and {upper}")
        return
    if key in _STRING_OPTIONS:
        if not isinstance(value, str):
            raise TypeError(f"{key} must be a string")
        return
    if key in _OPTIONAL_STRING_OPTIONS:
        if value is not None and not isinstance(value, str):
            raise TypeError(f"{key} must be a string or None")
        return
    if key in _STRING_LIST_OPTIONS:
        if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
            raise TypeError(f"{key} must be a list of strings")
        return
    if key in _BOOL_OPTIONS:
        if not isinstance(value, bool):
            raise TypeError(f"{key} must be a bool")
        return
    if key in _CALLBACK_OPTIONS:
        if value is not None and not callable(value):
            raise TypeError(f"{key} must be callable or None")
        return
    raise TypeError(f"Unknown config key: {key!r}")


def _is_supplied(options: Mapping[str, object], key: str) -> bool:
    """Report whether a configuration supplied ``key`` at all.

    An absent key and ``None`` both mean unset. So does the value in
    :data:`UNSET_OPTION_VALUES` for an option this surface declares as a plain
    integer: a caller who passes it cannot be told apart from one who passed
    nothing.
    """
    value = options.get(key)
    return value is not None and value != UNSET_OPTION_VALUES.get(key)


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
        raise ValueError(f"{pair[0]} and {pair[1]} are required together, and {unset[0]} is unset")


def _validate_conditional_minimums(options: Mapping[str, object]) -> None:
    """Apply the floors that hold only while a companion option is set.

    Also a whole-configuration check: the companion is another key, so the
    per-key range check cannot see it. Enforcing it at the entry point is what
    keeps a value accepted here from being refused later by the native layer.

    Args:
        options: Every option the configuration carries, unset keys included.

    Raises:
        ValueError: If a supplied value is below the floor its companion brings
            into force.
    """
    for key, floor in CONDITIONAL_OPTION_MINIMUMS.items():
        if not _is_supplied(options, floor.companion) or not _is_supplied(options, key):
            continue
        value = options[key]
        if not isinstance(value, int) or value >= floor.minimum:
            continue
        maximum = OPTION_RANGES[key].maximum
        upper = "unbounded" if maximum is None else str(maximum)
        raise ValueError(
            f"{key} must be {floor.minimum} through {upper} when {floor.companion} is set"
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
            one option of a required-together pair is supplied alone, or if a
            value is below the floor its companion brings into force.
    """
    for key, value in options.items():
        validate_option(key, value)
    effective = {**(base or {}), **options}
    _validate_required_together(effective)
    _validate_conditional_minimums(effective)
