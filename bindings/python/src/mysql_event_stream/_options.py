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

from ._contract import OPTION_RANGES

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


def validate_options(options: Mapping[str, object]) -> None:
    """Validate every supplied option at the entry point that received it.

    Args:
        options: Public option names mapped to the values supplied for them.

    Raises:
        TypeError: If any key is unrecognized or any value has the wrong type.
        ValueError: If an integer option falls outside its accepted range.
    """
    for key, value in options.items():
        validate_option(key, value)
