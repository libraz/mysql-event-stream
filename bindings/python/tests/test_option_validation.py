"""Entry-point option validation for every surface that accepts options.

The cross-binding contract declares a runtime type and, for integers, a range
for every shared option. The property asserted here is that each entry point
rejects a violation *itself*, before the value can reach the native layer: the
whole point is that a wrong-typed option is a ``TypeError`` from the call the
caller made, not a ctypes argument error or a wrapped-around fixed-width
integer surfacing later from ``connect()``.

The cases are enumerated rather than hand-picked. Three dimensions are crossed
in full -- entry point x option the entry point accepts x a value that violates
the option's declared type or range -- with the option's declared type read from
the same tables the validator uses, so an option added there without a matching
case here cannot go unnoticed.
"""

from __future__ import annotations

import inspect
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from mysql_event_stream._contract import OPTION_RANGES
from mysql_event_stream._options import (
    _BOOL_OPTIONS,
    _CALLBACK_OPTIONS,
    _OPTIONAL_STRING_OPTIONS,
    _STRING_LIST_OPTIONS,
    _STRING_OPTIONS,
)
from mysql_event_stream.client import BinlogClient
from mysql_event_stream.engine import CdcEngine
from mysql_event_stream.types import ClientConfig

# Values that violate each declared type. bool is listed under "integer"
# deliberately: Python's bool is an int subclass, so accepting it would let
# ``port=True`` through as port 1.
_WRONG_TYPED_VALUES: dict[str, tuple[object, ...]] = {
    "integer": ("1", 1.5, True, None),
    "string": (1, None, b"bytes", ["list"]),
    "optional_string": (1, b"bytes", ["list"]),
    "string_list": ("not-a-list", [1], None, ("tuple",)),
    "bool": (1, "yes", None),
    "callback": (1, "handler"),
}


def _declared_type(option: str) -> str:
    """Return the contract's declared runtime type for ``option``."""
    if option in OPTION_RANGES:
        return "integer"
    if option in _STRING_OPTIONS:
        return "string"
    if option in _OPTIONAL_STRING_OPTIONS:
        return "optional_string"
    if option in _STRING_LIST_OPTIONS:
        return "string_list"
    if option in _BOOL_OPTIONS:
        return "bool"
    if option in _CALLBACK_OPTIONS:
        return "callback"
    raise AssertionError(f"{option} has no declared type in the contract tables")


def _out_of_range_values(option: str) -> tuple[int, ...]:
    """Return the integers just outside ``option``'s accepted window."""
    minimum, maximum = OPTION_RANGES[option]
    if maximum is None:
        return (minimum - 1,)
    return (minimum - 1, maximum + 1)


def _override(option: str, value: object) -> dict[str, Any]:
    """Build the single-keyword override for a contract option named at runtime.

    The option name comes from the contract tables rather than from source, so
    the keyword cannot be matched against a declared parameter here.
    """
    return {option: value}


def _binlog_client_kwargs(option: str, value: object) -> None:
    """Construct a BinlogClient with one keyword option overridden."""
    with patch("mysql_event_stream.client.get_library") as get_library:
        try:
            BinlogClient(**_override(option, value))
        except (TypeError, ValueError):
            assert not get_library.called, "libmes was loaded before the option was rejected"
            raise
        raise AssertionError(f"BinlogClient accepted {option}={value!r}")


def _binlog_client_config(option: str, value: object) -> None:
    """Construct a BinlogClient from a pre-built ClientConfig."""
    config = ClientConfig(**_override(option, value))
    with patch("mysql_event_stream.client.get_library") as get_library:
        try:
            BinlogClient(config=config)
        except (TypeError, ValueError):
            assert not get_library.called, "libmes was loaded before the option was rejected"
            raise
        raise AssertionError(f"BinlogClient accepted a ClientConfig with {option}={value!r}")


def _enable_metadata(option: str, value: object) -> None:
    """Call CdcEngine.enable_metadata with one keyword option overridden."""
    lib = MagicMock()
    lib.mes_create.return_value = 0xE0
    with patch("mysql_event_stream.engine.get_library", return_value=lib):
        engine = CdcEngine()
    try:
        engine.enable_metadata(**_override(option, value))
    except (TypeError, ValueError):
        lib.mes_engine_set_metadata_conn.assert_not_called()
        raise
    else:
        raise AssertionError(f"enable_metadata accepted {option}={value!r}")
    finally:
        engine.close()


def _keyword_options(func: Callable[..., Any]) -> list[str]:
    """List the option names a callable accepts as keywords."""
    return [name for name in inspect.signature(func).parameters if name not in {"self", "config"}]


@dataclass(frozen=True)
class EntryPoint:
    """One public call that accepts options, and the options it accepts."""

    name: str
    options: list[str]
    invoke: Callable[[str, object], None]


ENTRY_POINTS = [
    EntryPoint(
        "BinlogClient(**options)",
        _keyword_options(BinlogClient.__init__),
        _binlog_client_kwargs,
    ),
    EntryPoint(
        "BinlogClient(config=ClientConfig(...))",
        [name for name in _keyword_options(BinlogClient.__init__) if name != "lib_path"],
        _binlog_client_config,
    ),
    EntryPoint(
        "CdcEngine.enable_metadata(**options)",
        _keyword_options(CdcEngine.enable_metadata),
        _enable_metadata,
    ),
]


def _cases(*, out_of_range: bool) -> list[tuple[EntryPoint, str, object]]:
    """Cross every entry point with its options and the rejected values."""
    cases: list[tuple[EntryPoint, str, object]] = []
    for entry in ENTRY_POINTS:
        for option in entry.options:
            declared = _declared_type(option)
            if out_of_range:
                if declared == "integer":
                    cases.extend((entry, option, value) for value in _out_of_range_values(option))
            else:
                cases.extend((entry, option, value) for value in _WRONG_TYPED_VALUES[declared])
    return cases


def _case_id(case: tuple[EntryPoint, str, object]) -> str:
    entry, option, value = case
    return f"{entry.name}-{option}-{value!r}"


WRONG_TYPE_CASES = _cases(out_of_range=False)
OUT_OF_RANGE_CASES = _cases(out_of_range=True)


class TestEveryEntryPointRejectsWrongTypes:
    """A wrong-typed option is a TypeError from the entry point that took it."""

    def test_the_matrix_covers_every_option_of_every_entry_point(self) -> None:
        for entry in ENTRY_POINTS:
            covered = {option for case_entry, option, _ in WRONG_TYPE_CASES if case_entry is entry}
            assert covered == set(entry.options), entry.name
        # Guard the generator itself: a silently emptied table would make every
        # case below pass by never running.
        assert len(WRONG_TYPE_CASES) == 188
        assert len(OUT_OF_RANGE_CASES) == 42

    @pytest.mark.parametrize("case", WRONG_TYPE_CASES, ids=_case_id)
    def test_rejects_a_wrong_typed_value(self, case: tuple[EntryPoint, str, object]) -> None:
        entry, option, value = case
        with pytest.raises(TypeError, match=option):
            entry.invoke(option, value)

    @pytest.mark.parametrize("case", OUT_OF_RANGE_CASES, ids=_case_id)
    def test_rejects_an_out_of_range_integer(self, case: tuple[EntryPoint, str, object]) -> None:
        entry, option, value = case
        with pytest.raises(ValueError, match=option):
            entry.invoke(option, value)


class TestValidOptionsStillReachTheNativeLayer:
    """The positive control: the validator rejects violations, not everything."""

    @patch("mysql_event_stream.client.load_client_library", return_value=True)
    @patch("mysql_event_stream.client.get_library")
    def test_binlog_client_accepts_every_option_at_its_bounds(
        self, mock_load: MagicMock, mock_load_client: MagicMock
    ) -> None:
        lib = MagicMock()
        lib.mes_client_create.return_value = 0xDEAD
        mock_load.return_value = lib

        client = BinlogClient(
            host="mysql.example",
            port=65535,
            user="replica",
            password="secret",
            server_id=4294967295,
            start_gtid=None,
            # The offset and its file are supplied together, as the contract
            # requires; the file option carries no bound of its own.
            start_binlog_file="binlog.000001",
            start_binlog_position=4294967295,
            connect_timeout_s=0,
            read_timeout_s=0,
            ssl_mode=4,
            ssl_ca="ca.pem",
            ssl_cert="cert.pem",
            ssl_key="key.pem",
            max_queue_size=0,
            max_queue_bytes=0,
            max_event_size=4294967295,
            allow_public_key_retrieval=True,
            lib_path=None,
        )
        assert client._handle == 0xDEAD
        client.close()

    def test_enable_metadata_accepts_every_option_at_its_bounds(self) -> None:
        lib = MagicMock()
        lib.mes_create.return_value = 0xE0
        lib.mes_engine_set_metadata_conn.return_value = 0
        with (
            patch("mysql_event_stream.engine.get_library", return_value=lib),
            patch("mysql_event_stream.engine.load_client_library", return_value=True),
        ):
            engine = CdcEngine()
            engine.enable_metadata(
                host="mysql.example",
                port=1,
                user="replica",
                password="secret",
                server_id=1,
                connect_timeout_s=0,
                read_timeout_s=0,
                ssl_mode=0,
                ssl_ca="",
                ssl_cert="",
                ssl_key="",
                allow_public_key_retrieval=False,
            )
            lib.mes_engine_set_metadata_conn.assert_called_once()
            engine.close()


class TestRejectionHappensBeforeConnect:
    """A violation must not be deferred to the first native call."""

    def test_a_wrong_typed_port_never_reaches_connect(self) -> None:
        with patch("mysql_event_stream.client.get_library") as get_library:
            with pytest.raises(TypeError, match="port must be an integer"):
                BinlogClient(port="3306")  # type: ignore[arg-type]
            get_library.assert_not_called()

    def test_a_negative_queue_size_never_reaches_the_ctypes_field(self) -> None:
        """A negative size would otherwise be reinterpreted as a huge size_t."""
        with patch("mysql_event_stream.client.get_library") as get_library:
            with pytest.raises(ValueError, match="max_queue_size must be between 0"):
                BinlogClient(max_queue_size=-1)
            with pytest.raises(ValueError, match="max_queue_bytes must be between 0"):
                BinlogClient(max_queue_bytes=-1)
            get_library.assert_not_called()

    def test_zero_server_id_keeps_its_dedicated_message(self) -> None:
        with pytest.raises(ValueError, match="server_id must be non-zero"):
            BinlogClient(server_id=0)
