"""The column-type table must say the same thing on every surface.

The contract lives in three documentation comments: the canonical table in
``core/include/mes.h`` and the table each binding restates for its own users.
Nothing but a test stops them from drifting apart -- which is how TIMESTAMP
came to be documented as an ``int`` while the core produced a string.

Every file writes its rows as ``<runtime type> => <MYSQL TYPE> <MYSQL TYPE>...``
so all three can be compared mechanically. The Node binding runs the same
comparison in ``bindings/node/tests/type-mapping.test.ts``, so either test
runner catches a one-sided edit.
"""

from __future__ import annotations

import math
import re
import struct
from pathlib import Path

import pytest

from mysql_event_stream import CdcEngine

from .helpers import build_column_table_map_body, build_column_write_rows_body, build_event

REPO_ROOT = Path(__file__).resolve().parents[3]

# Runtime type each MES_COL_* value produces in each binding.
RUNTIME_TYPES = {
    "MES_COL_INT": {"node": "number | bigint", "python": "int"},
    "MES_COL_DOUBLE": {"node": "number", "python": "float"},
    "MES_COL_STRING": {"node": "string", "python": "str"},
    "MES_COL_BYTES": {"node": "Uint8Array", "python": "bytes"},
}

ROW_PATTERN = re.compile(
    r"^[\s*]*([A-Za-z_][A-Za-z0-9_ |]*?)\s*=>\s*([A-Z0-9]+(?: [A-Z0-9]+)*)\s*$"
)

EXPECTED_COLUMN_TYPES = {
    "BIGINT",
    "BINARY",
    "BIT",
    "BLOB",
    "CHAR",
    "DATE",
    "DATETIME",
    "DECIMAL",
    "DOUBLE",
    "ENUM",
    "FLOAT",
    "GEOMETRY",
    "INT",
    "JSON",
    "MEDIUMINT",
    "SET",
    "SMALLINT",
    "TEXT",
    "TIME",
    "TIMESTAMP",
    "TINYINT",
    "VARBINARY",
    "VARCHAR",
    "VECTOR",
    "YEAR",
}


def parse_table(file: str, targets: list[str]) -> dict[str, str]:
    """Parse the ``<target> => <TYPES>`` rows a documentation table declares."""
    mapping: dict[str, str] = {}
    rows = 0
    for line in (REPO_ROOT / file).read_text(encoding="utf-8").splitlines():
        match = ROW_PATTERN.match(line)
        if match is None or match.group(1) not in targets:
            continue
        rows += 1
        for mysql_type in match.group(2).split(" "):
            assert mysql_type not in mapping, f"{file} lists {mysql_type} more than once"
            mapping[mysql_type] = match.group(1)
    assert rows == len(targets), f"{file} must declare one row per column type category"
    return mapping


def canonical_table() -> dict[str, str]:
    return parse_table("core/include/mes.h", list(RUNTIME_TYPES))


def test_canonical_table_covers_every_representable_column_type() -> None:
    assert set(canonical_table()) == EXPECTED_COLUMN_TYPES


@pytest.mark.parametrize(
    ("binding", "file"),
    [
        ("python", "bindings/python/src/mysql_event_stream/types.py"),
        ("node", "bindings/node/src/types.ts"),
    ],
)
def test_binding_doc_matches_the_canonical_table(binding: str, file: str) -> None:
    canonical = canonical_table()
    expected = {
        mysql_type: RUNTIME_TYPES[target][binding] for mysql_type, target in canonical.items()
    }
    targets = [types[binding] for types in RUNTIME_TYPES.values()]
    assert parse_table(file, targets) == expected


@pytest.mark.parametrize(
    ("mysql_type", "expected"),
    [
        ("TIMESTAMP", "MES_COL_STRING"),
        ("ENUM", "MES_COL_INT"),
        ("SET", "MES_COL_INT"),
        ("VECTOR", "MES_COL_BYTES"),
    ],
)
def test_categories_that_have_been_documented_wrongly_before(
    mysql_type: str, expected: str
) -> None:
    """A table can agree with itself on every surface and still be wrong.

    TIMESTAMP is text from the decoder. ENUM and SET are ordinals kept out of
    the charset index space, so they can never carry the text of their labels.
    VECTOR is inside that index space with the binary collation, so it is
    always bytes.
    """
    assert canonical_table()[mysql_type] == expected


# MYSQL_TYPE_* codes and event types the wire-decoding cases below build.
_FLOAT, _DOUBLE, _TIMESTAMP2, _DATETIME2, _TIME2 = 4, 5, 17, 18, 19
_TABLE_MAP_EVENT, _WRITE_ROWS_EVENT = 19, 30


def _fraction(micros: int, fsp: int) -> bytes:
    """The fractional-seconds trailer MySQL writes at precision ``fsp``."""
    if fsp == 0:
        return b""
    if fsp <= 2:
        return (micros // 10000).to_bytes(1, "big")
    if fsp <= 4:
        return (micros // 100).to_bytes(2, "big")
    return micros.to_bytes(3, "big")


def _time2(hour: int, minute: int, second: int, micros: int = 0, fsp: int = 0) -> bytes:
    packed = (hour << 12) | (minute << 6) | second
    return (packed + 0x800000).to_bytes(3, "big") + _fraction(micros, fsp)


def _datetime2(
    year: int, month: int, day: int, hms: tuple[int, int, int], micros: int = 0, fsp: int = 0
) -> bytes:
    hour, minute, second = hms
    ymd = ((year * 13 + month) << 5) | day
    packed = (ymd << 17) | (hour << 12) | (minute << 6) | second
    return (packed + 0x8000000000).to_bytes(5, "big") + _fraction(micros, fsp)


def _timestamp2(seconds: int, micros: int = 0, fsp: int = 0) -> bytes:
    return seconds.to_bytes(4, "big") + _fraction(micros, fsp)


def _decode_one(lib_path: str, column_type: int, metadata: bytes, payload: bytes | None) -> object:
    """Decode one row of one column from real TABLE_MAP + WRITE_ROWS bytes."""
    table_map = build_column_table_map_body(1, "testdb", "t", column_type, metadata)
    rows = build_column_write_rows_body(1, payload)
    with CdcEngine(lib_path=lib_path) as engine:
        engine.feed(build_event(_TABLE_MAP_EVENT, 1000, table_map))
        engine.feed(build_event(_WRITE_ROWS_EVENT, 1000, rows))
        event = engine.next_event()
        assert event is not None and event.after is not None
        assert engine.next_event() is None
        return event.after["0"]


_TEMPORAL_CASES = [
    ("TIME", _TIME2, 0, _time2(12, 34, 56), "12:34:56"),
    ("TIME fsp 6", _TIME2, 6, _time2(12, 34, 56, 123456, 6), "12:34:56.123456"),
    ("DATETIME", _DATETIME2, 0, _datetime2(2026, 9, 11, (12, 34, 56)), "2026-09-11 12:34:56"),
    (
        "DATETIME fsp 3",
        _DATETIME2,
        3,
        _datetime2(2026, 9, 11, (12, 34, 56), 123000, 3),
        "2026-09-11 12:34:56.123",
    ),
    ("TIMESTAMP", _TIMESTAMP2, 0, _timestamp2(1735689600), "1735689600"),
    (
        "TIMESTAMP fsp 6",
        _TIMESTAMP2,
        6,
        _timestamp2(1735689600, 123456, 6),
        "1735689600.123456",
    ),
]

_FLOATING_VALUES = [1.5, -0.0, math.inf, -math.inf, math.nan]


class TestWireDecodedValues:
    """The documented Python types, reached from binlog bytes through CdcEngine."""

    @pytest.mark.parametrize(
        ("column_type", "fsp", "payload", "expected"),
        [case[1:] for case in _TEMPORAL_CASES],
        ids=[case[0] for case in _TEMPORAL_CASES],
    )
    def test_temporal_values_keep_their_declared_precision(
        self, lib_path: str, column_type: int, fsp: int, payload: bytes, expected: str
    ) -> None:
        value = _decode_one(lib_path, column_type, bytes([fsp]), payload)
        assert isinstance(value, str)
        assert value == expected

    @pytest.mark.parametrize(
        ("column_type", "fmt"), [(_FLOAT, "<f"), (_DOUBLE, "<d")], ids=["FLOAT", "DOUBLE"]
    )
    @pytest.mark.parametrize("number", _FLOATING_VALUES, ids=repr)
    def test_floating_values_keep_non_finite_values_and_the_sign_of_zero(
        self, lib_path: str, column_type: int, fmt: str, number: float
    ) -> None:
        width = struct.calcsize(fmt)
        value = _decode_one(lib_path, column_type, bytes([width]), struct.pack(fmt, number))
        assert isinstance(value, float)
        if math.isnan(number):
            assert math.isnan(value)
        else:
            assert value == number
            assert math.copysign(1.0, value) == math.copysign(1.0, number)

    @pytest.mark.parametrize(
        ("column_type", "fsp"), [(_TIME2, 0), (_DATETIME2, 0), (_TIMESTAMP2, 0), (_DOUBLE, 8)]
    )
    def test_null_is_none(self, lib_path: str, column_type: int, fsp: int) -> None:
        assert _decode_one(lib_path, column_type, bytes([fsp]), None) is None
