"""Differential tests for the column marshalling fast path.

``_convert_columns`` copies column payloads by slicing a payload window laid
over the C buffer rather than by calling ``ctypes.string_at`` per column. Every
case here runs it against a deliberately naive reference that reads one field
at a time and copies with ``string_at``, and requires the two to agree exactly
-- same keys, same value types, same distinction between ``None``, ``""`` and
``b""``. The reference is what the C ABI contract says the conversion means;
the fast path is only allowed to be faster.
"""

from __future__ import annotations

import ctypes
import math
import random
from typing import Any

import pytest

from mysql_event_stream._ffi import (
    MES_COL_BYTES,
    MES_COL_DOUBLE,
    MES_COL_INT,
    MES_COL_NULL,
    MES_COL_STRING,
    MESColumn,
)
from mysql_event_stream.engine import _PAYLOAD_WINDOW_BYTES, _convert_columns

INT64_MIN = -(2**63)
INT64_MAX = 2**63 - 1


def reference_convert_columns(
    cols: ctypes.Array[MESColumn], count: int, name_cache: dict[bytes, str] | None = None
) -> dict[str, Any]:
    """Field-at-a-time reference for the contract ``_convert_columns`` implements."""
    result: dict[str, Any] = {}
    for i in range(count):
        col = cols[i]

        raw_name = col.col_name or b""
        if raw_name and name_cache is not None:
            name = name_cache.get(raw_name)
            if name is None:
                if len(name_cache) >= 8192:
                    name_cache.clear()
                name = raw_name.decode("utf-8", errors="replace")
                name_cache[raw_name] = name
        else:
            name = raw_name.decode("utf-8", errors="replace") if raw_name else ""
        key = name if name else str(i)

        col_type = col.type
        if col_type == MES_COL_NULL:
            result[key] = None
        elif col_type == MES_COL_INT:
            result[key] = col.int_val
        elif col_type == MES_COL_DOUBLE:
            result[key] = col.double_val
        elif col_type == MES_COL_STRING:
            if col.str_data and col.str_len > 0:
                result[key] = ctypes.string_at(col.str_data, col.str_len).decode(
                    "utf-8", errors="surrogateescape"
                )
            else:
                result[key] = ""
        elif col_type == MES_COL_BYTES:
            if col.str_data and col.str_len > 0:
                result[key] = ctypes.string_at(col.str_data, col.str_len)
            else:
                result[key] = b""
        else:
            result[key] = None

    return result


class ColumnSpec:
    """One column's C-side content, kept alive for the duration of a test."""

    def __init__(
        self,
        col_type: int,
        *,
        int_val: int = 0,
        double_val: float = 0.0,
        payload: bytes | None = None,
        name: bytes | None = None,
    ) -> None:
        self.col_type = col_type
        self.int_val = int_val
        self.double_val = double_val
        self.payload = payload
        self.name = name


def build_array(specs: list[ColumnSpec]) -> ctypes.Array[MESColumn]:
    """Materialise an owned ``mes_column_t`` array from column specs."""
    array = (MESColumn * len(specs))()
    keepalive: list[Any] = []
    for i, spec in enumerate(specs):
        array[i].type = spec.col_type
        array[i].int_val = spec.int_val
        array[i].double_val = spec.double_val
        if spec.payload is None:
            array[i].str_data = None
            array[i].str_len = 0
        else:
            buf = ctypes.create_string_buffer(spec.payload, len(spec.payload))
            keepalive.append(buf)
            array[i].str_data = ctypes.cast(buf, ctypes.c_void_p).value
            array[i].str_len = len(spec.payload)
        if spec.name is None:
            array[i].col_name = None
        else:
            name_buf = ctypes.create_string_buffer(spec.name)
            keepalive.append(name_buf)
            array[i].col_name = ctypes.cast(name_buf, ctypes.c_char_p).value
    # Payload and name buffers are referenced only by raw address, so pin them
    # to the array; without this the values would be freed before the read.
    array._keepalive = keepalive  # type: ignore[attr-defined]
    return array


def assert_identical(actual: dict[str, Any], expected: dict[str, Any]) -> None:
    """Compare two conversion results including value types and NaN payloads."""
    assert list(actual) == list(expected)
    for key, want in expected.items():
        got = actual[key]
        assert type(got) is type(want), f"{key}: {type(got)} != {type(want)}"
        if isinstance(want, float) and math.isnan(want):
            assert math.isnan(got)
        else:
            assert got == want, key


def check_differential(specs: list[ColumnSpec]) -> dict[str, Any]:
    """Run both implementations over the same array, with and without a cache."""
    array = build_array(specs)
    count = len(specs)

    uncached = _convert_columns(array, count)
    assert_identical(uncached, reference_convert_columns(array, count))

    fast_cache: dict[bytes, str] = {}
    reference_cache: dict[bytes, str] = {}
    cached = _convert_columns(array, count, fast_cache)
    assert_identical(cached, reference_convert_columns(array, count, reference_cache))
    assert fast_cache == reference_cache
    # A warm cache must not change the answer.
    assert_identical(_convert_columns(array, count, fast_cache), cached)
    return uncached


class TestColumnTypes:
    def test_null_column(self) -> None:
        assert check_differential([ColumnSpec(MES_COL_NULL, name=b"maybe")]) == {"maybe": None}

    @pytest.mark.parametrize("value", [0, 1, -1, 42, INT64_MIN, INT64_MAX])
    def test_int_column(self, value: int) -> None:
        assert check_differential([ColumnSpec(MES_COL_INT, int_val=value, name=b"n")]) == {
            "n": value
        }

    @pytest.mark.parametrize(
        "value",
        [0.0, -0.0, 1.5, -2.25e300, 5e-324, float("inf"), float("-inf"), float("nan")],
    )
    def test_double_column(self, value: float) -> None:
        check_differential([ColumnSpec(MES_COL_DOUBLE, double_val=value, name=b"d")])

    @pytest.mark.parametrize(
        "payload",
        [
            b"hello",
            b"\xe6\x97\xa5\xe6\x9c\xac\xe8\xaa\x9e",  # multibyte UTF-8
            b"caf\xe9",  # latin-1, invalid UTF-8 -> surrogateescape
            b"\xff\xfe\xfd",  # not decodable at all
            b"with\x00embedded\x00nul",
            b"\x00",
            b"x" * 1000,
        ],
    )
    def test_string_column(self, payload: bytes) -> None:
        result = check_differential([ColumnSpec(MES_COL_STRING, payload=payload, name=b"s")])
        assert result["s"].encode("utf-8", errors="surrogateescape") == payload

    @pytest.mark.parametrize(
        "payload",
        [b"\x00\x01\xff", b"\x00" * 16, bytes(range(256)), b"\x89PNG\r\n\x1a\n"],
    )
    def test_bytes_column(self, payload: bytes) -> None:
        assert check_differential([ColumnSpec(MES_COL_BYTES, payload=payload, name=b"b")]) == {
            "b": payload
        }

    def test_empty_string_is_str_not_none(self) -> None:
        # str_data NULL and a valid pointer with str_len 0 must both give "".
        assert check_differential([ColumnSpec(MES_COL_STRING, name=b"s")]) == {"s": ""}
        assert check_differential([ColumnSpec(MES_COL_STRING, payload=b"", name=b"s")]) == {"s": ""}

    def test_empty_bytes_is_bytes_not_none(self) -> None:
        assert check_differential([ColumnSpec(MES_COL_BYTES, name=b"b")]) == {"b": b""}
        assert check_differential([ColumnSpec(MES_COL_BYTES, payload=b"", name=b"b")]) == {"b": b""}

    @pytest.mark.parametrize("col_type", [5, 99, -1, 2**31 - 1])
    def test_unknown_type_is_none(self, col_type: int) -> None:
        assert check_differential([ColumnSpec(col_type, int_val=7, name=b"u")]) == {"u": None}


class TestPayloadWindowBoundary:
    """The payload window is a size ceiling, and a slice past it would truncate."""

    @pytest.mark.parametrize(
        "length",
        [
            1,
            _PAYLOAD_WINDOW_BYTES - 1,
            _PAYLOAD_WINDOW_BYTES,
            _PAYLOAD_WINDOW_BYTES + 1,
            _PAYLOAD_WINDOW_BYTES * 2 + 3,
        ],
    )
    def test_payload_length_around_the_window(self, length: int) -> None:
        payload = bytes((i * 7 + 1) % 256 for i in range(length))
        result = check_differential(
            [
                ColumnSpec(MES_COL_BYTES, payload=payload, name=b"blob"),
                ColumnSpec(MES_COL_STRING, payload=b"a" * length, name=b"text"),
            ]
        )
        assert result["blob"] == payload
        assert len(result["blob"]) == length
        assert len(result["text"]) == length


class TestColumnKeys:
    def test_missing_name_falls_back_to_index(self) -> None:
        specs = [
            ColumnSpec(MES_COL_INT, int_val=1),
            ColumnSpec(MES_COL_INT, int_val=2, name=b""),
            ColumnSpec(MES_COL_INT, int_val=3, name=b"third"),
        ]
        assert check_differential(specs) == {"0": 1, "1": 2, "third": 3}

    def test_non_ascii_column_name(self) -> None:
        assert check_differential([ColumnSpec(MES_COL_INT, int_val=1, name="名前".encode())]) == {
            "名前": 1
        }

    def test_invalid_utf8_column_name_substitutes(self) -> None:
        """An undecodable identifier costs one character, never the whole row."""
        array = build_array([ColumnSpec(MES_COL_INT, int_val=1, name=b"bad\xff")])
        assert _convert_columns(array, 1) == {"bad�": 1}
        assert _convert_columns(array, 1, {}) == {"bad�": 1}
        assert reference_convert_columns(array, 1) == {"bad�": 1}

    def test_duplicate_names_keep_the_last_value(self) -> None:
        specs = [
            ColumnSpec(MES_COL_INT, int_val=1, name=b"dup"),
            ColumnSpec(MES_COL_STRING, payload=b"second", name=b"dup"),
        ]
        assert check_differential(specs) == {"dup": "second"}

    def test_zero_count_is_empty(self) -> None:
        assert check_differential([]) == {}


class TestMixedArrays:
    def test_wide_mixed_row(self) -> None:
        specs = [
            ColumnSpec(MES_COL_INT, int_val=-9, name=b"id"),
            ColumnSpec(MES_COL_NULL, name=b"deleted_at"),
            ColumnSpec(MES_COL_STRING, payload=b"2026-08-15 04:00:00.123456", name=b"created_at"),
            ColumnSpec(MES_COL_DOUBLE, double_val=1.25, name=b"ratio"),
            ColumnSpec(MES_COL_BYTES, payload=b"\x00\x01\x02", name=b"blob"),
            ColumnSpec(MES_COL_STRING, payload=b"", name=b"note"),
            ColumnSpec(MES_COL_BYTES, name=b"vector"),
            ColumnSpec(MES_COL_INT, int_val=INT64_MAX),
        ]
        assert check_differential(specs) == {
            "id": -9,
            "deleted_at": None,
            "created_at": "2026-08-15 04:00:00.123456",
            "ratio": 1.25,
            "blob": b"\x00\x01\x02",
            "note": "",
            "vector": b"",
            "7": INT64_MAX,
        }

    def test_partial_count_reads_only_the_requested_prefix(self) -> None:
        specs = [
            ColumnSpec(MES_COL_INT, int_val=1, name=b"a"),
            ColumnSpec(MES_COL_INT, int_val=2, name=b"b"),
            ColumnSpec(MES_COL_INT, int_val=3, name=b"c"),
        ]
        array = build_array(specs)
        assert_identical(_convert_columns(array, 2), reference_convert_columns(array, 2))
        assert _convert_columns(array, 2) == {"a": 1, "b": 2}

    def test_pointer_argument_matches_array_argument(self) -> None:
        # _convert_event passes a POINTER(MESColumn), not an Array.
        specs = [
            ColumnSpec(MES_COL_STRING, payload=b"via pointer", name=b"s"),
            ColumnSpec(MES_COL_BYTES, payload=b"\xff\x00", name=b"b"),
        ]
        array = build_array(specs)
        pointer = ctypes.cast(array, ctypes.POINTER(MESColumn))
        assert_identical(_convert_columns(pointer, 2), reference_convert_columns(array, 2))

    def test_randomised_rows_match_the_reference(self) -> None:
        rng = random.Random(20260815)
        types = [MES_COL_NULL, MES_COL_INT, MES_COL_DOUBLE, MES_COL_STRING, MES_COL_BYTES, 77]
        for _ in range(200):
            specs = []
            for i in range(rng.randint(0, 32)):
                col_type = rng.choice(types)
                payload: bytes | None = None
                if col_type in (MES_COL_STRING, MES_COL_BYTES):
                    choice = rng.random()
                    if choice < 0.15:
                        payload = None
                    elif choice < 0.3:
                        payload = b""
                    else:
                        payload = bytes(rng.randrange(256) for _ in range(rng.randint(1, 64)))
                name: bytes | None = None
                if rng.random() < 0.8:
                    name = f"col_{i}".encode() if rng.random() < 0.9 else b""
                specs.append(
                    ColumnSpec(
                        col_type,
                        int_val=rng.randint(INT64_MIN, INT64_MAX),
                        double_val=rng.uniform(-1e12, 1e12),
                        payload=payload,
                        name=name,
                    )
                )
            check_differential(specs)
