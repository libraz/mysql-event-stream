"""Tests for mysql-event-stream type definitions."""

import ctypes

from mysql_event_stream import BinlogPosition, ChangeEvent, ColumnType, ColumnValue, EventType
from mysql_event_stream._ffi import (
    MES_ERR_CHECKSUM,
    MES_ERR_CONNECT,
    MES_ERR_DECODE,
    MES_ERR_DECODE_COLUMN,
    MES_ERR_DECODE_ROW,
    MES_ERR_PARSE,
)
from mysql_event_stream.types import (
    ChecksumError,
    DecodeError,
    ParseError,
    exception_for_rc,
)


class TestColumnValue:
    def test_null(self) -> None:
        col = ColumnValue.null()
        assert col.type == ColumnType.NULL
        assert col.value is None

    def test_int(self) -> None:
        col = ColumnValue.int_val(42)
        assert col.type == ColumnType.INT
        assert col.value == 42

    def test_double(self) -> None:
        col = ColumnValue.double_val(3.14)
        assert col.type == ColumnType.DOUBLE
        assert col.value == 3.14

    def test_string(self) -> None:
        col = ColumnValue.string_val("hello")
        assert col.type == ColumnType.STRING
        assert col.value == "hello"

    def test_bytes(self) -> None:
        col = ColumnValue.bytes_val(b"\x01\x02")
        assert col.type == ColumnType.BYTES
        assert col.value == b"\x01\x02"

    def test_frozen(self) -> None:
        col = ColumnValue.int_val(1)
        try:
            col.value = 2  # type: ignore[misc]
        except AttributeError:
            pass
        else:
            raise AssertionError("Should have raised AttributeError")


class TestAbiStructSizes:
    """The ctypes layout must match the C ABI exactly."""

    def test_event_size_matches_libmes(self, lib_path: str) -> None:
        from mysql_event_stream._ffi import MESEvent, load_library

        lib = load_library(lib_path)
        assert ctypes.sizeof(MESEvent) == lib.mes_sizeof_event()

    def test_column_size_matches_libmes(self, lib_path: str) -> None:
        from mysql_event_stream._ffi import MESColumn, load_library

        lib = load_library(lib_path)
        assert ctypes.sizeof(MESColumn) == lib.mes_sizeof_column()


class TestColumnValueName:
    def test_default_name_empty(self) -> None:
        cv = ColumnValue.null()
        assert cv.name == ""

    def test_name_preserved(self) -> None:
        cv = ColumnValue(type=ColumnType.INT, value=42, name="id")
        assert cv.name == "id"


class TestChangeEvent:
    def test_insert_event(self) -> None:
        event = ChangeEvent(
            type=EventType.INSERT,
            database="testdb",
            table="users",
            before=None,
            after={"id": 1},
            timestamp=1000,
            position=BinlogPosition(file="binlog.000001", offset=4),
        )
        assert event.type == EventType.INSERT
        assert event.database == "testdb"
        assert event.after is not None
        assert len(event.after) == 1
        assert event.after["id"] == 1

    def test_names_resolved_defaults_true(self) -> None:
        event = ChangeEvent(
            type=EventType.INSERT,
            database="db",
            table="t",
            before=None,
            after={"id": 1},
            timestamp=1,
            position=BinlogPosition(),
        )
        assert event.names_resolved is True

    def test_names_resolved_settable(self) -> None:
        event = ChangeEvent(
            type=EventType.INSERT,
            database="db",
            table="t",
            before=None,
            after={"0": 1},
            timestamp=1,
            position=BinlogPosition(),
            names_resolved=False,
        )
        assert event.names_resolved is False


class TestExceptionForRc:
    def test_checksum(self) -> None:
        exc = exception_for_rc(MES_ERR_CHECKSUM, "boom")
        assert isinstance(exc, ChecksumError)
        assert str(exc) == "boom"

    def test_decode_variants(self) -> None:
        for code in (MES_ERR_DECODE, MES_ERR_DECODE_COLUMN, MES_ERR_DECODE_ROW):
            assert isinstance(exception_for_rc(code, "x"), DecodeError)

    def test_parse(self) -> None:
        assert isinstance(exception_for_rc(MES_ERR_PARSE, "x"), ParseError)

    def test_other_falls_back_to_runtime_error(self) -> None:
        exc = exception_for_rc(MES_ERR_CONNECT, "x")
        assert type(exc) is RuntimeError

    def test_all_subclass_runtime_error(self) -> None:
        for code in (MES_ERR_CHECKSUM, MES_ERR_DECODE, MES_ERR_PARSE, MES_ERR_CONNECT):
            assert isinstance(exception_for_rc(code, "x"), RuntimeError)


class TestBinlogPosition:
    def test_default(self) -> None:
        pos = BinlogPosition()
        assert pos.file == ""
        assert pos.offset == 0

    def test_values(self) -> None:
        pos = BinlogPosition(file="binlog.000001", offset=154)
        assert pos.file == "binlog.000001"
        assert pos.offset == 154
