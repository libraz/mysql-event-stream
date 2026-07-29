"""Tests for mysql-event-stream type definitions."""

import ctypes

import mysql_event_stream
from mysql_event_stream import (
    BinlogPosition,
    ChangeEvent,
    ClientConfig,
    ColumnType,
    ColumnValue,
    EventType,
    MesErrorCode,
)
from mysql_event_stream._ffi import (
    MES_ERR_CHECKSUM,
    MES_ERR_CONNECT,
    MES_ERR_DECODE,
    MES_ERR_DECODE_COLUMN,
    MES_ERR_DECODE_ROW,
    MES_ERR_GTID_TAGGED_UNSUPPORTED,
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

    def test_every_exception_carries_its_native_code(self) -> None:
        for code in (MES_ERR_CHECKSUM, MES_ERR_DECODE, MES_ERR_PARSE, MES_ERR_CONNECT):
            assert exception_for_rc(code, "x").code == code  # type: ignore[attr-defined]


def test_public_error_code_matches_c_abi_values() -> None:
    assert MesErrorCode.AUTH == MES_ERR_CONNECT + 1
    assert MesErrorCode.GTID_PURGED == 405
    assert MesErrorCode.GTID_TAGGED_UNSUPPORTED == MES_ERR_GTID_TAGGED_UNSUPPORTED


class TestClientConfig:
    def test_max_queue_size_default_is_zero(self) -> None:
        # 0 is the sentinel that selects the engine default (10000);
        # it does not mean "unlimited" for the client config.
        assert ClientConfig().max_queue_size == 0
        assert ClientConfig().max_queue_bytes == 48 * 1024 * 1024
        assert ClientConfig().max_event_size == 32 * 1024 * 1024

    def test_max_queue_size_docstring_documents_default(self) -> None:
        assert ClientConfig.__doc__ is not None
        assert "10000" in ClientConfig.__doc__

    def test_password_is_not_in_repr(self) -> None:
        assert "secret" not in repr(ClientConfig(password="secret"))


class TestDeprecatedTypes:
    def test_legacy_helpers_are_not_star_exports(self) -> None:
        assert "ColumnType" not in mysql_event_stream.__all__
        assert "ColumnValue" not in mysql_event_stream.__all__

    def test_column_type_marked_deprecated(self) -> None:
        assert ColumnType.__doc__ is not None
        assert "deprecated" in ColumnType.__doc__.lower()

    def test_column_value_marked_deprecated(self) -> None:
        assert ColumnValue.__doc__ is not None
        assert "deprecated" in ColumnValue.__doc__.lower()


class TestChangeEventDocstring:
    def test_documents_special_column_representations(self) -> None:
        doc = ChangeEvent.__doc__
        assert doc is not None
        assert "JSON" in doc
        assert "ENUM" in doc
        assert "SET" in doc
        assert "BIT" in doc


class TestBinlogPosition:
    def test_default(self) -> None:
        pos = BinlogPosition()
        assert pos.file == ""
        assert pos.offset == 0

    def test_values(self) -> None:
        pos = BinlogPosition(file="binlog.000001", offset=154)
        assert pos.file == "binlog.000001"
        assert pos.offset == 154
