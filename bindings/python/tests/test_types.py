"""Tests for mysql-event-stream type definitions."""

import ctypes

import pytest

import mysql_event_stream
from mysql_event_stream import (
    BinlogPosition,
    ChangeEvent,
    ClientConfig,
    ColumnType,
    ColumnValue,
    EventType,
)
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
    MesConnectionError,
    MesError,
    MesErrorCode,
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

    def test_other_falls_back_to_the_coded_base(self) -> None:
        exc = exception_for_rc(MES_ERR_CONNECT, "x")
        assert type(exc) is MesError

    def test_all_subclass_runtime_error(self) -> None:
        for code in (MES_ERR_CHECKSUM, MES_ERR_DECODE, MES_ERR_PARSE, MES_ERR_CONNECT):
            assert isinstance(exception_for_rc(code, "x"), RuntimeError)

    def test_every_exception_carries_its_native_code(self) -> None:
        for code in (MES_ERR_CHECKSUM, MES_ERR_DECODE, MES_ERR_PARSE, MES_ERR_CONNECT):
            assert exception_for_rc(code, "x").code == code


_MAPPED_CODES = (
    MES_ERR_CHECKSUM,
    MES_ERR_DECODE,
    MES_ERR_DECODE_COLUMN,
    MES_ERR_DECODE_ROW,
    MES_ERR_PARSE,
    MES_ERR_CONNECT,
)


class TestExceptionCompatibility:
    """A handler written against the exception surface must keep working.

    Sharing a base may only ever narrow what a caller sees: an existing
    ``except`` clause has to catch the same codes it caught before, and the
    only code whose type changes is the one that used to arrive uncategorized.
    """

    def test_runtime_error_still_catches_every_mapped_code(self) -> None:
        for code in _MAPPED_CODES:
            caught: RuntimeError | None = None
            try:
                raise exception_for_rc(code, "boom")
            except RuntimeError as err:
                caught = err
            assert caught is not None, f"code {code} escaped an except RuntimeError handler"

    def test_each_category_still_raises_its_own_type(self) -> None:
        expected: dict[int, type[MesError]] = {
            MES_ERR_CHECKSUM: ChecksumError,
            MES_ERR_DECODE: DecodeError,
            MES_ERR_DECODE_COLUMN: DecodeError,
            MES_ERR_DECODE_ROW: DecodeError,
            MES_ERR_PARSE: ParseError,
        }
        for code, category in expected.items():
            assert type(exception_for_rc(code, "x")) is category

    def test_a_category_handler_catches_only_its_own_codes(self) -> None:
        decode_codes = (MES_ERR_DECODE, MES_ERR_DECODE_COLUMN, MES_ERR_DECODE_ROW)
        for code in _MAPPED_CODES:
            try:
                raise exception_for_rc(code, "boom")
            except ChecksumError:
                assert code == MES_ERR_CHECKSUM
            except DecodeError:
                assert code in decode_codes
            except ParseError:
                assert code == MES_ERR_PARSE
            except MesError:
                assert code not in (MES_ERR_CHECKSUM, MES_ERR_PARSE, *decode_codes)

    def test_the_fallback_code_reaches_a_handler_that_can_read_it(self) -> None:
        """The uncategorized codes are the ones callers most need to classify."""
        try:
            raise exception_for_rc(MES_ERR_CONNECT, "boom")
        except MesError as err:
            assert err.code == MES_ERR_CONNECT

    def test_the_message_stays_the_only_exception_argument(self) -> None:
        exc = exception_for_rc(MES_ERR_PARSE, "boom")
        assert str(exc) == "boom"
        assert exc.args == ("boom",)

    def test_an_error_built_without_a_code_reports_an_internal_one(self) -> None:
        assert MesError("boom").code == MesErrorCode.INTERNAL
        assert ParseError("boom").code == MesErrorCode.INTERNAL
        assert MesConnectionError("boom").code == MesErrorCode.INTERNAL


class TestConnectionErrorCategory:
    """A connection failure stays where Python puts every other socket failure.

    ``MesConnectionError`` declares the same ``code`` as ``MesError`` without
    sharing a base with it. Joining the two hierarchies would make an existing
    ``except RuntimeError`` clause start swallowing connection failures it was
    never written to handle, which is why the split is pinned here.
    """

    def test_it_is_still_caught_as_a_connection_error(self) -> None:
        with pytest.raises(ConnectionError) as excinfo:
            raise MesConnectionError("refused", MES_ERR_CONNECT)
        caught = excinfo.value
        assert isinstance(caught, MesConnectionError)
        assert caught.code == MES_ERR_CONNECT

    def test_it_is_still_caught_as_an_os_error(self) -> None:
        with pytest.raises(OSError) as excinfo:
            raise MesConnectionError("refused", MES_ERR_CONNECT)
        caught = excinfo.value
        assert isinstance(caught, MesConnectionError)
        assert caught.code == MES_ERR_CONNECT

    def test_it_stays_out_of_the_runtime_error_hierarchy(self) -> None:
        error = MesConnectionError("refused", MES_ERR_CONNECT)
        assert not isinstance(error, RuntimeError)
        assert not isinstance(error, MesError)

    def test_the_message_is_not_read_as_an_errno_pair(self) -> None:
        """Two arguments are how OSError spells (errno, strerror)."""
        error = MesConnectionError("refused", MES_ERR_CONNECT)
        assert str(error) == "refused"
        assert error.args == ("refused",)
        assert error.errno is None
        assert error.strerror is None


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
