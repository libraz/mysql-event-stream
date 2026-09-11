"""Tests for CdcEngine - the Python wrapper around libmes."""

import ctypes
import inspect
import re
from collections.abc import Callable
from unittest.mock import MagicMock, patch

import pytest

from mysql_event_stream import CdcEngine, ChecksumError, EventType, ParseError
from mysql_event_stream._ffi import (
    MES_COL_BYTES,
    MES_COL_INT,
    MES_COL_STRING,
    MES_ERR_AUTH,
    MES_ERR_INVALID_ARG,
    MES_OK,
    MESColumn,
    MESEvent,
)
from mysql_event_stream.engine import _convert_columns, _convert_event

from .helpers import (
    build_delete_rows_body,
    build_event,
    build_event_no_checksum,
    build_rotate_body,
    build_table_map_body,
    build_update_rows_body,
    build_write_rows_body,
)


class TestEngineLifecycle:
    def test_create_and_close(self, lib_path: str) -> None:
        engine = CdcEngine(lib_path=lib_path)
        assert not engine.has_events()
        engine.close()

    def test_context_manager(self, lib_path: str) -> None:
        with CdcEngine(lib_path=lib_path) as engine:
            assert not engine.has_events()

    def test_double_close(self, lib_path: str) -> None:
        engine = CdcEngine(lib_path=lib_path)
        engine.close()
        engine.close()  # Should not crash

    def test_error_after_close(self, lib_path: str) -> None:
        engine = CdcEngine(lib_path=lib_path)
        engine.close()
        with pytest.raises(RuntimeError, match="closed"):
            engine.feed(b"\x00")


class TestFeed:
    def test_empty_feed(self, lib_path: str) -> None:
        with CdcEngine(lib_path=lib_path) as engine:
            consumed = engine.feed(b"")
            assert consumed == 0

    def test_no_event_initially(self, lib_path: str) -> None:
        with CdcEngine(lib_path=lib_path) as engine:
            assert engine.next_event() is None

    def test_negative_max_queue_size_rejected(self, lib_path: str) -> None:
        with (
            CdcEngine(lib_path=lib_path) as engine,
            pytest.raises(ValueError, match="non-negative"),
        ):
            engine.set_max_queue_size(-1)

    @pytest.mark.parametrize("value", ["10000", 1.5, True, None])
    def test_wrong_typed_max_queue_size_rejected(self, lib_path: str, value: object) -> None:
        """A bool would otherwise pass as a queue of one event."""
        with (
            CdcEngine(lib_path=lib_path) as engine,
            pytest.raises(TypeError, match="max_size must be an integer"),
        ):
            engine.set_max_queue_size(value)  # type: ignore[arg-type]

    @pytest.mark.parametrize("value", ["32768", 1.5, True, None])
    def test_wrong_typed_max_event_size_rejected(self, lib_path: str, value: object) -> None:
        with (
            CdcEngine(lib_path=lib_path) as engine,
            pytest.raises(TypeError, match="max_event_size must be an integer"),
        ):
            engine.set_max_event_size(value)  # type: ignore[arg-type]

    @pytest.mark.parametrize(
        ("setter", "option"),
        [
            ("set_include_databases", "include_databases"),
            ("set_include_tables", "include_tables"),
            ("set_exclude_tables", "exclude_tables"),
        ],
    )
    @pytest.mark.parametrize("value", ["db", [1], None, ("db",)])
    def test_wrong_typed_filter_list_rejected(
        self, lib_path: str, setter: str, option: str, value: object
    ) -> None:
        """A bare string would otherwise be encoded one character per filter."""
        with (
            CdcEngine(lib_path=lib_path) as engine,
            pytest.raises(TypeError, match=f"{option} must be a list of strings"),
        ):
            getattr(engine, setter)(value)

    def test_checksum_none_override(self, lib_path: str) -> None:
        with CdcEngine(lib_path=lib_path) as engine:
            engine.set_checksum_enabled(False)
            tm = build_event_no_checksum(19, 1000, build_table_map_body(1, "db", "t"))
            wr = build_event_no_checksum(30, 1000, build_write_rows_body(1, 73))
            assert engine.feed(tm + wr) == len(tm + wr)
            event = engine.next_event()
            assert event is not None
            assert event.after is not None
            assert event.after["0"] == 73

    def test_corrupted_checksum_is_typed_error(self, lib_path: str) -> None:
        with CdcEngine(lib_path=lib_path) as engine:
            event = bytearray(build_event(19, 1000, build_table_map_body(1, "db", "t")))
            event[20] ^= 0x40
            with pytest.raises(ChecksumError, match="checksum mismatch"):
                engine.feed(event)


class TestInsertEvent:
    def test_insert(self, lib_path: str) -> None:
        with CdcEngine(lib_path=lib_path) as engine:
            tm_body = build_table_map_body(1, "testdb", "users")
            tm_event = build_event(19, 1000, tm_body)
            wr_body = build_write_rows_body(1, 42)
            wr_event = build_event(30, 1000, wr_body)

            engine.feed(tm_event + wr_event)

            assert engine.has_events()
            event = engine.next_event()
            assert event is not None
            assert event.type == EventType.INSERT
            assert event.database == "testdb"
            assert event.table == "users"
            assert event.before is None
            assert event.after is not None
            assert len(event.after) == 1
            assert event.after["0"] == 42
            assert event.timestamp == 1000
            # Standalone mode (no metadata connection): the TABLE_MAP has no
            # names, so positional keys are not reported as resolved names.
            assert event.names_resolved is False

            assert engine.next_event() is None


class TestUpdateEvent:
    def test_update(self, lib_path: str) -> None:
        with CdcEngine(lib_path=lib_path) as engine:
            tm = build_event(19, 2000, build_table_map_body(1, "testdb", "users"))
            ur = build_event(31, 2000, build_update_rows_body(1, 10, 20))

            engine.feed(tm + ur)

            event = engine.next_event()
            assert event is not None
            assert event.type == EventType.UPDATE
            assert event.before is not None
            assert event.before["0"] == 10
            assert event.after is not None
            assert event.after["0"] == 20


class TestDeleteEvent:
    def test_delete(self, lib_path: str) -> None:
        with CdcEngine(lib_path=lib_path) as engine:
            tm = build_event(19, 3000, build_table_map_body(1, "testdb", "users"))
            dr = build_event(32, 3000, build_delete_rows_body(1, 99))

            engine.feed(tm + dr)

            event = engine.next_event()
            assert event is not None
            assert event.type == EventType.DELETE
            assert event.before is not None
            assert event.before["0"] == 99
            assert event.after is None


class TestRotateEvent:
    def test_rotate(self, lib_path: str) -> None:
        with CdcEngine(lib_path=lib_path) as engine:
            rot = build_event(4, 0, build_rotate_body(4, "binlog.000002"))
            engine.feed(rot)

            pos = engine.get_position()
            assert pos.file == "binlog.000002"
            assert pos.offset == 4


class TestReset:
    def test_reset(self, lib_path: str) -> None:
        with CdcEngine(lib_path=lib_path) as engine:
            tm = build_event(19, 1000, build_table_map_body(1, "db", "t"))
            wr = build_event(30, 1000, build_write_rows_body(1, 1))
            engine.feed(tm + wr)

            assert engine.has_events()
            engine.reset()
            # reset clears parser state but deliberately retains already
            # decoded events so callers can drain them after a feed error.
            event = engine.next_event()
            assert event is not None
            assert event.after is not None
            assert event.after["0"] == 1
            assert not engine.has_events()


class TestMultipleEvents:
    def test_multiple(self, lib_path: str) -> None:
        with CdcEngine(lib_path=lib_path) as engine:
            tm = build_event(19, 1000, build_table_map_body(1, "db", "t"))
            wr1 = build_event(30, 1000, build_write_rows_body(1, 10))
            tm2 = build_event(19, 1001, build_table_map_body(1, "db", "t"))
            wr2 = build_event(30, 1001, build_write_rows_body(1, 20))

            engine.feed(tm + wr1 + tm2 + wr2)

            e1 = engine.next_event()
            e2 = engine.next_event()
            assert e1 is not None
            assert e2 is not None
            assert e1.after is not None
            assert e1.after["0"] == 10
            assert e2.after is not None
            assert e2.after["0"] == 20
            assert engine.next_event() is None


def _make_string_column(data: bytes, col_name: bytes | None = None) -> MESColumn:
    """Build a MESColumn of type STRING with the given raw bytes."""
    col = MESColumn()
    col.type = MES_COL_STRING
    col.int_val = 0
    col.double_val = 0.0
    # Allocate a ctypes buffer and store its address in the c_void_p field
    buf = ctypes.create_string_buffer(data, len(data))
    col.str_data = ctypes.cast(buf, ctypes.c_void_p).value
    col.str_len = len(data)
    if col_name is not None:
        name_buf = ctypes.create_string_buffer(col_name)
        col.col_name = ctypes.cast(name_buf, ctypes.c_void_p).value
    else:
        col.col_name = None
    # Keep references alive so buffers are not garbage collected
    col._keep_alive = (buf, name_buf if col_name is not None else None)  # type: ignore[attr-defined]
    return col


def _make_bytes_column(data: bytes) -> MESColumn:
    """Build a MESColumn of type BYTES with the given raw bytes."""
    col = MESColumn()
    col.type = MES_COL_BYTES
    col.int_val = 0
    col.double_val = 0.0
    buf = ctypes.create_string_buffer(data, len(data))
    col.str_data = ctypes.cast(buf, ctypes.c_void_p).value
    col.str_len = len(data)
    col.col_name = None
    col._keep_alive = (buf,)  # type: ignore[attr-defined]
    return col


class TestConvertEvent:
    """Test conversion of C ABI event types."""

    def test_unknown_event_type_raises_parse_error(self) -> None:
        raw = MESEvent()
        raw.type = 99

        with pytest.raises(ParseError, match="Unknown event type: 99") as error:
            _convert_event(raw)
        assert error.value.code == 100

    def test_undecodable_identifiers_substitute_instead_of_losing_the_event(self) -> None:
        """next_event() either returns an event or raises with a C ABI code.

        The identifiers are server-supplied bytes, so a byte UTF-8 rejects must
        not turn a delivered event into an uncoded UnicodeDecodeError: the
        consumer would lose the change and have nothing to classify.
        """
        raw = MESEvent()
        raw.type = 0
        raw.database = b"db\xff"
        raw.table = b"tbl\xfe"
        raw.binlog_file = b"binlog.\xfd0001"
        raw.binlog_offset = 4

        event = _convert_event(raw)
        assert event.database == "db�"
        assert event.table == "tbl�"
        assert event.position.file == "binlog.�0001"
        assert event.position.offset == 4

    def test_source_sql_is_exposed(self) -> None:
        raw = MESEvent()
        raw.type = 0
        raw.database = b"testdb"
        raw.table = b"users"
        raw.source_sql = b"INSERT INTO users VALUES (42)"

        event = _convert_event(raw)
        assert event.source_sql == "INSERT INTO users VALUES (42)"


class TestConvertColumns:
    """Test _convert_columns handles c_void_p str_data correctly."""

    def test_string_column(self) -> None:
        col = _make_string_column(b"hello world", b"greeting")
        arr = (MESColumn * 1)(col)
        result = _convert_columns(arr, 1)
        assert result["greeting"] == "hello world"

    def test_bytes_column(self) -> None:
        col = _make_bytes_column(b"\x00\x01\xff")
        arr = (MESColumn * 1)(col)
        result = _convert_columns(arr, 1)
        assert result["0"] == b"\x00\x01\xff"

    def test_non_utf8_string_uses_surrogateescape(self) -> None:
        # latin1 encoded e-acute: 0xe9 is not valid UTF-8
        latin1_bytes = b"caf\xe9"
        col = _make_string_column(latin1_bytes)
        arr = (MESColumn * 1)(col)
        result = _convert_columns(arr, 1)
        # Should not raise, and should use surrogateescape
        value = result["0"]
        assert isinstance(value, str)
        # Round-trip back to bytes via surrogateescape
        assert value.encode("utf-8", errors="surrogateescape") == latin1_bytes

    def test_empty_string_column(self) -> None:
        col = MESColumn()
        col.type = MES_COL_STRING
        col.str_data = None
        col.str_len = 0
        col.col_name = None
        arr = (MESColumn * 1)(col)
        result = _convert_columns(arr, 1)
        assert result["0"] == ""

    def test_empty_bytes_column(self) -> None:
        col = MESColumn()
        col.type = MES_COL_BYTES
        col.str_data = None
        col.str_len = 0
        col.col_name = None
        arr = (MESColumn * 1)(col)
        result = _convert_columns(arr, 1)
        assert result["0"] == b""


def _make_int_column(value: int) -> MESColumn:
    """Build a MESColumn of type INT with the given value."""
    col = MESColumn()
    col.type = MES_COL_INT
    col.int_val = value
    col.double_val = 0.0
    col.str_data = None
    col.str_len = 0
    col.col_name = None
    return col


class TestSpecialColumnRepresentations:
    """Document how special MySQL column types surface to Python.

    The C engine flattens every MySQL type onto the small C-ABI set
    (NULL/INT/DOUBLE/STRING/BYTES), so the binding sees JSON as bytes and
    ENUM/SET/BIT as integers. These tests pin that contract.
    """

    def test_json_column_is_bytes(self) -> None:
        # JSON is delivered as MES_COL_BYTES holding MySQL binary JSON.
        binary_json = b"\x00\x01\x00\x0c\x00\x0bhello"
        col = _make_bytes_column(binary_json)
        arr = (MESColumn * 1)(col)
        result = _convert_columns(arr, 1)
        assert isinstance(result["0"], bytes)
        assert result["0"] == binary_json

    def test_enum_column_is_int_index(self) -> None:
        # ENUM is delivered as MES_COL_INT carrying the 1-based index.
        col = _make_int_column(2)
        arr = (MESColumn * 1)(col)
        result = _convert_columns(arr, 1)
        assert result["0"] == 2
        assert isinstance(result["0"], int)

    def test_set_column_is_int_bitmask(self) -> None:
        # SET is delivered as MES_COL_INT carrying a bitmask (members 1 and 3).
        col = _make_int_column(0b101)
        arr = (MESColumn * 1)(col)
        result = _convert_columns(arr, 1)
        assert result["0"] == 0b101
        assert isinstance(result["0"], int)

    def test_bit_column_is_int(self) -> None:
        # BIT is delivered as MES_COL_INT carrying the bit value.
        col = _make_int_column(0xFF)
        arr = (MESColumn * 1)(col)
        result = _convert_columns(arr, 1)
        assert result["0"] == 0xFF
        assert isinstance(result["0"], int)

    def test_enum_set_and_bit_are_decoded_from_real_binlog_rows(self, lib_path: str) -> None:
        def table_map_body(table_id: int, column_type: int, metadata: bytes) -> bytes:
            body = bytearray(table_id.to_bytes(6, "little"))
            body.extend(b"\x00\x00\x02db\x00\x01t\x00\x01")
            body.append(column_type)
            body.append(len(metadata))
            body.extend(metadata)
            body.append(0x01)
            return bytes(body)

        def write_rows_body(table_id: int, value: bytes) -> bytes:
            return table_id.to_bytes(6, "little") + b"\x00\x00\x02\x00\x01\x01\x00" + value

        # ENUM and SET arrive as MYSQL_TYPE_STRING with their real type in
        # TABLE_MAP metadata; BIT has its native wire type and bit width.
        cases = [
            (71, 0xFE, b"\xf7\x01", b"\x02", 2),
            (72, 0xFE, b"\xf8\x01", b"\x05", 5),
            (73, 0x10, b"\x01\x00", b"\xff", 255),
        ]
        with CdcEngine(lib_path=lib_path) as engine:
            engine.set_checksum_enabled(False)
            for table_id, column_type, metadata, raw_value, expected in cases:
                engine.feed(
                    build_event_no_checksum(19, 1, table_map_body(table_id, column_type, metadata))
                )
                engine.feed(build_event_no_checksum(30, 2, write_rows_body(table_id, raw_value)))
                event = engine.next_event()
                assert event is not None and event.after is not None
                assert event.after["0"] == expected
                assert isinstance(event.after["0"], int)


def test_column_name_cache_is_reused_across_rows() -> None:
    col = _make_int_column(7)
    col.col_name = b"id"
    arr = (MESColumn * 1)(col)
    cache: dict[bytes, str] = {}

    assert _convert_columns(arr, 1, cache) == {"id": 7}
    cached_name = cache[b"id"]
    assert _convert_columns(arr, 1, cache) == {"id": 7}
    assert cache[b"id"] is cached_name


class TestPositionTextIsNeverFatal:
    """A binlog filename the server sends must not raise on the way out."""

    def test_undecodable_binlog_filename_substitutes(self) -> None:
        lib = MagicMock()
        lib.mes_create.return_value = 0xBEEF
        lib.mes_get_position.return_value = MES_OK

        def fake_get_position(
            _handle: object,
            file_ptr: object,
            offset: object,
        ) -> int:
            ctypes.cast(file_ptr, ctypes.POINTER(ctypes.c_char_p))[0] = b"binlog.\xff0007"
            ctypes.cast(offset, ctypes.POINTER(ctypes.c_uint64))[0] = 154
            return MES_OK

        lib.mes_get_position.side_effect = fake_get_position
        with patch("mysql_event_stream.engine.get_library", return_value=lib):
            engine = CdcEngine()
        position = engine.get_position()
        assert position.file == "binlog.�0007"
        assert position.offset == 154
        engine.close()


class TestNativeErrorCodes:
    """Every native failure path must carry the C-ABI code on the exception.

    The README tells callers to branch on ``.code`` rather than on the message
    text, so a bare ``RuntimeError`` from any of these paths is a broken
    promise, not a cosmetic difference.
    """

    @staticmethod
    def _engine(lib: MagicMock) -> CdcEngine:
        lib.mes_create.return_value = 0xBEEF
        with patch("mysql_event_stream.engine.get_library", return_value=lib):
            return CdcEngine()

    @pytest.mark.parametrize(
        ("native_name", "call"),
        [
            ("mes_get_position", lambda engine: engine.get_position()),
            ("mes_set_max_queue_size", lambda engine: engine.set_max_queue_size(10)),
            ("mes_set_max_event_size", lambda engine: engine.set_max_event_size(1024)),
            ("mes_reset", lambda engine: engine.reset()),
            ("mes_set_include_databases", lambda engine: engine.set_include_databases(["db"])),
            ("mes_set_include_tables", lambda engine: engine.set_include_tables(["t"])),
            ("mes_set_exclude_tables", lambda engine: engine.set_exclude_tables(["t"])),
        ],
    )
    def test_setter_failures_carry_the_code(
        self, native_name: str, call: Callable[[CdcEngine], object]
    ) -> None:
        lib = MagicMock()
        getattr(lib, native_name).return_value = MES_ERR_INVALID_ARG
        engine = self._engine(lib)
        with pytest.raises(RuntimeError) as excinfo:
            call(engine)
        assert excinfo.value.code == MES_ERR_INVALID_ARG
        engine.close()

    def test_metadata_connection_failure_carries_the_code(self) -> None:
        lib = MagicMock()
        lib.mes_engine_set_metadata_conn.return_value = MES_ERR_AUTH
        engine = self._engine(lib)
        with (
            patch("mysql_event_stream.engine.load_client_library", return_value=True),
            pytest.raises(RuntimeError) as excinfo,
        ):
            engine.enable_metadata(host="127.0.0.1", port=3306)
        assert excinfo.value.code == MES_ERR_AUTH
        engine.close()

    def test_closed_engine_reports_invalid_argument(self, lib_path: str) -> None:
        engine = CdcEngine(lib_path=lib_path)
        engine.close()
        with pytest.raises(RuntimeError) as excinfo:
            engine.get_position()
        assert excinfo.value.code == MES_ERR_INVALID_ARG


class TestSizeLimitDocumentation:
    """Each size limit states what 0 means for itself.

    The two limits resolve 0 differently, so a reader who takes the meaning
    from the neighbouring setter raises the per-event ceiling to 1 GiB while
    expecting the 64 MiB default.
    """

    @staticmethod
    def _zero_meaning(method: Callable[..., object]) -> str:
        """Return what the doc says about 0, up to the end of that sentence."""
        stated = re.search(r"\b0\b[^.]*", inspect.getdoc(method) or "")
        assert stated is not None, f"{method.__name__} documents the meaning of 0"
        return stated.group(0)

    def test_queue_size_zero_restores_the_default(self) -> None:
        assert "10000" in self._zero_meaning(CdcEngine.set_max_queue_size)

    def test_event_size_zero_resolves_to_the_hard_cap(self) -> None:
        assert "1 GiB" in self._zero_meaning(CdcEngine.set_max_event_size)
