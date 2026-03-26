"""Tests for CdcEngine - the Python wrapper around libmes."""

import ctypes

import pytest

from mysql_event_stream import CdcEngine, EventType
from mysql_event_stream._ffi import MES_COL_BYTES, MES_COL_STRING, MESColumn
from mysql_event_stream.engine import _convert_columns

from .helpers import (
    build_delete_rows_body,
    build_event,
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
