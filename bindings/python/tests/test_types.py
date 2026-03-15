"""Tests for mysql-event-stream type definitions."""

from mysql_event_stream import BinlogPosition, ChangeEvent, ColumnType, ColumnValue, EventType


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


class TestChangeEvent:
    def test_insert_event(self) -> None:
        event = ChangeEvent(
            type=EventType.INSERT,
            database="testdb",
            table="users",
            before=None,
            after=[ColumnValue.int_val(1)],
            timestamp=1000,
            position=BinlogPosition(file="binlog.000001", offset=4),
        )
        assert event.type == EventType.INSERT
        assert event.database == "testdb"
        assert event.after is not None
        assert len(event.after) == 1


class TestBinlogPosition:
    def test_default(self) -> None:
        pos = BinlogPosition()
        assert pos.file == ""
        assert pos.offset == 0

    def test_values(self) -> None:
        pos = BinlogPosition(file="binlog.000001", offset=154)
        assert pos.file == "binlog.000001"
        assert pos.offset == 154
