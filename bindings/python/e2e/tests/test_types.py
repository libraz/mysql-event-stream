"""E2E tests for column type handling."""

from __future__ import annotations

import pytest
from lib.mysql_client import MysqlClient
from lib.streaming_collector import StreamingCollector

from mysql_event_stream import EventType


@pytest.mark.types
class TestColumnTypes:
    """Verify that various MySQL column types are decoded correctly."""

    def test_null_value(self, mysql: MysqlClient, collector: StreamingCollector) -> None:
        """NULL column values are correctly detected."""
        mysql.insert("users", name="NullUser")

        events = collector.wait_for_events(table="users", event_type=EventType.INSERT)

        ev = events[0]
        assert ev.after is not None
        null_vals = [v for v in ev.after.values() if v is None]
        assert len(null_vals) > 0

        # Column name assertions (users table)
        assert "id" in ev.after
        assert "name" in ev.after

    def test_integer_types(self, mysql: MysqlClient, collector: StreamingCollector) -> None:
        """INT and BIGINT columns are decoded as integer values."""
        mysql.insert("items", name="int_test", value=2147483647)

        events = collector.wait_for_events(table="items", event_type=EventType.INSERT)

        ev = events[0]
        assert ev.after is not None
        assert isinstance(ev.after["id"], int)
        assert ev.after["name"] == "int_test"
        assert ev.after["value"] == 2147483647

    def test_utf8_string(self, mysql: MysqlClient, collector: StreamingCollector) -> None:
        """UTF-8 strings including CJK characters are correctly decoded."""
        mysql.insert("items", name="日本語テスト", value=1)

        events = collector.wait_for_events(table="items", event_type=EventType.INSERT)

        ev = events[0]
        assert ev.after is not None
        assert isinstance(ev.after["id"], int)
        assert ev.after["name"] == "日本語テスト"
        assert ev.after["value"] == 1

    def test_double_value(self, mysql: MysqlClient, collector: StreamingCollector) -> None:
        """DOUBLE column values are decoded correctly."""
        mysql.insert("users", name="DoubleUser", balance="1234.56", score=3.14159)

        events = collector.wait_for_events(table="users", event_type=EventType.INSERT)

        ev = events[0]
        assert ev.after is not None
        assert ev.after["name"] == "DoubleUser"
        assert isinstance(ev.after["score"], float)
        assert ev.after["score"] == pytest.approx(3.14159)
        assert isinstance(ev.after["balance"], str)
        assert ev.after["balance"] == "1234.56"
        assert isinstance(ev.after["created_at"], str)
        assert isinstance(ev.after["updated_at"], str)

        # Column name assertions (users table)
        assert "id" in ev.after
        assert "name" in ev.after
        assert "score" in ev.after

    def test_binary_and_longtext_types(
        self, mysql: MysqlClient, collector: StreamingCollector
    ) -> None:
        """BINARY is bytes while LONGTEXT is text when charset metadata is available."""
        binary_value = bytes(range(16))
        mysql.execute("DELETE FROM charset_values")
        mysql.execute(
            "INSERT INTO charset_values (id, binary_value, text_value) VALUES (%s, %s, %s)",
            (1, binary_value, "charset metadata text"),
        )

        events = collector.wait_for_events(table="charset_values", event_type=EventType.INSERT)
        assert events[0].after is not None
        values = list(events[0].after.values())
        assert binary_value in values
        assert "charset metadata text" in values

    def test_enum_set_bit_and_overflowing_unsigned_bigint(
        self, mysql: MysqlClient, collector: StreamingCollector
    ) -> None:
        """ENUM/SET/BIT are ints and overflowing unsigned BIGINT is a string."""
        mysql.execute("DELETE FROM type_mapping_values")
        mysql.execute(
            "INSERT INTO type_mapping_values "
            "(id, enum_value, set_value, bit_value, unsigned_value) "
            "VALUES (1, 'second', 'a,c', b'10101010', 18446744073709551615)"
        )

        events = collector.wait_for_events(table="type_mapping_values", event_type=EventType.INSERT)
        assert events[0].after is not None
        assert events[0].after["enum_value"] == 2
        assert events[0].after["set_value"] == 5
        assert events[0].after["bit_value"] == 170
        assert events[0].after["unsigned_value"] == "18446744073709551615"
