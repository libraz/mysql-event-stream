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
        int_vals = [v for v in ev.after.values() if isinstance(v, int)]
        assert len(int_vals) > 0

        # Column name assertions (items: id, name, value)
        assert "id" in ev.after
        assert "name" in ev.after
        assert "value" in ev.after

    def test_utf8_string(self, mysql: MysqlClient, collector: StreamingCollector) -> None:
        """UTF-8 strings including CJK characters are correctly decoded."""
        mysql.insert("items", name="日本語テスト", value=1)

        events = collector.wait_for_events(table="items", event_type=EventType.INSERT)

        ev = events[0]
        assert ev.after is not None
        string_vals = [v for v in ev.after.values() if isinstance(v, str) and v]
        has_japanese = any("日本語" in v for v in string_vals)
        assert has_japanese

        # Column name assertions (items: id, name, value)
        assert "id" in ev.after
        assert "name" in ev.after
        assert "value" in ev.after

    def test_double_value(self, mysql: MysqlClient, collector: StreamingCollector) -> None:
        """DOUBLE column values are decoded correctly."""
        mysql.insert("users", name="DoubleUser", score=3.14159)

        events = collector.wait_for_events(table="users", event_type=EventType.INSERT)

        ev = events[0]
        assert ev.after is not None
        double_vals = [v for v in ev.after.values() if isinstance(v, float)]
        assert len(double_vals) > 0

        # Column name assertions (users table)
        assert "id" in ev.after
        assert "name" in ev.after
        assert "score" in ev.after
