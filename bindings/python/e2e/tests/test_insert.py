"""E2E tests for INSERT event detection."""

from __future__ import annotations

import pytest
from lib.mysql_client import MysqlClient
from lib.streaming_collector import StreamingCollector

from mysql_event_stream import EventType


@pytest.mark.insert
class TestInsert:
    """Verify that INSERT operations produce correct ChangeEvents."""

    def test_simple_insert(self, mysql: MysqlClient, collector: StreamingCollector) -> None:
        """INSERT into a simple table produces an INSERT ChangeEvent."""
        row_id = mysql.insert("items", name="test_item", value=42)
        assert row_id > 0

        events = collector.wait_for_events(table="items", event_type=EventType.INSERT)

        ev = events[0]
        assert ev.database == "mes_test"
        assert ev.table == "items"
        assert ev.type == EventType.INSERT
        assert ev.before is None
        assert ev.after is not None
        assert len(ev.after) >= 2

    def test_insert_multiple_rows(self, mysql: MysqlClient, collector: StreamingCollector) -> None:
        """Multiple INSERTs produce multiple INSERT ChangeEvents."""
        mysql.insert("items", name="item_a", value=1)
        mysql.insert("items", name="item_b", value=2)
        mysql.insert("items", name="item_c", value=3)

        events = collector.wait_for_events(table="items", event_type=EventType.INSERT, count=3)
        assert len(events) == 3

    def test_insert_with_various_types(
        self, mysql: MysqlClient, collector: StreamingCollector
    ) -> None:
        """INSERT into users table with various column types."""
        mysql.insert(
            "users",
            name="Alice",
            email="alice@example.com",
            age=30,
            balance="1234.56",
            score=3.14,
            is_active=1,
            bio="Hello, world!",
        )

        events = collector.wait_for_events(table="users", event_type=EventType.INSERT)

        ev = events[0]
        assert ev.after is not None
        assert len(ev.after) > 0
