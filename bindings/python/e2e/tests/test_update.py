"""E2E tests for UPDATE event detection."""

from __future__ import annotations

import pytest
from lib.mysql_client import MysqlClient
from lib.streaming_collector import StreamingCollector

from mysql_event_stream import EventType


@pytest.mark.update
class TestUpdate:
    """Verify that UPDATE operations produce correct ChangeEvents."""

    def test_simple_update(self, mysql: MysqlClient, collector: StreamingCollector) -> None:
        """UPDATE produces an UPDATE ChangeEvent with before and after images."""
        row_id = mysql.insert("items", name="original", value=10)

        mysql.update("items", f"id = {row_id}", name="updated", value=20)

        events = collector.wait_for_events(table="items", event_type=EventType.UPDATE)

        ev = events[0]
        assert ev.database == "mes_test"
        assert ev.table == "items"
        assert ev.type == EventType.UPDATE
        assert ev.before is not None
        assert ev.after is not None

        # Column name assertions (items: id, name, value)
        assert "id" in ev.before
        assert "name" in ev.before
        assert "value" in ev.before
        assert "id" in ev.after
        assert "name" in ev.after
        assert "value" in ev.after

    def test_update_multiple_columns(
        self, mysql: MysqlClient, collector: StreamingCollector
    ) -> None:
        """UPDATE that changes multiple columns in the users table."""
        mysql.insert(
            "users",
            name="Bob",
            email="bob@example.com",
            age=25,
            is_active=1,
        )

        mysql.update(
            "users",
            "name = 'Bob'",
            email="bob_new@example.com",
            age=26,
        )

        events = collector.wait_for_events(table="users", event_type=EventType.UPDATE)

        ev = events[0]
        assert ev.before is not None
        assert ev.after is not None
        assert len(ev.before) == len(ev.after)

        # Column name assertions (users table)
        assert "id" in ev.before
        assert "name" in ev.before
        assert "id" in ev.after
        assert "name" in ev.after
        assert "email" in ev.after
        assert "age" in ev.after
