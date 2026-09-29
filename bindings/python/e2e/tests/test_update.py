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

        # Each image must equal the literal value the triggering DML wrote --
        # "col" in ev.before would pass for null, a swapped before/after
        # image, or a value landing in the wrong column.
        assert ev.before["id"] == row_id
        assert ev.before["name"] == "original"
        assert ev.before["value"] == 10
        assert ev.after["id"] == row_id
        assert ev.after["name"] == "updated"
        assert ev.after["value"] == 20

    def test_update_multiple_columns(
        self, mysql: MysqlClient, collector: StreamingCollector
    ) -> None:
        """UPDATE that changes multiple columns in the users table."""
        row_id = mysql.insert(
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

        # Each image must equal the literal value the triggering DML wrote.
        # The untouched columns (id, name) must carry the same value in both
        # images; only email and age were named in the UPDATE.
        assert ev.before["id"] == row_id
        assert ev.before["name"] == "Bob"
        assert ev.before["email"] == "bob@example.com"
        assert ev.before["age"] == 25
        assert ev.after["id"] == row_id
        assert ev.after["name"] == "Bob"
        assert ev.after["email"] == "bob_new@example.com"
        assert ev.after["age"] == 26
