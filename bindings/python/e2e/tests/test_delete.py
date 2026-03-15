"""E2E tests for DELETE event detection."""

from __future__ import annotations

import pytest
from lib.mysql_client import MysqlClient
from lib.streaming_collector import StreamingCollector

from mysql_event_stream import EventType


@pytest.mark.delete
class TestDelete:
    """Verify that DELETE operations produce correct ChangeEvents."""

    def test_simple_delete(self, mysql: MysqlClient, collector: StreamingCollector) -> None:
        """DELETE produces a DELETE ChangeEvent with before image only."""
        row_id = mysql.insert("items", name="to_delete", value=99)

        mysql.delete("items", f"id = {row_id}")

        events = collector.wait_for_events(table="items", event_type=EventType.DELETE)

        ev = events[0]
        assert ev.database == "mes_test"
        assert ev.table == "items"
        assert ev.type == EventType.DELETE
        assert ev.before is not None
        assert ev.after is None

    def test_delete_multiple_rows(self, mysql: MysqlClient, collector: StreamingCollector) -> None:
        """DELETE of multiple rows produces one ChangeEvent per row."""
        mysql.insert("items", name="del_a", value=1)
        mysql.insert("items", name="del_b", value=2)
        mysql.insert("items", name="del_c", value=3)

        mysql.delete("items", "value IN (1, 2, 3)")

        events = collector.wait_for_events(table="items", event_type=EventType.DELETE, count=3)

        assert len(events) == 3
        for ev in events:
            assert ev.before is not None
            assert ev.after is None
