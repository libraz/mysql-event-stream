"""E2E tests for MariaDB ANNOTATE_ROWS source SQL."""

from __future__ import annotations

import os

import pytest
from lib.mysql_client import MysqlClient
from lib.streaming_collector import StreamingCollector

from mysql_event_stream import EventType

# ANNOTATE_ROWS is MariaDB-only, and the server withholds it unless the dump
# request asks for it. These tests are the surface proof that the request
# carries that flag: without it source_sql is permanently empty.
pytestmark = [
    pytest.mark.streaming,
    pytest.mark.skipif(
        os.environ.get("DB_FLAVOR") != "mariadb",
        reason="ANNOTATE_ROWS is emitted by MariaDB only",
    ),
]


class TestAnnotateRows:
    """The originating statement reaches ChangeEvent.source_sql for every DML kind."""

    def test_insert_carries_the_originating_statement(
        self, mysql: MysqlClient, collector: StreamingCollector
    ) -> None:
        """INSERT carries the originating statement."""
        sql = "INSERT INTO items (name, value) VALUES ('annotate_insert', 1)"
        mysql.execute(sql)

        events = collector.wait_for_events(table="items", event_type=EventType.INSERT)

        assert events[0].source_sql == sql

    def test_update_carries_the_originating_statement(
        self, mysql: MysqlClient, collector: StreamingCollector
    ) -> None:
        """UPDATE carries the originating statement."""
        mysql.execute("INSERT INTO items (name, value) VALUES ('annotate_update', 1)")
        sql = "UPDATE items SET value = 2 WHERE name = 'annotate_update'"
        mysql.execute(sql)

        events = collector.wait_for_events(table="items", event_type=EventType.UPDATE)

        assert events[0].source_sql == sql

    def test_delete_carries_the_originating_statement(
        self, mysql: MysqlClient, collector: StreamingCollector
    ) -> None:
        """DELETE carries the originating statement."""
        mysql.execute("INSERT INTO items (name, value) VALUES ('annotate_delete', 1)")
        sql = "DELETE FROM items WHERE name = 'annotate_delete'"
        mysql.execute(sql)

        events = collector.wait_for_events(table="items", event_type=EventType.DELETE)

        assert events[0].source_sql == sql
