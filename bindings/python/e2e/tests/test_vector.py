"""E2E tests for MySQL 9 VECTOR columns."""

from __future__ import annotations

import struct
from collections.abc import Generator

import pytest
from lib.mysql_client import MysqlClient
from lib.streaming_collector import StreamingCollector

from mysql_event_stream import EventType

TABLE = "vec_charset_values"

# Two binary-charset character columns (embedding, payload) against one utf8mb4
# column make the server pack DEFAULT_CHARSET as "binary, with one exception at
# character-column index 1". VECTOR occupies a slot in that index space, so a
# decoder that skipped it would land the exception on `payload` and surface
# `label` as bytes instead of str.
CREATE_TABLE = (
    f"CREATE TABLE {TABLE} ("
    "id INT NOT NULL PRIMARY KEY, "
    "embedding VECTOR(3) NOT NULL, "
    "label VARCHAR(64) CHARACTER SET utf8mb4 NOT NULL, "
    "payload BLOB NOT NULL) ENGINE=InnoDB"
)


@pytest.mark.types
class TestVectorColumns:
    """VECTOR reaches the Python surface as bytes and keeps charset indexing intact.

    Both facts are decoded in the C++ core; these assert they survive the
    ctypes marshalling boundary, mirroring the Node binding's coverage.
    """

    # Declared as a staticmethod: a class-scoped fixture runs once per class
    # while each test gets a fresh instance, so pytest deprecates binding one
    # to `self`.
    @pytest.fixture(autouse=True, scope="class")
    @staticmethod
    def vector_table(mysql: MysqlClient) -> Generator[None, None, None]:
        """Create the VECTOR table, or skip the class where VECTOR does not exist."""
        if not mysql.supports_vector():
            pytest.skip("VECTOR type requires MySQL 9.0+")
        mysql.execute(f"DROP TABLE IF EXISTS {TABLE}")
        mysql.execute(CREATE_TABLE)
        yield
        mysql.execute(f"DROP TABLE IF EXISTS {TABLE}")

    @pytest.fixture(autouse=True)
    def clean_vector_table(self, mysql: MysqlClient) -> None:
        """Empty the table before each test, ahead of the collector's start GTID."""
        mysql.execute(f"DELETE FROM {TABLE}")

    def test_insert_exposes_vector_as_bytes_without_shifting_string_charsets(
        self, mysql: MysqlClient, collector: StreamingCollector
    ) -> None:
        """INSERT surfaces the vector as bytes and leaves the charset assignment correct."""
        mysql.execute(
            f"INSERT INTO {TABLE} VALUES "
            "(1, TO_VECTOR('[1.0, 2.0, 3.0]'), 'vector label', 'payload bytes')"
        )

        events = collector.wait_for_events(table=TABLE, event_type=EventType.INSERT)

        after = events[0].after
        assert after is not None
        assert after["id"] == 1

        # 3 float32 elements, little-endian, exactly as the server stored them.
        embedding = after["embedding"]
        assert isinstance(embedding, bytes)
        assert len(embedding) == 12
        assert struct.unpack("<3f", embedding) == pytest.approx((1.0, 2.0, 3.0))

        assert after["label"] == "vector label"
        assert isinstance(after["label"], str)
        assert after["payload"] == b"payload bytes"
        assert isinstance(after["payload"], bytes)

    def test_update_reports_both_vector_images(
        self, mysql: MysqlClient, collector: StreamingCollector
    ) -> None:
        """UPDATE carries the vector in both the before and after images."""
        mysql.execute(
            f"INSERT INTO {TABLE} VALUES "
            "(1, TO_VECTOR('[1.0, 2.0, 3.0]'), 'before', 'payload bytes')"
        )
        mysql.execute(
            f"UPDATE {TABLE} SET embedding = TO_VECTOR('[4.0, 5.0, 6.0]'), "
            "label = 'after' WHERE id = 1"
        )

        events = collector.wait_for_events(table=TABLE, event_type=EventType.UPDATE)

        before = events[0].before
        after = events[0].after
        assert before is not None
        assert after is not None
        assert struct.unpack("<3f", before["embedding"]) == pytest.approx((1.0, 2.0, 3.0))
        assert struct.unpack("<3f", after["embedding"]) == pytest.approx((4.0, 5.0, 6.0))
        assert before["label"] == "before"
        assert after["label"] == "after"

    def test_delete_reports_the_vector_in_the_before_image(
        self, mysql: MysqlClient, collector: StreamingCollector
    ) -> None:
        """DELETE carries the vector in the before image."""
        mysql.execute(
            f"INSERT INTO {TABLE} VALUES "
            "(1, TO_VECTOR('[7.0, 8.0, 9.0]'), 'doomed', 'payload bytes')"
        )
        mysql.execute(f"DELETE FROM {TABLE} WHERE id = 1")

        events = collector.wait_for_events(table=TABLE, event_type=EventType.DELETE)

        before = events[0].before
        assert before is not None
        assert struct.unpack("<3f", before["embedding"]) == pytest.approx((7.0, 8.0, 9.0))
        assert before["label"] == "doomed"
