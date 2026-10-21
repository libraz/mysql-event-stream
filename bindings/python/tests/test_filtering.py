"""Filter lifecycle tests against the real library."""

from __future__ import annotations

from collections.abc import Callable

import pytest

from mysql_event_stream import CdcEngine, LogLevel, set_log_callback

from .helpers import build_event, build_table_map_body, build_write_rows_body

TABLE_MAP_EVENT = 19
WRITE_ROWS_EVENT = 30


def _table_map() -> bytes:
    return build_event(TABLE_MAP_EVENT, 1000, build_table_map_body(1, "mydb", "users"))


def _write_row(value: int) -> bytes:
    return build_event(WRITE_ROWS_EVENT, 1000, build_write_rows_body(1, value))


class TestFilterLifecycle:
    def teardown_method(self) -> None:
        set_log_callback(None)

    @pytest.mark.parametrize(
        ("setter", "blocking"),
        [
            ("set_include_databases", ["otherdb"]),
            ("set_include_tables", ["mydb.other"]),
            ("set_exclude_tables", ["mydb.users"]),
        ],
    )
    def test_empty_list_lifts_a_restriction_on_a_registered_table(
        self, lib_path: str, setter: str, blocking: list[str]
    ) -> None:
        with CdcEngine(lib_path=lib_path) as engine:
            apply: Callable[[list[str]], None] = getattr(engine, setter)
            engine.feed(_table_map())

            apply(blocking)
            engine.feed(_write_row(10))
            assert not engine.has_events()

            apply([])
            engine.feed(_write_row(20))
            event = engine.next_event()
            assert event is not None
            assert event.table == "users"
            assert not engine.has_events()

    def test_warns_once_at_reset_and_once_at_close_when_an_include_filter_matched_nothing(
        self, lib_path: str
    ) -> None:
        messages: list[str] = []
        set_log_callback(lambda _level, message: messages.append(message), LogLevel.WARN)

        engine = CdcEngine(lib_path=lib_path)
        engine.set_include_tables(["mydb.missing"])
        engine.feed(_table_map())
        engine.reset()
        engine.feed(_table_map())
        engine.close()

        warnings = [m for m in messages if "event=include_filter_matched_nothing" in m]
        assert len(warnings) == 2
