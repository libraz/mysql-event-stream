"""Streaming event collector for E2E tests.

Runs BinlogClient + CdcEngine in a background thread to collect
CDC events triggered by DML operations in the main thread.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable

from mysql_event_stream import CdcEngine, ChangeEvent
from mysql_event_stream.client import BinlogClient


class StreamingCollector:
    """Collects CDC events from a BinlogClient stream.

    Usage::

        collector = StreamingCollector(lib_path, server_id=100)
        collector.start()
        try:
            mysql.insert("items", name="test", value=42)
            events = collector.wait_for_events(table="items", count=1)
            assert events[0].type == EventType.INSERT
        finally:
            collector.stop()
    """

    def __init__(
        self,
        lib_path: str,
        *,
        host: str = "127.0.0.1",
        port: int = 13308,
        user: str = "root",
        password: str = "test_root_password",
        server_id: int = 100,
        start_gtid: str = "",
    ) -> None:
        self._lib_path = lib_path
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self._server_id = server_id
        self._start_gtid = start_gtid
        self._events: list[ChangeEvent] = []
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._started = threading.Event()
        self._thread: threading.Thread | None = None
        self._error: Exception | None = None

    def start(self) -> None:
        """Start the streaming collector in a background thread."""
        self._stop.clear()
        self._started.clear()
        self._error = None
        self._events.clear()
        self._thread = threading.Thread(target=self._worker, daemon=True)
        self._thread.start()
        if not self._started.wait(timeout=30):
            raise RuntimeError("Streaming collector failed to start within 30s")
        if self._error:
            raise self._error

    def stop(self) -> None:
        """Stop the streaming collector."""
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=10)
            self._thread = None

    def wait_for_events(
        self,
        *,
        table: str | None = None,
        event_type: object = None,
        count: int = 1,
        timeout: float = 30.0,
        predicate: Callable[[ChangeEvent], bool] | None = None,
    ) -> list[ChangeEvent]:
        """Wait for matching events to arrive.

        Args:
            table: Filter by table name.
            event_type: Filter by EventType.
            count: Minimum number of matching events to collect.
            timeout: Maximum wait time in seconds.
            predicate: Custom filter function.

        Returns:
            List of matching ChangeEvent objects.

        Raises:
            TimeoutError: If not enough events arrive within timeout.
        """
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            with self._lock:
                matching = self._filter_events(table, event_type, predicate)
                if len(matching) >= count:
                    return matching[:count]
            time.sleep(0.05)

        with self._lock:
            matching = self._filter_events(table, event_type, predicate)
        raise TimeoutError(
            f"Expected {count} events (table={table}, type={event_type}) "
            f"but got {len(matching)} within {timeout}s"
        )

    @property
    def all_events(self) -> list[ChangeEvent]:
        """Get a snapshot of all collected events."""
        with self._lock:
            return list(self._events)

    def _filter_events(
        self,
        table: str | None,
        event_type: object,
        predicate: Callable[[ChangeEvent], bool] | None,
    ) -> list[ChangeEvent]:
        result = self._events
        if table:
            result = [e for e in result if e.table == table]
        if event_type is not None:
            result = [e for e in result if e.type == event_type]
        if predicate:
            result = [e for e in result if predicate(e)]
        return result

    def _worker(self) -> None:
        try:
            with BinlogClient(
                host=self._host,
                port=self._port,
                user=self._user,
                password=self._password,
                server_id=self._server_id,
                start_gtid=self._start_gtid,
                lib_path=self._lib_path,
            ) as client:
                client.connect()
                client.start()
                self._started.set()

                with CdcEngine(lib_path=self._lib_path) as engine:
                    engine.enable_metadata(
                        host=self._host,
                        port=self._port,
                        user=self._user,
                        password=self._password,
                    )
                    while not self._stop.is_set():
                        result = client.poll()
                        if result.is_heartbeat or not result.data:
                            continue
                        engine.feed(result.data)
                        while (ev := engine.next_event()) is not None:
                            with self._lock:
                                self._events.append(ev)
        except Exception as e:
            self._error = e
            self._started.set()
