"""Tests for the single-owner contract between CdcStream and the native handles.

``mes_engine_t`` and ``mes_client_t`` are not thread-safe and ``mes_destroy`` must
not run while a worker thread is inside a call on the same handle, so every
blocking native call is dispatched to exactly one tracked worker at a time.
"""

from __future__ import annotations

import asyncio
import contextlib
import threading
import time
from unittest.mock import patch

import pytest

from mysql_event_stream.stream import CdcStream
from mysql_event_stream.types import BinlogPosition, ChangeEvent, EventType, PollResult

# Upper bound for a double that parks a worker thread: keeps a regression from
# hanging the suite instead of failing it.
_BLOCK_TIMEOUT_S = 5.0


def _change_event(table: str) -> ChangeEvent:
    """Build a distinguishable event of the type the stream declares it yields.

    A double yielding anything else would let a delivery assertion pass against
    the double's own shape rather than against the iterated surface.
    """
    return ChangeEvent(
        type=EventType.INSERT,
        database="mes_test",
        table=table,
        before=None,
        after={"id": 1},
        timestamp=1735689600,
        position=BinlogPosition(file="binlog.000001", offset=4),
    )


async def _wait_for(flag: threading.Event) -> None:
    """Yield to the event loop until a worker thread signals ``flag``."""
    deadline = time.monotonic() + _BLOCK_TIMEOUT_S
    while not flag.is_set():
        if time.monotonic() > deadline:
            raise AssertionError("the worker thread never reached the native call")
        await asyncio.sleep(0.01)


class _FakeEngine:
    """Engine double recording which thread ran each native call."""

    def __init__(self, events: list[ChangeEvent] | None = None) -> None:
        # Events only become available once bytes have been fed, matching the
        # engine: nothing can be drained before a feed produced it.
        self._undecoded = list(events or [])
        self._decoded: list[ChangeEvent] = []
        self.feed_calls: list[bytes] = []
        self.feed_threads: list[int] = []
        self.next_event_threads: list[int] = []
        self.metadata_thread: int | None = None
        self.closed = False

    def feed(self, chunk: bytes) -> int:
        self.feed_calls.append(chunk)
        self.feed_threads.append(threading.get_ident())
        self._decoded.extend(self._undecoded)
        self._undecoded.clear()
        return len(chunk)

    def next_event(self) -> ChangeEvent | None:
        self.next_event_threads.append(threading.get_ident())
        return self._decoded.pop(0) if self._decoded else None

    def enable_metadata(self, **_kwargs: object) -> None:
        self.metadata_thread = threading.get_ident()

    def reset(self) -> None:
        return None

    def set_max_event_size(self, _size: int) -> None:
        return None

    def set_max_queue_size(self, _size: int) -> None:
        return None

    def set_checksum_enabled(self, _enabled: bool) -> None:
        return None

    def set_include_databases(self, _databases: list[str]) -> None:
        return None

    def set_include_tables(self, _tables: list[str]) -> None:
        return None

    def set_exclude_tables(self, _tables: list[str]) -> None:
        return None

    def close(self) -> None:
        self.closed = True


class _BlockingFeedEngine(_FakeEngine):
    """Engine double that parks a worker thread inside ``feed()``."""

    def __init__(self) -> None:
        super().__init__()
        self.entered_feed = threading.Event()
        self.release_feed = threading.Event()
        self.feed_returned = False
        self.destroyed_during_feed = False

    def feed(self, chunk: bytes) -> int:
        self.entered_feed.set()
        self.release_feed.wait(_BLOCK_TIMEOUT_S)
        self.feed_returned = True
        return super().feed(chunk)

    def close(self) -> None:
        if self.entered_feed.is_set() and not self.feed_returned:
            self.destroyed_during_feed = True
        super().close()


class _FakeClient:
    """Client double whose ``poll()`` returns one chunk without blocking."""

    def __init__(self, chunk: bytes = b"\x01\x02") -> None:
        self._chunk = chunk
        self.poll_calls = 0
        self.stop_calls = 0
        self.closed = False
        self.connect_thread: int | None = None
        self.start_thread: int | None = None
        self.current_gtid = ""
        self.checksum_enabled = True

    def connect(self) -> None:
        self.connect_thread = threading.get_ident()

    def start(self) -> None:
        self.start_thread = threading.get_ident()

    def poll(self) -> PollResult:
        self.poll_calls += 1
        return PollResult(data=self._chunk, is_heartbeat=False, checksum_enabled=True)

    def stop(self) -> None:
        self.stop_calls += 1

    def close(self) -> None:
        self.closed = True


class _BlockingPollClient(_FakeClient):
    """Client double that parks a worker thread inside ``poll()``."""

    def __init__(self) -> None:
        super().__init__()
        self.entered_poll = threading.Event()
        self.release_poll = threading.Event()

    def poll(self) -> PollResult:
        # Counted on entry, so a test can observe the call while it is blocked.
        self.poll_calls += 1
        self.entered_poll.set()
        self.release_poll.wait(_BLOCK_TIMEOUT_S)
        return PollResult(data=self._chunk, is_heartbeat=False, checksum_enabled=True)

    def stop(self) -> None:
        super().stop()
        self.release_poll.set()


def _started_stream(client: object, engine: object) -> CdcStream:
    """Wire a stream to test doubles as if ``_start()`` had already run."""
    stream = CdcStream(host="127.0.0.1")
    stream._started = True
    stream._client = client  # type: ignore[assignment]
    stream._engine = engine  # type: ignore[assignment]
    return stream


class TestSingleIteration:
    """Only one consumer may drive a stream at a time."""

    @pytest.mark.timeout(20)
    async def test_second_entry_is_rejected_while_the_first_is_in_flight(self) -> None:
        client = _BlockingPollClient()
        expected = _change_event("orders")
        engine = _FakeEngine(events=[expected])
        stream = _started_stream(client, engine)

        first = asyncio.create_task(stream.__anext__())
        await _wait_for(client.entered_poll)

        # Direct __anext__ and a second async-for are the two entry paths, and
        # both must be refused before any native call is dispatched.
        with pytest.raises(RuntimeError, match="already being iterated"):
            await stream.__anext__()

        async def second_loop() -> None:
            async for _ in stream:
                break

        with pytest.raises(RuntimeError, match="already being iterated"):
            await second_loop()

        assert client.poll_calls == 1
        assert engine.feed_calls == []

        client.release_poll.set()
        assert await first == expected
        await stream.close()

    @pytest.mark.timeout(20)
    async def test_a_later_iteration_is_accepted_once_the_first_returns(self) -> None:
        # The guard is scoped to concurrency: it must be released on every exit
        # path, or one delivered event would poison the stream for good.
        client = _FakeClient()
        expected = [_change_event("first"), _change_event("second")]
        engine = _FakeEngine(events=list(expected))
        stream = _started_stream(client, engine)

        async for received in stream:
            assert received == expected[0]
            break
        async for received in stream:
            assert received == expected[1]
            break

        await stream.close()


class TestCloseWaitsForWorkers:
    """close() must not destroy a handle a worker thread is still using."""

    @pytest.mark.timeout(20)
    async def test_close_waits_for_a_worker_inside_feed(self) -> None:
        client = _FakeClient()
        engine = _BlockingFeedEngine()
        stream = _started_stream(client, engine)

        iterating = asyncio.create_task(stream.__anext__())
        await _wait_for(engine.entered_feed)

        closing = asyncio.create_task(stream.close())
        await asyncio.sleep(0.2)
        assert not closing.done(), "close() returned while a worker was inside feed()"

        engine.release_feed.set()
        await asyncio.wait_for(closing, _BLOCK_TIMEOUT_S)

        assert engine.closed
        assert not engine.destroyed_during_feed
        with contextlib.suppress(BaseException):
            await iterating

    @pytest.mark.timeout(20)
    async def test_close_waits_for_a_worker_inside_feed_after_a_cancelled_await(self) -> None:
        # Cancelling the awaiting task abandons the result, never the worker:
        # the thread keeps running inside the native handle.
        client = _FakeClient()
        engine = _BlockingFeedEngine()
        stream = _started_stream(client, engine)

        iterating = asyncio.create_task(stream.__anext__())
        await _wait_for(engine.entered_feed)

        iterating.cancel()
        with pytest.raises(asyncio.CancelledError):
            await iterating

        closing = asyncio.create_task(stream.close())
        await asyncio.sleep(0.2)
        assert not closing.done(), "close() returned while a worker was inside feed()"

        engine.release_feed.set()
        await asyncio.wait_for(closing, _BLOCK_TIMEOUT_S)

        assert engine.closed
        assert not engine.destroyed_during_feed


class TestEventLoopOffloading:
    """Blocking and per-row work belongs on the worker, not on the event loop."""

    @pytest.mark.timeout(20)
    async def test_a_poll_batch_costs_one_dispatch_and_no_loop_side_marshalling(self) -> None:
        loop_thread = threading.get_ident()
        client = _FakeClient()
        expected = [_change_event("a"), _change_event("b"), _change_event("c")]
        engine = _FakeEngine(events=list(expected))
        stream = _started_stream(client, engine)

        delivered = [await stream.__anext__() for _ in range(3)]

        assert delivered == expected
        assert client.poll_calls == 1
        assert len(engine.feed_calls) == 1
        assert engine.feed_threads and loop_thread not in engine.feed_threads
        assert engine.next_event_threads and loop_thread not in engine.next_event_threads

        await stream.close()

    @pytest.mark.timeout(20)
    async def test_the_metadata_connection_is_dispatched_off_the_event_loop(self) -> None:
        loop_thread = threading.get_ident()
        client = _FakeClient()
        engine = _FakeEngine()

        with (
            patch("mysql_event_stream.stream.CdcEngine", return_value=engine),
            patch("mysql_event_stream.stream.BinlogClient", return_value=client),
        ):
            stream = CdcStream(host="127.0.0.1")
            await stream._start()

        assert engine.metadata_thread is not None
        assert engine.metadata_thread != loop_thread
        assert client.connect_thread is not None
        assert client.connect_thread != loop_thread
        assert client.start_thread is not None
        assert client.start_thread != loop_thread

        await stream.close()
