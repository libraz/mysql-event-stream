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

from mysql_event_stream._ffi import MES_ERR_STREAM
from mysql_event_stream.stream import CdcStream
from mysql_event_stream.types import (
    BinlogPosition,
    ChangeEvent,
    EventType,
    MesConnectionError,
    MesErrorCode,
    PollResult,
)

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
        self.close_calls = 0

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

    def set_max_queue_bytes(self, _size: int) -> None:
        return None

    def set_trailer_pre_verified(self, _pre_verified: bool) -> None:
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
        self.close_calls += 1


class _BlockingFeedEngine(_FakeEngine):
    """Engine double that parks a worker thread inside ``feed()``."""

    def __init__(self, events: list[ChangeEvent] | None = None) -> None:
        super().__init__(events)
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
    """Client double whose ``poll_batch()`` returns one chunk without blocking."""

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

    def poll_batch(self) -> list[PollResult]:
        self.poll_calls += 1
        return [PollResult(data=self._chunk, is_heartbeat=False, checksum_enabled=True)]

    def stop(self) -> None:
        self.stop_calls += 1

    def close(self) -> None:
        self.closed = True


class _BlockingPollClient(_FakeClient):
    """Client double that parks a worker thread inside ``poll_batch()``."""

    def __init__(self) -> None:
        super().__init__()
        self.entered_poll = threading.Event()
        self.release_poll = threading.Event()

    def poll_batch(self) -> list[PollResult]:
        # Counted on entry, so a test can observe the call while it is blocked.
        self.poll_calls += 1
        self.entered_poll.set()
        self.release_poll.wait(_BLOCK_TIMEOUT_S)
        return [PollResult(data=self._chunk, is_heartbeat=False, checksum_enabled=True)]

    def stop(self) -> None:
        super().stop()
        self.release_poll.set()


class _SlowConnectClient(_FakeClient):
    """Client double whose ``connect()`` parks a worker that ``stop()`` cannot free."""

    def __init__(self) -> None:
        super().__init__()
        self.entered_connect = threading.Event()
        self.release_connect = threading.Event()
        self.connect_returned = False
        self.start_calls = 0
        self.close_calls = 0
        self.destroyed_during_connect = False

    def connect(self) -> None:
        self.entered_connect.set()
        self.release_connect.wait(_BLOCK_TIMEOUT_S)
        self.connect_returned = True

    def start(self) -> None:
        self.start_calls += 1

    def close(self) -> None:
        if self.entered_connect.is_set() and not self.connect_returned:
            self.destroyed_during_connect = True
        self.close_calls += 1
        super().close()


class _TimedConnectClient(_FakeClient):
    """Client double whose handshake takes a fixed time on the worker."""

    def __init__(self, connect_s: float) -> None:
        super().__init__()
        self._connect_s = connect_s
        self.connect_calls = 0

    def connect(self) -> None:
        self.connect_calls += 1
        time.sleep(self._connect_s)


class _DroppingClient(_FakeClient):
    """Client double whose stream drops on the first poll."""

    def __init__(self) -> None:
        super().__init__()
        self.close_calls = 0

    def poll_batch(self) -> list[PollResult]:
        self.poll_calls += 1
        raise MesConnectionError("stream dropped", MES_ERR_STREAM)

    def close(self) -> None:
        self.close_calls += 1
        super().close()


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
        with pytest.raises(RuntimeError, match="already being iterated") as direct:
            await stream.__anext__()
        assert direct.value.code == MesErrorCode.INVALID_ARG  # type: ignore[attr-defined]

        async def second_loop() -> None:
            async for _ in stream:
                break

        with pytest.raises(RuntimeError, match="already being iterated") as looped:
            await second_loop()
        assert looped.value.code == MesErrorCode.INVALID_ARG  # type: ignore[attr-defined]

        assert client.poll_calls == 1
        assert engine.feed_calls == []

        client.release_poll.set()
        assert await first == expected
        await stream.close()

    @pytest.mark.timeout(20)
    async def test_a_second_iteration_is_refused_after_the_first_ends(self) -> None:
        # One stream supports one iteration: a later loop would silently resume
        # mid-stream, after events the first loop already took.
        client = _FakeClient()
        expected = [_change_event("first"), _change_event("second")]
        engine = _FakeEngine(events=list(expected))
        stream = _started_stream(client, engine)

        async for received in stream:
            assert received == expected[0]
            break

        with pytest.raises(RuntimeError, match="already being iterated") as refused:
            async for _ in stream:
                break
        assert refused.value.code == MesErrorCode.INVALID_ARG  # type: ignore[attr-defined]
        assert client.poll_calls == 1

        await stream.close()

    @pytest.mark.timeout(20)
    async def test_the_concurrency_guard_is_released_between_steps(self) -> None:
        # The concurrency guard must be released on every exit path, or one
        # delivered event would poison the iteration for good.
        client = _FakeClient()
        expected = [_change_event("first"), _change_event("second")]
        engine = _FakeEngine(events=list(expected))
        stream = _started_stream(client, engine)

        assert await stream.__anext__() == expected[0]
        assert await stream.__anext__() == expected[1]

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
        # The feed ends by itself; the client is stopped only by its own close.
        assert client.stop_calls == 0
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


class TestCancelledAwaitResumes:
    """Cancelling one await abandons the wait, never the stream."""

    @pytest.mark.timeout(20)
    async def test_timed_out_awaits_resume_the_in_flight_poll(self) -> None:
        client = _BlockingPollClient()
        expected = _change_event("orders")
        engine = _FakeEngine(events=[expected])
        stream = _started_stream(client, engine)

        for _ in range(3):
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(stream.__anext__(), 0.05)

        # Nothing stopped the client, charged a retry, or issued a second poll.
        assert client.stop_calls == 0
        assert stream._reconnect_attempts == 0
        assert client.poll_calls == 1

        client.release_poll.set()
        assert await asyncio.wait_for(stream.__anext__(), _BLOCK_TIMEOUT_S) == expected
        assert client.poll_calls == 1
        await stream.close()

    @pytest.mark.timeout(20)
    async def test_a_cancel_landing_after_the_poll_returned_keeps_its_result(self) -> None:
        client = _BlockingPollClient()
        expected = _change_event("orders")
        engine = _FakeEngine(events=[expected])
        stream = _started_stream(client, engine)

        consumer = asyncio.create_task(stream.__anext__())
        await _wait_for(client.entered_poll)
        dispatch = stream._native_task
        assert dispatch is not None
        # Runs after the shield has taken the result, before the consumer
        # resumes: the cancellation lands on a call that already finished.
        dispatch.add_done_callback(lambda _: consumer.cancel())
        client.release_poll.set()
        with pytest.raises(asyncio.CancelledError):
            await consumer

        assert await asyncio.wait_for(stream.__anext__(), _BLOCK_TIMEOUT_S) == expected
        assert client.poll_calls == 1
        await stream.close()

    @pytest.mark.timeout(20)
    async def test_a_cancelled_await_resumes_the_in_flight_feed(self) -> None:
        client = _FakeClient()
        expected = _change_event("orders")
        engine = _BlockingFeedEngine(events=[expected])
        stream = _started_stream(client, engine)

        iterating = asyncio.create_task(stream.__anext__())
        await _wait_for(engine.entered_feed)
        iterating.cancel()
        with pytest.raises(asyncio.CancelledError):
            await iterating

        engine.release_feed.set()
        assert await asyncio.wait_for(stream.__anext__(), _BLOCK_TIMEOUT_S) == expected
        # The polled bytes were fed once, by the dispatch the cancel abandoned.
        assert engine.feed_calls == [b"\x01\x02"]
        assert client.poll_calls == 1
        assert client.stop_calls == 0
        await stream.close()


class TestCancellationNeverOutrunsAWorker:
    """A cancelled wait that guards a handle ends only once the worker has left it."""

    @pytest.mark.timeout(20)
    async def test_a_cancelled_quiesce_waits_and_dispatches_nothing_new(self) -> None:
        client = _SlowConnectClient()
        stream = _started_stream(client, _FakeEngine())

        abandoned = asyncio.create_task(stream._dispatch(client.connect))
        await _wait_for(client.entered_connect)
        abandoned.cancel()
        with pytest.raises(asyncio.CancelledError):
            await abandoned

        successor = asyncio.create_task(stream._dispatch(client.start))
        await asyncio.sleep(0.05)
        successor.cancel()
        await asyncio.sleep(0.1)
        assert not successor.done(), "the cancelled wait returned while connect() was running"

        client.release_connect.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(successor, _BLOCK_TIMEOUT_S)
        # The cancellation arrived before the predecessor settled, so the call
        # that was waiting behind it never ran.
        assert client.start_calls == 0
        assert stream._native_task is None
        await stream.close()

    @pytest.mark.timeout(20)
    async def test_a_cancelled_close_releases_each_handle_exactly_once(self) -> None:
        client = _SlowConnectClient()
        engine = _FakeEngine()
        stream = _started_stream(client, engine)

        connecting = asyncio.create_task(stream._dispatch(client.connect))
        await _wait_for(client.entered_connect)

        closing = asyncio.create_task(stream.close())
        await asyncio.sleep(0.05)
        closing.cancel()
        await asyncio.sleep(0.1)
        assert not closing.done(), "close() gave up its wait while connect() was running"
        assert client.close_calls == 0
        assert engine.close_calls == 0

        client.release_connect.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(closing, _BLOCK_TIMEOUT_S)
        await asyncio.wait_for(stream.close(), _BLOCK_TIMEOUT_S)

        assert client.close_calls == 1
        assert engine.close_calls == 1
        assert not client.destroyed_during_connect
        await connecting


class TestCancelledLifecycleResumes:
    """A timed-out await during start or reconnect resumes the same sequence."""

    @pytest.mark.timeout(20)
    async def test_timed_out_awaits_during_startup_complete_one_handshake(self) -> None:
        client = _TimedConnectClient(connect_s=0.3)
        expected = _change_event("orders")
        engine = _FakeEngine(events=[expected])

        with (
            patch("mysql_event_stream.stream.CdcEngine", return_value=engine) as engine_cls,
            patch("mysql_event_stream.stream.BinlogClient", return_value=client) as client_cls,
        ):
            stream = CdcStream(host="127.0.0.1")
            timeouts = 0
            while True:
                try:
                    received = await asyncio.wait_for(stream.__anext__(), 0.2)
                    break
                except TimeoutError:
                    timeouts += 1
                    assert timeouts < 50, "the event never arrived"

        assert received == expected
        assert timeouts >= 1
        assert client_cls.call_count == 1
        assert engine_cls.call_count == 1
        assert client.connect_calls == 1
        assert not engine.closed
        await stream.close()

    @pytest.mark.timeout(20)
    async def test_timed_out_awaits_during_reconnect_charge_one_retry(self) -> None:
        dropping = _DroppingClient()
        healthy = _FakeClient()
        expected = _change_event("orders")
        engine = _FakeEngine(events=[expected])

        with (
            patch("mysql_event_stream.stream.backoff_delay_ms", return_value=200),
            patch("mysql_event_stream.stream.CdcEngine", return_value=engine),
            patch(
                "mysql_event_stream.stream.BinlogClient", side_effect=[dropping, healthy]
            ) as client_cls,
        ):
            stream = CdcStream(host="127.0.0.1")
            timeouts = 0
            attempts_seen: set[int] = set()
            while True:
                try:
                    received = await asyncio.wait_for(stream.__anext__(), 0.05)
                    break
                except TimeoutError:
                    timeouts += 1
                    attempts_seen.add(stream._reconnect_attempts)
                    assert timeouts < 100, "the event never arrived"

        assert received == expected
        # The backoff outlasted several awaits, and none of them restarted it.
        assert timeouts >= 2
        assert 1 in attempts_seen
        assert attempts_seen <= {0, 1}
        assert client_cls.call_count == 2
        assert dropping.close_calls == 1
        assert healthy.poll_calls == 1
        await stream.close()


class _PromoteOnStopClient(_FakeClient):
    """Client double that promotes the last polled batch's checkpoint on stop().

    A poll promotes the previous batch, as the native client does; promoting on
    stop() as well is the worst case, under which a stop issued while that
    batch is still being fed would cover rows no consumer has received.
    """

    def __init__(self) -> None:
        super().__init__()
        self.current_gtid = "uuid:1"
        self._delivered = ""

    def poll_batch(self) -> list[PollResult]:
        if self._delivered:
            self.current_gtid = self._delivered
        self._delivered = "uuid:1-2"
        return super().poll_batch()

    def stop(self) -> None:
        super().stop()
        if self._delivered:
            self.current_gtid = self._delivered


class TestCheckpointDuringFeed:
    """A checkpoint read while a batch is being fed never covers that batch."""

    @pytest.mark.timeout(20)
    @pytest.mark.parametrize("cancel_first", [False, True], ids=["close", "cancel-then-close"])
    async def test_close_during_a_feed_keeps_the_checkpoint_behind_it(
        self, cancel_first: bool
    ) -> None:
        client = _PromoteOnStopClient()
        engine = _BlockingFeedEngine(events=[_change_event("orders")])
        stream = _started_stream(client, engine)

        iterating = asyncio.create_task(stream.__anext__())
        await _wait_for(engine.entered_feed)
        if cancel_first:
            iterating.cancel()
            with pytest.raises(asyncio.CancelledError):
                await iterating

        closing = asyncio.create_task(stream.close())
        await asyncio.sleep(0.05)
        engine.release_feed.set()
        await asyncio.wait_for(closing, _BLOCK_TIMEOUT_S)
        with contextlib.suppress(BaseException):
            await iterating

        # The batch's row never reached the consumer, so the checkpoint the
        # stream retains stays at the one before it.
        assert stream.current_gtid == "uuid:1"


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
