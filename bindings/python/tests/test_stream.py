"""Tests for CdcStream resource management and reconnect logic."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, PropertyMock, patch

import pytest

from mysql_event_stream._contract import NON_RETRYABLE_ERROR_CODES
from mysql_event_stream._ffi import (
    MES_ERR_DISCONNECTED,
    MES_ERR_GTID_PURGED,
    MES_ERR_GTID_TAGGED_UNSUPPORTED,
    MES_ERR_INVALID_ARG,
    MES_ERR_PARSE,
    MES_ERR_QUEUE_FULL,
)
from mysql_event_stream.stream import CdcStream
from mysql_event_stream.types import PollResult, exception_for_rc

from .contract_fixture import load_binding_contract

contract = load_binding_contract()


async def _run_in_test(func: object, *args: object, **kwargs: object) -> object:
    """Execute a to_thread target synchronously while preserving its arguments."""
    return func(*args, **kwargs)  # type: ignore[operator]


class TestStreamClose:
    """Verify that close() delegates to BinlogClient.close()."""

    @pytest.mark.asyncio
    async def test_close_delegates_to_client_close(self) -> None:
        stream = CdcStream.__new__(CdcStream)
        stream._closed = False
        stream._engine = MagicMock()

        mock_client = MagicMock()
        stream._client = mock_client

        await stream.close()

        # CdcStream.close() should call BinlogClient.close() which
        # internally calls stop() and disconnect(). No redundant calls.
        mock_client.close.assert_called_once()
        mock_client.stop.assert_not_called()
        mock_client.disconnect.assert_not_called()
        assert stream._client is None
        assert stream._engine is None

    def test_filters_are_applied_to_the_engine(self) -> None:
        stream = CdcStream(
            include_databases=["mydb"],
            include_tables=["mydb.orders"],
            exclude_tables=["mydb.audit_log"],
        )
        engine = MagicMock()
        stream._engine = engine

        stream._apply_filters()

        engine.set_include_databases.assert_called_once_with(["mydb"])
        engine.set_include_tables.assert_called_once_with(["mydb.orders"])
        engine.set_exclude_tables.assert_called_once_with(["mydb.audit_log"])

    def test_metadata_error_callback_receives_failures(self) -> None:
        callback = MagicMock()
        stream = CdcStream(on_metadata_error=callback)
        error = RuntimeError("metadata connection refused")

        stream._report_metadata_error(error)

        callback.assert_called_once_with(error)

    @pytest.mark.asyncio
    async def test_close_idempotent(self) -> None:
        stream = CdcStream.__new__(CdcStream)
        stream._closed = False
        stream._client = MagicMock()
        stream._engine = MagicMock()

        await stream.close()
        await stream.close()  # Should not raise

    @pytest.mark.asyncio
    async def test_close_awaits_inflight_poll(self) -> None:
        # When a poll() task is in flight, close() must stop the client to
        # unblock it and await its completion before destroying the client.
        stream = CdcStream.__new__(CdcStream)
        stream._closed = False
        stream._engine = MagicMock()
        mock_client = MagicMock()
        stream._client = mock_client

        release = asyncio.Event()
        awaited = False

        async def fake_poll() -> None:
            nonlocal awaited
            await release.wait()
            awaited = True

        poll_task = asyncio.ensure_future(fake_poll())
        stream._native_task = poll_task

        # stop() must release the in-flight poll so close() can complete.
        mock_client.stop.side_effect = lambda: release.set()

        await stream.close()

        assert awaited, "close() did not await the in-flight poll task"
        mock_client.stop.assert_called_once()
        mock_client.close.assert_called_once()
        assert stream._native_task is None


class TestCheckpointRetention:
    """The checkpoint must outlive the native client that published it."""

    @pytest.mark.asyncio
    @patch("mysql_event_stream.stream.CdcEngine")
    @patch("mysql_event_stream.stream.BinlogClient")
    async def test_retains_current_gtid_after_an_early_break(
        self, mock_client_cls: MagicMock, mock_engine_cls: MagicMock
    ) -> None:
        client = MagicMock()
        client.current_gtid = "uuid:1-42"
        client.poll.return_value = PollResult(b"\x01", False)
        mock_client_cls.return_value = client
        engine = MagicMock()
        event = MagicMock()
        engine.next_event.side_effect = [event, None]
        engine.feed.return_value = 1
        mock_engine_cls.return_value = engine

        stream = CdcStream(host="127.0.0.1")
        async with stream:
            async for received in stream:
                assert received is event
                break

        # The client is released, yet the checkpoint the caller has to persist
        # after leaving the scope is still readable.
        assert stream._client is None
        assert stream.current_gtid == "uuid:1-42"

    @pytest.mark.asyncio
    async def test_close_caches_the_checkpoint_after_the_poll_returns(self) -> None:
        # The accessor takes the same client lock a blocking poll holds, so
        # reading it before the in-flight poll is unblocked would stall close().
        stream = CdcStream.__new__(CdcStream)
        stream._closed = False
        stream._engine = MagicMock()
        client = MagicMock()
        stream._client = client

        release = asyncio.Event()
        poll_finished = False
        read_while_polling: list[bool] = []

        async def fake_poll() -> None:
            nonlocal poll_finished
            await release.wait()
            poll_finished = True

        def read_gtid() -> str:
            read_while_polling.append(not poll_finished)
            return "uuid:1-9"

        type(client).current_gtid = PropertyMock(side_effect=read_gtid)
        client.stop.side_effect = lambda: release.set()
        stream._native_task = asyncio.ensure_future(fake_poll())

        await stream.close()

        assert read_while_polling == [False]
        assert stream.current_gtid == "uuid:1-9"

    @pytest.mark.asyncio
    @patch("mysql_event_stream.stream.CdcEngine")
    @patch("mysql_event_stream.stream.BinlogClient")
    async def test_reads_the_native_checkpoint_per_poll_batch(
        self, mock_client_cls: MagicMock, mock_engine_cls: MagicMock
    ) -> None:
        client = MagicMock()
        gtid_reads = PropertyMock(return_value="uuid:1-7")
        type(client).current_gtid = gtid_reads
        client.poll.return_value = PollResult(b"\x01", False)
        mock_client_cls.return_value = client
        engine = MagicMock()
        event = MagicMock()
        engine.next_event.side_effect = [event, event, event, None]
        engine.feed.return_value = 1
        mock_engine_cls.return_value = engine

        delivered = 0
        reads_while_iterating = 0
        stream = CdcStream(host="127.0.0.1")
        async with stream:
            async for _ in stream:
                delivered += 1
                reads_while_iterating = gtid_reads.call_count
                if delivered == 3:
                    break

        assert delivered == 3
        # One poll batch delivered all three events, so one native read covers
        # them all; the count must not scale with row events.
        assert (
            reads_while_iterating <= contract["checkpointRetention"]["maxNativeReadsPerPollBatch"]
        )
        assert client.poll.call_count == 1
        assert stream.current_gtid == "uuid:1-7"


class TestStreamConfigure:
    def test_rejects_invalid_runtime_values_without_mutating_stream(self) -> None:
        stream = CdcStream()
        with pytest.raises(ValueError, match="server_id"):
            stream.configure(server_id=0)
        with pytest.raises(TypeError, match="read_timeout_s"):
            stream.configure(read_timeout_s="fast")
        with pytest.raises(TypeError, match="allow_public_key_retrieval"):
            stream.configure(allow_public_key_retrieval=1)
        with pytest.raises(TypeError, match="Unknown config key"):
            stream.configure(not_a_setting=True)
        assert stream._server_id == 1

    def test_accepts_every_runtime_option_shape(self) -> None:
        stream = CdcStream()
        stream.configure(
            host="mysql.example",
            port=3307,
            user="replica",
            password="secret",
            server_id=2,
            start_gtid="",
            start_binlog_file=None,
            start_binlog_position=0,
            connect_timeout_s=0,
            read_timeout_s=1,
            ssl_mode=4,
            ssl_ca="ca.pem",
            ssl_cert="cert.pem",
            ssl_key="key.pem",
            max_queue_size=1,
            max_queue_bytes=1,
            max_event_size=1,
            include_databases=["db"],
            include_tables=["db.t"],
            exclude_tables=["db.skip"],
            allow_public_key_retrieval=True,
            lib_path=None,
            max_reconnect_attempts=0,
            on_metadata_error=None,
        )
        assert stream._port == 3307
        assert stream._include_tables == ["db.t"]


class TestStreamStartFailure:
    """Verify that _start() cleans up on failure."""

    @pytest.mark.asyncio
    @patch("mysql_event_stream.stream.CdcEngine")
    @patch("mysql_event_stream.stream.BinlogClient")
    async def test_start_cleanup_on_connect_failure(
        self, mock_client_cls: MagicMock, mock_engine_cls: MagicMock
    ) -> None:
        mock_client = MagicMock()
        mock_client.connect.side_effect = ConnectionError("refused")
        mock_client_cls.return_value = mock_client

        mock_engine = MagicMock()
        mock_engine_cls.return_value = mock_engine

        stream = CdcStream(host="127.0.0.1")

        with pytest.raises(ConnectionError, match="refused"):
            await stream._start()

        # Resources should be cleaned up
        mock_engine.close.assert_called_once()
        mock_client.close.assert_called_once()
        assert stream._client is None
        assert stream._engine is None

    @pytest.mark.asyncio
    @patch("mysql_event_stream.stream.CdcEngine")
    @patch("mysql_event_stream.stream.BinlogClient")
    async def test_start_propagates_client_checksum_mode(
        self, mock_client_cls: MagicMock, mock_engine_cls: MagicMock
    ) -> None:
        mock_client = MagicMock()
        mock_client.checksum_enabled = False
        mock_client_cls.return_value = mock_client
        mock_engine = MagicMock()
        mock_engine_cls.return_value = mock_engine

        stream = CdcStream(host="127.0.0.1")
        await stream._start()

        mock_engine.set_checksum_enabled.assert_called_once_with(False)
        await stream.close()
        assert not stream._started

    @pytest.mark.asyncio
    @patch("mysql_event_stream.stream.CdcEngine")
    @patch("mysql_event_stream.stream.BinlogClient")
    async def test_start_forwards_read_timeout_to_metadata_connection(
        self, mock_client_cls: MagicMock, mock_engine_cls: MagicMock
    ) -> None:
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client
        mock_engine = MagicMock()
        mock_engine_cls.return_value = mock_engine

        stream = CdcStream(host="127.0.0.1", read_timeout_s=7)
        await stream._start()

        assert mock_engine.enable_metadata.call_args.kwargs["read_timeout_s"] == 7
        await stream.close()

    @pytest.mark.asyncio
    @patch("mysql_event_stream.stream.CdcEngine")
    @patch("mysql_event_stream.stream.BinlogClient")
    async def test_start_cleanup_on_start_failure(
        self, mock_client_cls: MagicMock, mock_engine_cls: MagicMock
    ) -> None:
        mock_client = MagicMock()
        mock_client.connect.return_value = None
        mock_client.start.side_effect = RuntimeError("stream failed")
        mock_client_cls.return_value = mock_client

        mock_engine = MagicMock()
        mock_engine_cls.return_value = mock_engine

        stream = CdcStream(host="127.0.0.1")

        with pytest.raises(RuntimeError, match="stream failed"):
            await stream._start()

        mock_engine.close.assert_called_once()
        mock_client.close.assert_called_once()
        assert stream._client is None
        assert stream._engine is None


class TestReconnectAttempts:
    """Verify reconnect attempt counting.

    With the > check, max_reconnect_attempts=N allows N reconnect attempts.
    max_reconnect_attempts=1 means try one reconnect, then give up.
    """

    async def _reconnect_with_checkpoint(self, stream: CdcStream, checkpoint: str) -> MagicMock:
        """Run one _reconnect() whose dropped client reported ``checkpoint``."""
        previous_client = MagicMock()
        previous_client.current_gtid = checkpoint
        stream._client = previous_client
        stream._engine = MagicMock()
        replacement_client = MagicMock()
        replacement_client.checksum_enabled = True

        with (
            patch.object(stream, "_wait_for_backoff", new=AsyncMock()),
            patch(
                "mysql_event_stream.stream.BinlogClient", return_value=replacement_client
            ) as client_cls,
            patch("asyncio.to_thread", new=AsyncMock()),
        ):
            await stream._reconnect()

        return client_cls

    @pytest.mark.asyncio
    async def test_reconnect_keeps_file_position_without_a_checkpoint(self) -> None:
        # An empty checkpoint must never become an empty GTID set: the server
        # answers that with every binlog it still retains.
        stream = CdcStream(start_binlog_file="binlog.000001", start_binlog_position=4)
        client_cls = await self._reconnect_with_checkpoint(stream, "")

        kwargs = client_cls.call_args.kwargs
        assert kwargs["start_gtid"] is None
        assert kwargs["start_binlog_file"] == "binlog.000001"
        assert kwargs["start_binlog_position"] == 4

    @pytest.mark.asyncio
    async def test_reconnect_keeps_current_position_mode_without_a_checkpoint(self) -> None:
        stream = CdcStream()
        client_cls = await self._reconnect_with_checkpoint(stream, "")

        kwargs = client_cls.call_args.kwargs
        assert kwargs["start_gtid"] is None
        assert kwargs["start_binlog_file"] is None

    @pytest.mark.asyncio
    async def test_reconnect_keeps_explicit_start_gtid_without_a_checkpoint(self) -> None:
        stream = CdcStream(start_gtid="3E11FA47-71CA-11E1-9E33-C80AA9429562:1-2")
        client_cls = await self._reconnect_with_checkpoint(stream, "")

        kwargs = client_cls.call_args.kwargs
        assert kwargs["start_gtid"] == "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-2"

    @pytest.mark.asyncio
    async def test_reconnect_prefers_a_published_checkpoint_over_the_anchor(self) -> None:
        stream = CdcStream(start_binlog_file="binlog.000001", start_binlog_position=4)
        client_cls = await self._reconnect_with_checkpoint(
            stream, "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5"
        )

        kwargs = client_cls.call_args.kwargs
        assert kwargs["start_gtid"] == "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5"
        assert kwargs["start_binlog_file"] is None
        assert kwargs["start_binlog_position"] == 0

    @pytest.mark.asyncio
    @patch("mysql_event_stream.stream.CdcEngine")
    @patch("mysql_event_stream.stream.BinlogClient")
    async def test_initial_connect_failure_retries_with_the_same_start_mode(
        self, mock_client_cls: MagicMock, mock_engine_cls: MagicMock
    ) -> None:
        client = MagicMock()
        client.poll.side_effect = RuntimeError("immediate drop")
        mock_client_cls.side_effect = [ConnectionError("connect refused"), client]
        engine = MagicMock()
        engine.next_event.return_value = None
        mock_engine_cls.return_value = engine
        stream = CdcStream(host="127.0.0.1", max_reconnect_attempts=1)

        with (
            patch.object(CdcStream, "_wait_for_backoff", new=AsyncMock()),
            pytest.raises(RuntimeError, match="immediate drop"),
        ):
            await stream.__anext__()

        # No client existed on the first attempt, so no checkpoint could have
        # been published. The implicit "snapshot the current position" start
        # mode has to survive the retry intact.
        assert mock_client_cls.call_count == 2
        kwargs = mock_client_cls.call_args_list[1].kwargs
        assert kwargs["start_gtid"] is None
        assert kwargs["start_binlog_file"] is None

    @pytest.mark.asyncio
    async def test_max_reconnect_attempts_one_allows_one_retry(self) -> None:
        """max_reconnect_attempts=1 allows exactly 1 reconnect attempt."""
        stream = CdcStream.__new__(CdcStream)
        stream._closed = False
        stream._started = True
        stream._max_reconnect_attempts = 1
        stream._reconnect_attempts = 0

        mock_client = MagicMock()
        mock_engine = MagicMock()
        mock_engine.next_event.return_value = None
        stream._client = mock_client
        stream._engine = mock_engine

        reconnect_count = 0

        async def fake_reconnect(self: CdcStream) -> None:
            nonlocal reconnect_count
            reconnect_count += 1
            self._client = MagicMock()
            self._engine = mock_engine

        with (
            patch.object(CdcStream, "_reconnect", fake_reconnect),
            patch("asyncio.to_thread", side_effect=RuntimeError("connection lost")),
            pytest.raises(RuntimeError, match="connection lost"),
        ):
            await stream.__anext__()

        # First failure: attempts=1, 1 > 1 false -> reconnect
        # Second failure: attempts=2, 2 > 1 true -> raise RuntimeError
        assert reconnect_count == 1

    @pytest.mark.asyncio
    async def test_max_reconnect_attempts_two_allows_two_retries(self) -> None:
        """max_reconnect_attempts=2 should allow exactly 2 reconnects."""
        stream = CdcStream.__new__(CdcStream)
        stream._closed = False
        stream._started = True
        stream._max_reconnect_attempts = 2
        stream._reconnect_attempts = 0

        mock_engine = MagicMock()
        mock_engine.next_event.return_value = None
        stream._client = MagicMock()
        stream._engine = mock_engine

        reconnect_count = 0

        async def fake_reconnect(self: CdcStream) -> None:
            nonlocal reconnect_count
            reconnect_count += 1
            self._client = MagicMock()
            self._engine = mock_engine

        with (
            patch.object(CdcStream, "_reconnect", fake_reconnect),
            patch("asyncio.to_thread", side_effect=RuntimeError("connection lost")),
            pytest.raises(RuntimeError, match="connection lost"),
        ):
            await stream.__anext__()

        # First failure: attempts=1, 1 > 2 false -> reconnect
        # Second failure: attempts=2, 2 > 2 false -> reconnect
        # Third failure: attempts=3, 3 > 2 true -> raise RuntimeError
        assert reconnect_count == 2

    @pytest.mark.asyncio
    async def test_exhausted_budget_raises_the_failure_with_its_code(self) -> None:
        """The failure that exhausted the budget reaches the caller unchanged."""
        stream = CdcStream.__new__(CdcStream)
        stream._closed = False
        stream._started = True
        stream._max_reconnect_attempts = 1
        stream._reconnect_attempts = 0
        stream._backoff_task = None
        stream._native_task = None

        engine = MagicMock()
        engine.next_event.return_value = None
        stream._engine = engine
        stream._client = MagicMock()

        dropped = RuntimeError("stream dropped")
        dropped.code = MES_ERR_DISCONNECTED  # type: ignore[attr-defined]

        async def fake_reconnect(self: CdcStream) -> None:
            self._client = MagicMock()
            self._engine = engine

        with (
            patch.object(CdcStream, "_reconnect", fake_reconnect),
            patch.object(CdcStream, "_wait_for_backoff", new=AsyncMock()),
            patch("asyncio.to_thread", side_effect=dropped),
            pytest.raises(RuntimeError) as raised,
        ):
            await stream.__anext__()

        assert raised.value is dropped
        assert getattr(raised.value, "code", None) == MES_ERR_DISCONNECTED

    @pytest.mark.asyncio
    async def test_zero_reconnect_attempts_no_retry(self) -> None:
        """max_reconnect_attempts=0 disables reconnection entirely.

        The original error is re-raised after close() is called.
        """
        stream = CdcStream.__new__(CdcStream)
        stream._closed = False
        stream._started = True
        stream._max_reconnect_attempts = 0
        stream._reconnect_attempts = 0

        mock_client = MagicMock()
        mock_engine = MagicMock()
        mock_engine.next_event.return_value = None
        stream._client = mock_client
        stream._engine = mock_engine

        with (
            patch("asyncio.to_thread", side_effect=RuntimeError("connection lost")),
            pytest.raises(RuntimeError, match="connection lost"),
        ):
            await stream.__anext__()

        # No reconnect should be attempted; stream should be closed
        assert stream._closed

    @pytest.mark.asyncio
    @patch("mysql_event_stream.stream.CdcEngine")
    @patch("mysql_event_stream.stream.BinlogClient")
    async def test_constructor_failures_share_budget_with_immediate_poll_drop(
        self, mock_client_cls: MagicMock, mock_engine_cls: MagicMock
    ) -> None:
        client = MagicMock()
        client.poll.return_value = PollResult(b"bad event", False)
        client.poll.side_effect = RuntimeError("immediate drop")
        mock_client_cls.side_effect = [
            ConnectionError("connect refused 1"),
            ConnectionError("connect refused 2"),
            client,
        ]
        engine = MagicMock()
        engine.next_event.return_value = None
        mock_engine_cls.return_value = engine
        stream = CdcStream(host="127.0.0.1", max_reconnect_attempts=2)

        with (
            patch.object(CdcStream, "_wait_for_backoff", new=AsyncMock()),
            pytest.raises(RuntimeError, match="immediate drop"),
        ):
            await stream.__anext__()

        assert mock_client_cls.call_count == 3
        client.connect.assert_called_once()
        client.start.assert_called_once()
        client.poll.assert_called_once()

    @pytest.mark.asyncio
    async def test_reconnect_connect_failures_remain_inside_retry_loop(self) -> None:
        stream = CdcStream.__new__(CdcStream)
        stream._closed = False
        stream._started = True
        stream._max_reconnect_attempts = 3
        stream._reconnect_attempts = 0
        stream._backoff_task = None
        stream._native_task = None

        engine = MagicMock()
        engine.next_event.return_value = None
        stream._engine = engine
        stream._client = MagicMock()
        reconnect = AsyncMock(
            side_effect=[ConnectionError("connect 1"), ConnectionError("connect 2"), None]
        )

        with (
            patch.object(stream, "_reconnect", reconnect),
            patch("asyncio.to_thread", side_effect=RuntimeError("poll drop")),
            pytest.raises(RuntimeError, match="poll drop"),
        ):
            await stream.__anext__()

        assert reconnect.await_count == 3

    @pytest.mark.asyncio
    async def test_close_interrupts_backoff(self) -> None:
        stream = CdcStream(host="127.0.0.1", max_reconnect_attempts=10)
        stream._client = MagicMock()
        stream._engine = MagicMock()
        stream._reconnect_attempts = 10

        backoff = asyncio.create_task(stream._wait_for_backoff())
        await asyncio.sleep(0)
        await stream.close()

        assert backoff.done()
        assert stream._backoff_task is None


class TestRetryClassification:
    """Every contract error code has to be classified the same on both surfaces."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("code", sorted(NON_RETRYABLE_ERROR_CODES))
    async def test_permanent_error_surfaces_on_the_first_attempt(self, code: int) -> None:
        error = RuntimeError("permanent stream error")
        error.code = code  # type: ignore[attr-defined]
        stream = CdcStream(host="127.0.0.1", max_reconnect_attempts=10)

        with (
            patch.object(CdcStream, "_start", new=AsyncMock(side_effect=error)) as start,
            patch.object(CdcStream, "_wait_for_backoff", new=AsyncMock()) as backoff,
            pytest.raises(RuntimeError, match="permanent stream error"),
        ):
            await stream.__anext__()

        assert start.await_count == 1
        backoff.assert_not_awaited()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "code", sorted(entry["code"] for entry in contract["retryableErrorCodes"])
    )
    async def test_transient_error_consumes_the_retry_budget(self, code: int) -> None:
        error = RuntimeError("transient stream error")
        error.code = code  # type: ignore[attr-defined]
        stream = CdcStream(host="127.0.0.1", max_reconnect_attempts=1)

        with (
            patch.object(CdcStream, "_start", new=AsyncMock(side_effect=error)) as start,
            patch.object(CdcStream, "_wait_for_backoff", new=AsyncMock()) as backoff,
            pytest.raises(RuntimeError, match="transient stream error"),
        ):
            await stream.__anext__()

        # The initial attempt plus the one allowed retry.
        assert start.await_count == 2
        assert backoff.await_count == 1

    @pytest.mark.asyncio
    @patch("mysql_event_stream.stream.CdcEngine")
    @patch("mysql_event_stream.stream.BinlogClient")
    async def test_permanent_configuration_error_surfaces_on_the_first_connect(
        self, mock_client_cls: MagicMock, mock_engine_cls: MagicMock
    ) -> None:
        # max_queue_bytes below max_event_size is rejected by the native
        # connect with MES_ERR_INVALID_ARG. That is a configuration mistake, so
        # it must not burn the reconnect budget before reaching the caller.
        error = ConnectionError("max_queue_bytes must be greater than max_event_size")
        error.code = MES_ERR_INVALID_ARG  # type: ignore[attr-defined]
        client = MagicMock()
        client.connect.side_effect = error
        mock_client_cls.return_value = client
        mock_engine_cls.return_value = MagicMock()

        stream = CdcStream(host="127.0.0.1", max_queue_bytes=1024)
        with (
            patch.object(CdcStream, "_wait_for_backoff", new=AsyncMock()) as backoff,
            pytest.raises(ConnectionError, match="max_queue_bytes"),
        ):
            await stream.__anext__()

        assert mock_client_cls.call_count == 1
        backoff.assert_not_awaited()


class TestEngineFailures:
    """Engine failures must follow the same cleanup/retry policy as poll failures."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "error_code",
        [MES_ERR_PARSE, MES_ERR_QUEUE_FULL, MES_ERR_GTID_PURGED, MES_ERR_GTID_TAGGED_UNSUPPORTED],
    )
    async def test_feed_permanent_error_closes_stream_without_retrying(
        self, error_code: int
    ) -> None:
        client = MagicMock()
        client.poll.return_value = PollResult(b"abcdef", False)
        engine = MagicMock()
        engine.next_event.return_value = None
        permanent_error = exception_for_rc(error_code, "permanent error")
        engine.feed.side_effect = permanent_error

        stream = CdcStream.__new__(CdcStream)
        stream._closed = False
        stream._started = True
        stream._max_reconnect_attempts = 10
        stream._reconnect_attempts = 0
        stream._backoff_task = None
        stream._native_task = None
        stream._client = client
        stream._engine = engine

        with (
            patch("asyncio.to_thread", new=AsyncMock(side_effect=_run_in_test)),
            pytest.raises(type(permanent_error), match="permanent error"),
        ):
            await stream.__anext__()

        client.close.assert_called_once()
        engine.close.assert_called_once()
        assert stream._closed

    @pytest.mark.asyncio
    async def test_feed_retains_unconsumed_packet_suffix(self) -> None:
        client = MagicMock()
        client.poll.return_value = PollResult(b"abcdef", False)
        engine = MagicMock()
        decoded = MagicMock()
        engine.next_event.side_effect = [decoded, None]
        engine.feed.return_value = 2
        stream = CdcStream(host="127.0.0.1")
        stream._started = True
        stream._client = client
        stream._engine = engine

        with patch("asyncio.to_thread", new=AsyncMock(side_effect=_run_in_test)):
            assert await stream.__anext__() is decoded

        engine.feed.assert_called_once_with(b"abcdef")
        assert stream._leftover == b"cdef"
