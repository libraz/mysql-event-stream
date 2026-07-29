"""Tests for CdcStream resource management and reconnect logic."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from mysql_event_stream._ffi import (
    MES_ERR_GTID_PURGED,
    MES_ERR_GTID_TAGGED_UNSUPPORTED,
    MES_ERR_PARSE,
    MES_ERR_QUEUE_FULL,
)
from mysql_event_stream.stream import CdcStream
from mysql_event_stream.types import PollResult, exception_for_rc


async def _run_in_test(func: object, *args: object) -> object:
    """Execute a to_thread target synchronously while preserving its arguments."""
    return func(*args)  # type: ignore[operator]


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
        stream._poll_task = poll_task

        # stop() must release the in-flight poll so close() can complete.
        mock_client.stop.side_effect = lambda: release.set()

        await stream.close()

        assert awaited, "close() did not await the in-flight poll task"
        mock_client.stop.assert_called_once()
        mock_client.close.assert_called_once()
        assert stream._poll_task is None


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

    @pytest.mark.asyncio
    async def test_reconnect_passes_an_empty_checkpoint_explicitly(self) -> None:
        stream = CdcStream(start_binlog_file="binlog.000001", start_binlog_position=4)
        previous_client = MagicMock()
        previous_client.current_gtid = ""
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

        kwargs = client_cls.call_args.kwargs
        assert kwargs["start_gtid"] == ""
        assert kwargs["start_binlog_file"] is None
        assert kwargs["start_binlog_position"] == 0

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
            pytest.raises(RuntimeError, match="Max reconnect attempts"),
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
            pytest.raises(RuntimeError, match="Max reconnect attempts"),
        ):
            await stream.__anext__()

        # First failure: attempts=1, 1 > 2 false -> reconnect
        # Second failure: attempts=2, 2 > 2 false -> reconnect
        # Third failure: attempts=3, 3 > 2 true -> raise RuntimeError
        assert reconnect_count == 2

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
            pytest.raises(RuntimeError, match="Max reconnect attempts"),
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
        stream._poll_task = None

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
            pytest.raises(RuntimeError, match="Max reconnect attempts"),
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
        stream._poll_task = None
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
        engine.next_event.side_effect = [None, decoded]
        engine.feed.return_value = 2
        stream = CdcStream(host="127.0.0.1")
        stream._started = True
        stream._client = client
        stream._engine = engine

        with patch("asyncio.to_thread", new=AsyncMock(side_effect=_run_in_test)):
            assert await stream.__anext__() is decoded

        engine.feed.assert_called_once_with(b"abcdef")
        assert stream._leftover == b"cdef"
