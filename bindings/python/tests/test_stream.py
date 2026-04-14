"""Tests for CdcStream resource management and reconnect logic."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from mysql_event_stream.stream import CdcStream


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

    @pytest.mark.asyncio
    async def test_close_idempotent(self) -> None:
        stream = CdcStream.__new__(CdcStream)
        stream._closed = False
        stream._client = MagicMock()
        stream._engine = MagicMock()

        await stream.close()
        await stream.close()  # Should not raise


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
        assert not stream._started

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
