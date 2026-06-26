"""Tests for BinlogClient resource management."""

from __future__ import annotations

import threading
from unittest.mock import MagicMock, patch

from mysql_event_stream.client import BinlogClient


class TestClientClose:
    """Verify that close() calls stop, disconnect, and destroy in order."""

    @patch("mysql_event_stream.client.load_client_library", return_value=True)
    @patch("mysql_event_stream.client.get_library")
    def test_close_calls_stop_disconnect_destroy(
        self, mock_load: MagicMock, mock_load_client: MagicMock
    ) -> None:
        lib = MagicMock()
        lib.mes_client_create.return_value = 0xDEAD
        mock_load.return_value = lib

        client = BinlogClient()
        handle = client._handle

        call_order: list[str] = []
        lib.mes_client_stop.side_effect = lambda h: call_order.append("stop")
        lib.mes_client_disconnect.side_effect = lambda h: call_order.append("disconnect")
        lib.mes_client_destroy.side_effect = lambda h: call_order.append("destroy")

        client.close()

        assert call_order == ["stop", "disconnect", "destroy"]
        lib.mes_client_stop.assert_called_once_with(handle)
        lib.mes_client_disconnect.assert_called_once_with(handle)
        lib.mes_client_destroy.assert_called_once_with(handle)
        assert client._handle is None

    @patch("mysql_event_stream.client.load_client_library", return_value=True)
    @patch("mysql_event_stream.client.get_library")
    def test_close_idempotent(
        self, mock_load: MagicMock, mock_load_client: MagicMock
    ) -> None:
        lib = MagicMock()
        lib.mes_client_create.return_value = 0xDEAD
        mock_load.return_value = lib

        client = BinlogClient()
        client.close()
        client.close()  # Second call should be a no-op

        lib.mes_client_destroy.assert_called_once()

    @patch("mysql_event_stream.client.load_client_library", return_value=True)
    @patch("mysql_event_stream.client.get_library")
    def test_context_manager_calls_close(
        self, mock_load: MagicMock, mock_load_client: MagicMock
    ) -> None:
        lib = MagicMock()
        lib.mes_client_create.return_value = 0xDEAD
        mock_load.return_value = lib

        with BinlogClient():
            pass

        lib.mes_client_stop.assert_called_once()
        lib.mes_client_disconnect.assert_called_once()
        lib.mes_client_destroy.assert_called_once()


class TestClosePollRace:
    """close() must not destroy the handle while poll() is in flight."""

    @patch("mysql_event_stream.client.load_client_library", return_value=True)
    @patch("mysql_event_stream.client.get_library")
    def test_close_waits_for_inflight_poll(
        self, mock_load: MagicMock, mock_load_client: MagicMock
    ) -> None:
        lib = MagicMock()
        lib.mes_client_create.return_value = 0xDEAD
        mock_load.return_value = lib

        poll_entered = threading.Event()
        release_poll = threading.Event()
        destroy_during_poll = threading.Event()
        in_poll = threading.Event()

        def fake_poll(_handle: object) -> MagicMock:
            in_poll.set()
            poll_entered.set()
            # Block until stop() releases us, simulating a blocking C poll().
            release_poll.wait(timeout=5)
            in_poll.clear()
            result = MagicMock()
            result.error = 0
            result.is_heartbeat = False
            result.size = 0
            result.data = None
            return result

        def fake_stop(_handle: object) -> None:
            release_poll.set()

        def fake_destroy(_handle: object) -> None:
            # If poll() is still running when destroy fires, that is the bug.
            if in_poll.is_set():
                destroy_during_poll.set()

        lib.mes_client_poll.side_effect = fake_poll
        lib.mes_client_stop.side_effect = fake_stop
        lib.mes_client_destroy.side_effect = fake_destroy

        client = BinlogClient()

        poller = threading.Thread(target=client.poll)
        poller.start()
        assert poll_entered.wait(timeout=5)

        # close() from another thread must block until poll() returns.
        client.close()
        poller.join(timeout=5)

        assert not destroy_during_poll.is_set(), "destroy() ran while poll() was in flight"
        lib.mes_client_destroy.assert_called_once()
        assert client._handle is None
