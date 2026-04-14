"""Tests for BinlogClient resource management."""

from __future__ import annotations

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
