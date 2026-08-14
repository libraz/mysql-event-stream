"""Tests for BinlogClient resource management."""

from __future__ import annotations

import ctypes
import threading
from unittest.mock import MagicMock, patch

import pytest

from mysql_event_stream._ffi import (
    MES_ERR_DISCONNECTED,
    MES_ERR_INVALID_ARG,
    MES_ERR_STREAM,
    MES_OK,
    MESPollResult,
)
from mysql_event_stream.client import BinlogClient
from mysql_event_stream.types import ServerFlavor


def test_client_ffi_signatures_are_applied_to_the_real_library(lib_path: str) -> None:
    """Exercise load_client_library instead of replacing both FFI layers with mocks."""
    from mysql_event_stream._ffi import (
        MESClientConfig,
        MESPollResult,
        load_client_library,
        load_library,
    )

    lib = load_library(lib_path)
    assert load_client_library(lib) is True
    assert lib.mes_client_create.restype is ctypes.c_void_p
    assert lib.mes_client_create.argtypes == []
    assert lib.mes_client_connect.restype is ctypes.c_int32
    assert lib.mes_client_connect.argtypes == [ctypes.c_void_p, ctypes.POINTER(MESClientConfig)]
    assert lib.mes_client_poll.restype is MESPollResult
    assert lib.mes_client_poll.argtypes == [ctypes.c_void_p]
    assert lib.mes_client_poll_batch.restype is ctypes.c_int32
    assert lib.mes_client_poll_batch.argtypes == [
        ctypes.c_void_p,
        ctypes.POINTER(MESPollResult),
        ctypes.c_size_t,
        ctypes.POINTER(ctypes.c_size_t),
    ]
    assert lib.mes_client_queued_bytes.restype is ctypes.c_size_t


def test_constructor_rejects_zero_server_id() -> None:
    with pytest.raises(ValueError, match="server_id must be non-zero"):
        BinlogClient(server_id=0)


@patch("mysql_event_stream.client.load_client_library", return_value=True)
@patch("mysql_event_stream.client.get_library")
def test_poll_batch_copies_all_native_results(
    mock_load: MagicMock, mock_load_client: MagicMock
) -> None:
    lib = MagicMock()
    lib.mes_client_create.return_value = 0xDEAD
    payload = (ctypes.c_uint8 * 3)(0x01, 0x02, 0x03)

    def poll_batch(
        handle: object,
        results: ctypes.POINTER(MESPollResult),
        capacity: int,
        result_count: ctypes.POINTER(ctypes.c_size_t),
    ) -> int:
        assert handle == 0xDEAD
        assert capacity == 4
        results[0].error = 0
        results[0].data = ctypes.cast(payload, ctypes.POINTER(ctypes.c_uint8))
        results[0].size = len(payload)
        results[0].is_heartbeat = 0
        results[1].error = 0
        results[1].data = None
        results[1].size = 0
        results[1].is_heartbeat = 1
        ctypes.cast(result_count, ctypes.POINTER(ctypes.c_size_t))[0] = 2
        return 0

    lib.mes_client_poll_batch.side_effect = poll_batch
    mock_load.return_value = lib

    client = BinlogClient()
    actual = client.poll_batch(4)
    assert [(result.data, result.is_heartbeat) for result in actual] == [
        (b"\x01\x02\x03", False),
        (None, True),
    ]
    with pytest.raises(ValueError, match="between 1 and 1024"):
        client.poll_batch(0)
    client.close()


def _make_client(lib: MagicMock) -> BinlogClient:
    lib.mes_client_create.return_value = 0xDEAD
    lib.mes_error_string.return_value = b"disconnected"
    lib.mes_client_last_error.return_value = b"reader thread ended"
    return BinlogClient()


def _batch_of(*errors: int) -> object:
    """Build a poll_batch side effect writing one result per error code."""
    payload = (ctypes.c_uint8 * 3)(0x01, 0x02, 0x03)

    def poll_batch(
        _handle: object,
        results: ctypes.POINTER(MESPollResult),
        _capacity: int,
        result_count: ctypes.POINTER(ctypes.c_size_t),
    ) -> int:
        for index, error in enumerate(errors):
            results[index].error = error
            results[index].is_heartbeat = 0
            if error == 0:
                results[index].data = ctypes.cast(payload, ctypes.POINTER(ctypes.c_uint8))
                results[index].size = len(payload)
            else:
                results[index].data = None
                results[index].size = 0
        ctypes.cast(result_count, ctypes.POINTER(ctypes.c_size_t))[0] = len(errors)
        return 0

    return poll_batch


@patch("mysql_event_stream.client.load_client_library", return_value=True)
@patch("mysql_event_stream.client.get_library")
def test_poll_batch_delivers_events_that_precede_a_terminal_error(
    mock_load: MagicMock, mock_load_client: MagicMock
) -> None:
    """Events written before the terminal element must reach the caller.

    The native checkpoint advances on the next poll as if the whole batch was
    consumed, so dropping them loses those events permanently.
    """
    lib = MagicMock()
    lib.mes_client_poll_batch.side_effect = _batch_of(MES_OK, MES_OK, MES_ERR_DISCONNECTED)
    mock_load.return_value = lib

    client = _make_client(lib)
    delivered = client.poll_batch(8)
    assert [result.data for result in delivered] == [b"\x01\x02\x03", b"\x01\x02\x03"]

    # The terminal error surfaces on the next call, exactly once.
    with pytest.raises(RuntimeError) as excinfo:
        client.poll_batch(8)
    assert excinfo.value.code == MES_ERR_DISCONNECTED  # type: ignore[attr-defined]
    assert str(excinfo.value).strip() not in ("", ":")

    lib.mes_client_poll_batch.side_effect = _batch_of(MES_OK)
    assert len(client.poll_batch(8)) == 1
    client.close()


@patch("mysql_event_stream.client.load_client_library", return_value=True)
@patch("mysql_event_stream.client.get_library")
def test_poll_batch_raises_at_once_when_no_event_precedes_the_error(
    mock_load: MagicMock, mock_load_client: MagicMock
) -> None:
    lib = MagicMock()
    lib.mes_client_poll_batch.side_effect = _batch_of(MES_ERR_DISCONNECTED)
    mock_load.return_value = lib

    client = _make_client(lib)
    with pytest.raises(RuntimeError) as excinfo:
        client.poll_batch(8)
    assert excinfo.value.code == MES_ERR_DISCONNECTED  # type: ignore[attr-defined]
    client.close()


@patch("mysql_event_stream.client.load_client_library", return_value=True)
@patch("mysql_event_stream.client.get_library")
def test_a_latched_terminal_error_also_surfaces_from_poll(
    mock_load: MagicMock, mock_load_client: MagicMock
) -> None:
    lib = MagicMock()
    lib.mes_client_poll_batch.side_effect = _batch_of(MES_OK, MES_ERR_STREAM)
    mock_load.return_value = lib

    client = _make_client(lib)
    assert len(client.poll_batch(8)) == 1
    with pytest.raises(RuntimeError) as excinfo:
        client.poll()
    assert excinfo.value.code == MES_ERR_STREAM  # type: ignore[attr-defined]
    lib.mes_client_poll.assert_not_called()
    client.close()


@patch("mysql_event_stream.client.load_client_library", return_value=True)
@patch("mysql_event_stream.client.get_library")
def test_every_native_failure_carries_the_c_abi_code(
    mock_load: MagicMock, mock_load_client: MagicMock
) -> None:
    """A caller branching on ``.code`` must never meet a bare RuntimeError."""
    lib = MagicMock()
    mock_load.return_value = lib

    lib.mes_client_set_max_event_size.return_value = MES_ERR_INVALID_ARG
    client = _make_client(lib)
    with pytest.raises(RuntimeError) as excinfo:
        client.connect()
    assert excinfo.value.code == MES_ERR_INVALID_ARG  # type: ignore[attr-defined]

    lib.mes_client_set_max_event_size.return_value = MES_OK
    lib.mes_client_set_max_queue_bytes.return_value = MES_ERR_INVALID_ARG
    with pytest.raises(RuntimeError) as excinfo:
        client.connect()
    assert excinfo.value.code == MES_ERR_INVALID_ARG  # type: ignore[attr-defined]

    client.close()
    with pytest.raises(RuntimeError) as excinfo:
        client.poll()
    assert excinfo.value.code == MES_ERR_INVALID_ARG  # type: ignore[attr-defined]


class TestClientClose:
    """Verify that close() calls stop, disconnect, and destroy in order."""

    @patch("mysql_event_stream.client.load_client_library", return_value=True)
    @patch("mysql_event_stream.client.get_library")
    def test_constructor_defers_connect_until_explicit_call(
        self, mock_load: MagicMock, mock_load_client: MagicMock
    ) -> None:
        lib = MagicMock()
        lib.mes_client_create.return_value = 0xDEAD
        lib.mes_client_set_max_event_size.return_value = 0
        lib.mes_client_set_max_queue_bytes.return_value = 0
        lib.mes_client_connect.return_value = 0
        mock_load.return_value = lib

        client = BinlogClient()
        lib.mes_client_connect.assert_not_called()

        client.connect()
        lib.mes_client_connect.assert_called_once()
        client.close()

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
    def test_close_idempotent(self, mock_load: MagicMock, mock_load_client: MagicMock) -> None:
        lib = MagicMock()
        lib.mes_client_create.return_value = 0xDEAD
        mock_load.return_value = lib

        client = BinlogClient()
        client.close()
        client.close()  # Second call should be a no-op

        lib.mes_client_destroy.assert_called_once()

    @patch("mysql_event_stream.client.load_client_library", return_value=True)
    @patch("mysql_event_stream.client.get_library")
    def test_checksum_mode_property(
        self, mock_load: MagicMock, mock_load_client: MagicMock
    ) -> None:
        lib = MagicMock()
        lib.mes_client_create.return_value = 0xDEAD
        lib.mes_client_checksum_enabled.return_value = 1
        mock_load.return_value = lib

        client = BinlogClient()
        assert client.checksum_enabled is True
        lib.mes_client_checksum_enabled.assert_called_once_with(0xDEAD)
        client.close()

    @patch("mysql_event_stream.client.load_client_library", return_value=True)
    @patch("mysql_event_stream.client.get_library")
    def test_flavor_property_uses_the_native_value(
        self, mock_load: MagicMock, mock_load_client: MagicMock
    ) -> None:
        lib = MagicMock()
        lib.mes_client_create.return_value = 0xDEAD
        lib.mes_client_flavor.return_value = ServerFlavor.MARIADB
        mock_load.return_value = lib

        client = BinlogClient()
        assert client.flavor is ServerFlavor.MARIADB
        client.close()

    @patch("mysql_event_stream.client.load_client_library", return_value=True)
    @patch("mysql_event_stream.client.get_library")
    def test_queue_and_crc_observability_properties(
        self, mock_load: MagicMock, mock_load_client: MagicMock
    ) -> None:
        lib = MagicMock()
        lib.mes_client_create.return_value = 0xDEAD
        lib.mes_client_queued_bytes.return_value = 123
        lib.mes_client_get_max_queue_bytes.return_value = 456
        lib.mes_client_get_max_event_size.return_value = 789
        lib.mes_client_crc_errors.return_value = 2
        mock_load.return_value = lib

        client = BinlogClient()
        assert client.queued_bytes == 123
        assert client.max_queue_bytes == 456
        assert client.max_event_size == 789
        assert client.crc_errors == 2
        client.close()

    @patch("mysql_event_stream.client.load_client_library", return_value=True)
    @patch("mysql_event_stream.client.get_library")
    def test_connection_and_streaming_states_are_separate(
        self, mock_load: MagicMock, mock_load_client: MagicMock
    ) -> None:
        lib = MagicMock()
        lib.mes_client_create.return_value = 0xDEAD
        lib.mes_client_is_connected.return_value = 0
        lib.mes_client_is_streaming.return_value = 1
        mock_load.return_value = lib

        client = BinlogClient()
        assert client.is_connected is False
        assert client.is_streaming is True
        client.close()

    @patch("mysql_event_stream.client.load_client_library", return_value=True)
    @patch("mysql_event_stream.client.get_library")
    def test_connect_propagates_event_and_queue_byte_limits(
        self, mock_load: MagicMock, mock_load_client: MagicMock
    ) -> None:
        lib = MagicMock()
        lib.mes_client_create.return_value = 0xDEAD
        lib.mes_client_set_max_event_size.return_value = 0
        lib.mes_client_set_max_queue_bytes.return_value = 0
        lib.mes_client_connect.return_value = 0
        mock_load.return_value = lib

        client = BinlogClient(max_event_size=128 * 1024 * 1024, max_queue_bytes=512 * 1024 * 1024)
        client.connect()

        lib.mes_client_set_max_event_size.assert_called_once_with(0xDEAD, 128 * 1024 * 1024)
        lib.mes_client_set_max_queue_bytes.assert_called_once_with(0xDEAD, 512 * 1024 * 1024)
        client.close()

    @patch("mysql_event_stream.client.load_client_library", return_value=True)
    @patch("mysql_event_stream.client.get_library")
    def test_connect_distinguishes_current_empty_gtid_and_file_position(
        self, mock_load: MagicMock, mock_load_client: MagicMock
    ) -> None:
        lib = MagicMock()
        lib.mes_client_create.return_value = 0xDEAD
        lib.mes_client_set_max_event_size.return_value = 0
        lib.mes_client_set_max_queue_bytes.return_value = 0
        lib.mes_client_connect.return_value = 0
        mock_load.return_value = lib

        current = BinlogClient()
        current.connect()
        current_config = lib.mes_client_connect.call_args.args[1]._obj
        assert current_config.start_position_mode == 0
        current.close()

        lib.mes_client_connect.reset_mock()
        empty = BinlogClient(start_gtid="")
        empty.connect()
        empty_config = lib.mes_client_connect.call_args.args[1]._obj
        assert empty_config.start_position_mode == 1
        assert empty_config.start_gtid == b""
        empty.close()

        lib.mes_client_connect.reset_mock()
        position = BinlogClient(start_binlog_file="binlog.000123", start_binlog_position=9876)
        position.connect()
        position_config = lib.mes_client_connect.call_args.args[1]._obj
        assert position_config.start_position_mode == 2
        assert position_config.binlog_file == b"binlog.000123"
        assert position_config.binlog_position == 9876
        position.close()

    @patch("mysql_event_stream.client.load_client_library", return_value=True)
    @patch("mysql_event_stream.client.get_library")
    def test_connect_rejects_conflicting_or_invalid_file_position(
        self, mock_load: MagicMock, mock_load_client: MagicMock
    ) -> None:
        lib = MagicMock()
        lib.mes_client_create.return_value = 0xDEAD
        mock_load.return_value = lib

        with pytest.raises(ValueError, match="cannot be combined"):
            BinlogClient(start_gtid="uuid:1-1", start_binlog_file="binlog.000001").connect()
        with pytest.raises(ValueError, match="4 through"):
            BinlogClient(start_binlog_file="binlog.000001", start_binlog_position=3).connect()

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

    @patch("mysql_event_stream.client.load_client_library", return_value=True)
    @patch("mysql_event_stream.client.get_library")
    def test_close_waits_for_inflight_current_gtid_read(
        self, mock_load: MagicMock, mock_load_client: MagicMock
    ) -> None:
        """A property read must not touch a handle after close() destroys it."""
        lib = MagicMock()
        lib.mes_client_create.return_value = 0xDEAD
        mock_load.return_value = lib

        read_entered = threading.Event()
        release_read = threading.Event()
        destroy_during_read = threading.Event()
        in_read = threading.Event()

        def fake_current_gtid(_handle: object) -> bytes:
            in_read.set()
            read_entered.set()
            release_read.wait(timeout=5)
            in_read.clear()
            return b"uuid:1-2"

        def fake_destroy(_handle: object) -> None:
            if in_read.is_set():
                destroy_during_read.set()

        lib.mes_client_current_gtid.side_effect = fake_current_gtid
        lib.mes_client_destroy.side_effect = fake_destroy
        client = BinlogClient()

        reader = threading.Thread(target=lambda: client.current_gtid)
        reader.start()
        assert read_entered.wait(timeout=5)

        closer = threading.Thread(target=client.close)
        closer.start()
        # close() must wait for the property call's handle lock, not destroy
        # the native client while mes_client_current_gtid() is in flight.
        assert closer.is_alive()
        release_read.set()
        reader.join(timeout=5)
        closer.join(timeout=5)

        assert not destroy_during_read.is_set()
        lib.mes_client_destroy.assert_called_once()
        assert client._handle is None
