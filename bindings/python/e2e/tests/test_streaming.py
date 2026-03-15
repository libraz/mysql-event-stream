"""E2E tests for BinlogClient connection and GTID tracking.

Tests BinlogClient-specific functionality that isn't covered by
the regular INSERT/UPDATE/DELETE tests (which also use streaming).
"""

from __future__ import annotations

import threading
import time

import pytest
from lib.mysql_client import MysqlClient

from mysql_event_stream import CdcEngine
from mysql_event_stream.client import BinlogClient

MYSQL_HOST = "127.0.0.1"
MYSQL_PORT = 13307
MYSQL_USER = "root"
MYSQL_PASSWORD = "test_root_password"


@pytest.mark.streaming
class TestBinlogClient:
    """BinlogClient connection-level tests."""

    def test_connect_and_validate(self, lib_path: str) -> None:
        """BinlogClient connects to MySQL and validates server config."""
        with BinlogClient(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            server_id=200,
            lib_path=lib_path,
        ) as client:
            client.connect()
            assert client.is_connected

    def test_gtid_tracking(self, mysql: MysqlClient, lib_path: str) -> None:
        """BinlogClient tracks GTID after receiving events."""
        gtid_result: list[str] = []
        stop = threading.Event()

        def stream_worker() -> None:
            with BinlogClient(
                host=MYSQL_HOST,
                port=MYSQL_PORT,
                user=MYSQL_USER,
                password=MYSQL_PASSWORD,
                server_id=201,
                lib_path=lib_path,
            ) as client:
                client.connect()
                client.start()
                with CdcEngine(lib_path=lib_path) as engine:
                    while not stop.is_set():
                        result = client.poll()
                        if result.is_heartbeat or not result.data:
                            continue
                        engine.feed(result.data)
                        while (ev := engine.next_event()) is not None:
                            if ev.table == "items":
                                gtid_result.append(client.current_gtid)
                                stop.set()
                                return

        t = threading.Thread(target=stream_worker, daemon=True)
        t.start()
        time.sleep(1)

        mysql.insert("items", name="gtid_test", value=1)

        t.join(timeout=30)
        assert not t.is_alive(), "Streaming thread did not finish"
        assert len(gtid_result) == 1
        assert ":" in gtid_result[0], f"Expected GTID format uuid:gno, got: {gtid_result[0]}"

    def test_connection_error(self, lib_path: str) -> None:
        """Connecting to unreachable host raises ConnectionError."""
        with (
            BinlogClient(
                host="192.0.2.1",  # TEST-NET, unreachable
                port=3306,
                user="root",
                password="",
                server_id=202,
                connect_timeout_s=2,
                lib_path=lib_path,
            ) as client,
            pytest.raises(ConnectionError),
        ):
            client.connect()
