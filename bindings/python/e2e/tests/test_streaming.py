"""E2E tests for BinlogClient connection and GTID tracking.

Tests BinlogClient-specific functionality that isn't covered by
the regular INSERT/UPDATE/DELETE tests (which also use streaming).
"""

from __future__ import annotations

import os
import threading
import time
from pathlib import Path

import pytest
from conftest import MYSQL_HOST, MYSQL_PASSWORD, MYSQL_PORT, MYSQL_USER
from lib.mysql_client import MysqlClient

from mysql_event_stream import CdcEngine, LogLevel, set_log_callback
from mysql_event_stream.client import BinlogClient


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
        start_gtid = mysql.get_current_gtid()

        def stream_worker() -> None:
            with BinlogClient(
                host=MYSQL_HOST,
                port=MYSQL_PORT,
                user=MYSQL_USER,
                password=MYSQL_PASSWORD,
                server_id=201,
                start_gtid=start_gtid,
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
        # MySQL GTID: "uuid:gno" (contains ':'), MariaDB GTID: "domain-server-seq" (contains '-')
        assert len(gtid_result[0]) > 0, "Expected non-empty GTID"
        assert ":" in gtid_result[0] or "-" in gtid_result[0], (
            f"Expected GTID format, got: {gtid_result[0]}"
        )

    def test_reader_thread_log_callback(self, mysql: MysqlClient, lib_path: str) -> None:
        """A native reader-thread log reaches the ctypes callback with the GIL held."""
        start_gtid = mysql.get_current_gtid()
        callback_seen = threading.Event()
        callback_thread_ids: list[int] = []
        main_thread_id = threading.get_ident()

        def handler(_level: LogLevel, message: str) -> None:
            if "event=binlog_reader_started" in message:
                callback_thread_ids.append(threading.get_ident())
                callback_seen.set()

        is_mariadb = os.environ.get("DB_FLAVOR") == "mariadb"
        ca_path = Path(__file__).resolve().parents[4] / "e2e" / "docker" / "certs" / "ca.pem"
        set_log_callback(handler, LogLevel.DEBUG)
        try:
            with BinlogClient(
                host=MYSQL_HOST,
                port=MYSQL_PORT,
                user=MYSQL_USER,
                password=MYSQL_PASSWORD,
                server_id=203,
                start_gtid=start_gtid,
                ssl_mode=0 if is_mariadb else 3,
                ssl_ca="" if is_mariadb else str(ca_path),
                lib_path=lib_path,
            ) as client:
                client.connect()
                client.start()
                assert callback_seen.wait(10), "native reader thread did not emit startup log"
        finally:
            set_log_callback(None)

        assert callback_thread_ids and callback_thread_ids[0] != main_thread_id

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
