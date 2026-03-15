"""BinlogClient - MySQL binlog streaming client."""

from __future__ import annotations

import ctypes

from ._ffi import (
    MES_ERR_CONNECT,
    MES_ERR_DISCONNECTED,
    MES_ERR_STREAM,
    MES_ERR_VALIDATION,
    MES_OK,
    MESClientConfig,
    load_client_library,
    load_library,
)
from .types import ClientConfig, PollResult

_ERROR_MESSAGES = {
    MES_ERR_CONNECT: "Connection failed",
    MES_ERR_VALIDATION: "Server validation failed",
    MES_ERR_STREAM: "Streaming error",
    MES_ERR_DISCONNECTED: "Not connected",
}


class BinlogClient:
    """MySQL binlog streaming client.

    Connects to MySQL and receives binlog events via COM_BINLOG_DUMP_GTID.

    Usage::

        with BinlogClient(host="127.0.0.1", user="root") as client:
            client.connect()
            client.start()
            with CdcEngine() as engine:
                while True:
                    result = client.poll()
                    if result.data:
                        engine.feed(result.data)
                        while (event := engine.next_event()) is not None:
                            print(event)
    """

    def __init__(
        self,
        *,
        host: str = "127.0.0.1",
        port: int = 3306,
        user: str = "root",
        password: str = "",
        server_id: int = 1,
        start_gtid: str = "",
        connect_timeout_s: int = 10,
        read_timeout_s: int = 30,
        lib_path: str | None = None,
    ) -> None:
        """Create a new BinlogClient.

        Args:
            host: MySQL host.
            port: MySQL port.
            user: MySQL user.
            password: MySQL password.
            server_id: Unique replica server ID.
            start_gtid: GTID to start from (empty = current position).
            connect_timeout_s: Connection timeout in seconds.
            read_timeout_s: Read timeout in seconds.
            lib_path: Explicit path to libmes shared library.

        Raises:
            RuntimeError: If client support is not available.
            OSError: If the shared library cannot be found.
        """
        self._lib = load_library(lib_path)
        if not load_client_library(self._lib):
            raise RuntimeError(
                "BinlogClient is not available. Rebuild libmes with libmysqlclient installed"
            )

        self._config = ClientConfig(
            host=host,
            port=port,
            user=user,
            password=password,
            server_id=server_id,
            start_gtid=start_gtid,
            connect_timeout_s=connect_timeout_s,
            read_timeout_s=read_timeout_s,
        )
        self._handle: int | None = self._lib.mes_client_create()
        if not self._handle:
            raise RuntimeError("Failed to create BinlogClient")

    def connect(self) -> None:
        """Connect to MySQL server and validate configuration.

        Raises:
            ConnectionError: If connection or validation fails.
            RuntimeError: If client has been closed.
        """
        self._check_open()

        config = MESClientConfig(
            host=self._config.host.encode("utf-8"),
            port=self._config.port,
            user=self._config.user.encode("utf-8"),
            password=self._config.password.encode("utf-8"),
            server_id=self._config.server_id,
            start_gtid=self._config.start_gtid.encode("utf-8"),
            connect_timeout_s=self._config.connect_timeout_s,
            read_timeout_s=self._config.read_timeout_s,
        )

        rc = self._lib.mes_client_connect(self._handle, ctypes.byref(config))
        if rc != MES_OK:
            error_msg = self._get_last_error()
            base_msg = _ERROR_MESSAGES.get(rc, f"Error code {rc}")
            raise ConnectionError(f"{base_msg}: {error_msg}")

    def start(self) -> None:
        """Start binlog streaming.

        Raises:
            RuntimeError: If streaming cannot be started.
        """
        self._check_open()
        rc = self._lib.mes_client_start(self._handle)
        if rc != MES_OK:
            error_msg = self._get_last_error()
            base_msg = _ERROR_MESSAGES.get(rc, f"Error code {rc}")
            raise RuntimeError(f"{base_msg}: {error_msg}")

    def poll(self) -> PollResult:
        """Poll for next binlog event (blocking).

        Returns:
            PollResult with event data or heartbeat indicator.

        Raises:
            RuntimeError: If a streaming error occurs.
        """
        self._check_open()
        result = self._lib.mes_client_poll(self._handle)
        if result.error != MES_OK:
            error_msg = self._get_last_error()
            base_msg = _ERROR_MESSAGES.get(result.error, f"Error code {result.error}")
            raise RuntimeError(f"{base_msg}: {error_msg}")

        if result.is_heartbeat or result.size == 0:
            return PollResult(data=None, is_heartbeat=bool(result.is_heartbeat))

        # Copy data from C buffer to Python bytes
        data = ctypes.string_at(result.data, result.size)
        return PollResult(data=data, is_heartbeat=False)

    def disconnect(self) -> None:
        """Disconnect from MySQL server."""
        if self._handle:
            self._lib.mes_client_disconnect(self._handle)

    def close(self) -> None:
        """Destroy the client and free resources."""
        if self._handle:
            self._lib.mes_client_destroy(self._handle)
            self._handle = None

    @property
    def is_connected(self) -> bool:
        """Check if client is connected."""
        if not self._handle:
            return False
        return bool(self._lib.mes_client_is_connected(self._handle) == 1)

    @property
    def current_gtid(self) -> str:
        """Get current GTID position."""
        if not self._handle:
            return ""
        raw = self._lib.mes_client_current_gtid(self._handle)
        return raw.decode("utf-8") if raw else ""

    def __enter__(self) -> BinlogClient:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def __del__(self) -> None:
        self.close()

    def _check_open(self) -> None:
        if not self._handle:
            raise RuntimeError("BinlogClient has been closed")

    def _get_last_error(self) -> str:
        if not self._handle:
            return ""
        raw = self._lib.mes_client_last_error(self._handle)
        return raw.decode("utf-8") if raw else ""
