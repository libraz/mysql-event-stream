"""CdcStream - High-level async iterator for MySQL CDC events."""

from __future__ import annotations

import asyncio
import warnings

from .client import BinlogClient
from .engine import CdcEngine
from .types import ChangeEvent


class CdcStream:
    """Async iterator that streams MySQL CDC events.

    Usage::

        async for event in CdcStream(host="127.0.0.1", user="root"):
            print(event)

    Or with async context manager::

        async with CdcStream(host="127.0.0.1", user="root") as stream:
            async for event in stream:
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
        ssl_mode: int = 0,
        ssl_ca: str = "",
        ssl_cert: str = "",
        ssl_key: str = "",
        max_queue_size: int = 0,
        lib_path: str | None = None,
        max_reconnect_attempts: int = 10,
    ) -> None:
        """Create a new CdcStream.

        Args:
            host: MySQL host.
            port: MySQL port.
            user: MySQL user.
            password: MySQL password.
            server_id: Unique replica server ID.
            start_gtid: GTID to start from (empty = current position).
            connect_timeout_s: Connection timeout in seconds.
            read_timeout_s: Read timeout in seconds.
            ssl_mode: SSL mode (0=disabled, 1=preferred, 2=required,
                3=verify_ca, 4=verify_identity).
            ssl_ca: Path to CA certificate file (empty to skip).
            ssl_cert: Path to client certificate file (empty to skip).
            ssl_key: Path to client private key file (empty to skip).
            max_queue_size: Maximum event queue size (0 = unlimited).
            lib_path: Explicit path to libmes shared library.
            max_reconnect_attempts: Maximum reconnection attempts
                (default 10, 0 = disabled).
        """
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self._server_id = server_id
        self._start_gtid = start_gtid
        self._connect_timeout_s = connect_timeout_s
        self._read_timeout_s = read_timeout_s
        self._ssl_mode = ssl_mode
        self._ssl_ca = ssl_ca
        self._ssl_cert = ssl_cert
        self._ssl_key = ssl_key
        self._max_queue_size = max_queue_size
        self._lib_path = lib_path
        self._max_reconnect_attempts = max_reconnect_attempts
        self._reconnect_attempts = 0

        self._client: BinlogClient | None = None
        self._engine: CdcEngine | None = None
        self._started = False
        self._closed = False

    async def __aenter__(self) -> CdcStream:
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()

    def __aiter__(self) -> CdcStream:
        return self

    def configure(self, **kwargs: object) -> None:
        """Override config properties before streaming starts.

        Args:
            host: MySQL host.
            port: MySQL port.
            user: MySQL user.
            password: MySQL password.
            server_id: Unique replica server ID.
            start_gtid: GTID to start from.
            connect_timeout_s: Connection timeout in seconds.
            read_timeout_s: Read timeout in seconds.
            lib_path: Explicit path to libmes shared library.

        Raises:
            RuntimeError: If streaming has already started.
        """
        if self._started:
            raise RuntimeError("Cannot configure after streaming has started")

        field_map = {
            "host": "_host",
            "port": "_port",
            "user": "_user",
            "password": "_password",
            "server_id": "_server_id",
            "start_gtid": "_start_gtid",
            "connect_timeout_s": "_connect_timeout_s",
            "read_timeout_s": "_read_timeout_s",
            "ssl_mode": "_ssl_mode",
            "ssl_ca": "_ssl_ca",
            "ssl_cert": "_ssl_cert",
            "ssl_key": "_ssl_key",
            "max_queue_size": "_max_queue_size",
            "lib_path": "_lib_path",
            "max_reconnect_attempts": "_max_reconnect_attempts",
        }
        for key, value in kwargs.items():
            attr = field_map.get(key)
            if attr is None:
                raise TypeError(f"Unknown config key: {key!r}")
            setattr(self, attr, value)

    async def __anext__(self) -> ChangeEvent:
        if self._closed:
            raise StopAsyncIteration

        if not self._started:
            self._start()

        assert self._client is not None
        assert self._engine is not None

        while True:
            ev = self._engine.next_event()
            if ev is not None:
                self._reconnect_attempts = 0
                return ev

            try:
                result = await asyncio.to_thread(self._client.poll)
            except (RuntimeError, ConnectionError) as err:
                if self._closed:
                    raise StopAsyncIteration from err
                if self._max_reconnect_attempts == 0:
                    await self.close()
                    raise StopAsyncIteration from err

                self._reconnect_attempts += 1
                if self._reconnect_attempts >= self._max_reconnect_attempts:
                    await self.close()
                    raise StopAsyncIteration from err

                await self._reconnect()
                continue

            if result.data:
                self._engine.feed(result.data)

    async def close(self) -> None:
        """Stop the stream and release all resources."""
        if self._closed:
            return
        self._closed = True
        if self._client is not None:
            self._client.stop()
            self._client.disconnect()
            self._client.close()
            self._client = None
        if self._engine is not None:
            self._engine.close()
            self._engine = None

    @property
    def current_gtid(self) -> str:
        """Get the current GTID position."""
        if self._client is None:
            return ""
        return self._client.current_gtid

    async def _reconnect(self) -> None:
        """Reconnect with linear backoff using last known GTID."""
        gtid = self.current_gtid
        if self._client is not None:
            self._client.stop()
            self._client.disconnect()
            self._client.close()
            self._client = None

        delay = 1.0 * min(self._reconnect_attempts, 10)
        await asyncio.sleep(delay)

        if gtid:
            self._start_gtid = gtid

        self._client = BinlogClient(
            host=self._host,
            port=self._port,
            user=self._user,
            password=self._password,
            server_id=self._server_id,
            start_gtid=self._start_gtid,
            connect_timeout_s=self._connect_timeout_s,
            read_timeout_s=self._read_timeout_s,
            ssl_mode=self._ssl_mode,
            ssl_ca=self._ssl_ca,
            ssl_cert=self._ssl_cert,
            ssl_key=self._ssl_key,
            max_queue_size=self._max_queue_size,
            lib_path=self._lib_path,
        )
        assert self._engine is not None
        self._engine.reset()
        self._client.connect()
        self._client.start()

    def _start(self) -> None:
        """Create client and engine, connect and start streaming."""
        self._client = BinlogClient(
            host=self._host,
            port=self._port,
            user=self._user,
            password=self._password,
            server_id=self._server_id,
            start_gtid=self._start_gtid,
            connect_timeout_s=self._connect_timeout_s,
            read_timeout_s=self._read_timeout_s,
            ssl_mode=self._ssl_mode,
            ssl_ca=self._ssl_ca,
            ssl_cert=self._ssl_cert,
            ssl_key=self._ssl_key,
            max_queue_size=self._max_queue_size,
            lib_path=self._lib_path,
        )
        self._engine = CdcEngine(lib_path=self._lib_path)
        try:
            try:
                self._engine.enable_metadata(
                    host=self._host,
                    port=self._port,
                    user=self._user,
                    password=self._password,
                    connect_timeout_s=self._connect_timeout_s,
                    ssl_mode=self._ssl_mode,
                    ssl_ca=self._ssl_ca,
                    ssl_cert=self._ssl_cert,
                    ssl_key=self._ssl_key,
                )
            except RuntimeError as exc:
                warnings.warn(
                    f"Failed to enable column name metadata: {exc}. "
                    "Column names will use numeric indices.",
                    stacklevel=2,
                )
            self._client.connect()
            self._client.start()
            self._started = True
        except Exception:
            if self._engine is not None:
                self._engine.close()
                self._engine = None
            if self._client is not None:
                self._client.close()
                self._client = None
            raise
