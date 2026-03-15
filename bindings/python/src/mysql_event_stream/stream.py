"""CdcStream - High-level async iterator for MySQL CDC events."""

from __future__ import annotations

import asyncio

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
        lib_path: str | None = None,
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
            lib_path: Explicit path to libmes shared library.
        """
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self._server_id = server_id
        self._start_gtid = start_gtid
        self._connect_timeout_s = connect_timeout_s
        self._read_timeout_s = read_timeout_s
        self._lib_path = lib_path

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
                return ev

            try:
                result = await asyncio.to_thread(self._client.poll)
            except (RuntimeError, ConnectionError) as err:
                await self.close()
                raise StopAsyncIteration from err

            if result.data:
                self._engine.feed(result.data)

    async def close(self) -> None:
        """Stop the stream and release all resources."""
        if self._closed:
            return
        self._closed = True
        if self._client is not None:
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
            lib_path=self._lib_path,
        )
        self._engine = CdcEngine(lib_path=self._lib_path)
        self._client.connect()
        self._client.start()
        self._started = True
