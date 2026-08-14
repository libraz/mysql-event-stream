"""CdcStream - High-level async iterator for MySQL CDC events."""

from __future__ import annotations

import asyncio
import contextlib
import random
from collections.abc import Callable
from typing import cast

from ._contract import (
    NON_RETRYABLE_ERROR_CODES,
    OPTION_RANGES,
    backoff_delay_ms,
)
from .client import BinlogClient
from .engine import CdcEngine
from .types import ChangeEvent, PollResult

# Public option name -> attribute holding it. Drives both construction-time and
# configure() validation so the two paths can never accept different values.
_FIELD_MAP = {
    "host": "_host",
    "port": "_port",
    "user": "_user",
    "password": "_password",
    "server_id": "_server_id",
    "start_gtid": "_start_gtid",
    "start_binlog_file": "_start_binlog_file",
    "start_binlog_position": "_start_binlog_position",
    "connect_timeout_s": "_connect_timeout_s",
    "read_timeout_s": "_read_timeout_s",
    "ssl_mode": "_ssl_mode",
    "ssl_ca": "_ssl_ca",
    "ssl_cert": "_ssl_cert",
    "ssl_key": "_ssl_key",
    "max_queue_size": "_max_queue_size",
    "max_queue_bytes": "_max_queue_bytes",
    "max_event_size": "_max_event_size",
    "include_databases": "_include_databases",
    "include_tables": "_include_tables",
    "exclude_tables": "_exclude_tables",
    "allow_public_key_retrieval": "_allow_public_key_retrieval",
    "lib_path": "_lib_path",
    "max_reconnect_attempts": "_max_reconnect_attempts",
    "on_metadata_error": "_on_metadata_error",
}


def _validate_stream_option(key: str, value: object) -> None:
    """Validate a configuration value against the cross-binding contract."""
    if key in OPTION_RANGES:
        minimum, maximum = OPTION_RANGES[key]
        if isinstance(value, bool) or not isinstance(value, int):
            raise TypeError(f"{key} must be an integer")
        if value < minimum or (maximum is not None and value > maximum):
            upper = "unbounded" if maximum is None else str(maximum)
            raise ValueError(f"{key} must be between {minimum} and {upper}")
        return
    if key in {"host", "user", "password", "ssl_ca", "ssl_cert", "ssl_key"}:
        if not isinstance(value, str):
            raise TypeError(f"{key} must be a string")
        return
    if key in {"start_gtid", "start_binlog_file", "lib_path"}:
        if value is not None and not isinstance(value, str):
            raise TypeError(f"{key} must be a string or None")
        return
    if key.startswith(("include_", "exclude_")):
        if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
            raise TypeError(f"{key} must be a list of strings")
        return
    if key == "allow_public_key_retrieval" and not isinstance(value, bool):
        raise TypeError("allow_public_key_retrieval must be a bool")
    if key == "on_metadata_error" and value is not None and not callable(value):
        raise TypeError("on_metadata_error must be callable or None")


class CdcStream:
    """Async iterator that streams MySQL CDC events.

    Leaving the iteration early does not release the native client on its own,
    so scope the stream and let ``__aexit__`` close it::

        async with CdcStream(host="127.0.0.1", user="root") as stream:
            async for event in stream:
                print(event)
                break
        # stream.current_gtid still holds the last checkpoint here.

    Without the context manager, call :meth:`aclose` (or :meth:`close`)
    explicitly once iteration is done.
    """

    def __init__(
        self,
        *,
        host: str = "127.0.0.1",
        port: int = 3306,
        user: str = "root",
        password: str = "",
        server_id: int = 1,
        start_gtid: str | None = None,
        start_binlog_file: str | None = None,
        start_binlog_position: int = 0,
        connect_timeout_s: int = 10,
        read_timeout_s: int = 30,
        ssl_mode: int = 1,
        ssl_ca: str = "",
        ssl_cert: str = "",
        ssl_key: str = "",
        max_queue_size: int = 0,
        max_queue_bytes: int = 48 * 1024 * 1024,
        max_event_size: int = 32 * 1024 * 1024,
        include_databases: list[str] | None = None,
        include_tables: list[str] | None = None,
        exclude_tables: list[str] | None = None,
        allow_public_key_retrieval: bool = False,
        lib_path: str | None = None,
        max_reconnect_attempts: int = 10,
        on_metadata_error: Callable[[RuntimeError], None] | None = None,
    ) -> None:
        """Create a new CdcStream.

        Args:
            host: MySQL host.
            port: MySQL port.
            user: MySQL user.
            password: MySQL password.
            server_id: Unique replica server ID.
            start_gtid: GTID set to start from. ``None`` snapshots the current
                server position; ``""`` explicitly starts from the empty set.
            start_binlog_file: Binlog file for an exact file/offset start.
                Requires ``start_binlog_position`` and cannot be combined with
                ``start_gtid``.
            start_binlog_position: Binlog offset for an exact file/offset
                start; must be at least 4.
            connect_timeout_s: Connection timeout in seconds.
            read_timeout_s: Read timeout in seconds.
            ssl_mode: SSL mode (0=disabled, 1=preferred, 2=required,
                3=verify_ca, 4=verify_identity).
            ssl_ca: Path to CA certificate file (empty to skip).
            ssl_cert: Path to client certificate file (empty to skip).
            ssl_key: Path to client private key file (empty to skip).
            max_queue_size: Maximum event queue size (0 = default 10000).
            max_queue_bytes: Total queued payload byte budget (default 48 MiB;
                0 restores the default).
            max_event_size: Maximum binlog event size accepted by the client
                and parser (default 32 MiB; 0 = 1 GiB hard cap). Raise
                max_queue_bytes when raising this limit.
            include_databases: Exact, case-sensitive database names to include.
            include_tables: Case-sensitive `database.table`, bare table names,
                or trailing-``*`` prefixes to include.
            exclude_tables: Case-sensitive `database.table`, bare table names,
                or trailing-``*`` prefixes to exclude.
            allow_public_key_retrieval: Permit unauthenticated RSA key retrieval
                without TLS. MITM-sensitive; prefer verified TLS.
            lib_path: Explicit path to libmes shared library.
            max_reconnect_attempts: Maximum reconnection attempts
                (default 10, 0 = disabled).
            on_metadata_error: Called when the optional metadata connection
                cannot be enabled. Column names then fall back to indices.
                Without it the failure is silent, matching the library-wide rule
                that nothing is written to stderr on the caller's behalf.

        Raises:
            TypeError: If an option has the wrong type.
            ValueError: If an option falls outside its accepted range.
        """
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self._server_id = server_id
        self._start_gtid = start_gtid
        self._start_binlog_file = start_binlog_file
        self._start_binlog_position = start_binlog_position
        self._connect_timeout_s = connect_timeout_s
        self._read_timeout_s = read_timeout_s
        self._ssl_mode = ssl_mode
        self._ssl_ca = ssl_ca
        self._ssl_cert = ssl_cert
        self._ssl_key = ssl_key
        self._max_queue_size = max_queue_size
        self._max_queue_bytes = max_queue_bytes
        self._max_event_size = max_event_size
        self._include_databases = list(include_databases or [])
        self._include_tables = list(include_tables or [])
        self._exclude_tables = list(exclude_tables or [])
        self._allow_public_key_retrieval = allow_public_key_retrieval
        self._lib_path = lib_path
        self._max_reconnect_attempts = max_reconnect_attempts
        self._on_metadata_error = on_metadata_error
        # Construction accepts exactly what configure() accepts: both paths
        # range-check against the same contract table.
        for key, attr in _FIELD_MAP.items():
            _validate_stream_option(key, getattr(self, attr))
        self._reconnect_attempts = 0

        self._client: BinlogClient | None = None
        self._engine: CdcEngine | None = None
        self._started = False
        self._closed = False
        # Tracks an in-flight poll worker so close() can await its completion
        # before destroying the client (prevents a use-after-free).
        self._poll_task: asyncio.Task[list[PollResult] | PollResult] | None = None
        self._pending_poll_results: list[PollResult] = []
        self._backoff_task: asyncio.Task[None] | None = None
        self._leftover = b""
        # Retains the last non-empty checkpoint after close() releases the
        # native client, so it stays readable once the `async with` scope ends.
        self._last_gtid = ""

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
            start_binlog_file: Binlog filename for an exact file/offset start.
            start_binlog_position: Binlog offset for an exact file/offset start.
            connect_timeout_s: Connection timeout in seconds.
            read_timeout_s: Read timeout in seconds.
            ssl_mode: TLS mode from 0 (disabled) through 4 (verify identity).
            ssl_ca: CA certificate path for verified TLS.
            ssl_cert: Client certificate path.
            ssl_key: Client private-key path.
            max_queue_size: Internal client event-count limit (0 uses default).
            max_queue_bytes: Internal client payload-byte limit (0 uses default).
            max_event_size: Maximum accepted binlog event size in bytes.
            include_databases: Exact database-name include list.
            include_tables: Case-sensitive table-name include list; a trailing
                ``*`` is a prefix wildcard.
            exclude_tables: Case-sensitive table-name exclude list; a trailing
                ``*`` is a prefix wildcard.
            allow_public_key_retrieval: Allow non-TLS caching_sha2 RSA key retrieval.
            lib_path: Explicit path to libmes shared library.
            max_reconnect_attempts: Retry budget (0 disables reconnecting).
            on_metadata_error: Optional callback for metadata connection errors.

        Raises:
            RuntimeError: If streaming has already started.
        """
        if self._started:
            raise RuntimeError("Cannot configure after streaming has started")

        for key, value in kwargs.items():
            attr = _FIELD_MAP.get(key)
            if attr is None:
                raise TypeError(f"Unknown config key: {key!r}")
            _validate_stream_option(key, value)
            setattr(self, attr, list(cast(list[str], value)) if isinstance(value, list) else value)

    async def __anext__(self) -> ChangeEvent:
        # Note: close() is safe to call during iteration. It sets
        # _closed=True and calls client.stop(), which unblocks the worker
        # thread inside poll(). The next __anext__ iteration will then
        # observe _closed and return StopAsyncIteration cleanly.
        if self._closed:
            raise StopAsyncIteration

        while not self._started:
            try:
                await self._start()
            except Exception as err:
                await self._consume_retry(err)
                await self._wait_for_backoff()
                if self._closed:
                    raise StopAsyncIteration from err

        # Explicit checks over `assert`: _start() guarantees both are set
        # when it returns normally, but assertions vanish under `python -O`
        # and we want a clear error if an internal invariant is ever
        # violated (e.g. a subclass override of _start()).
        if self._client is None or self._engine is None:
            raise RuntimeError("Internal error: stream not properly started")

        while True:
            try:
                ev = self._engine.next_event()
                if ev is not None:
                    # A decoded event is the only progress signal that can
                    # reset the retry budget. Receiving framing metadata alone
                    # must not make a permanently undecodable event retry
                    # forever.
                    self._reconnect_attempts = 0
                    return ev

                pending_results = getattr(self, "_pending_poll_results", [])
                if pending_results:
                    result = pending_results.pop(0)
                else:
                    # Track the worker so close() can await it before tearing
                    # down the client. Real clients use one blocking batch call
                    # followed by a non-blocking queue drain; lightweight test
                    # doubles that only implement poll() remain supported.
                    poll_method: Callable[[], PollResult | list[PollResult]]
                    if callable(getattr(type(self._client), "poll_batch", None)):
                        poll_method = self._client.poll_batch
                    else:
                        poll_method = self._client.poll
                    poll_task = cast(
                        asyncio.Task[PollResult | list[PollResult]],
                        asyncio.create_task(asyncio.to_thread(poll_method)),
                    )
                    self._poll_task = poll_task
                    try:
                        polled = await poll_task
                    finally:
                        self._poll_task = None
                    if isinstance(polled, PollResult):
                        result = polled
                    elif polled:
                        result = polled[0]
                        self._pending_poll_results = list(polled[1:])
                    else:
                        continue
                leftover = getattr(self, "_leftover", b"")
                if result.data or leftover:
                    chunk = leftover + (result.data or b"")
                    # Decoding can process a full configured event. Keep that
                    # CPU-bound ctypes call off the asyncio event-loop just
                    # like the blocking native poll/connect/start calls.
                    consumed = await asyncio.to_thread(self._engine.feed, chunk)
                    self._leftover = chunk[consumed:]
            except asyncio.CancelledError:
                # The Future returned by to_thread is cancelled, but the
                # underlying C poll() call keeps blocking. Signal the C
                # layer to unblock it so the worker thread can exit and
                # the thread pool slot is released promptly.
                if self._client is not None:
                    self._client.stop()
                raise
            except Exception as err:
                if self._closed:
                    raise StopAsyncIteration from err
                reconnect_error: Exception = err
                while True:
                    await self._consume_retry(reconnect_error)
                    try:
                        await self._reconnect()
                    except Exception as next_error:
                        if self._closed:
                            raise StopAsyncIteration from next_error
                        reconnect_error = next_error
                        continue
                    if self._closed:
                        raise StopAsyncIteration from reconnect_error
                    break
                continue

    async def aclose(self) -> None:
        """Release the stream from an iteration that ended early.

        ``async for`` never finalizes the iterator it borrows, so an early
        ``break`` leaves the native client alive. This is the async-iterator
        spelling of :meth:`close`; ``async with`` calls it for you.
        """
        await self.close()

    async def close(self) -> None:
        """Stop the stream and release all resources."""
        if self._closed:
            return
        self._closed = True
        self._started = False
        backoff_task = getattr(self, "_backoff_task", None)
        if backoff_task is not None and not backoff_task.done():
            backoff_task.cancel()
            with contextlib.suppress(BaseException):
                await backoff_task
        self._backoff_task = None
        # Unblock and await any in-flight poll() before destroying the client so
        # the worker thread is not still inside the C poll() call during destroy.
        poll_task = getattr(self, "_poll_task", None)
        if poll_task is not None:
            if self._client is not None:
                self._client.stop()
            with contextlib.suppress(BaseException):
                await poll_task
            self._poll_task = None
        # Capture the checkpoint before the client goes away: callers persist it
        # after leaving the iteration scope. This has to run once no poll is in
        # flight, because the accessor takes the same lock a blocking poll holds.
        self._cache_current_gtid()
        if self._client is not None:
            # close() internally calls stop() and disconnect()
            self._client.close()
            self._client = None
        if self._engine is not None:
            self._engine.close()
            self._engine = None

    @property
    def current_gtid(self) -> str:
        """Get the delivered, committed checkpoint candidate.

        The stream is at-least-once, not exactly-once. Persist this value only
        after application processing succeeds. The last non-empty value survives
        :meth:`close`, so it can still be read after the ``async with`` scope
        ends.
        """
        self._cache_current_gtid()
        return cast(str, getattr(self, "_last_gtid", ""))

    def _cache_current_gtid(self) -> None:
        """Retain the client's checkpoint so it outlives the native handle."""
        if self._client is None:
            return
        gtid = self._client.current_gtid
        if gtid:
            self._last_gtid = gtid

    def _adopt_resume_position(self, checkpoint: str) -> None:
        """Point the successor connection at ``checkpoint``, or keep the start mode.

        This is the only place a start position is rewritten, so the rule holds
        on every reconnect path. An empty checkpoint means none was ever
        published: the connection died before its first commit, or it was
        anchored to a file offset that produces no GTID. Forwarding that as
        ``start_gtid=""`` would request the empty GTID set, which the server
        reads as "send every binlog you still retain". Keep the configured start
        mode instead -- at worst the successor replays from the original anchor.

        Args:
            checkpoint: GTID set reported by the dropped connection, or ``""``.
        """
        if not checkpoint:
            return
        self._start_gtid = checkpoint
        self._start_binlog_file = None
        self._start_binlog_position = 0

    async def _reconnect(self) -> None:
        """Perform one reconnect attempt using the last known GTID."""
        if self._closed:
            return
        gtid = self.current_gtid
        if self._client is not None:
            # close() internally calls stop() and disconnect()
            self._client.close()
            self._client = None

        await self._wait_for_backoff()

        # Re-check after sleep: close() may have been called while we slept.
        if self._closed:
            return

        self._adopt_resume_position(gtid)
        # The engine resumes from a GTID checkpoint after reconnect. Bytes
        # buffered from the dropped transport must not be replayed into the
        # new connection's parser state.
        self._leftover = b""
        self._pending_poll_results = []

        self._client = BinlogClient(
            host=self._host,
            port=self._port,
            user=self._user,
            password=self._password,
            server_id=self._server_id,
            start_gtid=self._start_gtid,
            start_binlog_file=self._start_binlog_file,
            start_binlog_position=self._start_binlog_position,
            connect_timeout_s=self._connect_timeout_s,
            read_timeout_s=self._read_timeout_s,
            ssl_mode=self._ssl_mode,
            ssl_ca=self._ssl_ca,
            ssl_cert=self._ssl_cert,
            ssl_key=self._ssl_key,
            max_queue_size=self._max_queue_size,
            max_queue_bytes=self._max_queue_bytes,
            max_event_size=self._max_event_size,
            allow_public_key_retrieval=self._allow_public_key_retrieval,
            lib_path=self._lib_path,
        )
        self._pending_poll_results = []
        # Explicit check instead of `assert`: assertions are stripped when
        # Python is run with -O and we would then silently call .reset() on
        # None and crash with AttributeError. A RuntimeError here gives a
        # useful diagnostic even in optimised builds.
        if self._engine is None:
            raise RuntimeError("Internal error: engine missing during reconnect")
        self._engine.reset()
        self._engine.set_max_event_size(self._max_event_size)
        self._engine.set_max_queue_size(self._max_queue_size)
        self._apply_filters()
        # Metadata is optional; column names fall back to indices.
        try:
            self._engine.enable_metadata(
                host=self._host,
                port=self._port,
                user=self._user,
                password=self._password,
                server_id=self._server_id,
                connect_timeout_s=self._connect_timeout_s,
                read_timeout_s=self._read_timeout_s,
                ssl_mode=self._ssl_mode,
                ssl_ca=self._ssl_ca,
                ssl_cert=self._ssl_cert,
                ssl_key=self._ssl_key,
                allow_public_key_retrieval=self._allow_public_key_retrieval,
            )
        except RuntimeError as exc:
            self._report_metadata_error(exc)
        await asyncio.to_thread(self._client.connect)
        await asyncio.to_thread(self._client.start)
        self._engine.set_checksum_enabled(self._client.checksum_enabled)
        # Do NOT reset _reconnect_attempts here. The counter should only
        # reset when a real event is successfully received (in __anext__),
        # not when a reconnection completes. Otherwise, a server that
        # accepts connections but immediately drops the stream would
        # trigger infinite reconnections regardless of max_reconnect_attempts.

    async def _consume_retry(self, error: Exception) -> None:
        """Charge one retry to the shared construct/connect/start/poll budget."""
        code = getattr(error, "code", None)
        if (
            self._max_reconnect_attempts == 0
            or isinstance(error, ValueError)
            or code in NON_RETRYABLE_ERROR_CODES
        ):
            await self.close()
            raise error

        self._reconnect_attempts += 1
        if self._reconnect_attempts > self._max_reconnect_attempts:
            await self.close()
            raise RuntimeError(
                f"Max reconnect attempts ({self._max_reconnect_attempts}) exceeded"
            ) from error

    async def _wait_for_backoff(self) -> None:
        """Wait for jittered backoff, interruptible by close()."""
        delay = backoff_delay_ms(self._reconnect_attempts, random.random()) / 1000.0
        task = asyncio.create_task(asyncio.sleep(delay))
        self._backoff_task = task
        try:
            await task
        except asyncio.CancelledError:
            if not self._closed:
                raise
        finally:
            if self._backoff_task is task:
                self._backoff_task = None

    async def _start(self) -> None:
        """Create client and engine, connect and start streaming."""
        self._pending_poll_results = []
        self._client = BinlogClient(
            host=self._host,
            port=self._port,
            user=self._user,
            password=self._password,
            server_id=self._server_id,
            start_gtid=self._start_gtid,
            start_binlog_file=self._start_binlog_file,
            start_binlog_position=self._start_binlog_position,
            connect_timeout_s=self._connect_timeout_s,
            read_timeout_s=self._read_timeout_s,
            ssl_mode=self._ssl_mode,
            ssl_ca=self._ssl_ca,
            ssl_cert=self._ssl_cert,
            ssl_key=self._ssl_key,
            max_queue_size=self._max_queue_size,
            max_queue_bytes=self._max_queue_bytes,
            max_event_size=self._max_event_size,
            allow_public_key_retrieval=self._allow_public_key_retrieval,
            lib_path=self._lib_path,
        )
        self._engine = CdcEngine(lib_path=self._lib_path)
        self._engine.set_max_event_size(self._max_event_size)
        self._engine.set_max_queue_size(self._max_queue_size)
        self._apply_filters()
        try:
            try:
                self._engine.enable_metadata(
                    host=self._host,
                    port=self._port,
                    user=self._user,
                    password=self._password,
                    server_id=self._server_id,
                    connect_timeout_s=self._connect_timeout_s,
                    read_timeout_s=self._read_timeout_s,
                    ssl_mode=self._ssl_mode,
                    ssl_ca=self._ssl_ca,
                    ssl_cert=self._ssl_cert,
                    ssl_key=self._ssl_key,
                    allow_public_key_retrieval=self._allow_public_key_retrieval,
                )
            except RuntimeError as exc:
                self._report_metadata_error(exc)
            await asyncio.to_thread(self._client.connect)
            await asyncio.to_thread(self._client.start)
            self._engine.set_checksum_enabled(self._client.checksum_enabled)
            self._started = True
        except Exception:
            if self._engine is not None:
                self._engine.close()
                self._engine = None
            if self._client is not None:
                self._client.close()
                self._client = None
            raise

    def _apply_filters(self) -> None:
        """Apply case-sensitive exact/prefix filters after construction or reset."""
        if self._engine is None:
            return
        self._engine.set_include_databases(self._include_databases)
        self._engine.set_include_tables(self._include_tables)
        self._engine.set_exclude_tables(self._exclude_tables)

    def _report_metadata_error(self, error: RuntimeError) -> None:
        """Report optional metadata failures consistently on start and reconnect.

        Without a handler the failure is silent: the library never writes
        diagnostics on the caller's behalf. Column names fall back to numeric
        indices, and embedders that want the detail pass ``on_metadata_error``
        or install the native log callback.
        """
        if self._on_metadata_error is not None:
            self._on_metadata_error(error)
