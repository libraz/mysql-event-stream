"""CdcStream - High-level async iterator for MySQL CDC events."""

from __future__ import annotations

import asyncio
import contextlib
import random
from collections import deque
from collections.abc import Callable
from typing import Any, ParamSpec, TypeVar, cast

from ._contract import (
    NON_RETRYABLE_ERROR_CODES,
    backoff_delay_ms,
)
from ._options import validate_options
from .client import BinlogClient
from .engine import CdcEngine
from .types import ChangeEvent, PollResult

_P = ParamSpec("_P")
_T = TypeVar("_T")

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

    One stream serves one consumer at a time: the native engine and client have
    a single owner, so entering a second iteration while another is in flight
    raises instead of splitting the event stream between the two.
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
                start; 4 through UINT32_MAX, since the first event begins after
                the file's 4-byte magic number. Requires
                ``start_binlog_file``: an offset naming no file is refused
                here rather than accepted and dropped. 0 is what a
                configuration that requested no file/offset start carries.
            connect_timeout_s: Connection timeout in seconds.
            read_timeout_s: Read timeout in seconds.
            ssl_mode: SSL mode (0=disabled, 1=preferred, 2=required,
                3=verify_ca, 4=verify_identity).
            ssl_ca: Path to CA certificate file (empty to skip).
            ssl_cert: Path to client certificate file (empty to skip).
            ssl_key: Path to client private key file (empty to skip).
            max_queue_size: Maximum event queue size (0 = default 10000).
            max_queue_bytes: Total queue byte budget (default 48 MiB; 0 restores
                the default). Charges each queued wire payload plus the GTID
                checkpoint held with it, so a source with a wide GTID set
                applies backpressure after fewer events.
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
            ValueError: If an option falls outside its accepted range, or if
                the configuration names two start modes that exclude each other.
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
        # check the whole configuration against the same contract table.
        validate_options({key: getattr(self, attr) for key, attr in _FIELD_MAP.items()})
        self._reconnect_attempts = 0

        self._client: BinlogClient | None = None
        self._engine: CdcEngine | None = None
        self._started = False
        self._closed = False
        # The single in-flight native dispatch. Every blocking call onto the
        # engine or client handle is tracked here, so close() has exactly one
        # thing to wait for before destroying either handle.
        self._native_task: asyncio.Task[Any] | None = None
        # True while __anext__ is running. The handles have a single owner, so a
        # second concurrent consumer is refused rather than served.
        self._iterating = False
        # Events the last feed already decoded, delivered from here without any
        # further native call.
        self._ready_events: deque[ChangeEvent] = deque()
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
            max_queue_bytes: Internal client queue byte limit (0 uses default).
                Charges wire payloads and their GTID checkpoints alike.
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
            TypeError: If a key is unrecognized or a value has the wrong type.
            ValueError: If a value falls outside its accepted range, leaves one
                option of a required-together pair supplied alone, or names two
                start modes that exclude each other.
        """
        if self._started:
            raise RuntimeError("Cannot configure after streaming has started")

        # Validated before anything is applied, and against the configuration
        # the overrides produce: an option that has to be supplied alongside
        # another may be overridden on its own while that one stays as the
        # constructor left it.
        configured = {key: getattr(self, attr) for key, attr in _FIELD_MAP.items()}
        validate_options(kwargs, base=configured)
        for key, value in kwargs.items():
            attr = _FIELD_MAP[key]
            setattr(self, attr, list(cast(list[str], value)) if isinstance(value, list) else value)

    async def _dispatch(self, func: Callable[_P, _T], *args: _P.args, **kwargs: _P.kwargs) -> _T:
        """Run one blocking native call on a worker thread, tracked for close().

        Every call onto the engine or client handle goes through here, so close()
        has a single slot to wait on. The task awaiting the result can be
        cancelled; the worker thread cannot, so the slot is released only once
        that thread has actually returned from the native call.

        Args:
            func: The native call to run off the event loop.
            *args: Positional arguments forwarded to ``func``.
            **kwargs: Keyword arguments forwarded to ``func``.

        Returns:
            Whatever ``func`` returned.
        """
        # A predecessor abandoned by a cancelled await may still hold the handle.
        await self._quiesce_native_calls()
        dispatch = asyncio.create_task(asyncio.to_thread(func, *args, **kwargs))
        self._native_task = dispatch
        try:
            # Shielded: cancelling the consumer must not cancel the dispatch,
            # because the worker thread would keep running regardless and the
            # slot would stop describing what is inside the handle.
            return await asyncio.shield(dispatch)
        finally:
            if dispatch.done() and self._native_task is dispatch:
                self._native_task = None

    async def _quiesce_native_calls(self) -> None:
        """Wait until no worker thread is inside a native call on our handles.

        Every path that destroys or resets a handle runs this first. A blocking
        poll returns only once the client is told to stop, and the task that
        awaited the dispatch may already have been cancelled, so what has to
        settle is the tracked dispatch rather than the cancellable await.
        """
        dispatch = getattr(self, "_native_task", None)
        if dispatch is None:
            return
        if self._client is not None:
            # stop() is the only client entry point callable while another
            # thread is inside the client, and it is what unblocks poll().
            self._client.stop()
        while not dispatch.done():
            with contextlib.suppress(BaseException):
                await asyncio.shield(dispatch)
        if getattr(self, "_native_task", None) is dispatch:
            self._native_task = None

    def _ready_queue(self) -> deque[ChangeEvent]:
        """Return the buffer holding the events the last feed decoded."""
        ready: deque[ChangeEvent] | None = getattr(self, "_ready_events", None)
        if ready is None:
            ready = deque()
            self._ready_events = ready
        return ready

    def _feed_and_drain(self, chunk: bytes) -> tuple[int, list[ChangeEvent]]:
        """Feed one poll batch into the engine and decode everything it queued.

        Runs as a single worker dispatch. Parsing and the per-column ctypes
        marshalling are what dominate the cost of an event, so both belong here
        and the event loop is left with nothing but buffer handoffs.

        Args:
            chunk: Leftover bytes followed by the bytes of one poll batch.

        Returns:
            The number of bytes the engine consumed, and the decoded events.

        Raises:
            RuntimeError: If the engine is gone, or the engine call fails.
        """
        engine = self._engine
        if engine is None:
            raise RuntimeError("Internal error: engine missing during feed")
        consumed = engine.feed(chunk)
        events: list[ChangeEvent] = []
        while (event := engine.next_event()) is not None:
            events.append(event)
        return consumed, events

    async def __anext__(self) -> ChangeEvent:
        # Note: close() is safe to call during iteration. It sets
        # _closed=True and calls client.stop(), which unblocks the worker
        # thread inside poll(). The next __anext__ iteration will then
        # observe _closed and return StopAsyncIteration cleanly.
        if self._closed:
            raise StopAsyncIteration
        # The engine and client have a single owner, so a second consumer is
        # refused here -- before any native call is dispatched -- instead of
        # splitting the byte stream between two iterations.
        if getattr(self, "_iterating", False):
            raise RuntimeError("CdcStream is already being iterated. Use a single async for loop.")
        self._iterating = True
        try:
            return await self._next_change_event()
        finally:
            self._iterating = False

    async def _next_change_event(self) -> ChangeEvent:
        """Deliver the next event, starting or reconnecting the stream as needed.

        Returns:
            The next decoded change event.

        Raises:
            StopAsyncIteration: If the stream was closed.
            Exception: The failure that ended the stream, re-raised as it was
                received once the retry budget is exhausted, so its ``code``
                still identifies the native error category.
            RuntimeError: If an internal invariant is violated.
        """
        while not self._started:
            try:
                await self._start()
            except Exception as err:
                await self._consume_retry(err)
                await self._wait_for_backoff()
                if self._closed:
                    raise StopAsyncIteration from err

        while True:
            if self._closed:
                raise StopAsyncIteration
            try:
                ready = self._ready_queue()
                if ready:
                    # A decoded event is the only progress signal that can
                    # reset the retry budget. Receiving framing metadata alone
                    # must not make a permanently undecodable event retry
                    # forever.
                    self._reconnect_attempts = 0
                    return ready.popleft()

                # Explicit checks over `assert`: _start() guarantees both are
                # set when it returns normally, but assertions vanish under
                # `python -O` and we want a clear error if an internal
                # invariant is ever violated (e.g. a subclass override of
                # _start()).
                client = self._client
                if client is None or self._engine is None:
                    raise RuntimeError("Internal error: stream not properly started")

                # Real clients use one blocking batch call followed by a
                # non-blocking queue drain; lightweight test doubles that only
                # implement poll() remain supported.
                poll_method: Callable[[], PollResult | list[PollResult]]
                if callable(getattr(type(client), "poll_batch", None)):
                    poll_method = client.poll_batch
                else:
                    poll_method = client.poll
                polled: PollResult | list[PollResult] = await self._dispatch(poll_method)
                results = [polled] if isinstance(polled, PollResult) else list(polled)
                chunk = getattr(self, "_leftover", b"") + b"".join(
                    result.data for result in results if result.data
                )
                if not chunk:
                    continue
                # One dispatch per poll batch: the whole batch is one byte
                # stream, so feeding it once and draining the events it
                # produced costs a single worker handoff however many rows it
                # carried.
                consumed, events = await self._dispatch(self._feed_and_drain, chunk)
                self._leftover = chunk[consumed:]
                ready.extend(events)
            except asyncio.CancelledError:
                # We stop awaiting the dispatch, but the worker thread keeps
                # blocking inside the C poll(). Signal the C layer to unblock it
                # so the thread can exit and release its pool slot. The dispatch
                # stays tracked, so close() still waits for that thread to leave
                # the handle before destroying it.
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
        # Destroying a handle a worker thread is still inside is a use-after-free
        # in C, not an exception, so every dispatch has to settle first.
        await self._quiesce_native_calls()
        # Capture the checkpoint before the client goes away: callers persist it
        # after leaving the iteration scope. Reading it after the dispatches have
        # settled is what makes it cover everything the last poll delivered.
        self._cache_current_gtid()
        if self._client is not None:
            # close() internally calls stop() and disconnect()
            self._client.close()
            self._client = None
        if self._engine is not None:
            self._engine.close()
            self._engine = None
        # Nothing can consume buffered events once iteration has ended.
        self._ready_queue().clear()

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
        # The dropped client and the engine are both about to be replaced or
        # reset, so no worker may still be inside a call on either of them.
        await self._quiesce_native_calls()
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
        # new connection's parser state, and events decoded from them are
        # dropped with the connection that produced them.
        self._leftover = b""
        self._ready_queue().clear()

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
            await self._enable_metadata()
        except RuntimeError as exc:
            self._report_metadata_error(exc)
        await self._dispatch(self._client.connect)
        await self._dispatch(self._client.start)
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
            # Raised as it came, like the fail-fast branch above: the documented
            # way to classify a stream failure is its ``code``, and a fresh
            # exception describing the exhausted budget would carry none.
            raise error

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
        self._ready_queue().clear()
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
                await self._enable_metadata()
            except RuntimeError as exc:
                self._report_metadata_error(exc)
            await self._dispatch(self._client.connect)
            await self._dispatch(self._client.start)
            self._engine.set_checksum_enabled(self._client.checksum_enabled)
            self._started = True
        except Exception:
            # Same rule as close(): nothing is destroyed while a worker thread
            # may still be inside a call on it.
            await self._quiesce_native_calls()
            if self._engine is not None:
                self._engine.close()
                self._engine = None
            if self._client is not None:
                self._client.close()
                self._client = None
            raise

    async def _enable_metadata(self) -> None:
        """Open the optional metadata connection off the event loop.

        The native call performs a full MySQL connect, TLS and auth handshake
        included, so it blocks exactly like connect() and start() and must not
        run on the loop thread.

        Raises:
            RuntimeError: If the metadata connection cannot be enabled.
        """
        if self._engine is None:
            raise RuntimeError("Internal error: engine missing during metadata setup")
        await self._dispatch(
            self._engine.enable_metadata,
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
