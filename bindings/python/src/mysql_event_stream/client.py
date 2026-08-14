"""BinlogClient - MySQL binlog streaming client."""

from __future__ import annotations

import ctypes
import threading

from ._contract import (
    POLL_BATCH_DEFAULT_MAX_EVENTS,
    POLL_BATCH_MAX_MAX_EVENTS,
    POLL_BATCH_MIN_MAX_EVENTS,
)
from ._ffi import (
    MES_ERR_INVALID_ARG,
    MES_OK,
    MESClientConfig,
    MESPollResult,
    get_library,
    load_client_library,
)
from .types import ClientConfig, PollResult, ServerFlavor, exception_for_rc


def validate_poll_batch_size(max_events: int) -> None:
    """Reject a batch capacity outside the window the C ABI accepts."""
    if (
        isinstance(max_events, bool)
        or not isinstance(max_events, int)
        or max_events < POLL_BATCH_MIN_MAX_EVENTS
        or max_events > POLL_BATCH_MAX_MAX_EVENTS
    ):
        raise ValueError(
            "max_events must be an integer between "
            f"{POLL_BATCH_MIN_MAX_EVENTS} and {POLL_BATCH_MAX_MAX_EVENTS}"
        )


def _error_message(lib: ctypes.CDLL, rc: int) -> str:
    """Read the canonical C-ABI error description without trusting a mock value."""
    raw = lib.mes_error_string(rc)
    if isinstance(raw, bytes):
        return raw.decode("utf-8", errors="replace")
    if isinstance(raw, str):
        return raw
    return f"error code {rc}"


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
        config: ClientConfig | None = None,
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
        allow_public_key_retrieval: bool = False,
        lib_path: str | None = None,
    ) -> None:
        """Create a new BinlogClient.

        Args:
            config: Pre-built configuration object. If provided, all other
                connection parameters are ignored (except ``lib_path``).
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
            ssl_mode: SSL mode. Use ``SslMode`` enum values (0=disabled,
                1=preferred, 2=required, 3=verify_ca, 4=verify_identity).
            ssl_ca: Path to CA certificate file (empty to skip).
            ssl_cert: Path to client certificate file (empty to skip).
            ssl_key: Path to client private key file (empty to skip).
            max_queue_size: Maximum internal event queue size. 0 selects the
                default of 10000.
            max_queue_bytes: Total queued payload byte budget. Defaults to
                48 MiB; 0 restores that default.
            max_event_size: Maximum binlog event size. Defaults to 32 MiB;
                0 resolves to the 1 GiB hard cap. Raise max_queue_bytes when
                raising this limit.
            allow_public_key_retrieval: Permit unauthenticated RSA key retrieval
                without TLS. MITM-sensitive; prefer verified TLS.
            lib_path: Explicit path to libmes shared library.

        Raises:
            RuntimeError: If client support is not available.
            OSError: If the shared library cannot be found.
        """
        if config is None and not (1 <= port <= 65535):
            raise ValueError(f"port must be 1-65535, got {port}")
        if config is not None and not (1 <= config.port <= 65535):
            raise ValueError(f"port must be 1-65535, got {config.port}")
        server_id = config.server_id if config is not None else server_id
        if server_id == 0:
            raise ValueError("server_id must be non-zero")

        self._lib = get_library(lib_path)
        if not load_client_library(self._lib):
            raise exception_for_rc(
                MES_ERR_INVALID_ARG,
                "BinlogClient is not available. Rebuild libmes with OpenSSL installed",
            )

        if config is not None:
            self._config = config
        else:
            self._config = ClientConfig(
                host=host,
                port=port,
                user=user,
                password=password,
                server_id=server_id,
                start_gtid=start_gtid,
                start_binlog_file=start_binlog_file,
                start_binlog_position=start_binlog_position,
                connect_timeout_s=connect_timeout_s,
                read_timeout_s=read_timeout_s,
                ssl_mode=ssl_mode,
                ssl_ca=ssl_ca,
                ssl_cert=ssl_cert,
                ssl_key=ssl_key,
                max_queue_size=max_queue_size,
                max_queue_bytes=max_queue_bytes,
                max_event_size=max_event_size,
                allow_public_key_retrieval=allow_public_key_retrieval,
            )
        # Serializes every native handle use against close()/destroy(). poll()
        # holds this lock for the duration of the blocking C call (and result
        # copy), so close() can wait for an in-flight call before destroying
        # the handle. RLock is required because error handling snapshots the
        # native error string while the original call still holds the lock.
        self._poll_lock = threading.RLock()
        # Serializes operations that can stop, disconnect, or destroy the
        # handle. It lets close() issue the thread-safe stop request before
        # waiting on _poll_lock, while preventing a concurrent stop() from
        # using a handle after close() has destroyed it.
        self._lifecycle_lock = threading.Lock()

        # Holds a terminal poll error observed mid-batch until the next poll
        # call; see poll_batch().
        self._latched_error: BaseException | None = None

        self._handle: int | None = self._lib.mes_client_create()
        if self._handle is None:
            raise exception_for_rc(MES_ERR_INVALID_ARG, "Failed to create BinlogClient")

    def connect(self) -> None:
        """Connect to MySQL server and validate configuration.

        Raises:
            ConnectionError: If connection or validation fails.
            RuntimeError: If client has been closed.
        """
        if self._config.max_event_size < 0 or self._config.max_event_size > 0xFFFFFFFF:
            raise ValueError(
                f"max_event_size must fit in uint32, got {self._config.max_event_size}"
            )
        if self._config.max_queue_bytes < 0:
            raise ValueError(
                f"max_queue_bytes must be non-negative, got {self._config.max_queue_bytes}"
            )
        if self._config.start_binlog_file is not None:
            if self._config.start_gtid is not None:
                raise ValueError("start_binlog_file cannot be combined with start_gtid")
            if (
                not self._config.start_binlog_file
                or self._config.start_binlog_position < 4
                or self._config.start_binlog_position > 0xFFFFFFFF
            ):
                raise ValueError(
                    "start_binlog_file and start_binlog_position "
                    "(4 through UINT32_MAX) are required"
                )
        # Keep explicit references to encoded bytes so they are not
        # garbage-collected before the C call completes (matters on
        # non-CPython runtimes like PyPy).
        host_b = self._config.host.encode("utf-8")
        user_b = self._config.user.encode("utf-8")
        password_b = self._config.password.encode("utf-8")
        start_gtid_b = (
            self._config.start_gtid.encode("utf-8") if self._config.start_gtid is not None else None
        )
        binlog_file_b = (
            self._config.start_binlog_file.encode("utf-8")
            if self._config.start_binlog_file is not None
            else None
        )
        ssl_ca_b = self._config.ssl_ca.encode("utf-8") if self._config.ssl_ca else None
        ssl_cert_b = self._config.ssl_cert.encode("utf-8") if self._config.ssl_cert else None
        ssl_key_b = self._config.ssl_key.encode("utf-8") if self._config.ssl_key else None

        config = MESClientConfig(
            host=host_b,
            port=self._config.port,
            user=user_b,
            password=password_b,
            server_id=self._config.server_id,
            start_gtid=start_gtid_b,
            connect_timeout_s=self._config.connect_timeout_s,
            read_timeout_s=self._config.read_timeout_s,
            ssl_mode=self._config.ssl_mode,
            ssl_ca=ssl_ca_b,
            ssl_cert=ssl_cert_b,
            ssl_key=ssl_key_b,
            max_queue_size=self._config.max_queue_size,
            allow_public_key_retrieval=int(self._config.allow_public_key_retrieval),
            start_position_mode=(
                2
                if self._config.start_binlog_file is not None
                else (0 if self._config.start_gtid is None else 1)
            ),
            binlog_file=binlog_file_b,
            binlog_position=self._config.start_binlog_position,
        )

        with self._poll_lock:
            self._check_open()
            # A new session cannot inherit the terminal error of the previous one.
            self._latched_error = None
            limit_rc = self._lib.mes_client_set_max_event_size(
                self._handle, self._config.max_event_size
            )
            if limit_rc != MES_OK:
                raise exception_for_rc(
                    limit_rc,
                    f"mes_client_set_max_event_size failed: {_error_message(self._lib, limit_rc)}",
                )
            limit_rc = self._lib.mes_client_set_max_queue_bytes(
                self._handle, self._config.max_queue_bytes
            )
            if limit_rc != MES_OK:
                raise exception_for_rc(
                    limit_rc,
                    f"mes_client_set_max_queue_bytes failed: {_error_message(self._lib, limit_rc)}",
                )

            rc = self._lib.mes_client_connect(self._handle, ctypes.byref(config))
            if rc != MES_OK:
                error_msg = self._get_last_error()
                base_msg = _error_message(self._lib, rc)
                error = ConnectionError(f"{base_msg}: {error_msg}")
                # Stable native error category used by the stream retry policy.
                error.code = rc  # type: ignore[attr-defined]
                raise error

    def start(self) -> None:
        """Start binlog streaming.

        Raises:
            RuntimeError: If streaming cannot be started.
        """
        with self._poll_lock:
            self._check_open()
            rc = self._lib.mes_client_start(self._handle)
            if rc != MES_OK:
                error_msg = self._get_last_error()
                base_msg = _error_message(self._lib, rc)
                error = RuntimeError(f"{base_msg}: {error_msg}")
                # Stable native error category used by the stream retry policy.
                error.code = rc  # type: ignore[attr-defined]
                raise error

    def poll(self) -> PollResult:
        """Poll for next binlog event (blocking).

        Returns:
            PollResult with event data or heartbeat indicator.

        Raises:
            RuntimeError: If a streaming error occurs.
        """
        # Hold the poll lock across the blocking C call AND the result copy.
        # close() takes the same lock before destroying the handle, so the C
        # buffer referenced by `result` cannot be freed while we read it.
        with self._poll_lock:
            self._check_open()
            latched = self._take_latched_error()
            if latched is not None:
                raise latched
            result = self._lib.mes_client_poll(self._handle)
            if result.error != MES_OK:
                error_msg = self._get_last_error()
                base_msg = _error_message(self._lib, result.error)
                # Map checksum/decode/parse codes to the same typed exceptions
                # the CdcEngine raises, so consumers get consistent types
                # regardless of which surface produced the error.
                raise exception_for_rc(result.error, f"{base_msg}: {error_msg}")

            if result.is_heartbeat or result.size == 0:
                return PollResult(data=None, is_heartbeat=bool(result.is_heartbeat))

            if not result.data:
                return PollResult(data=None, is_heartbeat=False)

            # Copy data from C buffer to Python bytes while still holding the lock.
            data = ctypes.string_at(result.data, result.size)
            return PollResult(data=data, is_heartbeat=False)

    def poll_batch(self, max_events: int = POLL_BATCH_DEFAULT_MAX_EVENTS) -> list[PollResult]:
        """Block for one wire event, then drain further queued events.

        This amortizes the Python worker-thread handoff for busy streams. The
        returned bytes are copied before the next native poll/batch call, so
        each result has the same ownership guarantee as :meth:`poll`.

        A terminal condition arrives as the final element of a batch whose
        earlier elements are real events. Those events are returned and the
        terminal error is raised by the next :meth:`poll` or
        :meth:`poll_batch` call: the native checkpoint advances on that next
        call as if the whole batch had been consumed, so discarding them would
        lose events permanently.
        """
        validate_poll_batch_size(max_events)
        with self._poll_lock:
            self._check_open()
            latched = self._take_latched_error()
            if latched is not None:
                raise latched
            raw_results = (MESPollResult * max_events)()
            count = ctypes.c_size_t(0)
            rc = self._lib.mes_client_poll_batch(
                self._handle, raw_results, max_events, ctypes.byref(count)
            )
            if rc != MES_OK:
                raise exception_for_rc(rc, f"mes_client_poll_batch failed (code {rc})")

            results: list[PollResult] = []
            for index in range(count.value):
                result = raw_results[index]
                if result.error != MES_OK:
                    error_msg = self._get_last_error()
                    base_msg = _error_message(self._lib, result.error)
                    terminal = exception_for_rc(result.error, f"{base_msg}: {error_msg}")
                    if not results:
                        raise terminal
                    self._latched_error = terminal
                    break
                if result.is_heartbeat or result.size == 0 or not result.data:
                    results.append(PollResult(data=None, is_heartbeat=bool(result.is_heartbeat)))
                else:
                    results.append(
                        PollResult(
                            data=ctypes.string_at(result.data, result.size), is_heartbeat=False
                        )
                    )
            return results

    def stop(self) -> None:
        """Request stream stop. Thread-safe; unblocks a pending poll()."""
        with self._lifecycle_lock:
            if self._handle is not None:
                self._lib.mes_client_stop(self._handle)

    def disconnect(self) -> None:
        """Disconnect from MySQL server."""
        with self._lifecycle_lock, self._poll_lock:
            if self._handle is not None:
                self._lib.mes_client_disconnect(self._handle)

    def close(self) -> None:
        """Stop, disconnect, and destroy the client, freeing all resources.

        Safe to call while another thread is blocked in :meth:`poll`: ``stop()``
        unblocks the pending poll, then the poll lock is acquired to wait for it
        to return before the handle is destroyed (avoiding a use-after-free).
        """
        with self._lifecycle_lock:
            if self._handle is not None:
                # Unblock an in-flight poll before waiting for _poll_lock.
                # _lifecycle_lock prevents another stop()/close() from using
                # the handle while this close operation owns its lifetime.
                self._lib.mes_client_stop(self._handle)
                with self._poll_lock:
                    if self._handle is not None:
                        self._lib.mes_client_disconnect(self._handle)
                        self._lib.mes_client_destroy(self._handle)
                        self._handle = None

    @property
    def is_connected(self) -> bool:
        """Check whether the authenticated transport is still usable."""
        with self._poll_lock:
            if self._handle is None:
                return False
            return bool(self._lib.mes_client_is_connected(self._handle) != 0)

    @property
    def is_streaming(self) -> bool:
        """Check whether poll() can still drain data or a terminal error."""
        with self._poll_lock:
            if self._handle is None:
                return False
            return bool(self._lib.mes_client_is_streaming(self._handle) != 0)

    @property
    def current_gtid(self) -> str:
        """Get the delivered, committed checkpoint candidate.

        The value advances after polling past a commit boundary. It is not a
        durable acknowledgement; persist it only after processing succeeds.
        """
        with self._poll_lock:
            if self._handle is None:
                return ""
            # Copy while holding the lock: the C pointer is only valid until
            # the next client call and close() may otherwise destroy it.
            raw = self._lib.mes_client_current_gtid(self._handle)
            return raw.decode("utf-8") if raw else ""

    @property
    def flavor(self) -> ServerFlavor:
        """Return the database flavor detected during connection."""
        with self._poll_lock:
            if self._handle is None:
                return ServerFlavor.MYSQL
            return ServerFlavor(self._lib.mes_client_flavor(self._handle))

    @property
    def checksum_enabled(self) -> bool:
        """Return the checksum mode for events produced by :meth:`poll`."""
        with self._poll_lock:
            if self._handle is None:
                return False
            return bool(self._lib.mes_client_checksum_enabled(self._handle))

    @property
    def queued_bytes(self) -> int:
        """Return currently charged payload bytes waiting in the event queue."""
        with self._poll_lock:
            return (
                0 if self._handle is None else int(self._lib.mes_client_queued_bytes(self._handle))
            )

    @property
    def max_queue_bytes(self) -> int:
        """Return the configured event-queue payload budget in bytes."""
        with self._poll_lock:
            return (
                0
                if self._handle is None
                else int(self._lib.mes_client_get_max_queue_bytes(self._handle))
            )

    @property
    def max_event_size(self) -> int:
        """Return the configured maximum individual binlog event size."""
        with self._poll_lock:
            return (
                0
                if self._handle is None
                else int(self._lib.mes_client_get_max_event_size(self._handle))
            )

    @property
    def crc_errors(self) -> int:
        """Return the number of CRC32-invalid events detected by this client."""
        with self._poll_lock:
            return 0 if self._handle is None else int(self._lib.mes_client_crc_errors(self._handle))

    def __enter__(self) -> BinlogClient:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def __del__(self) -> None:
        # __init__ can reject configuration before creating the native handle.
        # Guard both that partial-construction path and interpreter shutdown.
        if getattr(self, "_handle", None) is not None:
            self.close()

    def _check_open(self) -> None:
        # MES_ERR_INVALID_ARG matches the Node binding and keeps the stream
        # retry policy from treating a permanent lifecycle violation as a
        # transient failure.
        if self._handle is None:
            raise exception_for_rc(MES_ERR_INVALID_ARG, "BinlogClient has been closed")

    def _take_latched_error(self) -> BaseException | None:
        error = self._latched_error
        self._latched_error = None
        return error

    def _get_last_error(self) -> str:
        with self._poll_lock:
            if self._handle is None:
                return ""
            raw = self._lib.mes_client_last_error(self._handle)
            return raw.decode("utf-8") if raw else ""
