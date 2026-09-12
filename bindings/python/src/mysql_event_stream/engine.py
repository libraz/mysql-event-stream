"""CdcEngine - High-level Python wrapper for the mysql-event-stream C engine."""

from __future__ import annotations

import ctypes
from collections.abc import Callable
from typing import Any

from ._ffi import (
    MES_COL_BYTES,
    MES_COL_DOUBLE,
    MES_COL_INT,
    MES_COL_STRING,
    MES_ERR_CHECKSUM,
    MES_ERR_DECODE,
    MES_ERR_DECODE_COLUMN,
    MES_ERR_DECODE_ROW,
    MES_ERR_INVALID_ARG,
    MES_ERR_NO_EVENT,
    MES_ERR_PARSE,
    MES_OK,
    MESClientConfig,
    MESColumn,
    MESEvent,
    get_library,
    load_client_library,
)
from ._options import validate_option, validate_options
from .types import BinlogPosition, ChangeEvent, EventType, exception_for_rc

# ctypes' ``from_buffer`` deliberately rejects immutable bytes. CPython does
# guarantee that a bytes object's storage is contiguous and stable for its
# lifetime, however, so borrow that storage for the duration of mes_feed()
# instead of allocating a same-sized temporary copy.
_pybytes_as_string = ctypes.pythonapi.PyBytes_AsString
_pybytes_as_string.argtypes = [ctypes.py_object]
_pybytes_as_string.restype = ctypes.c_void_p


def _borrow_bytes(data: bytes) -> Any:
    return ctypes.cast(_pybytes_as_string(data), ctypes.POINTER(ctypes.c_uint8))


# Column payloads are copied by slicing a fixed-size ``c_char`` window laid over
# the payload address instead of by calling ``ctypes.string_at``. ``string_at``
# dispatches through libffi, and that per-call cost outweighs the copy itself at
# the payload sizes a row event carries; the window slice performs the same copy
# through the buffer protocol for roughly half the total.
#
# Placing the window is safe because ``from_address`` never reads memory -- only
# the slice does, and it reads exactly ``str_len`` bytes. The window size is an
# upper bound, not a claim about what is mapped. It does have to be respected,
# though: a slice longer than the window is silently truncated rather than
# rejected, so larger payloads fall back to ``string_at``, where the copy
# dominates the call overhead anyway.
#
# typeshed types slicing a ``c_char`` array as ``list[bytes] | bytes`` even
# though it always yields ``bytes``, so the alias is annotated rather than
# every call site.
_PAYLOAD_WINDOW_BYTES = 1 << 16
_payload_window_at: Callable[[int], Any] = (ctypes.c_char * _PAYLOAD_WINDOW_BYTES).from_address

# Cached ANNOTATE_ROWS statements are whole SQL texts rather than identifiers,
# so the ceiling is far below the column-name cache's: every row of one ROWS
# event is drained before the next statement appears, which is all the reuse
# window this has to cover.
_SOURCE_SQL_CACHE_MAX = 64


def _raise_for_rc(rc: int, op: str) -> None:
    """Translate a C-ABI error code into the best-fitting Python exception.

    Delegates to :func:`exception_for_rc` so engine and client surfaces map
    the same code to the same exception type. Every exception raised here is
    still a subclass of ``RuntimeError`` for backward compatibility; callers
    that want to distinguish categories can catch ``ChecksumError`` /
    ``DecodeError`` / ``ParseError`` directly.
    """
    if rc == MES_ERR_CHECKSUM:
        message = f"{op} failed: checksum mismatch (code {rc})"
    elif rc in (MES_ERR_DECODE, MES_ERR_DECODE_COLUMN, MES_ERR_DECODE_ROW):
        message = f"{op} failed: row decode error (code {rc})"
    elif rc == MES_ERR_PARSE:
        message = f"{op} failed: binlog parse error (code {rc})"
    else:
        message = f"{op} failed with error code {rc}"
    raise exception_for_rc(rc, message)


class CdcEngine:
    """MySQL CDC engine backed by the native libmes library.

    Usage::

        with CdcEngine() as engine:
            engine.feed(binlog_data)
            while (event := engine.next_event()) is not None:
                print(event)
    """

    def __init__(self, lib_path: str | None = None) -> None:
        """Create a new CDC engine instance.

        Args:
            lib_path: Explicit path to the libmes shared library.
                If None, searches standard locations.

        Raises:
            RuntimeError: If the engine cannot be created.
            OSError: If the shared library cannot be found.
        """
        self._lib = get_library(lib_path)
        self._client_lib_loaded = False
        self._column_name_cache: dict[bytes, str] = {}
        self._source_sql_cache: dict[bytes, str] = {}
        self._handle: int | None = self._lib.mes_create()
        if self._handle is None:
            raise exception_for_rc(MES_ERR_INVALID_ARG, "Failed to create CDC engine")

    def close(self) -> None:
        """Destroy the engine and free resources."""
        if self._handle is not None:
            self._lib.mes_destroy(self._handle)
            self._handle = None

    def __enter__(self) -> CdcEngine:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def __del__(self) -> None:
        # Guard against interpreter shutdown where self._lib may be None
        if self._lib is not None:
            self.close()

    def _check_open(self) -> None:
        """Raise RuntimeError if the engine has been closed.

        The exception carries ``MES_ERR_INVALID_ARG`` so a closed handle is
        classified like every other permanent misuse, matching the Node
        binding and keeping the retry policy off it.
        """
        if self._handle is None:
            raise exception_for_rc(MES_ERR_INVALID_ARG, "Engine has been closed")

    def feed(self, data: bytes | bytearray) -> int:
        """Feed raw binlog bytes into the engine.

        On a partial consume, re-feed from ``data[consumed:]`` (never from the
        start) or already-queued events are delivered twice.

        After this raises, the engine parse state is undefined: the only valid
        next operation is :meth:`reset`. Re-feeding without a reset is
        unsupported and may duplicate events or spin in a busy-loop.

        Args:
            data: Raw binlog byte stream.

        Returns:
            Number of bytes consumed by the engine.

        Raises:
            RuntimeError: If the engine is closed or feed fails. Call
                :meth:`reset` before feeding again.
        """
        self._check_open()
        if not data:
            return 0

        if isinstance(data, bytearray):
            buf = (ctypes.c_uint8 * len(data)).from_buffer(data)
        else:
            buf = _borrow_bytes(data)
        consumed = ctypes.c_size_t(0)
        rc = self._lib.mes_feed(self._handle, buf, len(data), ctypes.byref(consumed))
        if rc != MES_OK:
            _raise_for_rc(rc, "mes_feed (call reset() before feeding again)")
        return consumed.value

    def next_event(self) -> ChangeEvent | None:
        """Get the next change event.

        Returns:
            A ChangeEvent if available, or None if no events are pending.

        Raises:
            RuntimeError: If the engine is closed or an unexpected error occurs.
        """
        self._check_open()

        event_ptr = ctypes.POINTER(MESEvent)()
        while True:
            rc = self._lib.mes_next_event(self._handle, ctypes.byref(event_ptr))
            if rc == MES_ERR_NO_EVENT:
                return None
            if rc != MES_OK:
                _raise_for_rc(rc, "mes_next_event")
            return _convert_event(
                event_ptr.contents, self._column_name_cache, self._source_sql_cache
            )

    def has_events(self) -> bool:
        """Check if there are pending events.

        Returns:
            True if events are available, False otherwise.

        Raises:
            RuntimeError: If the engine is closed.
        """
        self._check_open()
        result: int = self._lib.mes_has_events(self._handle)
        return result != 0

    def get_position(self) -> BinlogPosition:
        """Get current binlog position.

        Returns:
            Current BinlogPosition.

        Raises:
            RuntimeError: If the engine is closed or the call fails.
        """
        self._check_open()

        file_ptr = ctypes.c_char_p()
        offset = ctypes.c_uint64(0)
        rc = self._lib.mes_get_position(self._handle, ctypes.byref(file_ptr), ctypes.byref(offset))
        if rc != MES_OK:
            _raise_for_rc(rc, "mes_get_position")

        file_str = file_ptr.value.decode("utf-8", errors="replace") if file_ptr.value else ""
        return BinlogPosition(file=file_str, offset=offset.value)

    def set_max_queue_size(self, max_size: int) -> None:
        """Set maximum event queue size for backpressure control.

        When the queue reaches this limit, feed() will stop consuming
        bytes early. Drain events via next_event() then re-feed.

        Args:
            max_size: Maximum queue size. 0 restores the bounded default of
                10000 events. There is no unlimited setting: an unbounded
                queue would let a producer that outruns the consumer grow it
                without limit.

        Raises:
            TypeError: If max_size is not an integer.
            ValueError: If max_size is negative.
            RuntimeError: If the engine is closed or the call fails.
        """
        self._check_open()
        if isinstance(max_size, bool) or not isinstance(max_size, int):
            raise TypeError(f"max_size must be an integer, got {type(max_size).__name__}")
        if max_size < 0:
            raise ValueError(f"max_size must be non-negative, got {max_size}")
        rc = self._lib.mes_set_max_queue_size(self._handle, max_size)
        if rc != MES_OK:
            _raise_for_rc(rc, "mes_set_max_queue_size")

    def set_max_event_size(self, max_event_size: int) -> None:
        """Override the maximum per-event size accepted by the parser.

        The default is 64 MiB, matching MySQL's default max_allowed_packet
        for binlog events. Workloads with very large BLOB/JSON columns
        and a raised max_allowed_packet on the server may need a larger
        ceiling. Values are clamped at the C layer to the range
        [header+checksum, 1 GiB].

        Args:
            max_event_size: Desired ceiling in bytes. 0 means "no limit" and
                resolves to the 1 GiB hard cap; it does not restore the 64 MiB
                default the way 0 restores the default queue size in
                :meth:`set_max_queue_size`. Passing it removes the guard
                against a single oversized event from an untrusted server.

        Raises:
            TypeError: If max_event_size is not an integer.
            ValueError: If max_event_size does not fit in uint32.
            RuntimeError: If the engine is closed or the call fails.
        """
        self._check_open()
        if isinstance(max_event_size, bool) or not isinstance(max_event_size, int):
            raise TypeError(
                f"max_event_size must be an integer, got {type(max_event_size).__name__}"
            )
        if max_event_size < 0 or max_event_size > 0xFFFFFFFF:
            raise ValueError(f"max_event_size must fit in uint32, got {max_event_size}")
        rc = self._lib.mes_set_max_event_size(self._handle, max_event_size)
        if rc != MES_OK:
            _raise_for_rc(rc, "mes_set_max_event_size")

    def get_max_event_size(self) -> int:
        """Return the currently configured maximum event size (bytes).

        Raises:
            RuntimeError: If the engine is closed.
        """
        self._check_open()
        result: int = self._lib.mes_get_max_event_size(self._handle)
        return result

    def set_checksum_enabled(self, enabled: bool) -> None:
        """Set whether raw binlog events carry a trailing CRC32 checksum.

        Disable this for ``binlog_checksum=NONE`` streams that begin after the
        format-description event. A later format-description event overrides
        this setting with its on-wire checksum descriptor.

        Args:
            enabled: ``True`` for CRC32 trailers, ``False`` for no trailer.

        Raises:
            TypeError: If enabled is not a bool.
            RuntimeError: If the engine is closed or the native call fails.
        """
        self._check_open()
        if not isinstance(enabled, bool):
            raise TypeError(f"enabled must be bool, got {type(enabled).__name__}")
        rc = self._lib.mes_set_checksum_enabled(self._handle, int(enabled))
        if rc != MES_OK:
            _raise_for_rc(rc, "mes_set_checksum_enabled")

    def reset(self) -> None:
        """Reset the engine, clearing all state.

        Raises:
            RuntimeError: If the engine is closed or the call fails.
        """
        self._check_open()
        rc = self._lib.mes_reset(self._handle)
        if rc != MES_OK:
            _raise_for_rc(rc, "mes_reset")

    def _set_string_filter(
        self,
        func: Any,
        names: list[str],
        func_name: str,
        option: str,
    ) -> None:
        """Call a C string-array filter function.

        Args:
            func: The ctypes function to call.
            names: List of filter strings.
            func_name: Function name for error messages.
            option: Public option name the list belongs to, for validation.

        Raises:
            TypeError: If names is not a list of strings.
            RuntimeError: If the engine is closed or the call fails.
        """
        self._check_open()
        # Rejected here rather than during the encode below, where a non-string
        # entry surfaces as an AttributeError naming neither the option nor the
        # offending value.
        validate_option(option, names)
        arr = (ctypes.c_char_p * len(names))(*(n.encode("utf-8") for n in names))
        rc = func(self._handle, arr, len(names))
        if rc != MES_OK:
            _raise_for_rc(rc, func_name)

    def set_include_databases(self, databases: list[str]) -> None:
        """Set database include filter.

        Only events from these databases are processed. An empty list
        clears the filter (all databases are allowed).

        Args:
            databases: List of database names.

        Raises:
            TypeError: If databases is not a list of strings.
            RuntimeError: If the engine is closed or the call fails.
        """
        self._set_string_filter(
            self._lib.mes_set_include_databases,
            databases,
            "mes_set_include_databases",
            "include_databases",
        )

    def set_include_tables(self, tables: list[str]) -> None:
        """Set table include filter.

        Only events from these tables are processed. An empty list
        clears the filter (all tables are allowed).

        Each entry is "database.table" or just "table" (matches any database).
        A trailing ``*`` performs a case-sensitive prefix match.

        Args:
            tables: List of table names.

        Raises:
            TypeError: If tables is not a list of strings.
            RuntimeError: If the engine is closed or the call fails.
        """
        self._set_string_filter(
            self._lib.mes_set_include_tables, tables, "mes_set_include_tables", "include_tables"
        )

    def set_exclude_tables(self, tables: list[str]) -> None:
        """Set table exclude filter.

        Events from these tables are skipped.

        Each entry is "database.table" or just "table" (matches any database).
        A trailing ``*`` performs a case-sensitive prefix match.

        Args:
            tables: List of table names.

        Raises:
            TypeError: If tables is not a list of strings.
            RuntimeError: If the engine is closed or the call fails.
        """
        self._set_string_filter(
            self._lib.mes_set_exclude_tables, tables, "mes_set_exclude_tables", "exclude_tables"
        )

    def enable_metadata(
        self,
        *,
        host: str = "127.0.0.1",
        port: int = 3306,
        user: str = "root",
        password: str = "",
        server_id: int = 1,
        connect_timeout_s: int = 10,
        read_timeout_s: int = 30,
        ssl_mode: int = 1,
        ssl_ca: str = "",
        ssl_cert: str = "",
        ssl_key: str = "",
        allow_public_key_retrieval: bool = False,
    ) -> None:
        """Enable metadata queries for column name resolution.

        Uses a separate MySQL connection to fetch column names via
        SHOW COLUMNS FROM.

        Args:
            host: MySQL host.
            port: MySQL port.
            user: MySQL user.
            password: MySQL password.
            server_id: MySQL server ID for the metadata connection.
            connect_timeout_s: Connection timeout in seconds.
            read_timeout_s: Read timeout in seconds.
            ssl_mode: SSL mode (0=disabled, 1=preferred, 2=required,
                3=verify_ca, 4=verify_identity).
            ssl_ca: Path to CA certificate file (empty to skip).
            ssl_cert: Path to client certificate file (empty to skip).
            ssl_key: Path to client private key file (empty to skip).
            allow_public_key_retrieval: Permit unauthenticated RSA key retrieval
                without TLS. MITM-sensitive; prefer verified TLS.

        Raises:
            TypeError: If an option has the wrong type.
            ValueError: If an option falls outside its accepted range.
            RuntimeError: If the engine is closed, client API is unavailable,
                or the metadata connection fails.
        """
        self._check_open()
        # Rejected here rather than at the ctypes boundary: the shared contract
        # fixes the type and range of every one of these options, and a value
        # that violates either would otherwise reach the C ABI as an argument
        # error or a wrapped-around fixed-width integer.
        validate_options(
            {
                "host": host,
                "port": port,
                "user": user,
                "password": password,
                "server_id": server_id,
                "connect_timeout_s": connect_timeout_s,
                "read_timeout_s": read_timeout_s,
                "ssl_mode": ssl_mode,
                "ssl_ca": ssl_ca,
                "ssl_cert": ssl_cert,
                "ssl_key": ssl_key,
                "allow_public_key_retrieval": allow_public_key_retrieval,
            }
        )
        if not self._client_lib_loaded:
            if not load_client_library(self._lib):
                raise exception_for_rc(
                    MES_ERR_INVALID_ARG, "Client API not available (built without MySQL support)"
                )
            self._client_lib_loaded = True

        # Keep explicit references to encoded bytes so they are not
        # garbage-collected before the C call completes (matters on
        # non-CPython runtimes like PyPy).
        host_b = host.encode("utf-8")
        user_b = user.encode("utf-8")
        password_b = password.encode("utf-8")
        ssl_ca_b = ssl_ca.encode("utf-8") if ssl_ca else None
        ssl_cert_b = ssl_cert.encode("utf-8") if ssl_cert else None
        ssl_key_b = ssl_key.encode("utf-8") if ssl_key else None

        cfg = MESClientConfig()
        cfg.host = host_b
        cfg.port = port
        cfg.user = user_b
        cfg.password = password_b
        cfg.server_id = server_id
        cfg.connect_timeout_s = connect_timeout_s
        cfg.read_timeout_s = read_timeout_s
        cfg.ssl_mode = ssl_mode
        cfg.ssl_ca = ssl_ca_b
        cfg.ssl_cert = ssl_cert_b
        cfg.ssl_key = ssl_key_b
        cfg.allow_public_key_retrieval = int(allow_public_key_retrieval)

        rc = self._lib.mes_engine_set_metadata_conn(self._handle, ctypes.byref(cfg))
        if rc != MES_OK:
            _raise_for_rc(rc, "mes_engine_set_metadata_conn")


def _convert_columns(
    cols: ctypes.Array[MESColumn], count: int, name_cache: dict[bytes, str] | None = None
) -> dict[str, Any]:
    """Convert C mes_column_t array to a Python dict."""
    result: dict[str, Any] = {}
    window_at = _payload_window_at
    for i in range(count):
        col = cols[i]

        # Key: column name if available, otherwise string index
        # Defensive: C API says col_name is never NULL, but guard against
        # edge cases in MariaDB or future server implementations.
        raw_name = col.col_name or b""
        if raw_name and name_cache is not None:
            name = name_cache.get(raw_name)
            if name is None:
                if len(name_cache) >= 8192:
                    name_cache.clear()
                name = raw_name.decode("utf-8", errors="replace")
                name_cache[raw_name] = name
        else:
            name = raw_name.decode("utf-8", errors="replace") if raw_name else ""
        key = name if name else str(i)

        # Ordered by how often each type turns up in a row: temporal, decimal
        # and character columns all reach the binding as MES_COL_STRING, which
        # makes it the most frequent arm by a wide margin.
        col_type = col.type
        if col_type == MES_COL_STRING:
            data = col.str_data
            length = col.str_len
            if data and length > 0:
                raw = (
                    window_at(data)[:length]
                    if length <= _PAYLOAD_WINDOW_BYTES
                    else ctypes.string_at(data, length)
                )
                result[key] = raw.decode("utf-8", errors="surrogateescape")
            else:
                result[key] = ""
        elif col_type == MES_COL_INT:
            result[key] = col.int_val
        elif col_type == MES_COL_BYTES:
            # A column whose C-side type is MES_COL_BYTES is by definition
            # a bytes value. Zero-length (str_len == 0) is a legitimate
            # empty payload and maps to b"", not None -- truly-null values
            # arrive with type == MES_COL_NULL and fall through to the final
            # arm, so conflating empty bytes with null here would lose
            # information. Reading a payload requires a non-null pointer, so
            # fall back to the explicit empty-bytes literal when the pointer
            # is null (which should only happen in defensive tests; real C
            # output always passes a valid pointer, even for empty vectors).
            data = col.str_data
            length = col.str_len
            if data and length > 0:
                result[key] = (
                    window_at(data)[:length]
                    if length <= _PAYLOAD_WINDOW_BYTES
                    else ctypes.string_at(data, length)
                )
            else:
                result[key] = b""
        elif col_type == MES_COL_DOUBLE:
            result[key] = col.double_val
        else:
            # MES_COL_NULL, and any type this binding does not know about.
            result[key] = None

    return result


def _convert_event(
    raw: MESEvent,
    name_cache: dict[bytes, str] | None = None,
    source_sql_cache: dict[bytes, str] | None = None,
) -> ChangeEvent:
    """Convert C mes_event_t to Python ChangeEvent.

    An unknown C-ABI event type is a parse failure. It is not skipped because
    consumers must never advance a checkpoint past an unrepresentable change.
    """
    try:
        event_type = EventType(raw.type)
    except ValueError:
        raise exception_for_rc(MES_ERR_PARSE, f"Unknown event type: {raw.type}") from None

    before: dict[str, Any] | None = None
    # Both conditions are needed: count guards array iteration, pointer
    # null-check prevents dereference. Order is safe because Python `and`
    # short-circuits on False (if count is 0, pointer is not checked).
    if raw.before_count > 0 and raw.before_columns:
        before = _convert_columns(raw.before_columns, raw.before_count, name_cache)

    after: dict[str, Any] | None = None
    if raw.after_count > 0 and raw.after_columns:  # same guard pattern as above
        after = _convert_columns(raw.after_columns, raw.after_count, name_cache)

    # Identifiers and positions arrive as server-supplied bytes. A byte the
    # server's charset allows but UTF-8 does not must not cost the caller the
    # whole event, so every one of them substitutes instead of raising -- the
    # same substitution the Node binding performs.
    db = raw.database.decode("utf-8", errors="replace") if raw.database else ""
    table = raw.table.decode("utf-8", errors="replace") if raw.table else ""
    binlog_file = raw.binlog_file.decode("utf-8", errors="replace") if raw.binlog_file else ""

    # One ANNOTATE_ROWS statement annotates every row of the statement it
    # introduces, which the server may split across several ROWS events, and
    # each of those rows arrives as a separate C event carrying the same
    # statement, so decoding it per row repeats identical work. The
    # cache is keyed on the statement bytes rather than on the C pointer: the
    # engine reuses its storage across events, so pointer identity would serve
    # the previous statement's text for a new statement at the same address.
    raw_sql = raw.source_sql or b""
    if raw_sql and source_sql_cache is not None:
        source_sql = source_sql_cache.get(raw_sql)
        if source_sql is None:
            if len(source_sql_cache) >= _SOURCE_SQL_CACHE_MAX:
                source_sql_cache.clear()
            source_sql = raw_sql.decode("utf-8", errors="replace")
            source_sql_cache[raw_sql] = source_sql
    else:
        source_sql = raw_sql.decode("utf-8", errors="replace") if raw_sql else ""

    return ChangeEvent(
        type=event_type,
        database=db,
        table=table,
        before=before,
        after=after,
        timestamp=raw.timestamp,
        position=BinlogPosition(file=binlog_file, offset=raw.binlog_offset),
        names_resolved=bool(raw.names_resolved),
        source_sql=source_sql,
    )
