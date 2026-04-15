"""CdcEngine - High-level Python wrapper for the mysql-event-stream C engine."""

from __future__ import annotations

import ctypes
from typing import Any

from ._ffi import (
    MES_COL_BYTES,
    MES_COL_DOUBLE,
    MES_COL_INT,
    MES_COL_NULL,
    MES_COL_STRING,
    MES_ERR_CHECKSUM,
    MES_ERR_DECODE,
    MES_ERR_DECODE_COLUMN,
    MES_ERR_DECODE_ROW,
    MES_ERR_NO_EVENT,
    MES_ERR_PARSE,
    MES_OK,
    MESClientConfig,
    MESColumn,
    MESEvent,
    get_library,
    load_client_library,
)
from .types import BinlogPosition, ChangeEvent, ChecksumError, DecodeError, EventType, ParseError


def _raise_for_rc(rc: int, op: str) -> None:
    """Translate a C-ABI error code into the best-fitting Python exception.

    Keeps backward compatibility: every exception raised here is still a
    subclass of ``RuntimeError``, so code that catches ``RuntimeError``
    continues to work.  Callers that want to distinguish categories can
    catch ``ChecksumError`` / ``DecodeError`` / ``ParseError`` directly.
    """
    if rc == MES_ERR_CHECKSUM:
        raise ChecksumError(f"{op} failed: checksum mismatch (code {rc})")
    if rc in (MES_ERR_DECODE, MES_ERR_DECODE_COLUMN, MES_ERR_DECODE_ROW):
        raise DecodeError(f"{op} failed: row decode error (code {rc})")
    if rc == MES_ERR_PARSE:
        raise ParseError(f"{op} failed: binlog parse error (code {rc})")
    raise RuntimeError(f"{op} failed with error code {rc}")


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
        self._handle: int | None = self._lib.mes_create()
        if self._handle is None:
            raise RuntimeError("Failed to create CDC engine")

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
        """Raise RuntimeError if the engine has been closed."""
        if self._handle is None:
            raise RuntimeError("Engine has been closed")

    def feed(self, data: bytes | bytearray) -> int:
        """Feed raw binlog bytes into the engine.

        Args:
            data: Raw binlog byte stream.

        Returns:
            Number of bytes consumed by the engine.

        Raises:
            RuntimeError: If the engine is closed or feed fails.
        """
        self._check_open()
        if not data:
            return 0

        if isinstance(data, bytearray):
            buf = (ctypes.c_uint8 * len(data)).from_buffer(data)
        else:
            buf = (ctypes.c_uint8 * len(data)).from_buffer_copy(data)
        consumed = ctypes.c_size_t(0)
        rc = self._lib.mes_feed(self._handle, buf, len(data), ctypes.byref(consumed))
        if rc != MES_OK:
            _raise_for_rc(rc, "mes_feed")
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
            result = _convert_event(event_ptr.contents)
            if result is not None:
                return result
            # Unknown event type was skipped; drain next event from the C queue
            continue

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
            raise RuntimeError(f"mes_get_position failed with error code {rc}")

        file_str = file_ptr.value.decode("utf-8") if file_ptr.value else ""
        return BinlogPosition(file=file_str, offset=offset.value)

    def set_max_queue_size(self, max_size: int) -> None:
        """Set maximum event queue size for backpressure control.

        When the queue reaches this limit, feed() will stop consuming
        bytes early. Drain events via next_event() then re-feed.

        Args:
            max_size: Maximum queue size. 0 means unlimited (default).

        Raises:
            RuntimeError: If the engine is closed or the call fails.
        """
        self._check_open()
        rc = self._lib.mes_set_max_queue_size(self._handle, max_size)
        if rc != MES_OK:
            raise RuntimeError(f"mes_set_max_queue_size failed with error code {rc}")

    def reset(self) -> None:
        """Reset the engine, clearing all state.

        Raises:
            RuntimeError: If the engine is closed or the call fails.
        """
        self._check_open()
        rc = self._lib.mes_reset(self._handle)
        if rc != MES_OK:
            raise RuntimeError(f"mes_reset failed with error code {rc}")

    def _set_string_filter(
        self,
        func: Any,
        names: list[str],
        func_name: str,
    ) -> None:
        """Call a C string-array filter function.

        Args:
            func: The ctypes function to call.
            names: List of filter strings.
            func_name: Function name for error messages.

        Raises:
            RuntimeError: If the engine is closed or the call fails.
        """
        self._check_open()
        arr = (ctypes.c_char_p * len(names))(*(n.encode("utf-8") for n in names))
        rc = func(self._handle, arr, len(names))
        if rc != MES_OK:
            raise RuntimeError(f"{func_name} failed with error code {rc}")

    def set_include_databases(self, databases: list[str]) -> None:
        """Set database include filter.

        Only events from these databases are processed. An empty list
        clears the filter (all databases are allowed).

        Args:
            databases: List of database names.

        Raises:
            RuntimeError: If the engine is closed or the call fails.
        """
        self._set_string_filter(
            self._lib.mes_set_include_databases, databases, "mes_set_include_databases"
        )

    def set_include_tables(self, tables: list[str]) -> None:
        """Set table include filter.

        Only events from these tables are processed. An empty list
        clears the filter (all tables are allowed).

        Each entry is "database.table" or just "table" (matches any database).

        Args:
            tables: List of table names.

        Raises:
            RuntimeError: If the engine is closed or the call fails.
        """
        self._set_string_filter(
            self._lib.mes_set_include_tables, tables, "mes_set_include_tables"
        )

    def set_exclude_tables(self, tables: list[str]) -> None:
        """Set table exclude filter.

        Events from these tables are skipped.

        Each entry is "database.table" or just "table" (matches any database).

        Args:
            tables: List of table names.

        Raises:
            RuntimeError: If the engine is closed or the call fails.
        """
        self._set_string_filter(
            self._lib.mes_set_exclude_tables, tables, "mes_set_exclude_tables"
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
        ssl_mode: int = 0,
        ssl_ca: str = "",
        ssl_cert: str = "",
        ssl_key: str = "",
    ) -> None:
        """Enable metadata queries for column name resolution.

        Uses a separate MySQL connection to fetch column names via
        SHOW COLUMNS FROM. Requires the library to be built with
        MySQL client support.

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

        Raises:
            RuntimeError: If the engine is closed, client API is unavailable,
                or the metadata connection fails.
        """
        self._check_open()
        if not (1 <= port <= 65535):
            raise ValueError(f"port must be 1-65535, got {port}")
        if not self._client_lib_loaded:
            if not load_client_library(self._lib):
                raise RuntimeError("Client API not available (built without MySQL support)")
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

        rc = self._lib.mes_engine_set_metadata_conn(self._handle, ctypes.byref(cfg))
        if rc != MES_OK:
            raise RuntimeError(f"Failed to connect metadata (error code {rc})")


def _convert_columns(cols: ctypes.Array[MESColumn], count: int) -> dict[str, Any]:
    """Convert C mes_column_t array to a Python dict."""
    result: dict[str, Any] = {}
    for i in range(count):
        col = cols[i]

        # Key: column name if available, otherwise string index
        # Defensive: C API says col_name is never NULL, but guard against
        # edge cases in MariaDB or future server implementations.
        name = col.col_name.decode("utf-8") if col.col_name else ""
        key = name if name else str(i)

        col_type = col.type
        if col_type == MES_COL_NULL:
            result[key] = None
        elif col_type == MES_COL_INT:
            result[key] = col.int_val
        elif col_type == MES_COL_DOUBLE:
            result[key] = col.double_val
        elif col_type == MES_COL_STRING:
            if col.str_data and col.str_len > 0:
                result[key] = ctypes.string_at(col.str_data, col.str_len).decode(
                    "utf-8", errors="surrogateescape"
                )
            else:
                result[key] = ""
        elif col_type == MES_COL_BYTES:
            # A column whose C-side type is MES_COL_BYTES is by definition
            # a bytes value. Zero-length (str_len == 0) is a legitimate
            # empty payload and maps to b"", not None -- truly-null values
            # arrive with type == MES_COL_NULL and are handled above, so
            # conflating empty bytes with null here would lose information.
            # ctypes.string_at requires a non-null pointer, so fall back to
            # the explicit empty-bytes literal when the pointer is null
            # (which should only happen in defensive tests; real C output
            # always passes a valid pointer, even for empty vectors).
            if col.str_data and col.str_len > 0:
                result[key] = ctypes.string_at(col.str_data, col.str_len)
            else:
                result[key] = b""
        else:
            result[key] = None

    return result


def _convert_event(raw: MESEvent) -> ChangeEvent | None:
    """Convert C mes_event_t to Python ChangeEvent.

    Returns None only when the event type is unrecognized (future MySQL type).
    Callers must distinguish this from "queue empty" via the C-layer return code.
    """
    try:
        event_type = EventType(raw.type)
    except ValueError:
        import warnings

        warnings.warn(
            f"Unknown event type {raw.type}; skipping event",
            stacklevel=2,
        )
        return None

    before: dict[str, Any] | None = None
    # Both conditions are needed: count guards array iteration, pointer
    # null-check prevents dereference. Order is safe because Python `and`
    # short-circuits on False (if count is 0, pointer is not checked).
    if raw.before_count > 0 and raw.before_columns:
        before = _convert_columns(raw.before_columns, raw.before_count)

    after: dict[str, Any] | None = None
    if raw.after_count > 0 and raw.after_columns:  # same guard pattern as above
        after = _convert_columns(raw.after_columns, raw.after_count)

    db = raw.database.decode("utf-8") if raw.database else ""
    table = raw.table.decode("utf-8") if raw.table else ""
    binlog_file = raw.binlog_file.decode("utf-8") if raw.binlog_file else ""

    return ChangeEvent(
        type=event_type,
        database=db,
        table=table,
        before=before,
        after=after,
        timestamp=raw.timestamp,
        position=BinlogPosition(file=binlog_file, offset=raw.binlog_offset),
    )
