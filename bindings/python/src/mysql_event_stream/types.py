"""Public type definitions for mysql-event-stream."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum, IntEnum
from typing import Any


class EventType(Enum):
    """CDC change event types."""

    INSERT = 0
    UPDATE = 1
    DELETE = 2


class SslMode(IntEnum):
    """SSL connection mode."""

    DISABLED = 0
    PREFERRED = 1
    REQUIRED = 2
    VERIFY_CA = 3
    VERIFY_IDENTITY = 4


class ServerFlavor(IntEnum):
    """Database server flavor reported after connection."""

    MYSQL = 0
    MARIADB = 1


class MesErrorCode(IntEnum):
    """Numeric error codes returned by the native C ABI."""

    OK = 0
    NULL_ARG = 1
    INVALID_ARG = 2
    INTERNAL = 99
    PARSE = 100
    CHECKSUM = 101
    DECODE = 200
    DECODE_COLUMN = 201
    DECODE_ROW = 202
    NO_EVENT = 300
    QUEUE_FULL = 301
    CONNECT = 400
    AUTH = 401
    VALIDATION = 402
    STREAM = 403
    DISCONNECTED = 404
    GTID_PURGED = 405
    GTID_TAGGED_UNSUPPORTED = 406


class ColumnType(Enum):
    """Column value types.

    .. deprecated::
        Legacy helper retained for backward compatibility. CDC events
        expose column values as plain Python values in a dict (see
        :class:`ChangeEvent`); this enum is never used to describe them.
        It exists only for callers that still construct :class:`ColumnValue`
        instances manually and has no counterpart in the Node binding.
    """

    NULL = "null"
    INT = "int"
    DOUBLE = "double"
    STRING = "string"
    BYTES = "bytes"


@dataclass(frozen=True, slots=True)
class ColumnValue:
    """A column value in a change event.

    .. deprecated::
        Legacy helper retained for backward compatibility. CDC events do not
        return :class:`ColumnValue` objects; :class:`ChangeEvent` exposes
        columns as a plain dict of Python values. This class is provided only
        for callers that construct column values manually and has no
        counterpart in the Node binding.
    """

    type: ColumnType
    value: None | int | float | str | bytes
    name: str = ""

    @staticmethod
    def null() -> ColumnValue:
        """Create a NULL column value."""
        return ColumnValue(type=ColumnType.NULL, value=None)

    @staticmethod
    def int_val(v: int) -> ColumnValue:
        """Create an integer column value."""
        return ColumnValue(type=ColumnType.INT, value=v)

    @staticmethod
    def double_val(v: float) -> ColumnValue:
        """Create a double column value."""
        return ColumnValue(type=ColumnType.DOUBLE, value=v)

    @staticmethod
    def string_val(v: str) -> ColumnValue:
        """Create a string column value."""
        return ColumnValue(type=ColumnType.STRING, value=v)

    @staticmethod
    def bytes_val(v: bytes) -> ColumnValue:
        """Create a bytes column value."""
        return ColumnValue(type=ColumnType.BYTES, value=v)


@dataclass(frozen=True, slots=True)
class BinlogPosition:
    """Position in binlog stream."""

    file: str = ""
    offset: int = 0


@dataclass(frozen=True, slots=True)
class ChangeEvent:
    """A CDC change event.

    Column values are represented as plain dicts keyed by column name.
    When column names are unavailable (standalone mode without metadata),
    string indices ("0", "1", ...) are used as keys.

    Values are typed as: None, int, float, str, or bytes. TINYINT through
    BIGINT within signed 64-bit range, YEAR, TIMESTAMP, ENUM, SET, and BIT
    are ``int``; FLOAT and DOUBLE are ``float``. DECIMAL and temporal values
    are ``str``. BIGINT UNSIGNED, SET, and BIT values above signed 64-bit
    range are exact decimal ``str`` values rather than overflowing integers.

    Special MySQL column types surface as follows:

    - JSON columns arrive as raw ``bytes`` holding MySQL's binary JSON
      representation (not decoded text). Use a MySQL binary-JSON parser to
      obtain a structured value.
    - ENUM columns arrive as the 1-based numeric index (``int``) into the
      column's value list, not the string label.
    - SET columns arrive as a numeric bitmask (``int``); bit i (LSB first)
      is set when the i-th member of the SET definition is present. Values
      above ``INT64_MAX`` arrive as exact decimal ``str`` values.
    - BIT columns arrive as an integer (``int``) holding the bit value; values
      above ``INT64_MAX`` arrive as exact decimal ``str`` values.
    - Character columns (including TEXT) arrive as ``str``; binary columns
      (including BINARY/VARBINARY/BLOB) arrive as ``bytes``. This distinction
      uses TABLE_MAP charset metadata. With ``binlog_row_metadata=NO_LOG``,
      BLOB-family columns conservatively remain ``bytes``.
      Invalid UTF-8 bytes use Python's ``surrogateescape`` handler, so a later
      ``value.encode("utf-8", errors="surrogateescape")`` round-trips the
      original bytes. Such strings are not directly JSON-serializable.

    ``names_resolved`` is False when any column name could not be resolved for
    this event's table (for example, no metadata side-connection is configured
    or it failed). In that case column keys fall back to string indices
    ("0", "1", ...).
    """

    type: EventType
    database: str
    table: str
    before: dict[str, Any] | None
    after: dict[str, Any] | None
    timestamp: int
    position: BinlogPosition
    names_resolved: bool = True
    source_sql: str = ""


@dataclass(frozen=True, slots=True)
class ClientConfig:
    """Configuration for BinlogClient.

    Attributes:
        host: MySQL host.
        port: MySQL port.
        user: MySQL user.
        password: MySQL password.
        server_id: Unique replica server ID.
        start_gtid: GTID set to start from. ``None`` snapshots the current
            server position; ``""`` explicitly starts from the empty GTID set.
        start_binlog_file: Binlog file for an exact file/offset start. Requires
            start_binlog_position and cannot be combined with start_gtid.
        start_binlog_position: Binlog offset for an exact file/offset start;
            must be at least 4.
        connect_timeout_s: Connection timeout in seconds.
        read_timeout_s: Read timeout in seconds.
        ssl_mode: SSL mode. Use ``SslMode`` enum values (0=disabled,
            1=preferred, 2=required, 3=verify_ca, 4=verify_identity).
        ssl_ca: Path to CA certificate file (empty uses the OS trust store in
            certificate-verification modes).
        ssl_cert: Path to client certificate file (empty to skip).
        ssl_key: Path to client private key file (empty to skip).
        max_queue_size: Maximum internal event queue size. 0 selects the
            default of 10000.
        max_queue_bytes: Total queued payload byte budget. Defaults to 48 MiB;
            0 restores that default.
        max_event_size: Maximum binlog event size accepted by the client and
            parser. Defaults to 32 MiB; 0 resolves to the 1 GiB hard cap.
            Raise max_queue_bytes when raising this limit.
        allow_public_key_retrieval: Permit unauthenticated RSA key retrieval
            without TLS. MITM-sensitive; prefer verified TLS.
    """

    host: str = "127.0.0.1"
    port: int = 3306
    user: str = "root"
    password: str = field(default="", repr=False)
    server_id: int = 1
    start_gtid: str | None = None
    start_binlog_file: str | None = None
    start_binlog_position: int = 0
    connect_timeout_s: int = 10
    read_timeout_s: int = 30
    ssl_mode: int = 1
    ssl_ca: str = ""
    ssl_cert: str = ""
    ssl_key: str = ""
    max_queue_size: int = 0
    max_queue_bytes: int = 48 * 1024 * 1024
    max_event_size: int = 32 * 1024 * 1024
    allow_public_key_retrieval: bool = False


@dataclass(frozen=True, slots=True)
class PollResult:
    """Result of a BinlogClient.poll() call."""

    data: bytes | None
    is_heartbeat: bool


# --- Typed exceptions -----------------------------------------------------
#
# These subclass RuntimeError to preserve backward compatibility: code
# that catches ``RuntimeError`` continues to work, while callers who want
# to distinguish checksum failures from decode errors from parse errors
# can now do so without parsing error-message strings.
#
# ConnectionError is intentionally not re-exported here; it is a Python
# built-in (subclass of OSError) and is raised by ``BinlogClient.connect``
# to match OS/socket conventions.


class ParseError(RuntimeError):
    """Raised when a binlog event fails to parse (e.g. truncated header)."""


class DecodeError(RuntimeError):
    """Raised when row or column decoding fails for a well-formed event."""


class ChecksumError(RuntimeError):
    """Raised when a CRC32 mismatch is detected on a binlog event."""


def exception_for_rc(rc: int, message: str) -> RuntimeError:
    """Map a C-ABI error code to the matching typed exception instance.

    Shared by :class:`CdcEngine` and :class:`BinlogClient` so that the same
    error code raises the same exception type regardless of which surface
    produced it. All returned exceptions subclass ``RuntimeError`` for
    backward compatibility.

    Args:
        rc: A ``MES_ERR_*`` error code.
        message: Human-readable message for the exception.

    Returns:
        A ``ChecksumError`` / ``DecodeError`` / ``ParseError`` for those
        categories, otherwise a plain ``RuntimeError``.
    """
    # Imported lazily to keep this module free of any native-library coupling
    # at import time.
    from ._ffi import (
        MES_ERR_CHECKSUM,
        MES_ERR_DECODE,
        MES_ERR_DECODE_COLUMN,
        MES_ERR_DECODE_ROW,
        MES_ERR_PARSE,
    )

    if rc == MES_ERR_CHECKSUM:
        error: RuntimeError = ChecksumError(message)
    elif rc in (MES_ERR_DECODE, MES_ERR_DECODE_COLUMN, MES_ERR_DECODE_ROW):
        error = DecodeError(message)
    elif rc == MES_ERR_PARSE:
        error = ParseError(message)
    else:
        error = RuntimeError(message)
    # Keep the C ABI category on every error path (including poll/feed), so
    # high-level retry policy never has to infer permanence from text.
    error.code = rc  # type: ignore[attr-defined]
    return error
