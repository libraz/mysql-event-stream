"""Public type definitions for mysql-event-stream."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any


class EventType(Enum):
    """CDC change event types."""

    INSERT = 0
    UPDATE = 1
    DELETE = 2


class ColumnType(Enum):
    """Column value types."""

    NULL = "null"
    INT = "int"
    DOUBLE = "double"
    STRING = "string"
    BYTES = "bytes"


@dataclass(frozen=True, slots=True)
class ColumnValue:
    """A column value in a change event."""

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

    Values are typed as: None, int, float, str, or bytes.
    """

    type: EventType
    database: str
    table: str
    before: dict[str, Any] | None
    after: dict[str, Any] | None
    timestamp: int
    position: BinlogPosition


@dataclass(frozen=True, slots=True)
class ClientConfig:
    """Configuration for BinlogClient."""

    host: str = "127.0.0.1"
    port: int = 3306
    user: str = "root"
    password: str = ""
    server_id: int = 1
    start_gtid: str = ""
    connect_timeout_s: int = 10
    read_timeout_s: int = 30


@dataclass(frozen=True, slots=True)
class PollResult:
    """Result of a BinlogClient.poll() call."""

    data: bytes | None
    is_heartbeat: bool
