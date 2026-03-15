"""Test helpers for building binlog event binary data.

Ports the C++ EventBuilder and BuildXxx helpers from core/tests/test_helpers.h
to Python using struct.pack for binary construction.
"""

import struct


def build_event(type_code: int, timestamp: int, body: bytes) -> bytes:
    """Build a complete binlog event with header, body, and checksum.

    Args:
        type_code: Binlog event type code (e.g. 19 for TABLE_MAP).
        timestamp: Event timestamp.
        body: Event body bytes.

    Returns:
        Complete binary event including 19-byte header, body, and 4-byte checksum.
    """
    event_length = 19 + len(body) + 4  # header + body + checksum
    header = struct.pack(
        "<IBIIIH",
        timestamp,  # 4 bytes LE
        type_code,  # 1 byte
        1,  # server_id, 4 bytes LE
        event_length,  # event_length, 4 bytes LE
        0,  # next_position, 4 bytes LE
        0,  # flags, 2 bytes LE
    )
    checksum = b"\x00\x00\x00\x00"
    return header + body + checksum


def _write_u48_le(value: int) -> bytes:
    """Write a 48-bit little-endian value (6 bytes)."""
    result = bytearray(6)
    for i in range(6):
        result[i] = (value >> (i * 8)) & 0xFF
    return bytes(result)


def build_table_map_body(table_id: int, db: str, table_name: str) -> bytes:
    """Build a TABLE_MAP_EVENT body with a single INT column.

    Args:
        table_id: Table ID (48-bit).
        db: Database name.
        table_name: Table name.

    Returns:
        Binary event body.
    """
    parts = bytearray()
    # table_id (6 bytes LE)
    parts.extend(_write_u48_le(table_id))
    # flags (2 bytes)
    parts.extend(b"\x00\x00")
    # db_name_length + db_name + null terminator
    parts.append(len(db))
    parts.extend(db.encode("ascii"))
    parts.append(0)
    # table_name_length + table_name + null terminator
    parts.append(len(table_name))
    parts.extend(table_name.encode("ascii"))
    parts.append(0)
    # column_count (packed int = 1)
    parts.append(1)
    # column type: INT (0x03 = MYSQL_TYPE_LONG)
    parts.append(0x03)
    # metadata_length = 0
    parts.append(0)
    # null_bitmap: 0x01 (column is nullable)
    parts.append(0x01)
    return bytes(parts)


def build_write_rows_body(table_id: int, value: int) -> bytes:
    """Build a WRITE_ROWS_EVENT V2 body with a single INT value.

    Args:
        table_id: Table ID (must match a preceding TABLE_MAP).
        value: The INT column value to insert.

    Returns:
        Binary event body.
    """
    parts = bytearray()
    parts.extend(_write_u48_le(table_id))
    # flags (2 bytes)
    parts.extend(b"\x00\x00")
    # var_header_len = 2 (V2)
    parts.extend(struct.pack("<H", 2))
    # column_count = 1
    parts.append(1)
    # columns_present bitmap
    parts.append(0x01)
    # null_bitmap (no nulls)
    parts.append(0x00)
    # INT value (4 bytes LE, signed)
    parts.extend(struct.pack("<i", value))
    return bytes(parts)


def build_update_rows_body(table_id: int, before_val: int, after_val: int) -> bytes:
    """Build an UPDATE_ROWS_EVENT V2 body with a single INT column.

    Args:
        table_id: Table ID.
        before_val: Value before update.
        after_val: Value after update.

    Returns:
        Binary event body.
    """
    parts = bytearray()
    parts.extend(_write_u48_le(table_id))
    # flags
    parts.extend(b"\x00\x00")
    # var_header_len = 2
    parts.extend(struct.pack("<H", 2))
    # column_count = 1
    parts.append(1)
    # columns_present bitmap (before)
    parts.append(0x01)
    # columns_present bitmap (after)
    parts.append(0x01)
    # Before row
    parts.append(0x00)  # null_bitmap
    parts.extend(struct.pack("<i", before_val))
    # After row
    parts.append(0x00)  # null_bitmap
    parts.extend(struct.pack("<i", after_val))
    return bytes(parts)


def build_delete_rows_body(table_id: int, value: int) -> bytes:
    """Build a DELETE_ROWS_EVENT V2 body with a single INT value.

    Args:
        table_id: Table ID.
        value: The INT column value being deleted.

    Returns:
        Binary event body.
    """
    parts = bytearray()
    parts.extend(_write_u48_le(table_id))
    # flags
    parts.extend(b"\x00\x00")
    # var_header_len = 2
    parts.extend(struct.pack("<H", 2))
    # column_count = 1
    parts.append(1)
    # columns_present bitmap
    parts.append(0x01)
    # null_bitmap (no nulls)
    parts.append(0x00)
    # INT value (4 bytes LE, signed)
    parts.extend(struct.pack("<i", value))
    return bytes(parts)


def build_rotate_body(position: int, filename: str) -> bytes:
    """Build a ROTATE_EVENT body.

    Args:
        position: Binlog position.
        filename: New binlog filename.

    Returns:
        Binary event body.
    """
    return struct.pack("<Q", position) + filename.encode("ascii")
