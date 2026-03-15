// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * Test utilities for building synthetic binlog events.
 * Mirrors the C++ EventBuilder from core/tests/test_helpers.h.
 */

/** Build a complete binlog event with header, body, and checksum. */
export function buildEvent(typeCode: number, timestamp: number, body: Uint8Array): Uint8Array {
  const eventLength = 19 + body.length + 4; // header + body + checksum
  const buf = new Uint8Array(eventLength);
  const view = new DataView(buf.buffer);

  // Header (19 bytes, little-endian)
  view.setUint32(0, timestamp, true); // timestamp
  buf[4] = typeCode; // type_code
  view.setUint32(5, 1, true); // server_id = 1
  view.setUint32(9, eventLength, true); // event_length
  view.setUint32(13, 0, true); // next_position
  view.setUint16(17, 0, true); // flags

  // Body
  buf.set(body, 19);

  // Checksum (4 zero bytes, already zero from Uint8Array initialization)

  return buf;
}

/** Build a TABLE_MAP_EVENT body with a single INT column. */
export function buildTableMapBody(tableId: number, db: string, tableName: string): Uint8Array {
  const parts: number[] = [];

  // table_id (6 bytes LE)
  parts.push(
    tableId & 0xff,
    (tableId >> 8) & 0xff,
    (tableId >> 16) & 0xff,
    (tableId >> 24) & 0xff,
    0,
    0,
  );
  // flags (2 bytes)
  parts.push(0, 0);
  // db_name_length + db_name + null terminator
  parts.push(db.length);
  for (const ch of db) parts.push(ch.charCodeAt(0));
  parts.push(0);
  // table_name_length + table_name + null terminator
  parts.push(tableName.length);
  for (const ch of tableName) parts.push(ch.charCodeAt(0));
  parts.push(0);
  // column_count (packed int = 1)
  parts.push(1);
  // column_types: LONG (0x03)
  parts.push(0x03);
  // metadata_length (packed int = 0, no metadata for INT)
  parts.push(0);
  // null_bitmap: 1 byte, bit 0 = 1 (nullable)
  parts.push(0x01);

  return new Uint8Array(parts);
}

/** Build a WRITE_ROWS_EVENT V2 body with a single INT value. */
export function buildWriteRowsBody(tableId: number, value: number): Uint8Array {
  const parts: number[] = [];

  // table_id (6 bytes LE)
  parts.push(
    tableId & 0xff,
    (tableId >> 8) & 0xff,
    (tableId >> 16) & 0xff,
    (tableId >> 24) & 0xff,
    0,
    0,
  );
  // flags (2 bytes)
  parts.push(0, 0);
  // var_header_len (2 bytes, V2) = 2
  parts.push(2, 0);
  // column_count (packed int = 1)
  parts.push(1);
  // columns_present bitmap (1 byte, bit 0 set)
  parts.push(0x01);
  // Row data:
  // null_bitmap (1 byte, no nulls)
  parts.push(0x00);
  // INT value (4 bytes LE)
  parts.push(value & 0xff, (value >> 8) & 0xff, (value >> 16) & 0xff, (value >> 24) & 0xff);

  return new Uint8Array(parts);
}

/** Build an UPDATE_ROWS_EVENT V2 body with before and after INT values. */
export function buildUpdateRowsBody(
  tableId: number,
  beforeVal: number,
  afterVal: number,
): Uint8Array {
  const parts: number[] = [];

  // table_id (6 bytes LE)
  parts.push(
    tableId & 0xff,
    (tableId >> 8) & 0xff,
    (tableId >> 16) & 0xff,
    (tableId >> 24) & 0xff,
    0,
    0,
  );
  // flags
  parts.push(0, 0);
  // var_header_len = 2
  parts.push(2, 0);
  // column_count = 1
  parts.push(1);
  // columns_present bitmap (before)
  parts.push(0x01);
  // columns_present bitmap (after) - UPDATE has two bitmaps
  parts.push(0x01);
  // Before row: null_bitmap + value
  parts.push(0x00);
  parts.push(
    beforeVal & 0xff,
    (beforeVal >> 8) & 0xff,
    (beforeVal >> 16) & 0xff,
    (beforeVal >> 24) & 0xff,
  );
  // After row: null_bitmap + value
  parts.push(0x00);
  parts.push(
    afterVal & 0xff,
    (afterVal >> 8) & 0xff,
    (afterVal >> 16) & 0xff,
    (afterVal >> 24) & 0xff,
  );

  return new Uint8Array(parts);
}

/** Build a DELETE_ROWS_EVENT V2 body with a single INT value. */
export function buildDeleteRowsBody(tableId: number, value: number): Uint8Array {
  const parts: number[] = [];

  // table_id (6 bytes LE)
  parts.push(
    tableId & 0xff,
    (tableId >> 8) & 0xff,
    (tableId >> 16) & 0xff,
    (tableId >> 24) & 0xff,
    0,
    0,
  );
  // flags
  parts.push(0, 0);
  // var_header_len = 2
  parts.push(2, 0);
  // column_count = 1
  parts.push(1);
  // columns_present bitmap
  parts.push(0x01);
  // null_bitmap (no nulls)
  parts.push(0x00);
  // INT value (4 bytes LE)
  parts.push(value & 0xff, (value >> 8) & 0xff, (value >> 16) & 0xff, (value >> 24) & 0xff);

  return new Uint8Array(parts);
}

/** Build a ROTATE_EVENT body. */
export function buildRotateBody(position: number, filename: string): Uint8Array {
  const parts: number[] = [];
  // position (8 bytes LE)
  parts.push(
    position & 0xff,
    (position >> 8) & 0xff,
    (position >> 16) & 0xff,
    (position >> 24) & 0xff,
  );
  parts.push(0, 0, 0, 0); // high 4 bytes = 0
  // filename
  for (const ch of filename) parts.push(ch.charCodeAt(0));
  return new Uint8Array(parts);
}

/** Concatenate multiple Uint8Arrays. */
export function concat(...arrays: Uint8Array[]): Uint8Array {
  const total = arrays.reduce((sum, a) => sum + a.length, 0);
  const result = new Uint8Array(total);
  let offset = 0;
  for (const arr of arrays) {
    result.set(arr, offset);
    offset += arr.length;
  }
  return result;
}
