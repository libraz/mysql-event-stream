// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * Synthetic binlog fixtures for an arbitrary column list.
 *
 * `helpers.ts` builds single-INT-column events, which is enough to exercise the
 * state machine but not the column conversion: reaching a chosen
 * `mes_column_t` shape needs a TABLE_MAP whose column types, type metadata,
 * signedness, collations and names are all under the caller's control, plus a
 * ROWS event carrying matching payload bytes. This module builds that pair on
 * top of the same event framing `helpers.ts` provides, so the addon's
 * conversion runs over real engine output rather than a hand-made struct.
 */

import { buildEvent, concat } from "./helpers.js";

/** Binlog event type codes used by the column fixtures. */
export const TABLE_MAP_EVENT = 19;
export const WRITE_ROWS_EVENT = 30;
export const ROTATE_EVENT = 4;

/** Binlog column type bytes (MySQL's MYSQL_TYPE_* values). */
export const ColType = {
  Tiny: 0x01,
  Short: 0x02,
  Long: 0x03,
  Float: 0x04,
  Double: 0x05,
  LongLong: 0x08,
  Int24: 0x09,
  Date: 0x0a,
  Year: 0x0d,
  Varchar: 0x0f,
  Bit: 0x10,
  Timestamp2: 0x11,
  Datetime2: 0x12,
  Time2: 0x13,
  Vector: 0xf2,
  Json: 0xf5,
  NewDecimal: 0xf6,
  Enum: 0xf7,
  Set: 0xf8,
  Blob: 0xfc,
  String: 0xfe,
  Geometry: 0xff,
} as const;

/** Collation ids the fixtures pick between, plus "absent" as `undefined`. */
export const Collation = {
  /** The binary collation; a character-family column carrying it is bytes. */
  Binary: 63,
  /** utf8mb4_0900_ai_ci: a text collation. */
  Utf8mb4: 255,
} as const;

/** One column of a fixture table, with the row payload it carries. */
export interface ColumnFixture {
  /** Binlog column type byte; see {@link ColType}. */
  type: number;
  /** Type metadata bytes in wire order, as the TABLE_MAP metadata block holds them. */
  meta?: readonly number[];
  /** Row payload bytes, or null for SQL NULL. */
  value: Uint8Array | null;
  /**
   * COLUMN_NAME entry. An empty name (or one column naming while another does
   * not) is legal and makes that column's key fall back to its index.
   */
  name?: string | Uint8Array;
  /** SIGNEDNESS bit, for the numeric types that carry one. */
  unsigned?: boolean;
  /**
   * Collation id. Leaving it off every character-family column suppresses the
   * charset metadata entirely, which is what `binlog_row_metadata=NO_LOG`
   * looks like on the wire.
   */
  collation?: number;
}

/** Optional metadata field types, matching MySQL's Optional_metadata_field_type. */
const FIELD_SIGNEDNESS = 1;
const FIELD_COLUMN_CHARSET = 3;
const FIELD_COLUMN_NAME = 4;

const encoder = new TextEncoder();

function bitmapBytes(count: number): number {
  return Math.ceil(count / 8);
}

/**
 * Pack `count` bits into bitmap bytes. MySQL's row bitmaps fill each byte from
 * the least significant bit; the SIGNEDNESS bitmap fills it from the most
 * significant one, hence `msbFirst`.
 */
function packBitmap(count: number, isSet: (index: number) => boolean, msbFirst = false): number[] {
  const out: number[] = [];
  for (let byte = 0; byte < bitmapBytes(count); byte++) {
    let value = 0;
    for (let bit = 0; bit < 8; bit++) {
      const index = byte * 8 + bit;
      if (index < count && isSet(index)) value |= msbFirst ? 0x80 >> bit : 1 << bit;
    }
    out.push(value);
  }
  return out;
}

/** Encode a MySQL length-encoded integer. */
function packedInt(value: number): number[] {
  if (value < 251) return [value];
  if (value <= 0xffff) return [0xfc, value & 0xff, (value >> 8) & 0xff];
  if (value <= 0xffffff) {
    return [0xfd, value & 0xff, (value >> 8) & 0xff, (value >> 16) & 0xff];
  }
  throw new RangeError(`packed integer beyond fixture range: ${value}`);
}

function nameBytes(name: string | Uint8Array | undefined): Uint8Array {
  if (name === undefined) return new Uint8Array(0);
  return typeof name === "string" ? encoder.encode(name) : name;
}

/**
 * Whether the column receives a SIGNEDNESS bit, mirroring MySQL's
 * `Field::has_signedness_information_type()`. The bits are packed positionally
 * over exactly these columns, so miscounting shifts every following bit.
 */
function isNumeric(column: ColumnFixture): boolean {
  switch (column.type) {
    case 0x00: // MYSQL_TYPE_DECIMAL (legacy)
    case ColType.Tiny:
    case ColType.Short:
    case ColType.Int24:
    case ColType.Long:
    case ColType.LongLong:
    case ColType.NewDecimal:
    case ColType.Float:
    case ColType.Double:
    case ColType.Year:
      return true;
    default:
      return false;
  }
}

/**
 * Whether the column occupies a slot in the COLUMN_CHARSET index space,
 * mirroring MySQL's `is_character_type()`. ENUM and SET travel as
 * MYSQL_TYPE_STRING but keep their collations elsewhere, so they take no slot;
 * VECTOR takes one.
 */
function isCharacter(column: ColumnFixture): boolean {
  switch (column.type) {
    case ColType.String: {
      const realType = column.meta?.[0] ?? 0;
      return realType !== ColType.Enum && realType !== ColType.Set;
    }
    case ColType.Varchar:
    case ColType.Vector:
    case ColType.Blob:
      return true;
    default:
      return false;
  }
}

function optionalMetadata(columns: readonly ColumnFixture[]): number[] {
  const out: number[] = [];

  const numeric = columns.filter(isNumeric);
  if (numeric.length > 0) {
    const bitmap = packBitmap(numeric.length, (index) => numeric[index]?.unsigned === true, true);
    out.push(FIELD_SIGNEDNESS, ...packedInt(bitmap.length), ...bitmap);
  }

  const character = columns.filter(isCharacter);
  if (character.some((column) => column.collation !== undefined)) {
    const value: number[] = [];
    for (const column of character) {
      if (column.collation === undefined) {
        throw new Error("COLUMN_CHARSET covers every character column or none of them");
      }
      value.push(...packedInt(column.collation));
    }
    out.push(FIELD_COLUMN_CHARSET, ...packedInt(value.length), ...value);
  }

  if (columns.some((column) => column.name !== undefined)) {
    // COLUMN_NAME holds one entry per column, so a column with no name of its
    // own contributes a zero-length entry rather than being skipped.
    const value: number[] = [];
    for (const column of columns) {
      const bytes = nameBytes(column.name);
      value.push(...packedInt(bytes.length), ...bytes);
    }
    out.push(FIELD_COLUMN_NAME, ...packedInt(value.length), ...value);
  }

  return out;
}

function tableId48(tableId: number): number[] {
  return [
    tableId & 0xff,
    (tableId >> 8) & 0xff,
    (tableId >> 16) & 0xff,
    (tableId >> 24) & 0xff,
    0,
    0,
  ];
}

/** Build a TABLE_MAP_EVENT body describing `columns`. */
export function buildColumnTableMapBody(
  tableId: number,
  db: string,
  table: string,
  columns: readonly ColumnFixture[],
): Uint8Array {
  if (columns.length === 0) throw new Error("a TABLE_MAP describes at least one column");

  const parts: number[] = [...tableId48(tableId)];
  parts.push(0, 0); // flags
  parts.push(db.length, ...encoder.encode(db), 0);
  parts.push(table.length, ...encoder.encode(table), 0);
  parts.push(...packedInt(columns.length));
  for (const column of columns) parts.push(column.type);

  const meta: number[] = [];
  for (const column of columns) meta.push(...(column.meta ?? []));
  parts.push(...packedInt(meta.length), ...meta);

  // null bitmap: every fixture column is nullable.
  parts.push(...new Array<number>(bitmapBytes(columns.length)).fill(0xff));
  parts.push(...optionalMetadata(columns));

  return new Uint8Array(parts);
}

/** Build a WRITE_ROWS_EVENT V2 body carrying one row of `columns`. */
export function buildColumnRowsBody(
  tableId: number,
  columns: readonly ColumnFixture[],
): Uint8Array {
  const parts: number[] = [...tableId48(tableId)];
  parts.push(0, 0); // flags
  parts.push(2, 0); // var_header_len (V2)
  parts.push(...packedInt(columns.length));

  // Every column is present: the decoder rejects partial row images.
  parts.push(...packBitmap(columns.length, () => true));
  parts.push(...packBitmap(columns.length, (index) => columns[index]?.value === null));

  for (const column of columns) {
    if (column.value !== null) parts.push(...column.value);
  }

  return new Uint8Array(parts);
}

/** Build the TABLE_MAP + WRITE_ROWS pair that yields one INSERT event. */
export function buildColumnEvents(
  tableId: number,
  db: string,
  table: string,
  columns: readonly ColumnFixture[],
  timestamp = 1000,
): Uint8Array {
  return concat(
    buildEvent(TABLE_MAP_EVENT, timestamp, buildColumnTableMapBody(tableId, db, table, columns)),
    buildEvent(WRITE_ROWS_EVENT, timestamp, buildColumnRowsBody(tableId, columns)),
  );
}

/* ---- Row payload encoders ---- */

/** Little-endian two's-complement integer of `width` bytes. */
export function intLe(value: bigint | number, width: number): Uint8Array {
  let raw = BigInt(value) & ((1n << BigInt(width * 8)) - 1n);
  const out = new Uint8Array(width);
  for (let i = 0; i < width; i++) {
    out[i] = Number(raw & 0xffn);
    raw >>= 8n;
  }
  return out;
}

/** Big-endian unsigned integer of `width` bytes. */
export function uintBe(value: bigint | number, width: number): Uint8Array {
  let raw = BigInt(value);
  const out = new Uint8Array(width);
  for (let i = width - 1; i >= 0; i--) {
    out[i] = Number(raw & 0xffn);
    raw >>= 8n;
  }
  return out;
}

/** IEEE-754 single precision, little-endian, as MySQL stores FLOAT. */
export function float32(value: number): Uint8Array {
  const out = new Uint8Array(4);
  new DataView(out.buffer).setFloat32(0, value, true);
  return out;
}

/** IEEE-754 double precision, little-endian, as MySQL stores DOUBLE. */
export function float64(value: number): Uint8Array {
  const out = new Uint8Array(8);
  new DataView(out.buffer).setFloat64(0, value, true);
  return out;
}

/** A payload behind a little-endian length prefix of `prefixWidth` bytes. */
export function lengthPrefixed(payload: Uint8Array, prefixWidth: number): Uint8Array {
  const out = new Uint8Array(prefixWidth + payload.length);
  out.set(intLe(payload.length, prefixWidth), 0);
  out.set(payload, prefixWidth);
  return out;
}

/** DATE: a 3-byte little-endian (year << 9) | (month << 5) | day. */
export function dateValue(year: number, month: number, day: number): Uint8Array {
  return intLe((year << 9) | (month << 5) | day, 3);
}

/** TIME2: the packed value offset by 0x800000, then the fraction at `fsp`. */
export function time2Value(
  hour: number,
  minute: number,
  second: number,
  micros = 0,
  fsp = 0,
): Uint8Array {
  const hms = BigInt((hour << 12) | (minute << 6) | second);
  return concat(uintBe(hms + 0x800000n, 3), fractionalValue(micros, fsp));
}

/** DATETIME2: the packed value offset by 0x8000000000, then the fraction. */
export function datetime2Value(
  year: number,
  month: number,
  day: number,
  hour: number,
  minute: number,
  second: number,
  micros = 0,
  fsp = 0,
): Uint8Array {
  const ymd = (BigInt(year * 13 + month) << 5n) | BigInt(day);
  const hms = BigInt((hour << 12) | (minute << 6) | second);
  const intpart = (ymd << 17n) | hms;
  return concat(uintBe(intpart + 0x8000000000n, 5), fractionalValue(micros, fsp));
}

/** TIMESTAMP2: big-endian epoch seconds, then the fraction at `fsp`. */
export function timestamp2Value(seconds: number, micros = 0, fsp = 0): Uint8Array {
  return concat(uintBe(seconds, 4), fractionalValue(micros, fsp));
}

/**
 * The fractional-seconds trailer MySQL writes for a column of precision `fsp`:
 * one byte per two digits, holding the microseconds scaled to that width.
 */
function fractionalValue(micros: number, fsp: number): Uint8Array {
  if (fsp === 0) return new Uint8Array(0);
  if (fsp <= 2) return uintBe(Math.trunc(micros / 10000), 1);
  if (fsp <= 4) return uintBe(Math.trunc(micros / 100), 2);
  return uintBe(micros, 3);
}
