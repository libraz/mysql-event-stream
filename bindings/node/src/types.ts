// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

/** CDC change event types. */
export type EventType = "INSERT" | "UPDATE" | "DELETE";

/**
 * Numeric error codes mirroring the C ABI `mes_error_t`. Errors thrown by the
 * native addon carry the matching value on their `code` property, so callers
 * can branch on category (e.g. `err.code === MesErrorCode.Auth`) instead of
 * matching message strings.
 */
export const MesErrorCode = {
  Ok: 0,
  NullArg: 1,
  InvalidArg: 2,
  Internal: 99,
  Parse: 100,
  Checksum: 101,
  Decode: 200,
  DecodeColumn: 201,
  DecodeRow: 202,
  NoEvent: 300,
  QueueFull: 301,
  Connect: 400,
  Auth: 401,
  Validation: 402,
  Stream: 403,
  Disconnected: 404,
} as const;

export type MesErrorCode = (typeof MesErrorCode)[keyof typeof MesErrorCode];

/** SSL connection mode. */
export const SslMode = {
  /** No SSL. */
  Disabled: 0,
  /** Use SSL if server supports it, fall back to plain. */
  Preferred: 1,
  /** Require SSL, do not verify server certificate. */
  Required: 2,
  /** Require SSL, verify server certificate against CA. */
  VerifyCa: 3,
  /** Require SSL, verify CA and hostname match. */
  VerifyIdentity: 4,
} as const;

export type SslMode = (typeof SslMode)[keyof typeof SslMode];

/**
 * A column value as represented in JavaScript.
 *
 * The mapping from MySQL column types is:
 * - `null` — SQL NULL.
 * - `number` — integer, decimal, floating-point, temporal, ENUM, SET and BIT
 *   columns. ENUM arrives as its 1-based numeric index, SET as its numeric
 *   bitmask, and BIT as the integer value of its bits. Integers outside the
 *   safe-integer range are returned as `bigint` instead.
 * - `bigint` — integers that do not fit in a JS safe integer.
 * - `string` — textual columns (CHAR, VARCHAR, TEXT). See the limitation below.
 * - `Uint8Array` — binary columns: BLOB, BINARY/VARBINARY, GEOMETRY, and JSON.
 *   JSON columns arrive as raw bytes in MySQL's internal binary JSON format,
 *   not as a decoded string or object.
 *
 * String limitation: textual columns are decoded as UTF-8. Data stored in a
 * non-UTF-8 character set (e.g. latin1, sjis) is not transcoded; invalid byte
 * sequences are replaced with the Unicode replacement character (U+FFFD), so
 * such columns may be lossy. For lossless access to non-UTF-8 text, treat the
 * column as binary at the schema level.
 */
export type ColumnValue = null | number | bigint | string | Uint8Array;

/**
 * A CDC change event.
 *
 * Column values are represented as plain records keyed by column name.
 * When column names are unavailable (standalone mode without metadata),
 * string indices ("0", "1", ...) are used as keys.
 *
 * See {@link ColumnValue} for how each MySQL column type is represented and for
 * the non-UTF-8 string limitation.
 */
export interface ChangeEvent {
  /** Event type. */
  type: EventType;
  /** Database name. */
  database: string;
  /** Table name. */
  table: string;
  /** Before image (populated for UPDATE and DELETE). See {@link ColumnValue}. */
  before: Record<string, ColumnValue> | null;
  /** After image (populated for INSERT and UPDATE). See {@link ColumnValue}. */
  after: Record<string, ColumnValue> | null;
  /** Unix timestamp of the event. */
  timestamp: number;
  /** Binlog position. */
  position: {
    file: string;
    offset: number | bigint;
  };
  /**
   * False when column names could not be resolved for this event's table
   * (e.g. the metadata side-connection failed), in which case column keys in
   * `before`/`after` fall back to numeric string indices ("0", "1", ...).
   */
  namesResolved: boolean;
}

/** BinlogClient connection configuration. */
export interface ClientConfig {
  host?: string;
  port?: number;
  user?: string;
  password?: string;
  serverId?: number;
  startGtid?: string;
  connectTimeoutS?: number;
  readTimeoutS?: number;
  /** SSL connection mode. */
  sslMode?: SslMode;
  /** Path to CA certificate file. */
  sslCa?: string;
  /** Path to client certificate file. */
  sslCert?: string;
  /** Path to client private key file. */
  sslKey?: string;
  /** Maximum internal event queue size (0 = default 10000). */
  maxQueueSize?: number;
}

/** CdcStream configuration options (extends ClientConfig). */
export interface StreamConfig extends ClientConfig {
  /** Maximum number of automatic reconnection attempts (default 10, 0 = disabled). */
  maxReconnectAttempts?: number;
  /**
   * Optional callback fired when the optional metadata connection fails.
   * If unset, metadata failures are silently tolerated (column names fall
   * back to numeric string indices). The library intentionally does NOT
   * write to stderr on its own; embedding applications should wire this
   * into their logger if they care about the failure.
   */
  onMetadataError?: (error: Error) => void;
}

/** Result from a BinlogClient poll() call. */
export interface PollResult {
  data: Uint8Array | null;
  isHeartbeat: boolean;
}
