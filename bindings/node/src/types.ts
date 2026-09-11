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
  GtidPurged: 405,
  GtidTaggedUnsupported: 406,
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

/** Database server flavor reported after connecting. */
export const ServerFlavor = {
  Mysql: 0,
  MariaDb: 1,
} as const;

export type ServerFlavor = (typeof ServerFlavor)[keyof typeof ServerFlavor];

/**
 * A column value as represented in JavaScript.
 *
 * SQL NULL is `null`. Every other MySQL column type maps to exactly one
 * JavaScript type. This table mirrors the canonical one in `core/include/mes.h`
 * and a test compares the two, so the two surfaces cannot drift apart:
 *
 *   number | bigint => TINYINT SMALLINT MEDIUMINT INT BIGINT YEAR BIT ENUM SET
 *   number          => FLOAT DOUBLE
 *   string          => CHAR VARCHAR TEXT DECIMAL DATE TIME DATETIME TIMESTAMP
 *   Uint8Array      => BINARY VARBINARY BLOB JSON GEOMETRY VECTOR
 *
 * Reading the rows:
 * - Integers arrive as `number`, or as `bigint` when the exact value does not
 *   fit in a JS safe integer. ENUM is its 1-based numeric index, SET its
 *   numeric bitmask, BIT the integer value of its bits.
 * - A BIGINT UNSIGNED, SET, or BIT value above signed `int64_t` arrives as an
 *   exact decimal `string`, because the core cannot represent it as an integer.
 * - Every TIMESTAMP variant is a `string` holding decimal Unix epoch seconds,
 *   with as many fractional digits as the column's declared precision (for
 *   example `"1735689600"` or `"1735689600.123456"`). DECIMAL and the other
 *   temporal types are formatted by the core as text too.
 * - Character and BLOB-family columns follow their charset, so a TEXT column
 *   with a binary collation is a `Uint8Array` and a BLOB with a text collation
 *   is a `string`. JSON arrives as raw bytes in MySQL's internal binary JSON
 *   format, not as a decoded string or object.
 *
 * String limitation: textual columns are decoded as UTF-8. Data stored in a
 * non-UTF-8 character set (e.g. latin1, sjis) is not transcoded; invalid byte
 * sequences are replaced with the Unicode replacement character (U+FFFD), so
 * such columns may be lossy. The binary/text distinction relies on TABLE_MAP
 * charset metadata; with `binlog_row_metadata=NO_LOG`, BLOB-family columns
 * conservatively remain `Uint8Array`.
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
   * False when any column name could not be resolved for this event's table
   * (for example, no metadata connection is configured or it failed). In that
   * case keys in `before`/`after` fall back to numeric string indices
   * ("0", "1", ...).
   */
  namesResolved: boolean;
  /** Original MariaDB SQL from ANNOTATE_ROWS, or an empty string when unavailable. */
  sourceSql: string;
}

/** BinlogClient connection configuration. */
export interface ClientConfig {
  host?: string;
  port?: number;
  user?: string;
  password?: string;
  serverId?: number;
  /** Omitted snapshots the current server set; an empty string explicitly starts from an empty set. */
  startGtid?: string;
  /** Binlog filename for an exact file/offset start. Requires startBinlogPosition. */
  startBinlogFile?: string;
  /** Binlog offset for an exact file/offset start (minimum 4). Requires startBinlogFile. */
  startBinlogPosition?: number;
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
  /**
   * Permit unauthenticated RSA key retrieval without TLS (default false).
   * This is MITM-sensitive; prefer VerifyCa or VerifyIdentity TLS.
   */
  allowPublicKeyRetrieval?: boolean;
  /** Maximum internal event queue size (0 = default 10000). */
  maxQueueSize?: number;
  /**
   * Total queue byte budget (default 48 MiB; 0 restores default). It charges
   * each queued wire payload plus the GTID checkpoint held with it, so a source
   * with a wide GTID set applies backpressure after fewer events.
   */
  maxQueueBytes?: number;
  /**
   * Maximum binlog event size accepted by both the client and parser
   * (default 32 MiB; 0 resolves to the 1 GiB hard cap). Configure a larger
   * maxQueueBytes value when raising this limit.
   */
  maxEventSize?: number;
}

/** CdcStream configuration options (extends ClientConfig). */
export interface StreamConfig extends ClientConfig {
  /** Exact, case-sensitive database names to include. Empty or omitted means all databases. */
  includeDatabases?: string[];
  /** Case-sensitive table names to include (`database.table`, bare name, or trailing-* prefix). */
  includeTables?: string[];
  /** Case-sensitive table names to exclude (`database.table`, bare name, or trailing-* prefix). */
  excludeTables?: string[];
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
