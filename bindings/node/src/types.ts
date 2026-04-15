// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

/** CDC change event types. */
export type EventType = "INSERT" | "UPDATE" | "DELETE";

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
 * A CDC change event.
 *
 * Column values are represented as plain records keyed by column name.
 * When column names are unavailable (standalone mode without metadata),
 * string indices ("0", "1", ...) are used as keys.
 *
 * Values are typed as: null, number, bigint, string, or Uint8Array.
 */
export interface ChangeEvent {
  /** Event type. */
  type: EventType;
  /** Database name. */
  database: string;
  /** Table name. */
  table: string;
  /** Before image (populated for UPDATE and DELETE). */
  before: Record<string, unknown> | null;
  /** After image (populated for INSERT and UPDATE). */
  after: Record<string, unknown> | null;
  /** Unix timestamp of the event. */
  timestamp: number;
  /** Binlog position. */
  position: {
    file: string;
    offset: number | bigint;
  };
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
