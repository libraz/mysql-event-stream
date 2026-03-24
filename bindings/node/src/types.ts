// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

/** CDC change event types. */
export type EventType = "INSERT" | "UPDATE" | "DELETE";

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
    offset: number;
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
  /** SSL mode: 0=disabled, 1=preferred, 2=required, 3=verify_ca, 4=verify_identity */
  sslMode?: number;
  /** Path to CA certificate file. */
  sslCa?: string;
  /** Path to client certificate file. */
  sslCert?: string;
  /** Path to client private key file. */
  sslKey?: string;
  /** Maximum number of automatic reconnection attempts (default 10, 0 = disabled). */
  maxReconnectAttempts?: number;
}

/** Result from a BinlogClient poll() call. */
export interface PollResult {
  data: Uint8Array | null;
  isHeartbeat: boolean;
}
