// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

/** CDC change event types. */
export type EventType = "INSERT" | "UPDATE" | "DELETE";

/** A column value in a change event. */
export interface ColumnValue {
  /** Column value type. */
  type: "null" | "int" | "double" | "string" | "bytes";
  /** The value. null for NULL columns, number for int/double, string for string, Uint8Array for bytes. */
  value: null | number | bigint | string | Uint8Array;
}

/** A CDC change event. */
export interface ChangeEvent {
  /** Event type. */
  type: EventType;
  /** Database name. */
  database: string;
  /** Table name. */
  table: string;
  /** Before image (populated for UPDATE and DELETE). */
  before: ColumnValue[] | null;
  /** After image (populated for INSERT and UPDATE). */
  after: ColumnValue[] | null;
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
}

/** Result from a BinlogClient poll() call. */
export interface PollResult {
  data: Uint8Array | null;
  isHeartbeat: boolean;
}
