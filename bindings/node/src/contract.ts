// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

import { MesErrorCode } from "./types.js";

/**
 * Mirror of `core/contracts/bindings.json`, the cross-binding behavioural
 * contract: error retryability, reconnect backoff, shared option defaults and
 * their accepted ranges.
 *
 * `tests/contract.test.ts` compares every constant below against that file, and
 * the Python binding mirrors the same table in `_contract.py`. Editing a value
 * here without editing the contract (or the other way round) fails the test.
 */

/** Error codes that indicate a permanent failure where reconnecting is futile. */
export const NON_RETRYABLE_ERROR_CODES: ReadonlySet<number> = new Set<number>([
  MesErrorCode.NullArg,
  MesErrorCode.InvalidArg,
  MesErrorCode.Parse,
  MesErrorCode.Checksum,
  MesErrorCode.Decode,
  MesErrorCode.DecodeColumn,
  MesErrorCode.DecodeRow,
  MesErrorCode.QueueFull,
  MesErrorCode.Auth,
  MesErrorCode.Validation,
  MesErrorCode.GtidPurged,
  MesErrorCode.GtidTaggedUnsupported,
]);

/** Linear backoff capped at `maxDelayMs`, then scaled by the jitter window. */
export const RECONNECT_POLICY = {
  baseDelayMs: 1000,
  maxDelayMs: 10_000,
  jitterMin: 0.5,
  jitterMax: 1.0,
} as const;

/**
 * Backoff delay before reconnect attempt number `attempt` (1-based).
 *
 * @param attempt Retry ordinal; the undelayed base grows linearly with it.
 * @param jitter Uniform sample in `[0, 1)` selecting a point in the jitter window.
 */
export function backoffDelayMs(attempt: number, jitter: number): number {
  const { baseDelayMs, maxDelayMs, jitterMin, jitterMax } = RECONNECT_POLICY;
  const undelayed = Math.min(baseDelayMs * attempt, maxDelayMs);
  return undelayed * (jitterMin + jitter * (jitterMax - jitterMin));
}

/** Defaults the stream materializes for options the caller left unset. */
export const STREAM_DEFAULTS = {
  host: "127.0.0.1",
  port: 3306,
  user: "root",
  password: "",
  serverId: 1,
  connectTimeoutS: 10,
  readTimeoutS: 30,
  sslMode: 1,
  sslCa: "",
  sslCert: "",
  sslKey: "",
  allowPublicKeyRetrieval: false,
  maxQueueSize: 0,
  maxQueueBytes: 48 * 1024 * 1024,
  maxEventSize: 32 * 1024 * 1024,
  maxReconnectAttempts: 10,
} as const;

/** Accepted range for an integer option; `null` upper bound means unbounded. */
export interface OptionRange {
  readonly min: number;
  readonly max: number | null;
}

/** Accepted ranges for every shared integer option. */
export const OPTION_RANGES = {
  port: { min: 1, max: 65535 },
  serverId: { min: 1, max: 4294967295 },
  connectTimeoutS: { min: 0, max: 4294967295 },
  readTimeoutS: { min: 0, max: 4294967295 },
  sslMode: { min: 0, max: 4 },
  maxQueueSize: { min: 0, max: null },
  maxQueueBytes: { min: 0, max: null },
  maxEventSize: { min: 0, max: 4294967295 },
  maxReconnectAttempts: { min: 0, max: null },
  startBinlogPosition: { min: 0, max: 4294967295 },
} as const satisfies Record<string, OptionRange>;

/** A floor that holds for an option only while its companion option is set. */
export interface ConditionalMinimum {
  readonly minimum: number;
  /** Option whose presence brings the floor into force. */
  readonly companion: string;
}

/**
 * Floors that hold only while a companion option is set. {@link OPTION_RANGES}
 * keeps the unconditional window, which stays in force otherwise:
 * `startBinlogPosition` is left at 0 when no file/offset start was requested,
 * so the tighter floor applies only once `startBinlogFile` names a file. The
 * first binlog event begins after the file's 4-byte magic number, so an offset
 * into a named file cannot be below 4.
 */
export const CONDITIONAL_OPTION_MINIMUMS = {
  startBinlogPosition: { minimum: 4, companion: "startBinlogFile" },
} as const satisfies Record<string, ConditionalMinimum>;

/**
 * Options that are supplied together or not at all. An offset without the file
 * it points into names no position a server can start from, so it is refused
 * rather than accepted and dropped. This is a different statement from the
 * conditional floor above, which says what a supplied value must be once its
 * companion is set rather than whether it may appear alone.
 *
 * Absence is how this surface leaves an option of a pair unset: a key that is
 * missing or explicitly `undefined` was not supplied, and every other value
 * was.
 */
export const REQUIRED_TOGETHER_OPTIONS: ReadonlyArray<readonly [string, string]> = [
  ["startBinlogFile", "startBinlogPosition"],
];

/** Accepted `maxEvents` window for a batched poll. */
export const POLL_BATCH = {
  defaultMaxEvents: 64,
  minMaxEvents: 1,
  maxMaxEvents: 1024,
} as const;

/** Accepted `mes_log_level_t` window for the process-wide log callback. */
export const LOG_LEVEL_RANGE = {
  min: 0,
  max: 3,
  default: 1,
} as const;

/**
 * What the binding does when the optional metadata connection fails and no
 * handler is configured: nothing. The library never writes diagnostics itself.
 */
export const METADATA_ERROR_DEFAULT = "silent";
