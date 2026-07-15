// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

import { loadNativeAddon } from "./native.js";

interface LogAddon {
  setLogCallback(callback: ((level: number, message: string) => void) | null, level: number): void;
}

const addon = loadNativeAddon<LogAddon>();

/**
 * Severity levels for native log messages (mirrors the C ABI
 * `mes_log_level_t`). Lower numbers are more severe.
 */
export const LogLevel = {
  Error: 0,
  Warn: 1,
  Info: 2,
  Debug: 3,
} as const;

export type LogLevel = (typeof LogLevel)[keyof typeof LogLevel];

/** A handler for native structured-log messages. */
export type LogHandler = (level: LogLevel, message: string) => void;

/**
 * Install (or remove) a process-wide handler for the native core's structured
 * log messages (decode warnings, truncation notices, auth traces, etc.).
 *
 * The handler is global to the loaded addon — not per-engine or per-client —
 * matching the C ABI. Messages are marshalled from the core's reader thread to
 * the JS event loop, so the handler always runs on the environment's JS
 * thread. The native queue holds at most 256 pending records; overflow is
 * dropped rather than blocking stream processing, then reported as one
 * `event=node_log_queue_overflow dropped=N` warning when delivery resumes.
 * A registered handler does not keep a process or Worker alive by itself.
 *
 * @param handler Called as `handler(level, message)` for each record, or `null`
 *   to remove the current handler.
 * @param level Maximum verbosity to deliver (default {@link LogLevel.Warn}).
 *   Messages more verbose than this are suppressed. Ignored when removing.
 */
export function setLogCallback(handler: LogHandler | null, level: LogLevel = LogLevel.Warn): void {
  if (handler === null) {
    addon.setLogCallback(null, level);
    return;
  }
  if (typeof handler !== "function") {
    throw new TypeError("setLogCallback expects a function or null");
  }
  addon.setLogCallback((lvl: number, message: string) => {
    handler(lvl as LogLevel, message);
  }, level);
}
