// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

import { loadNativeAddon } from "./native.js";
import type { ChangeEvent, ClientConfig } from "./types.js";
import { MesErrorCode } from "./types.js";
import { REFUSAL_ERROR_NAME } from "./validation.js";

interface NativeAddon {
  CdcEngine: new () => NativeEngine;
}

interface NativeEngine {
  feed(data: Uint8Array | Buffer): number;
  nextEvent(): ChangeEvent | null;
  hasEvents(): boolean;
  getPosition(): { file: string; offset: number | bigint };
  reset(): void;
  setMaxQueueSize(maxSize: number): void;
  setMaxQueueBytes(maxQueueBytes: number): void;
  getMaxQueueBytes(): number;
  setMaxEventSize(maxEventSize: number): void;
  getMaxEventSize(): number;
  setChecksumEnabled(enabled: boolean): void;
  setTrailerPreVerified(preVerified: boolean): void;
  getTrailerPreVerified(): boolean;
  setIncludeDatabases(databases: string[]): void;
  setIncludeTables(tables: string[]): void;
  setExcludeTables(tables: string[]): void;
  destroy(): void;
  enableMetadata(config: ClientConfig): void;
}

const addon = loadNativeAddon<NativeAddon>();

function destroyedError(): Error {
  const error = new Error("Engine has been destroyed") as Error & { code: number };
  error.name = REFUSAL_ERROR_NAME;
  error.code = MesErrorCode.InvalidArg;
  return error;
}

/** Native N-API based CDC engine for parsing MySQL 8.4 binlog streams. */
export class CdcEngine {
  private engine: NativeEngine | null;

  constructor() {
    this.engine = new addon.CdcEngine();
  }

  /** Create a new CDC engine instance (async for backward compatibility). */
  static async create(): Promise<CdcEngine> {
    return new CdcEngine();
  }

  /**
   * Feed raw binlog bytes into the engine. Returns the number of bytes
   * consumed; on a partial consume, feed the remainder next (never the whole
   * buffer again) or already-queued events are delivered twice.
   *
   * After this throws, the engine's parse state is undefined and the only
   * valid next operation is {@link reset}. Events decoded before the failing
   * one stay queued and can be drained with {@link nextEvent} after the reset.
   * Feeding again without a reset is unsupported: it may duplicate events or
   * make no progress.
   */
  feed(data: Uint8Array): number {
    this.ensureNotDestroyed();
    if (data.length === 0) return 0;
    return this.engine!.feed(data);
  }

  /** Get the next change event, or null if no events are available. */
  nextEvent(): ChangeEvent | null {
    this.ensureNotDestroyed();
    return this.engine!.nextEvent();
  }

  /** Check if there are pending events. */
  hasEvents(): boolean {
    this.ensureNotDestroyed();
    return this.engine!.hasEvents();
  }

  /** Get current binlog position. */
  getPosition(): { file: string; offset: number | bigint } {
    this.ensureNotDestroyed();
    return this.engine!.getPosition();
  }

  /** Reset parser state while retaining already decoded events for draining. */
  reset(): void {
    this.ensureNotDestroyed();
    this.engine!.reset();
  }

  /**
   * Set the maximum event queue size for backpressure control. When the queue
   * is full, {@link feed} stops consuming bytes early; drain with
   * {@link nextEvent} and feed the remainder.
   *
   * `0` restores the bounded default of 10,000 events. There is no unlimited
   * setting: an unbounded queue would let a producer that outruns the consumer
   * grow it without limit.
   */
  setMaxQueueSize(maxSize: number): void {
    this.ensureNotDestroyed();
    this.engine!.setMaxQueueSize(maxSize);
  }

  /**
   * Set the total byte budget for the queue of decoded events. {@link feed}
   * stops consuming bytes when either this budget or the
   * {@link setMaxQueueSize} entry count is reached, whichever comes first.
   *
   * This is the bound to reach for when the requirement is a memory limit: the
   * charge counts every decoded column payload a queued entry holds, and what
   * one entry costs varies by an order of magnitude with the table it came from
   * and any statement annotating it, so an entry count cannot express one.
   *
   * `0` restores the default of 48 MiB. The budget over the client's own queue
   * of undecoded events is a different one, set through `maxQueueBytes` on a
   * stream or client config, and does not apply to an engine fed directly.
   */
  setMaxQueueBytes(maxQueueBytes: number): void {
    this.ensureNotDestroyed();
    this.engine!.setMaxQueueBytes(maxQueueBytes);
  }

  /** Return the configured queue byte budget. */
  getMaxQueueBytes(): number {
    this.ensureNotDestroyed();
    return this.engine!.getMaxQueueBytes();
  }

  /**
   * Override the maximum per-event size accepted by the parser (bytes).
   * Default is 64 MiB. Values are clamped at the C layer to the range
   * [header+checksum, 1 GiB]. Raise this when the server's
   * max_allowed_packet is raised to accommodate very large BLOB/JSON
   * payloads.
   *
   * `0` means "no limit" and resolves to the 1 GiB hard cap — it does not
   * restore the 64 MiB default the way `0` restores the default queue size in
   * {@link setMaxQueueSize}. Passing it removes the guard against a single
   * oversized event from an untrusted server.
   */
  setMaxEventSize(maxEventSize: number): void {
    this.ensureNotDestroyed();
    this.engine!.setMaxEventSize(maxEventSize);
  }

  /** Return the currently configured maximum event size (bytes). */
  getMaxEventSize(): number {
    this.ensureNotDestroyed();
    return this.engine!.getMaxEventSize();
  }

  /**
   * Set whether raw events carry a trailing CRC32 checksum. Disable this for
   * checksum=NONE streams that begin after the format-description event. A
   * later format-description event overrides the setting with its descriptor.
   */
  setChecksumEnabled(enabled: boolean): void {
    this.ensureNotDestroyed();
    this.engine!.setChecksumEnabled(enabled);
  }

  /**
   * Declare that the events fed from here on have already had their trailing
   * CRC32 validated, so {@link feed} skips its own pass over the same bytes.
   *
   * Nothing checks the claim. Set it on a stream nothing validated and a corrupt
   * event is accepted in silence — no error, no log record, just a decoded row
   * carrying whatever the corruption produced. Only set it when the upstream
   * validation is known to cover every event reaching this engine, in every
   * configuration it can run in.
   *
   * Orthogonal to {@link setChecksumEnabled}, which decides whether an event's
   * last four bytes are a trailer at all. Framing is unchanged here, and that
   * switch is never an alternative: clearing it also strips four bytes from
   * every event body.
   */
  setTrailerPreVerified(preVerified: boolean): void {
    this.ensureNotDestroyed();
    this.engine!.setTrailerPreVerified(preVerified);
  }

  /** Return whether the engine is skipping its own trailer verification. */
  getTrailerPreVerified(): boolean {
    this.ensureNotDestroyed();
    return this.engine!.getTrailerPreVerified();
  }

  /** Set database include filter. Only events from these databases are processed. Empty array = all. */
  setIncludeDatabases(databases: string[]): void {
    this.ensureNotDestroyed();
    this.engine!.setIncludeDatabases(databases);
  }

  /** Set table include filter. Only events from these tables are processed. Empty array = all.
   *  Format: "database.table" or just "table" (matches any database).
   *  A trailing '*' performs a case-sensitive prefix match. */
  setIncludeTables(tables: string[]): void {
    this.ensureNotDestroyed();
    this.engine!.setIncludeTables(tables);
  }

  /** Set table exclude filter. Events from these tables are skipped.
   *  Format: "database.table" or just "table" (matches any database).
   *  A trailing '*' performs a case-sensitive prefix match. */
  setExcludeTables(tables: string[]): void {
    this.ensureNotDestroyed();
    this.engine!.setExcludeTables(tables);
  }

  /** Enable metadata queries for column name resolution. */
  enableMetadata(config: ClientConfig): void {
    this.ensureNotDestroyed();
    this.engine!.enableMetadata(config);
  }

  /** Destroy the engine and free native resources. */
  destroy(): void {
    if (!this.engine) return;
    this.engine.destroy();
    this.engine = null;
  }

  private ensureNotDestroyed(): void {
    if (!this.engine) throw destroyedError();
  }
}
