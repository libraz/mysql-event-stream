// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

import { BinlogClient } from "./client.js";
import { backoffDelayMs, NON_RETRYABLE_ERROR_CODES, STREAM_DEFAULTS } from "./contract.js";
import { CdcEngine } from "./engine.js";
import type { ChangeEvent, StreamConfig } from "./types.js";
import { validateStreamOptions, withStreamDefaults } from "./validation.js";

/** Concatenate two byte arrays into a new Uint8Array. */
function concatBytes(a: Uint8Array, b: Uint8Array): Uint8Array {
  const out = new Uint8Array(a.length + b.length);
  out.set(a, 0);
  out.set(b, a.length);
  return out;
}

/** Extract the numeric `code` an addon error carries, if any. */
function errorCode(err: unknown): number | undefined {
  if (err !== null && typeof err === "object" && "code" in err) {
    const code = (err as { code: unknown }).code;
    return typeof code === "number" ? code : undefined;
  }
  return undefined;
}

/**
 * High-level CDC stream that implements AsyncIterable for easy consumption.
 *
 * Leaving the iteration early does not release the native client on its own,
 * so scope the stream and let the disposal run:
 *
 * ```ts
 * await using stream = new CdcStream({ host: "127.0.0.1", user: "root" });
 * for await (const event of stream) {
 *   console.log(event);
 *   break;
 * }
 * // stream.currentGtid still holds the last checkpoint here.
 * ```
 *
 * Without `await using`, call {@link close} explicitly.
 */
export class CdcStream implements AsyncIterable<ChangeEvent>, AsyncDisposable {
  private config!: StreamConfig;
  private client: BinlogClient | null = null;
  private engine: CdcEngine | null = null;
  private closed = false;
  private iterator: AsyncGenerator<ChangeEvent> | null = null;
  private cancelBackoff: (() => void) | null = null;
  // Retains the last non-empty checkpoint after cleanup releases the native
  // client. This keeps the checkpoint available to code immediately following
  // a `for await` loop that exits with `break`.
  private lastGtid = "";

  /**
   * @param config Stream options. Unrecognized keys and values that do not
   *   match their documented type are rejected here rather than at first
   *   iteration, so a typo cannot leave the stream running on a default the
   *   caller never asked for.
   */
  constructor(config: StreamConfig) {
    validateStreamOptions(config);
    Object.defineProperty(this, "config", {
      value: withStreamDefaults(config),
      writable: true,
      configurable: true,
      enumerable: false,
    });
  }

  /**
   * Override config properties before streaming starts.
   *
   * @param overrides Options to replace, validated exactly as the constructor
   *   validates a whole config.
   */
  configure(overrides: Partial<StreamConfig>): void {
    if (this.iterator) {
      throw new Error("Cannot configure after streaming has started");
    }
    validateStreamOptions(overrides);
    this.config = { ...this.config, ...overrides };
  }

  [Symbol.asyncIterator](): AsyncIterator<ChangeEvent> {
    if (this.closed) {
      // Return an already-completed iterator if the stream has been closed
      return (async function* () {})();
    }
    if (this.iterator) {
      throw new Error("CdcStream is already being iterated. Use a single for-await loop.");
    }
    this.iterator = this.generate();
    return this.iterator;
  }

  async [Symbol.asyncDispose](): Promise<void> {
    await this.close();
  }

  /** Stop the stream and release all resources. */
  async close(): Promise<void> {
    this.closed = true;
    this.cancelBackoff?.();
    // A generator blocked in client.poll() cannot process iterator.return()
    // until the native poll is interrupted. Stop first, then wait for the
    // generator's finally block to release the remaining resources.
    this.client?.stop();
    if (this.iterator) {
      await this.iterator.return(undefined);
      this.iterator = null;
    }
    this.cleanup();
  }

  /**
   * Get the delivered, committed checkpoint candidate. Persist it only after
   * application processing succeeds; the stream does not provide exactly-once
   * delivery. The last non-empty value survives {@link close}, so it can still
   * be read after the iteration scope ends.
   */
  get currentGtid(): string {
    this.cacheCurrentGtid();
    return this.lastGtid;
  }

  private enableMetadataSafe(): void {
    // Metadata connection is optional -- column names will be numeric
    // string indices if it fails. The library does not write to stderr
    // on its own; the embedder receives failures via onMetadataError.
    try {
      this.engine!.enableMetadata(this.config);
    } catch (e) {
      const err = e instanceof Error ? e : new Error(String(e));
      this.config.onMetadataError?.(err);
    }
  }

  private applyFilters(): void {
    this.engine!.setIncludeDatabases(this.config.includeDatabases ?? []);
    this.engine!.setIncludeTables(this.config.includeTables ?? []);
    this.engine!.setExcludeTables(this.config.excludeTables ?? []);
  }

  private async *generate(): AsyncGenerator<ChangeEvent> {
    this.engine = new CdcEngine();
    this.engine.setMaxEventSize(this.config.maxEventSize ?? STREAM_DEFAULTS.maxEventSize);
    this.engine.setMaxQueueSize(this.config.maxQueueSize ?? STREAM_DEFAULTS.maxQueueSize);
    this.applyFilters();
    this.enableMetadataSafe();

    let reconnectAttempts = 0;
    const maxAttempts = Math.max(
      0,
      this.config.maxReconnectAttempts ?? STREAM_DEFAULTS.maxReconnectAttempts,
    );

    try {
      while (!this.closed) {
        // Bytes the engine declined to consume on the previous feed (e.g. when
        // a queue limit applies backpressure). They are prepended to the next
        // chunk so no data is lost. Scoped per-connection: on reconnect the
        // engine is reset and the stream resumes from a GTID, so any leftover
        // from the dropped connection must not carry over.
        let leftover: Uint8Array | null = null;
        try {
          // Construction performs connect(), so it belongs to the same retry
          // budget as start() and poll().
          this.client = new BinlogClient(this.config);
          this.client.start();
          this.engine!.setChecksumEnabled(this.client.checksumEnabled);

          while (!this.closed) {
            const results = await this.client.pollBatch();
            // The native checkpoint advances as events leave the client queue,
            // so the whole batch shares one value. Read it once here instead of
            // once per decoded row.
            this.cacheCurrentGtid();
            for (const result of results) {
              if (!result.data && !leftover) continue;

              let chunk: Uint8Array;
              if (leftover && result.data) {
                chunk = concatBytes(leftover, result.data);
              } else if (leftover) {
                chunk = leftover;
              } else {
                chunk = result.data as Uint8Array;
              }

              const consumed = this.engine!.feed(chunk);
              leftover = consumed < chunk.length ? chunk.subarray(consumed) : null;

              for (let ev = this.engine!.nextEvent(); ev !== null; ev = this.engine!.nextEvent()) {
                // A decoded event is the only progress signal that can reset
                // retry accounting. Framing metadata may be received before the
                // same permanently undecodable event on every reconnect.
                reconnectAttempts = 0;
                yield ev;
              }
            }
          }
        } catch (err) {
          if (this.closed) break;
          if (maxAttempts === 0) throw err;

          // Permanent failures (bad credentials, server misconfiguration) will
          // never succeed on retry. Fail fast instead of burning every
          // reconnect attempt plus backoff before surfacing the error.
          const code = errorCode(err);
          if (code !== undefined && NON_RETRYABLE_ERROR_CODES.has(code)) {
            throw err;
          }

          this.cacheCurrentGtid();
          const gtid = this.lastGtid;
          this.cleanupClient();

          reconnectAttempts++;
          if (reconnectAttempts > maxAttempts) throw err;

          await this.waitForBackoff(backoffDelayMs(reconnectAttempts, Math.random()));
          if (this.closed) break;

          this.engine!.reset();
          this.config = this.resumeConfig(gtid);
          this.applyFilters();
          // Re-enable metadata after engine reset. Keeps the Node binding
          // consistent with the Python binding, which re-runs
          // enable_metadata on every reconnect.
          this.enableMetadataSafe();
        }
      }
    } finally {
      // Ensure resources are released whether the generator exits normally,
      // via .return() (break in for-await), or via .throw().
      this.cleanup();
    }
  }

  /**
   * Derive the successor connection's config from a reconnect checkpoint.
   *
   * This is the only place a start position is rewritten, so the rule holds on
   * every reconnect path. An empty checkpoint means none was ever published:
   * the connection died before its first commit, or it was anchored to a file
   * offset that produces no GTID. Forwarding that as `startGtid: ""` would
   * request the empty GTID set, which the server reads as "send every binlog
   * you still retain". Keep the configured start mode instead — at worst the
   * successor replays from the original anchor.
   */
  private resumeConfig(checkpoint: string): StreamConfig {
    if (checkpoint === "") return this.config;
    return {
      ...this.config,
      startGtid: checkpoint,
      startBinlogFile: undefined,
      startBinlogPosition: undefined,
    };
  }

  private waitForBackoff(delayMs: number): Promise<void> {
    return new Promise((resolve) => {
      let timer: ReturnType<typeof setTimeout>;
      const finish = () => {
        clearTimeout(timer);
        if (this.cancelBackoff === cancel) this.cancelBackoff = null;
        resolve();
      };
      const cancel = () => finish();
      this.cancelBackoff = cancel;
      timer = setTimeout(finish, delayMs);
    });
  }

  private cleanupClient(): void {
    this.cacheCurrentGtid();
    if (this.client) {
      this.client.stop();
      this.client.disconnect();
      this.client.destroy();
      this.client = null;
    }
  }

  private cleanup(): void {
    this.cleanupClient();
    if (this.engine) {
      this.engine.destroy();
      this.engine = null;
    }
  }

  private cacheCurrentGtid(): void {
    const gtid = this.client?.currentGtid;
    if (gtid) {
      this.lastGtid = gtid;
    }
  }
}
