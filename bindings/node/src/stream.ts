// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

import { BinlogClient } from "./client.js";
import { CdcEngine } from "./engine.js";
import type { ChangeEvent, StreamConfig } from "./types.js";
import { MesErrorCode } from "./types.js";

/** Error codes that indicate a permanent failure where reconnecting is futile. */
const NON_RETRYABLE_CODES: ReadonlySet<number> = new Set([
  MesErrorCode.Auth,
  MesErrorCode.Validation,
]);

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

/** High-level CDC stream that implements AsyncIterable for easy consumption. */
export class CdcStream implements AsyncIterable<ChangeEvent>, AsyncDisposable {
  private config: StreamConfig;
  private client: BinlogClient | null = null;
  private engine: CdcEngine | null = null;
  private closed = false;
  private iterator: AsyncGenerator<ChangeEvent> | null = null;
  private cancelBackoff: (() => void) | null = null;

  constructor(config: StreamConfig) {
    this.config = config;
  }

  /** Override config properties before streaming starts. */
  configure(overrides: Partial<StreamConfig>): void {
    if (this.iterator) {
      throw new Error("Cannot configure after streaming has started");
    }
    if (overrides.port !== undefined && (overrides.port < 1 || overrides.port > 65535)) {
      throw new Error(`port must be 1-65535, got ${overrides.port}`);
    }
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
   * application processing succeeds; the stream does not provide exactly-once delivery.
   */
  get currentGtid(): string {
    return this.client?.currentGtid ?? "";
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

  private async *generate(): AsyncGenerator<ChangeEvent> {
    this.engine = new CdcEngine();
    this.engine.setMaxEventSize(this.config.maxEventSize ?? 64 * 1024 * 1024);
    this.enableMetadataSafe();

    let reconnectAttempts = 0;
    const maxAttempts = Math.max(0, this.config.maxReconnectAttempts ?? 10);

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
            const result = await this.client.poll();
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
            if (result.data) {
              // A real protocol event proves the connection made progress.
              // Merely accepting the connection/start command does not.
              reconnectAttempts = 0;
            }

            for (let ev = this.engine!.nextEvent(); ev !== null; ev = this.engine!.nextEvent()) {
              yield ev;
            }
          }
        } catch (err) {
          if (this.closed) break;
          if (maxAttempts === 0) throw err;

          // Permanent failures (bad credentials, server misconfiguration) will
          // never succeed on retry. Fail fast instead of burning every
          // reconnect attempt plus backoff before surfacing the error.
          const code = errorCode(err);
          if (code !== undefined && NON_RETRYABLE_CODES.has(code)) {
            throw err;
          }

          const gtid = this.client?.currentGtid ?? "";
          this.cleanupClient();

          reconnectAttempts++;
          if (reconnectAttempts > maxAttempts) throw err;

          // Linear backoff capped at kMaxDelayMs (10s), with 50%-100% jitter.
          // Aligned with the Python binding (max_delay_s = 10.0).
          const kBaseDelayMs = 1000;
          const kMaxDelayMs = 10_000;
          const kJitterMin = 0.5;
          const baseDelay = Math.min(kBaseDelayMs * reconnectAttempts, kMaxDelayMs);
          const delay = baseDelay * (kJitterMin + Math.random() * (1 - kJitterMin));
          await this.waitForBackoff(delay);
          if (this.closed) break;

          this.engine!.reset();
          if (gtid) {
            this.config = { ...this.config, startGtid: gtid };
          }
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
}
