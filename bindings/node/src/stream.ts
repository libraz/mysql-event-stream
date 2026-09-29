// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

import { BinlogClient } from "./client.js";
import { backoffDelayMs, NON_RETRYABLE_ERROR_CODES, STREAM_DEFAULTS } from "./contract.js";
import { CdcEngine } from "./engine.js";
import type { ChangeEvent, PollResult, StreamConfig } from "./types.js";
import { invalidArgument, validateStreamOptions, withStreamDefaults } from "./validation.js";

/** A run of bytes together with the checksum framing they were read under. */
interface FramedBytes {
  bytes: Uint8Array;
  checksumEnabled: boolean;
}

/**
 * Group a poll batch's results into runs of bytes sharing the same checksum
 * framing, each run concatenated once rather than grown one result at a time.
 * Consecutive events read under the same framing are one byte stream and are
 * fed as one; a framing change starts a new run, because the engine frames
 * whatever it is given with a single flag and the events on either side of the
 * change need different ones.
 */
function groupFramedResults(results: readonly PollResult[]): FramedBytes[] {
  const groups: FramedBytes[] = [];
  let run: Uint8Array[] = [];
  let framing: boolean | null = null;
  const flush = (): void => {
    if (run.length === 0) return;
    const bytes = new Uint8Array(run.reduce((total, chunk) => total + chunk.length, 0));
    let offset = 0;
    for (const chunk of run) {
      bytes.set(chunk, offset);
      offset += chunk.length;
    }
    groups.push({ bytes, checksumEnabled: framing as boolean });
    run = [];
  };
  for (const result of results) {
    if (!result.data) continue;
    if (framing !== result.checksumEnabled) {
      flush();
      framing = result.checksumEnabled;
    }
    run.push(result.data);
  }
  flush();
  return groups;
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
   *   validates a whole config. Options that have to be supplied together are
   *   judged against the configuration the update produces, so an override may
   *   name one of them while the other stays as the constructor left it.
   */
  configure(overrides: Partial<StreamConfig>): void {
    if (this.iterator) {
      throw invalidArgument("Cannot configure after streaming has started");
    }
    validateStreamOptions(overrides, this.config);
    this.config = { ...this.config, ...overrides };
  }

  [Symbol.asyncIterator](): AsyncIterator<ChangeEvent> {
    if (this.closed) {
      // Return an already-completed iterator if the stream has been closed
      return (async function* () {})();
    }
    if (this.iterator) {
      throw invalidArgument("CdcStream is already being iterated. Use a single for-await loop.");
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
    this.engine.setMaxQueueBytes(this.config.maxQueueBytes ?? STREAM_DEFAULTS.maxQueueBytes);
    // The client's reader thread already verified every queued event's CRC32
    // before it reached pollBatch(), so the engine frames the trailer without
    // computing it a second time. Set once: state_machine's reset() does not
    // clear this flag, so it holds across every reconnect on this engine.
    this.engine.setTrailerPreVerified(true);
    this.applyFilters();
    this.enableMetadataSafe();

    let reconnectAttempts = 0;
    const maxAttempts = Math.max(
      0,
      this.config.maxReconnectAttempts ?? STREAM_DEFAULTS.maxReconnectAttempts,
    );

    try {
      while (!this.closed) {
        // Bytes waiting for the engine, each run paired with the checksum
        // framing the client read it under. Consecutive events read under the
        // same framing are one byte stream and are fed as one; a framing change
        // starts a new segment, because the engine frames whatever it is given
        // with a single flag. A segment survives a feed the engine did not take
        // whole (queue backpressure) and is retried ahead of later bytes.
        // Scoped per-connection: on reconnect the engine is reset and the
        // stream resumes from a GTID, so nothing pending may carry over.
        const pending: FramedBytes[] = [];
        try {
          // Construction performs connect(), so it belongs to the same retry
          // budget as start() and poll().
          this.client = new BinlogClient(this.config);
          this.client.start();

          while (!this.closed) {
            // The next poll promotes the native checkpoint over every byte the
            // previous one returned (it covers up to, but not including, the
            // last pollBatch result), so it never runs while bytes from an
            // earlier batch are still unfed: doing so would report a
            // checkpoint covering events the consumer has not received yet.
            if (pending.length === 0) {
              const results = await this.client.pollBatch();
              // The native checkpoint advances as events leave the client
              // queue, so the whole batch shares one value. Read it once here
              // instead of once per decoded row.
              this.cacheCurrentGtid();
              pending.push(...groupFramedResults(results));
            }

            for (let head = pending[0]; head !== undefined; head = pending[0]) {
              // Stated per segment rather than tracked, because the engine
              // also moves this flag itself when a FORMAT_DESCRIPTION_EVENT
              // passes through it. What the segment was read under is the
              // only value that is known here.
              this.engine!.setChecksumEnabled(head.checksumEnabled);
              let consumed: number;
              try {
                consumed = this.engine!.feed(head.bytes);
              } catch (feedError) {
                // The engine's parse state is undefined after a failed feed;
                // reset() is the only call it accepts next, and it retains
                // whatever was already decoded. Deliver that before the
                // failure reaches the caller, mirroring the Python binding's
                // _feed_and_drain. The remaining unfed bytes are dropped: a
                // reconnect refetches from the last cached checkpoint, which
                // this batch has not advanced past yet.
                this.engine!.reset();
                for (
                  let ev = this.engine!.nextEvent();
                  ev !== null;
                  ev = this.engine!.nextEvent()
                ) {
                  reconnectAttempts = 0;
                  yield ev;
                }
                pending.length = 0;
                throw feedError;
              }
              const partial = consumed < head.bytes.length;
              if (partial) {
                head.bytes = head.bytes.subarray(consumed);
              } else {
                pending.shift();
              }

              for (let ev = this.engine!.nextEvent(); ev !== null; ev = this.engine!.nextEvent()) {
                // A decoded event is the only progress signal that can reset
                // retry accounting. Framing metadata may be received before
                // the same permanently undecodable event on every reconnect.
                reconnectAttempts = 0;
                yield ev;
              }
              // The engine did not take this run whole, which is queue
              // backpressure: hold what is left -- and everything behind it,
              // so order is kept -- and retry it alone on the next iteration,
              // before polling for more.
              if (partial) break;
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
