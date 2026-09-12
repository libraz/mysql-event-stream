// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

import { CdcStream } from "../../src/stream.js";
import type { ChangeEvent, ClientConfig, EventType } from "../../src/types.js";

interface WaitForEventsOptions {
  table?: string;
  type?: EventType;
  count?: number;
  timeout?: number;
}

/** Collects CDC events from a live MySQL binlog stream using CdcStream. */
export class StreamingCollector {
  private config: ClientConfig;
  private stream: CdcStream | null = null;
  private events: ChangeEvent[] = [];
  private stopped = false;
  private pollPromise: Promise<void> | null = null;
  // The stream's terminal error, held until a caller is in a position to see
  // it. Discarding it turns every connection, authentication or purged-GTID
  // failure into a bare count mismatch after the full wait, with the cause
  // gone.
  private streamError: Error | null = null;

  constructor(config: ClientConfig) {
    this.config = config;
  }

  /**
   * Start the binlog stream and begin collecting events.
   *
   * A configuration the stream refuses outright is raised from here, so it
   * surfaces at the call that made the mistake. A failure that needs the
   * network to happen -- a refused connection, a rejected login, a start
   * position the server has purged -- cannot be known before the first wait,
   * and is raised from {@link waitForEvents} instead.
   */
  async start(): Promise<void> {
    this.pollPromise = this.pollLoop();
    // Let the poll loop run up to its first suspension point, which is where a
    // refusal that needs no I/O has already landed.
    await Promise.resolve();
    this.throwIfStreamFailed();
  }

  /** Stop collecting and release resources. */
  async stop(): Promise<void> {
    this.stopped = true;
    if (this.stream) {
      await this.stream.close();
    }
    if (this.pollPromise) {
      await this.pollPromise;
    }
  }

  /**
   * Wait until matching events are collected.
   *
   * Polls the internal event buffer until enough matching events are found.
   *
   * @throws The stream's terminal error, as soon as one is seen -- a refused
   * connection or a rejected login ends the wait immediately rather than
   * running it out.
   * @throws If the deadline passes with fewer matching events than asked for.
   * Returning what it has would present a stream that never started as a
   * stream that produced nothing.
   */
  async waitForEvents(opts: WaitForEventsOptions = {}): Promise<ChangeEvent[]> {
    const { table, type, count = 1, timeout = 30_000 } = opts;
    const deadline = Date.now() + timeout;

    for (;;) {
      this.throwIfStreamFailed();
      const matched = this.matching(table, type);
      if (matched.length >= count) return matched;
      if (Date.now() >= deadline) {
        throw new Error(
          `Expected ${count} events (table=${table}, type=${type}) ` +
            `but got ${matched.length} within ${timeout}ms`,
        );
      }
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
  }

  /** Clear all collected events. */
  clearEvents(): void {
    this.events = [];
  }

  /** Collected events matching the optional table and type filters. */
  private matching(table?: string, type?: EventType): ChangeEvent[] {
    return this.events.filter((ev) => {
      if (table && ev.table !== table) return false;
      if (type && ev.type !== type) return false;
      return true;
    });
  }

  /** Re-raise the stream's terminal error, if it has one. */
  private throwIfStreamFailed(): void {
    if (this.streamError) throw this.streamError;
  }

  private async pollLoop(): Promise<void> {
    try {
      this.stream = new CdcStream(this.config);
      for await (const event of this.stream) {
        if (this.stopped) break;
        this.events.push(event);
      }
    } catch (err) {
      // A stop closes the stream out from under the iteration, so the error
      // that arrives then is the shutdown itself and carries nothing a test
      // wants. Anything raised while the collector still expects events is the
      // reason the events are not coming.
      if (!this.stopped) {
        this.streamError = err instanceof Error ? err : new Error(String(err));
      }
    }
  }
}
