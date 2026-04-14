// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

import { BinlogClient } from "./client.js";
import { CdcEngine } from "./engine.js";
import type { ChangeEvent, StreamConfig } from "./types.js";

/** High-level CDC stream that implements AsyncIterable for easy consumption. */
export class CdcStream implements AsyncIterable<ChangeEvent>, AsyncDisposable {
  private config: StreamConfig;
  private client: BinlogClient | null = null;
  private engine: CdcEngine | null = null;
  private closed = false;
  private iterator: AsyncGenerator<ChangeEvent> | null = null;

  constructor(config: StreamConfig) {
    this.config = config;
  }

  /** Override config properties before streaming starts. */
  configure(overrides: Partial<StreamConfig>): void {
    if (this.iterator) {
      throw new Error("Cannot configure after streaming has started");
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
    if (this.iterator) {
      await this.iterator.return(undefined);
      this.iterator = null;
    }
    this.cleanup();
  }

  /** Get the current GTID position. */
  get currentGtid(): string {
    return this.client?.currentGtid ?? "";
  }

  private async *generate(): AsyncGenerator<ChangeEvent> {
    this.engine = new CdcEngine();
    try {
      this.engine.enableMetadata(this.config);
    } catch (e) {
      // Metadata connection is optional -- column names will be indices
      const msg = e instanceof Error ? e.message : String(e);
      console.warn(`[mysql-event-stream] Metadata connection failed: ${msg}`);
    }

    let reconnectAttempts = 0;
    const maxAttempts = Math.max(0, this.config.maxReconnectAttempts ?? 10);

    try {
    while (!this.closed) {
      this.client = new BinlogClient(this.config);
      try {
        this.client.start();
        reconnectAttempts = 0;

        while (!this.closed) {
          const result = await this.client.poll();
          if (!result.data) continue;
          this.engine!.feed(result.data);
          for (let ev = this.engine!.nextEvent(); ev !== null; ev = this.engine!.nextEvent()) {
            yield ev;
          }
        }
      } catch (err) {
        if (this.closed) break;
        if (maxAttempts === 0) throw err;

        const gtid = this.client.currentGtid;
        this.cleanupClient();

        reconnectAttempts++;
        if (reconnectAttempts > maxAttempts) throw err;

        const kBaseDelayMs = 1000;
        const kMaxDelaySteps = 10;
        const kJitterMin = 0.5;
        const baseDelay = kBaseDelayMs * Math.min(reconnectAttempts, kMaxDelaySteps);
        const delay = baseDelay * (kJitterMin + Math.random() * (1 - kJitterMin));
        await new Promise((resolve) => setTimeout(resolve, delay));

        this.engine!.reset();
        if (gtid) {
          this.config = { ...this.config, startGtid: gtid };
        }
      }
    }
    } finally {
      // Ensure resources are released whether the generator exits normally,
      // via .return() (break in for-await), or via .throw().
      this.cleanup();
    }
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
