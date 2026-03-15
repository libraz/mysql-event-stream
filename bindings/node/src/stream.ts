// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

import { BinlogClient } from "./client.js";
import { CdcEngine } from "./engine.js";
import type { ChangeEvent, ClientConfig } from "./types.js";

/** High-level CDC stream that implements AsyncIterable for easy consumption. */
export class CdcStream implements AsyncIterable<ChangeEvent>, AsyncDisposable {
  private config: ClientConfig;
  private client: BinlogClient | null = null;
  private engine: CdcEngine | null = null;
  private closed = false;
  private iterator: AsyncGenerator<ChangeEvent> | null = null;

  constructor(config: ClientConfig) {
    this.config = config;
  }

  /** Override config properties before streaming starts. */
  configure(overrides: Partial<ClientConfig>): void {
    if (this.iterator) {
      throw new Error("Cannot configure after streaming has started");
    }
    this.config = { ...this.config, ...overrides };
  }

  [Symbol.asyncIterator](): AsyncIterator<ChangeEvent> {
    if (!this.iterator) {
      this.iterator = this.generate();
    }
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
    } catch {
      // Metadata connection is optional -- column names will be empty
    }
    this.client = new BinlogClient(this.config);
    try {
      this.client.start();
      while (!this.closed) {
        const result = await this.client.poll();
        if (!result.data) continue;
        this.engine.feed(result.data);
        for (let ev = this.engine.nextEvent(); ev !== null; ev = this.engine.nextEvent()) {
          yield ev;
        }
      }
    } finally {
      this.cleanup();
    }
  }

  private cleanup(): void {
    if (this.client) {
      this.client.disconnect();
      this.client.destroy();
      this.client = null;
    }
    if (this.engine) {
      this.engine.destroy();
      this.engine = null;
    }
  }
}
