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

  constructor(config: ClientConfig) {
    this.config = config;
  }

  /** Start the binlog stream and begin collecting events. */
  async start(): Promise<void> {
    this.pollPromise = this.pollLoop();
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
   * Polls the internal event buffer until enough matching events are found
   * or the timeout expires.
   */
  async waitForEvents(opts: WaitForEventsOptions = {}): Promise<ChangeEvent[]> {
    const { table, type, count = 1, timeout = 30_000 } = opts;
    const deadline = Date.now() + timeout;

    while (Date.now() < deadline) {
      const matched = this.events.filter((ev) => {
        if (table && ev.table !== table) return false;
        if (type && ev.type !== type) return false;
        return true;
      });
      if (matched.length >= count) return matched;
      await new Promise((resolve) => setTimeout(resolve, 100));
    }

    const matched = this.events.filter((ev) => {
      if (table && ev.table !== table) return false;
      if (type && ev.type !== type) return false;
      return true;
    });
    return matched;
  }

  /** Clear all collected events. */
  clearEvents(): void {
    this.events = [];
  }

  private async pollLoop(): Promise<void> {
    this.stream = new CdcStream(this.config);
    try {
      for await (const event of this.stream) {
        if (this.stopped) break;
        this.events.push(event);
      }
    } catch {
      // Swallow disconnect errors on stop
    }
  }
}
