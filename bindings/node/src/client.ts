// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

import { createRequire } from "node:module";
import type { ClientConfig, PollResult } from "./types.js";

const require = createRequire(import.meta.url);

interface NativeAddon {
  BinlogClient?: new () => NativeClient;
  hasClient: boolean;
}

interface NativeClient {
  connect(config: ClientConfig): void;
  start(): void;
  poll(): Promise<PollResult>;
  stop(): void;
  disconnect(): void;
  destroy(): void;
  readonly isConnected: boolean;
  readonly lastError: string;
  readonly currentGtid: string;
}

const addon: NativeAddon = require("../build/Release/mes-node.node");

/** BinlogClient for connecting to MySQL and streaming binlog events. */
export class BinlogClient {
  private client: NativeClient | null;

  constructor(config: ClientConfig) {
    if (!addon.hasClient || !addon.BinlogClient) {
      throw new Error("BinlogClient native addon not loaded");
    }
    this.client = new addon.BinlogClient();
    this.client.connect(config);
  }

  /** Start binlog streaming. */
  start(): void {
    this.ensureNotDestroyed();
    this.client!.start();
  }

  /** Poll for the next binlog event (non-blocking, returns Promise). */
  poll(): Promise<PollResult> {
    this.ensureNotDestroyed();
    return this.client!.poll();
  }

  /** Request stream stop. Thread-safe; unblocks a pending poll(). */
  stop(): void {
    if (this.client) {
      this.client.stop();
    }
  }

  /** Disconnect from MySQL server. */
  disconnect(): void {
    if (this.client) {
      this.client.disconnect();
    }
  }

  /** Destroy the client and free native resources. */
  destroy(): void {
    if (!this.client) return;
    this.client.destroy();
    this.client = null;
  }

  /** Check if client is connected. */
  get isConnected(): boolean {
    return this.client?.isConnected ?? false;
  }

  /** Get the last error message. */
  get lastError(): string {
    return this.client?.lastError ?? "";
  }

  /** Get the current GTID position. */
  get currentGtid(): string {
    return this.client?.currentGtid ?? "";
  }

  private ensureNotDestroyed(): void {
    if (!this.client) throw new Error("Client has been destroyed");
  }
}
