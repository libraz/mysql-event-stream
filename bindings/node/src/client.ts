// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

import { loadNativeAddon } from "./native.js";
import type { ClientConfig, PollResult } from "./types.js";

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
  readonly isStreaming: boolean;
  readonly lastError: string;
  readonly currentGtid: string;
  readonly checksumEnabled: boolean;
}

const addon = loadNativeAddon<NativeAddon>();

/** BinlogClient for connecting to MySQL and streaming binlog events. */
export class BinlogClient {
  private client: NativeClient | null;

  constructor(config: ClientConfig) {
    if (!addon.hasClient || !addon.BinlogClient) {
      throw new Error("BinlogClient native addon not loaded");
    }
    if (config.port !== undefined && (config.port < 1 || config.port > 65535)) {
      throw new Error(`port must be 1-65535, got ${config.port}`);
    }
    // If new addon.BinlogClient() throws, the exception propagates before
    // assignment completes — this.client stays null and the try block is
    // never entered. The catch below only handles connect() failures.
    this.client = new addon.BinlogClient();
    try {
      this.client.connect(config);
    } catch (e) {
      this.client.destroy();
      throw e;
    }
  }

  /** Start binlog streaming. */
  start(): void {
    this.ensureNotDestroyed();
    this.client!.start();
  }

  /**
   * Poll for the next binlog event.
   *
   * The returned Promise resolves on the libuv thread pool, but the underlying
   * native call blocks: it does not resolve until an event becomes available or
   * the stream stops (e.g. via {@link stop} or disconnect). Only one poll() may
   * be in flight at a time.
   */
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

  /** Whether poll() can still drain events or one queued terminal error. */
  get isStreaming(): boolean {
    return this.client?.isStreaming ?? false;
  }

  /** Get the last error message. */
  get lastError(): string {
    return this.client?.lastError ?? "";
  }

  /**
   * Get the delivered, committed checkpoint candidate. This advances only
   * after the caller polls past a commit boundary; it is not a durable ack.
   */
  get currentGtid(): string {
    return this.client?.currentGtid ?? "";
  }

  /** Checksum mode negotiated for events returned by poll(). */
  get checksumEnabled(): boolean {
    return this.client?.checksumEnabled ?? false;
  }

  private ensureNotDestroyed(): void {
    if (!this.client) throw new Error("Client has been destroyed");
  }
}
