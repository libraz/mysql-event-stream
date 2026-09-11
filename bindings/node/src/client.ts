// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

import { POLL_BATCH } from "./contract.js";
import { loadNativeAddon } from "./native.js";
import type { ClientConfig, PollResult, ServerFlavor } from "./types.js";
import { MesErrorCode } from "./types.js";
import { invalidArgument, validatePollBatchSize, validatePort } from "./validation.js";

interface NativeAddon {
  BinlogClient: new () => NativeClient;
}

interface NativeClient {
  connect(config: ClientConfig): void;
  start(): void;
  poll(): Promise<PollResult>;
  pollBatch(maxEvents?: number): Promise<PollResult[]>;
  stop(): void;
  disconnect(): void;
  destroy(): void;
  readonly isConnected: boolean;
  readonly isStreaming: boolean;
  readonly lastError: string;
  readonly currentGtid: string;
  readonly flavor: ServerFlavor;
  readonly checksumEnabled: boolean;
  readonly queuedBytes: number;
  readonly maxQueueBytes: number;
  readonly maxEventSize: number;
  readonly crcErrors: number;
}

const addon = loadNativeAddon<NativeAddon>();

function destroyedError(): Error {
  const error = new Error("Client has been destroyed") as Error & { code: number };
  error.name = "MesError";
  error.code = MesErrorCode.InvalidArg;
  return error;
}

/** BinlogClient for connecting to MySQL and streaming binlog events. */
export class BinlogClient {
  private client: NativeClient | null;

  constructor(config: ClientConfig) {
    validatePort(config.port);
    if (config.serverId !== undefined && config.serverId === 0) {
      throw invalidArgument("serverId must be non-zero");
    }
    // If new addon.BinlogClient() throws, the exception propagates before
    // assignment completes — this.client stays null and the try block is
    // never entered. The catch below only handles connect() failures.
    this.client = new addon.BinlogClient();
    try {
      this.client.connect(config);
    } catch (e) {
      this.client.destroy();
      // Re-thrown as raised: every native refusal already carries the numeric
      // code the stream's retry policy classifies on.
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

  /** Block for one event, then return further events already in the native queue. */
  pollBatch(maxEvents: number = POLL_BATCH.defaultMaxEvents): Promise<PollResult[]> {
    this.ensureNotDestroyed();
    validatePollBatchSize(maxEvents);
    return this.client!.pollBatch(maxEvents);
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

  /** Server flavor detected during connection. */
  get flavor(): ServerFlavor {
    return this.client?.flavor ?? 0;
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

  /** Bytes charged to the queue: wire payloads plus their checkpoint bookkeeping. */
  get queuedBytes(): number {
    return this.client?.queuedBytes ?? 0;
  }

  /** Configured queue byte budget. See {@link ClientConfig.maxQueueBytes}. */
  get maxQueueBytes(): number {
    return this.client?.maxQueueBytes ?? 0;
  }

  /** Configured maximum individual binlog event size. */
  get maxEventSize(): number {
    return this.client?.maxEventSize ?? 0;
  }

  /** Number of CRC32-invalid events detected by this client. */
  get crcErrors(): number {
    return this.client?.crcErrors ?? 0;
  }

  private ensureNotDestroyed(): void {
    if (!this.client) throw destroyedError();
  }
}
