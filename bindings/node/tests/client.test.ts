// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from "vitest";
import { BinlogClient } from "../src/client.js";
import { loadNativeAddon } from "../src/native.js";
import { MesErrorCode } from "../src/types.js";

interface NativeClientForTest {
  poll(): Promise<unknown>;
  pollBatch(maxEvents?: number): Promise<unknown[]>;
  disconnect(): void;
  destroy(): void;
  readonly isConnected: boolean;
  readonly isStreaming: boolean;
  readonly lastError: string;
  readonly currentGtid: string;
  readonly flavor: number;
  readonly checksumEnabled: boolean;
  readonly queuedBytes: number;
  readonly maxQueueBytes: number;
  readonly maxEventSize: number;
  readonly crcErrors: number;
}

describe("BinlogClient", () => {
  it("rejects a zero server ID before connecting", () => {
    try {
      new BinlogClient({ host: "127.0.0.1", serverId: 0 });
    } catch (error) {
      expect(error).toMatchObject({
        code: MesErrorCode.InvalidArg,
        message: "serverId must be non-zero",
      });
      return;
    }
    throw new Error("expected constructor to reject serverId=0");
  });

  it("connects during construction", () => {
    // Constructor connection is part of the Node lifecycle contract. With no
    // server on this port it must fail during construction, not at start().
    expect(() => new BinlogClient({ host: "127.0.0.1", port: 19999 })).toThrow();
  });

  it("reads every getter through the native client", () => {
    const native = loadNativeAddon<{ BinlogClient: new () => NativeClientForTest }>();
    const client = new native.BinlogClient();
    try {
      expect(client.isConnected).toBe(false);
      expect(client.isStreaming).toBe(false);
      expect(client.lastError).toBe("");
      expect(client.currentGtid).toBe("");
      expect(client.flavor).toBe(0);
      expect(client.checksumEnabled).toBe(true);
      expect(client.queuedBytes).toBe(0);
      expect(client.maxQueueBytes).toBeGreaterThan(0);
      expect(client.maxEventSize).toBeGreaterThan(0);
      expect(client.crcErrors).toBe(0);
    } finally {
      client.destroy();
    }
  });

  it("rejects lifecycle changes while a native poll is in flight", async () => {
    const native = loadNativeAddon<{ BinlogClient: new () => NativeClientForTest }>();
    const client = new native.BinlogClient();
    try {
      const pendingPoll = client.poll();
      expect(() => client.disconnect()).toThrow("Cannot disconnect while poll() is in progress");
      await expect(pendingPoll).rejects.toBeDefined();
    } finally {
      client.destroy();
    }
  });

  it("validates native pollBatch capacity", async () => {
    const native = loadNativeAddon<{ BinlogClient: new () => NativeClientForTest }>();
    const client = new native.BinlogClient();
    try {
      await expect(client.pollBatch(0)).rejects.toThrow("maxEvents must be an integer");
    } finally {
      client.destroy();
    }
  });

  it("rejects invalid byte and event limits before connecting", () => {
    expect(() => new BinlogClient({ host: "127.0.0.1", port: 19999, maxQueueBytes: -1 })).toThrow(
      /maxQueueBytes must be non-negative/,
    );
    expect(() => new BinlogClient({ host: "127.0.0.1", port: 19999, maxEventSize: -1 })).toThrow(
      /maxEventSize must fit in uint32/,
    );
  });

  it("rejects a non-numeric sslMode before connecting", () => {
    try {
      new BinlogClient({ host: "127.0.0.1", port: 19999, sslMode: "required" as never });
    } catch (error) {
      expect(error).toMatchObject({ code: MesErrorCode.InvalidArg });
      expect(error).toHaveProperty("message", expect.stringMatching(/sslMode must be a number/));
      return;
    }
    throw new Error("expected validation error");
  });

  it("exposes a MySQL flavor fallback after construction failure", () => {
    const client = Object.create(BinlogClient.prototype) as BinlogClient;
    expect(client.flavor).toBe(0);
  });

  it("should expose readonly properties after failed construction", () => {
    // Even when construction throws, the class interface is correct
    const client = Object.create(BinlogClient.prototype);
    // Accessing properties on a prototype-only object returns defaults
    expect(client.isConnected).toBe(false);
    expect(client.isStreaming).toBe(false);
    expect(client.lastError).toBe("");
    expect(client.currentGtid).toBe("");
    expect(client.checksumEnabled).toBe(false);
    expect(client.queuedBytes).toBe(0);
    expect(client.maxQueueBytes).toBe(0);
    expect(client.maxEventSize).toBe(0);
    expect(client.crcErrors).toBe(0);
  });

  it("destroy should be idempotent", () => {
    const client = Object.create(BinlogClient.prototype);
    // destroy on an uninitialized instance should not throw
    expect(() => client.destroy()).not.toThrow();
    expect(() => client.destroy()).not.toThrow();
  });

  it("stop should be safe on uninitialized client", () => {
    const client = Object.create(BinlogClient.prototype);
    expect(() => client.stop()).not.toThrow();
  });

  it("disconnect should be safe on uninitialized client", () => {
    const client = Object.create(BinlogClient.prototype);
    expect(() => client.disconnect()).not.toThrow();
  });
});
