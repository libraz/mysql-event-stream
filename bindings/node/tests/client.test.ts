// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from "vitest";
import { BinlogClient } from "../src/client.js";

describe("BinlogClient", () => {
  it("should throw when native addon is unavailable", () => {
    // BinlogClient requires a real MySQL connection; constructor throws
    // with ECONNREFUSED or similar when no server is running.
    expect(() => new BinlogClient({ host: "127.0.0.1", port: 19999 })).toThrow();
  });

  it("rejects invalid byte and event limits before connecting", () => {
    expect(() => new BinlogClient({ host: "127.0.0.1", port: 19999, maxQueueBytes: -1 })).toThrow(
      /maxQueueBytes must be non-negative/,
    );
    expect(() => new BinlogClient({ host: "127.0.0.1", port: 19999, maxEventSize: -1 })).toThrow(
      /maxEventSize must fit in uint32/,
    );
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
