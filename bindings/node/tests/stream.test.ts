// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it, vi } from "vitest";
import { CdcStream } from "../src/stream.js";
import { MesErrorCode } from "../src/types.js";

// Shared spies for the reconnect tests below. Hoisted so the vi.mock factories
// (which are themselves hoisted above imports) can reference them.
const mocks = vi.hoisted(() => ({
  clientCtor: vi.fn(),
  startImpl: vi.fn(),
}));

vi.mock("../src/client.js", () => ({
  BinlogClient: class {
    constructor(config: unknown) {
      mocks.clientCtor(config);
    }
    start(): void {
      mocks.startImpl();
    }
    get currentGtid(): string {
      return "";
    }
    stop(): void {}
    disconnect(): void {}
    destroy(): void {}
  },
}));

vi.mock("../src/engine.js", () => ({
  CdcEngine: class {
    enableMetadata(): void {}
    feed(): number {
      return 0;
    }
    nextEvent(): null {
      return null;
    }
    reset(): void {}
    destroy(): void {}
  },
}));

describe("CdcStream", () => {
  it("should create with config", () => {
    const stream = new CdcStream({
      host: "127.0.0.1",
      port: 3306,
      user: "root",
    });
    expect(stream).toBeDefined();
  });

  it("configure should update config before streaming", () => {
    const stream = new CdcStream({ host: "127.0.0.1" });
    // configure before iteration should not throw
    expect(() => stream.configure({ port: 3307 })).not.toThrow();
  });

  it("currentGtid should return empty string before streaming", () => {
    const stream = new CdcStream({ host: "127.0.0.1" });
    expect(stream.currentGtid).toBe("");
  });

  it("close should be safe before streaming starts", async () => {
    const stream = new CdcStream({ host: "127.0.0.1" });
    await expect(stream.close()).resolves.toBeUndefined();
  });

  it("close should be idempotent", async () => {
    const stream = new CdcStream({ host: "127.0.0.1" });
    await stream.close();
    await expect(stream.close()).resolves.toBeUndefined();
  });

  it("should implement AsyncDisposable", () => {
    const stream = new CdcStream({ host: "127.0.0.1" });
    expect(stream[Symbol.asyncDispose]).toBeDefined();
    expect(typeof stream[Symbol.asyncDispose]).toBe("function");
  });

  describe("reconnect policy", () => {
    it("fails fast on an auth error without retrying", async () => {
      mocks.clientCtor.mockClear();
      mocks.startImpl.mockReset();
      mocks.startImpl.mockImplementation(() => {
        const err: Error & { code?: number } = new Error("auth failed");
        err.code = MesErrorCode.Auth;
        throw err;
      });

      const stream = new CdcStream({
        host: "127.0.0.1",
        user: "root",
        maxReconnectAttempts: 10,
      });

      await expect(async () => {
        for await (const _ of stream) {
          // no events expected
        }
      }).rejects.toThrow("auth failed");

      // A non-retryable error must surface after the first attempt only.
      expect(mocks.clientCtor).toHaveBeenCalledTimes(1);
      expect(mocks.startImpl).toHaveBeenCalledTimes(1);
      await stream.close();
    });

    it("retries a transient (non-auth) error", async () => {
      mocks.clientCtor.mockClear();
      mocks.startImpl.mockReset();
      let attempts = 0;
      mocks.startImpl.mockImplementation(() => {
        attempts++;
        const err: Error & { code?: number } = new Error("stream error");
        err.code = MesErrorCode.Stream;
        // Throw twice (one retry), then give up after maxReconnectAttempts.
        throw err;
      });

      const stream = new CdcStream({
        host: "127.0.0.1",
        user: "root",
        maxReconnectAttempts: 1,
      });

      await expect(async () => {
        for await (const _ of stream) {
          // no events expected
        }
      }).rejects.toThrow("stream error");

      // With one allowed retry, start() runs the initial attempt plus one retry.
      expect(attempts).toBe(2);
      await stream.close();
    });
  });
});
