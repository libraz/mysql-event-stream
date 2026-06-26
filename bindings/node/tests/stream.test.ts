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
  // Returns the next poll result. Default: a single null-data poll that keeps
  // the loop idle (tests that need data override this).
  pollImpl: vi.fn(() => Promise.resolve({ data: null, isHeartbeat: false })),
  // Records the chunk fed and returns the number of bytes consumed. Default:
  // consume everything. Tests simulating backpressure override this.
  feedImpl: vi.fn((chunk: Uint8Array) => chunk.length),
}));

vi.mock("../src/client.js", () => ({
  BinlogClient: class {
    constructor(config: unknown) {
      mocks.clientCtor(config);
    }
    start(): void {
      mocks.startImpl();
    }
    poll(): Promise<{ data: Uint8Array | null; isHeartbeat: boolean }> {
      return mocks.pollImpl();
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
    feed(chunk: Uint8Array): number {
      return mocks.feedImpl(chunk);
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

  describe("partial feed consumption", () => {
    it("re-feeds unconsumed bytes on the next poll so no data is lost", async () => {
      mocks.clientCtor.mockClear();
      mocks.startImpl.mockReset();
      mocks.startImpl.mockImplementation(() => {});
      mocks.feedImpl.mockReset();
      mocks.pollImpl.mockReset();

      // First poll yields 10 bytes; the engine consumes only 4 (backpressure).
      // Second poll yields 6 more bytes. The leftover 6 from poll #1 must be
      // prepended, so the engine should see a 12-byte chunk on the second feed.
      const first = new Uint8Array([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
      const second = new Uint8Array([10, 11, 12, 13, 14, 15]);
      mocks.pollImpl
        .mockResolvedValueOnce({ data: first, isHeartbeat: false })
        .mockResolvedValueOnce({ data: second, isHeartbeat: false })
        // Afterwards yield empty polls so the loop idles but stays responsive
        // to close() (a real client unblocks poll() via stop()).
        .mockImplementation(
          () =>
            new Promise((resolve) =>
              setTimeout(() => resolve({ data: null, isHeartbeat: false }), 5),
            ),
        );

      const fedChunks: Uint8Array[] = [];
      mocks.feedImpl.mockImplementation((chunk: Uint8Array) => {
        fedChunks.push(chunk.slice());
        // Consume 4 bytes on the first feed, everything on later feeds.
        return fedChunks.length === 1 ? 4 : chunk.length;
      });

      const stream = new CdcStream({ host: "127.0.0.1", user: "root" });
      const iterator = stream[Symbol.asyncIterator]();
      // Drive the loop far enough for both polls and feeds to run.
      const nextPromise = iterator.next();
      await new Promise((resolve) => setTimeout(resolve, 20));
      await stream.close();
      await nextPromise.catch(() => {});

      expect(fedChunks.length).toBeGreaterThanOrEqual(2);
      // First feed: the raw 10-byte chunk.
      expect(Array.from(fedChunks[0])).toEqual(Array.from(first));
      // Second feed: leftover 6 bytes (4..9) prepended to the new 6 bytes.
      expect(Array.from(fedChunks[1])).toEqual([4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]);
    });
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
