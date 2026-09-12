// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it, vi } from "vitest";
import { NON_RETRYABLE_ERROR_CODES } from "../src/contract.js";
import { CdcStream } from "../src/stream.js";
import { type ChangeEvent, MesErrorCode, type StreamConfig } from "../src/types.js";
import { loadBindingContract } from "./contract-fixture.js";

const contract = loadBindingContract();

/** One poll result, shaped as the client hands one to the stream. */
interface PollResult {
  data: Uint8Array | null;
  isHeartbeat: boolean;
}

// Shared spies for the reconnect tests below. Hoisted so the vi.mock factories
// (which are themselves hoisted above imports) can reference them.
//
// Each spy is annotated with the signature it stands in for, so a mocked return
// value that the real surface could never produce fails here.
const mocks = vi.hoisted(() => ({
  clientCtor: vi.fn<(config: StreamConfig) => void>(),
  startImpl: vi.fn(),
  // Returns the next poll result. Default: a single null-data poll that keeps
  // the loop idle (tests that need data override this).
  pollImpl: vi.fn<() => Promise<PollResult>>(() =>
    Promise.resolve({ data: null, isHeartbeat: false }),
  ),
  // Records the chunk fed and returns the number of bytes consumed. Default:
  // consume everything. Tests simulating backpressure override this.
  feedImpl: vi.fn((chunk: Uint8Array) => chunk.length),
  checksumImpl: vi.fn(),
  maxEventSizeImpl: vi.fn(),
  maxQueueSizeImpl: vi.fn(),
  includeDatabasesImpl: vi.fn(),
  includeTablesImpl: vi.fn(),
  excludeTablesImpl: vi.fn(),
  stopImpl: vi.fn(),
  currentGtidImpl: vi.fn(() => ""),
  nextEventImpl: vi.fn<() => ChangeEvent | null>(() => null),
}));

vi.mock("../src/client.js", () => ({
  BinlogClient: class {
    constructor(config: StreamConfig) {
      mocks.clientCtor(config);
    }
    start(): void {
      mocks.startImpl();
    }
    poll(): Promise<PollResult> {
      return mocks.pollImpl();
    }
    pollBatch(): Promise<PollResult[]> {
      return this.poll().then((result) => [result]);
    }
    get currentGtid(): string {
      return mocks.currentGtidImpl();
    }
    get checksumEnabled(): boolean {
      return true;
    }
    stop(): void {
      mocks.stopImpl();
    }
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
    nextEvent(): ChangeEvent | null {
      return mocks.nextEventImpl();
    }
    setChecksumEnabled(enabled: boolean): void {
      mocks.checksumImpl(enabled);
    }
    setMaxEventSize(maxEventSize: number): void {
      mocks.maxEventSizeImpl(maxEventSize);
    }
    setMaxQueueSize(maxQueueSize: number): void {
      mocks.maxQueueSizeImpl(maxQueueSize);
    }
    setIncludeDatabases(databases: string[]): void {
      mocks.includeDatabasesImpl(databases);
    }
    setIncludeTables(tables: string[]): void {
      mocks.includeTablesImpl(tables);
    }
    setExcludeTables(tables: string[]): void {
      mocks.excludeTablesImpl(tables);
    }
    reset(): void {}
    destroy(): void {}
  },
}));

/**
 * A decoded row event, complete in every field the engine populates.
 *
 * Built here rather than as a partial literal per test: the stream forwards
 * whatever the engine returns, so an event fixture missing a field would let a
 * delivery assertion agree with itself while the real shape had moved on.
 *
 * @param overrides Fields a test cares about, replacing the defaults.
 */
function rowEvent(overrides: Partial<ChangeEvent> = {}): ChangeEvent {
  return {
    type: "INSERT",
    database: "db",
    table: "t",
    before: null,
    after: { id: 1 },
    timestamp: 0,
    position: { file: "binlog.000001", offset: 4 },
    namesResolved: true,
    sourceSql: "",
    ...overrides,
  };
}

/**
 * Config handed to the nth `BinlogClient` construction.
 *
 * @throws If the client was never constructed that many times, which would
 * otherwise read as an assertion against `undefined`.
 */
function constructedConfig(index: number): StreamConfig {
  const call = mocks.clientCtor.mock.calls[index];
  if (call === undefined) {
    throw new Error(`BinlogClient was constructed fewer than ${index + 1} times`);
  }
  return call[0];
}

describe("CdcStream", () => {
  it("should create with config", () => {
    const stream = new CdcStream({
      host: "127.0.0.1",
      port: 3306,
      user: "root",
    });
    expect(stream).toBeDefined();
  });

  it("does not enumerate credentials with the stream object", () => {
    const stream = new CdcStream({ host: "127.0.0.1", password: "not-for-logs" });
    expect(Object.keys(stream)).not.toContain("config");
    expect(JSON.stringify(stream)).not.toContain("not-for-logs");
  });

  it("applies exact-match filters when starting", async () => {
    mocks.includeDatabasesImpl.mockClear();
    mocks.includeTablesImpl.mockClear();
    mocks.excludeTablesImpl.mockClear();
    mocks.startImpl.mockReset();
    mocks.startImpl.mockImplementation(() => {});
    mocks.pollImpl.mockReset();
    mocks.pollImpl.mockResolvedValueOnce({ data: new Uint8Array([1]), isHeartbeat: false });
    mocks.nextEventImpl.mockReset();
    mocks.nextEventImpl.mockReturnValueOnce(rowEvent()).mockReturnValue(null);

    const stream = new CdcStream({
      host: "127.0.0.1",
      includeDatabases: ["mydb"],
      includeTables: ["mydb.orders"],
      excludeTables: ["mydb.audit_log"],
    });
    for await (const _ of stream) break;

    expect(mocks.includeDatabasesImpl).toHaveBeenCalledWith(["mydb"]);
    expect(mocks.includeTablesImpl).toHaveBeenCalledWith(["mydb.orders"]);
    expect(mocks.excludeTablesImpl).toHaveBeenCalledWith(["mydb.audit_log"]);
  });

  it("configure should update config before streaming", () => {
    const stream = new CdcStream({ host: "127.0.0.1" });
    // configure before iteration should not throw
    expect(() => stream.configure({ port: 3307 })).not.toThrow();
  });

  it("configure rejects unknown runtime keys", () => {
    const stream = new CdcStream({ host: "127.0.0.1" });
    expect(() => stream.configure({ unknown: true } as never)).toThrow("Unknown config key");
  });

  it("codes every lifecycle refusal the stream raises", async () => {
    const stream = new CdcStream({ host: "127.0.0.1" });
    // Taking the iterator marks the stream as started without running the
    // generator body, which is what both refusals below react to.
    stream[Symbol.asyncIterator]();

    const refusals: Array<{ label: string; call: () => void }> = [
      { label: "configure after start", call: () => stream.configure({ port: 3307 }) },
      { label: "a second iteration", call: () => stream[Symbol.asyncIterator]() },
    ];
    for (const { label, call } of refusals) {
      let thrown: unknown;
      try {
        call();
      } catch (error) {
        thrown = error;
      }
      const rejection = thrown as (Error & { code?: unknown }) | undefined;
      expect(rejection, label).toBeDefined();
      expect(rejection?.code, `${label} carries a code`).toBe(MesErrorCode.InvalidArg);
    }
    await stream.close();
  });

  it("currentGtid should return empty string before streaming", () => {
    const stream = new CdcStream({ host: "127.0.0.1" });
    expect(stream.currentGtid).toBe("");
  });

  it("retains currentGtid after a for-await break cleans up the client", async () => {
    mocks.startImpl.mockReset();
    mocks.startImpl.mockImplementation(() => {});
    mocks.pollImpl.mockReset();
    mocks.pollImpl.mockResolvedValueOnce({ data: new Uint8Array([1]), isHeartbeat: false });
    mocks.feedImpl.mockReset();
    mocks.feedImpl.mockImplementation((chunk: Uint8Array) => chunk.length);
    mocks.nextEventImpl.mockReset();
    const event = rowEvent();
    mocks.nextEventImpl.mockReturnValueOnce(event).mockReturnValue(null);
    mocks.currentGtidImpl.mockReset();
    mocks.currentGtidImpl.mockReturnValue("uuid:1-42");

    const stream = new CdcStream({ host: "127.0.0.1" });
    for await (const received of stream) {
      expect(received).toEqual(event);
      break;
    }

    expect(stream.currentGtid).toBe("uuid:1-42");
    mocks.currentGtidImpl.mockReturnValue("");
  });

  it("reads the native checkpoint per poll batch, not per row event", async () => {
    mocks.startImpl.mockReset();
    mocks.startImpl.mockImplementation(() => {});
    mocks.pollImpl.mockReset();
    mocks.pollImpl
      .mockResolvedValueOnce({ data: new Uint8Array([1]), isHeartbeat: false })
      .mockImplementation(
        () =>
          new Promise((resolve) =>
            setTimeout(() => resolve({ data: null, isHeartbeat: false }), 5),
          ),
      );
    mocks.feedImpl.mockReset();
    mocks.feedImpl.mockImplementation((chunk: Uint8Array) => chunk.length);
    mocks.nextEventImpl.mockReset();
    const event = rowEvent();
    mocks.nextEventImpl
      .mockReturnValueOnce(event)
      .mockReturnValueOnce(event)
      .mockReturnValueOnce(event)
      .mockReturnValue(null);
    mocks.currentGtidImpl.mockReset();
    mocks.currentGtidImpl.mockReturnValue("uuid:1-7");

    const stream = new CdcStream({ host: "127.0.0.1" });
    let delivered = 0;
    let readsWhileIterating = 0;
    for await (const _ of stream) {
      delivered++;
      readsWhileIterating = mocks.currentGtidImpl.mock.calls.length;
      if (delivered === 3) break;
    }

    expect(delivered).toBe(3);
    // One batch delivered all three events, so one native read covers them all.
    expect(readsWhileIterating).toBeLessThanOrEqual(
      contract.checkpointRetention.maxNativeReadsPerPollBatch,
    );
    expect(stream.currentGtid).toBe("uuid:1-7");
    mocks.currentGtidImpl.mockReturnValue("");
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

  it("propagates the client checksum mode to the raw engine", async () => {
    mocks.checksumImpl.mockReset();
    mocks.startImpl.mockReset();
    mocks.startImpl.mockImplementation(() => {});
    mocks.pollImpl.mockReset();
    mocks.pollImpl.mockImplementation(
      () =>
        new Promise((resolve) => setTimeout(() => resolve({ data: null, isHeartbeat: false }), 5)),
    );

    const stream = new CdcStream({ host: "127.0.0.1" });
    const next = stream[Symbol.asyncIterator]().next();
    await vi.waitFor(() => expect(mocks.checksumImpl).toHaveBeenCalledWith(true));
    await stream.close();
    await next;
  });

  it("propagates one event-size limit to the client and raw engine", async () => {
    mocks.clientCtor.mockClear();
    mocks.maxEventSizeImpl.mockClear();
    mocks.maxQueueSizeImpl.mockClear();
    mocks.pollImpl.mockReset();
    mocks.pollImpl.mockImplementation(
      () =>
        new Promise((resolve) => setTimeout(() => resolve({ data: null, isHeartbeat: false }), 5)),
    );

    const stream = new CdcStream({
      host: "127.0.0.1",
      maxEventSize: 128 * 1024 * 1024,
      maxQueueBytes: 512 * 1024 * 1024,
    });
    const next = stream[Symbol.asyncIterator]().next();
    await vi.waitFor(() => {
      expect(mocks.clientCtor).toHaveBeenCalledWith(
        expect.objectContaining({
          maxEventSize: 128 * 1024 * 1024,
          maxQueueBytes: 512 * 1024 * 1024,
        }),
      );
      expect(mocks.maxEventSizeImpl).toHaveBeenCalledWith(128 * 1024 * 1024);
      expect(mocks.maxQueueSizeImpl).toHaveBeenCalledWith(0);
    });
    await stream.close();
    await next;
  });

  it("close stops a pending idle poll before waiting for iterator return", async () => {
    mocks.clientCtor.mockClear();
    mocks.startImpl.mockReset();
    mocks.startImpl.mockImplementation(() => {});
    mocks.pollImpl.mockReset();
    mocks.stopImpl.mockReset();

    let releasePoll: (() => void) | undefined;
    mocks.pollImpl.mockImplementation(
      () =>
        new Promise((resolve) => {
          releasePoll = () => resolve({ data: null, isHeartbeat: false });
        }),
    );
    mocks.stopImpl.mockImplementation(() => releasePoll?.());

    const stream = new CdcStream({ host: "127.0.0.1" });
    const iterator = stream[Symbol.asyncIterator]();
    const nextPromise = iterator.next();
    await vi.waitFor(() => expect(mocks.pollImpl).toHaveBeenCalledTimes(1));

    await expect(stream.close()).resolves.toBeUndefined();
    await expect(nextPromise).resolves.toEqual({ done: true, value: undefined });
    expect(mocks.stopImpl).toHaveBeenCalled();
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

      const fed = fedChunks.map((chunk) => Array.from(chunk));
      expect(fed.length).toBeGreaterThanOrEqual(2);
      // First feed: the raw 10-byte chunk.
      expect(fed[0]).toEqual(Array.from(first));
      // Second feed: leftover 6 bytes (4..9) prepended to the new 6 bytes.
      expect(fed[1]).toEqual([4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]);
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

    it.each([...NON_RETRYABLE_ERROR_CODES])(
      "fails fast on a permanent stream error (%i)",
      async (code) => {
        mocks.clientCtor.mockReset();
        mocks.startImpl.mockReset();
        mocks.startImpl.mockImplementation(() => {
          const err: Error & { code?: number } = new Error("permanent stream error");
          err.code = code;
          throw err;
        });

        const stream = new CdcStream({
          host: "127.0.0.1",
          maxReconnectAttempts: 10,
        });
        await expect(async () => {
          for await (const _ of stream) {
            // no events expected
          }
        }).rejects.toThrow("permanent stream error");

        expect(mocks.clientCtor).toHaveBeenCalledTimes(1);
        expect(mocks.startImpl).toHaveBeenCalledTimes(1);
        await stream.close();
      },
    );

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

    it("charges constructor failures to the same retry budget", async () => {
      mocks.clientCtor.mockReset();
      mocks.startImpl.mockReset();
      mocks.pollImpl.mockReset();
      let constructorAttempts = 0;
      mocks.clientCtor.mockImplementation(() => {
        constructorAttempts++;
        if (constructorAttempts <= 2) throw new Error("connect refused");
      });
      mocks.startImpl.mockImplementation(() => {
        const err: Error & { code?: number } = new Error("auth failed");
        err.code = MesErrorCode.Auth;
        throw err;
      });

      const stream = new CdcStream({
        host: "127.0.0.1",
        maxReconnectAttempts: 2,
      });
      await expect(async () => {
        for await (const _ of stream) {
          // no events expected
        }
      }).rejects.toThrow("auth failed");

      expect(constructorAttempts).toBe(3);
      expect(mocks.startImpl).toHaveBeenCalledTimes(1);
    });

    it("does not reset the budget after accept then immediate poll failure", async () => {
      mocks.clientCtor.mockReset();
      mocks.startImpl.mockReset();
      mocks.startImpl.mockImplementation(() => {});
      mocks.pollImpl.mockReset();
      mocks.pollImpl.mockRejectedValue(new Error("immediate drop"));

      const stream = new CdcStream({
        host: "127.0.0.1",
        maxReconnectAttempts: 1,
      });
      await expect(async () => {
        for await (const _ of stream) {
          // no events expected
        }
      }).rejects.toThrow("immediate drop");

      expect(mocks.clientCtor).toHaveBeenCalledTimes(2);
      expect(mocks.startImpl).toHaveBeenCalledTimes(2);
      expect(mocks.pollImpl).toHaveBeenCalledTimes(2);
    });

    it("keeps the file/position anchor when no checkpoint was published", async () => {
      mocks.clientCtor.mockClear();
      mocks.startImpl.mockReset();
      mocks.startImpl.mockImplementation(() => {});
      mocks.pollImpl.mockReset();
      mocks.pollImpl.mockRejectedValue(new Error("temporary drop"));
      mocks.currentGtidImpl.mockReset();
      mocks.currentGtidImpl.mockReturnValue("");

      const stream = new CdcStream({
        host: "127.0.0.1",
        startBinlogFile: "binlog.000001",
        startBinlogPosition: 4,
        maxReconnectAttempts: 1,
      });
      await expect(async () => {
        for await (const _ of stream) {
          // no events expected
        }
      }).rejects.toThrow("temporary drop");

      expect(mocks.clientCtor).toHaveBeenCalledTimes(2);
      const resumed = constructedConfig(1);
      // An empty checkpoint must never become an empty GTID set: the server
      // answers that with every binlog it still retains.
      expect(resumed.startGtid).toBeUndefined();
      expect(resumed.startBinlogFile).toBe("binlog.000001");
      expect(resumed.startBinlogPosition).toBe(4);
    });

    it("keeps the current-position start mode when the first connect fails", async () => {
      mocks.clientCtor.mockReset();
      let attempts = 0;
      mocks.clientCtor.mockImplementation(() => {
        attempts++;
        if (attempts === 1) throw new Error("connect refused");
      });
      mocks.startImpl.mockReset();
      mocks.startImpl.mockImplementation(() => {
        throw new Error("still down");
      });
      mocks.pollImpl.mockReset();
      mocks.currentGtidImpl.mockReset();
      mocks.currentGtidImpl.mockReturnValue("");

      const stream = new CdcStream({ host: "127.0.0.1", maxReconnectAttempts: 1 });
      await expect(async () => {
        for await (const _ of stream) {
          // no events expected
        }
      }).rejects.toThrow("still down");

      expect(mocks.clientCtor).toHaveBeenCalledTimes(2);
      // The client was never constructed on the first attempt, so there is no
      // checkpoint to resume from. The implicit "snapshot the current position"
      // start mode has to survive intact.
      const retried = constructedConfig(1);
      expect(retried.startGtid).toBeUndefined();
      expect(retried.startBinlogFile).toBeUndefined();
      expect(retried.startBinlogPosition).toBeUndefined();
    });

    it("resumes from a published checkpoint instead of the file/position anchor", async () => {
      mocks.clientCtor.mockReset();
      mocks.startImpl.mockReset();
      mocks.startImpl.mockImplementation(() => {});
      mocks.pollImpl.mockReset();
      mocks.pollImpl.mockRejectedValue(new Error("temporary drop"));
      mocks.currentGtidImpl.mockReset();
      mocks.currentGtidImpl.mockReturnValue("3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5");

      const stream = new CdcStream({
        host: "127.0.0.1",
        startBinlogFile: "binlog.000001",
        startBinlogPosition: 4,
        maxReconnectAttempts: 1,
      });
      await expect(async () => {
        for await (const _ of stream) {
          // no events expected
        }
      }).rejects.toThrow("temporary drop");

      expect(mocks.clientCtor).toHaveBeenCalledTimes(2);
      const resumed = constructedConfig(1);
      expect(resumed.startGtid).toBe("3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5");
      expect(resumed.startBinlogFile).toBeUndefined();
      expect(resumed.startBinlogPosition).toBeUndefined();
    });

    it("keeps an explicitly configured startGtid when no checkpoint was published", async () => {
      mocks.clientCtor.mockReset();
      mocks.startImpl.mockReset();
      mocks.startImpl.mockImplementation(() => {});
      mocks.pollImpl.mockReset();
      mocks.pollImpl.mockRejectedValue(new Error("temporary drop"));
      mocks.currentGtidImpl.mockReset();
      mocks.currentGtidImpl.mockReturnValue("");

      const stream = new CdcStream({
        host: "127.0.0.1",
        startGtid: "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-2",
        maxReconnectAttempts: 1,
      });
      await expect(async () => {
        for await (const _ of stream) {
          // no events expected
        }
      }).rejects.toThrow("temporary drop");

      expect(mocks.clientCtor).toHaveBeenCalledTimes(2);
      expect(constructedConfig(1).startGtid).toBe("3E11FA47-71CA-11E1-9E33-C80AA9429562:1-2");
    });

    it("close interrupts reconnect backoff", async () => {
      mocks.clientCtor.mockReset();
      mocks.startImpl.mockReset();
      mocks.startImpl.mockImplementation(() => {
        throw new Error("temporary outage");
      });
      mocks.pollImpl.mockReset();

      const stream = new CdcStream({
        host: "127.0.0.1",
        maxReconnectAttempts: 10,
      });
      const iterator = stream[Symbol.asyncIterator]();
      const nextPromise = iterator.next();
      await vi.waitFor(() => expect(mocks.startImpl).toHaveBeenCalledTimes(1));

      await expect(stream.close()).resolves.toBeUndefined();
      await expect(nextPromise).resolves.toEqual({ done: true, value: undefined });
      expect(mocks.startImpl).toHaveBeenCalledTimes(1);
    });
  });
});
