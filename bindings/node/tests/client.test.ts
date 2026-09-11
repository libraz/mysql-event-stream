// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from "vitest";
import { BinlogClient } from "../src/client.js";
import { loadNativeAddon } from "../src/native.js";
import { MesErrorCode, type SslMode } from "../src/types.js";

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
  it("exports the client class unconditionally, with no build-capability flag", () => {
    // OpenSSL is a required dependency of the core, so there is no build of
    // this addon without the client. A flag saying otherwise would describe a
    // configuration no supported build can produce.
    const addon = loadNativeAddon<Record<string, unknown>>();
    expect(typeof addon.BinlogClient).toBe("function");
    expect(Object.keys(addon)).not.toContain("hasClient");
  });

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

  it("keeps the password out of both addon config paths", () => {
    // The addon stages the credential in a C++ string for the C ABI and wipes
    // that copy when it goes out of scope. Freed memory is not observable from
    // JS; what is observable is that both ways out of config parsing still
    // behave — the failed connect and the validation rejection that returns
    // before the C ABI is reached — and that neither surfaces the password.
    const password = "staged-secret-not-for-errors";

    let connectError: unknown;
    try {
      new BinlogClient({ host: "127.0.0.1", port: 19999, password });
    } catch (error) {
      connectError = error;
    }
    expect(connectError).toBeDefined();
    expect(String((connectError as Error).message)).not.toContain(password);

    let rejectionError: unknown;
    try {
      new BinlogClient({
        host: "127.0.0.1",
        port: 19999,
        password,
        sslMode: 99 as unknown as SslMode,
      });
    } catch (error) {
      rejectionError = error;
    }
    expect(rejectionError).toMatchObject({ code: MesErrorCode.InvalidArg });
    expect(String((rejectionError as Error).message)).not.toContain(password);
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

  it("defers the native destroy until an in-flight poll completes", async () => {
    // The worker holds the raw mes_client_t* on the libuv thread pool, so
    // destroy() must not release it while pending_workers_ > 0. maxQueueBytes
    // reads through the handle: it is positive while the handle lives and
    // zero once it has been released.
    const native = loadNativeAddon<{ BinlogClient: new () => NativeClientForTest }>();
    const client = new native.BinlogClient();
    expect(client.maxQueueBytes).toBeGreaterThan(0);

    const pendingPoll = client.poll();
    expect(() => client.destroy()).not.toThrow();
    expect(client.maxQueueBytes).toBeGreaterThan(0);

    await expect(pendingPoll).rejects.toBeDefined();

    // The last worker completing on the main thread runs the deferred destroy.
    expect(client.maxQueueBytes).toBe(0);
    expect(client.isConnected).toBe(false);

    // Exactly once: a second destroy() is a no-op, not a double free.
    expect(() => client.destroy()).not.toThrow();
    expect(client.maxQueueBytes).toBe(0);
  });

  it("validates native pollBatch capacity", async () => {
    const native = loadNativeAddon<{ BinlogClient: new () => NativeClientForTest }>();
    const client = new native.BinlogClient();
    try {
      await expect(client.pollBatch(0)).rejects.toThrow("maxEvents must be an integer");
      await expect(client.pollBatch(0)).rejects.toMatchObject({ code: MesErrorCode.InvalidArg });
    } finally {
      client.destroy();
    }
  });

  // The stream layer treats a rejection it cannot classify as retryable, so a
  // lifecycle violation without a code burns the whole reconnect budget, and
  // an empty native message leaves the operator with no diagnosis at all.
  it("rejects a poll on a client that never started, with a code and a reason", async () => {
    const native = loadNativeAddon<{ BinlogClient: new () => NativeClientForTest }>();
    const client = new native.BinlogClient();
    try {
      // Sequentially: a second in-flight poll would be rejected as a
      // concurrent poll instead of reaching the disconnected path.
      for (const start of [() => client.poll(), () => client.pollBatch(4)]) {
        const error = (await start().then(
          () => undefined,
          (e: unknown) => e,
        )) as (Error & { code?: number }) | undefined;
        expect(error?.code).toBe(MesErrorCode.Disconnected);
        expect(error?.message).toMatch(/failed: \S/);
      }
    } finally {
      client.destroy();
    }
  });

  it("rejects poll and pollBatch on a destroyed native client with a code", async () => {
    const native = loadNativeAddon<{ BinlogClient: new () => NativeClientForTest }>();
    const client = new native.BinlogClient();
    client.destroy();
    await expect(client.poll()).rejects.toMatchObject({
      code: MesErrorCode.InvalidArg,
      message: "Client has been destroyed",
    });
    await expect(client.pollBatch(4)).rejects.toMatchObject({ code: MesErrorCode.InvalidArg });
  });

  it("throws a coded error from every method of a destroyed client", () => {
    const client = Object.create(BinlogClient.prototype) as BinlogClient;
    for (const call of [
      () => client.start(),
      () => client.poll(),
      () => client.pollBatch(),
    ] as const) {
      expect(call).toThrow("Client has been destroyed");
      try {
        call();
      } catch (error) {
        expect(error).toMatchObject({ code: MesErrorCode.InvalidArg, name: "MesError" });
      }
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
