// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from "vitest";
import { CdcStream } from "../src/stream.js";

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
});
