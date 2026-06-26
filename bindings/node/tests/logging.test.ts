// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

import { afterEach, describe, expect, it } from "vitest";
import { LogLevel, setLogCallback } from "../src/logging.js";

describe("setLogCallback", () => {
  afterEach(() => {
    // Always detach so a registered callback never leaks into later tests.
    setLogCallback(null);
  });

  it("exposes the log level constants", () => {
    expect(LogLevel.Error).toBe(0);
    expect(LogLevel.Warn).toBe(1);
    expect(LogLevel.Info).toBe(2);
    expect(LogLevel.Debug).toBe(3);
  });

  it("accepts a handler without throwing", () => {
    expect(() => setLogCallback(() => {}, LogLevel.Debug)).not.toThrow();
  });

  it("accepts null to detach without throwing", () => {
    setLogCallback(() => {});
    expect(() => setLogCallback(null)).not.toThrow();
  });

  it("can be re-registered repeatedly", () => {
    expect(() => {
      setLogCallback(() => {});
      setLogCallback(() => {});
      setLogCallback(null);
    }).not.toThrow();
  });

  it("rejects a non-function, non-null argument", () => {
    // @ts-expect-error intentional misuse
    expect(() => setLogCallback(42)).toThrow();
  });
});
