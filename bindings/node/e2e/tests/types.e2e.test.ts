// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from "vitest";
import type { ClientConfig, ColumnValue } from "../../src/types.js";
import { MysqlClient } from "../lib/mysql-client.js";
import { StreamingCollector } from "../lib/streaming-collector.js";
import { waitUntil } from "../lib/wait.js";

const CLIENT_CONFIG: ClientConfig = {
  host: "127.0.0.1",
  port: 13307,
  user: "root",
  password: "test_root_password",
  serverId: 104,
  startGtid: "",
  connectTimeoutS: 10,
  readTimeoutS: 30,
};

describe("Column type handling", () => {
  let mysql: MysqlClient;
  let collector: StreamingCollector;

  beforeAll(async () => {
    mysql = new MysqlClient();
    await waitUntil(() => mysql.ping(), {
      timeout: 60_000,
      interval: 2_000,
      description: "MySQL to be ready",
    });
  });

  beforeEach(async () => {
    await mysql.truncate("items");
    await mysql.truncate("users");
    collector = new StreamingCollector(CLIENT_CONFIG);
    await collector.start();
  });

  afterEach(async () => {
    await collector.stop();
  });

  afterAll(async () => {
    await mysql.close();
  });

  it("NULL column values are correctly detected", async () => {
    await mysql.insert("users", { name: "NullUser" });

    const events = await collector.waitForEvents({
      table: "users",
      type: "INSERT",
      count: 1,
      timeout: 10_000,
    });

    expect(events.length).toBeGreaterThanOrEqual(1);
    const ev = events[0]!;
    expect(ev.after).not.toBeNull();
    const nullCols = ev.after!.filter((c: ColumnValue) => c.type === "null");
    expect(nullCols.length).toBeGreaterThan(0);
  });

  it("INT column values are decoded as integers", async () => {
    await mysql.insert("items", { name: "int_test", value: 2147483647 });

    const events = await collector.waitForEvents({
      table: "items",
      type: "INSERT",
      count: 1,
      timeout: 10_000,
    });

    expect(events.length).toBeGreaterThanOrEqual(1);
    const ev = events[0]!;
    expect(ev.after).not.toBeNull();
    const intCols = ev.after!.filter((c: ColumnValue) => c.type === "int");
    expect(intCols.length).toBeGreaterThan(0);
  });

  it("UTF-8 strings including CJK characters are correctly decoded", async () => {
    await mysql.insert("items", { name: "\u65E5\u672C\u8A9E\u30C6\u30B9\u30C8", value: 1 });

    const events = await collector.waitForEvents({
      table: "items",
      type: "INSERT",
      count: 1,
      timeout: 10_000,
    });

    expect(events.length).toBeGreaterThanOrEqual(1);
    const ev = events[0]!;
    expect(ev.after).not.toBeNull();
    const stringCols = ev.after!.filter((c: ColumnValue) => c.type === "string" && c.value);
    const hasJapanese = stringCols.some((c: ColumnValue) =>
      String(c.value).includes("\u65E5\u672C\u8A9E"),
    );
    expect(hasJapanese).toBe(true);
  });

  it("DOUBLE column values are decoded correctly", async () => {
    await mysql.insert("users", { name: "DoubleUser", score: 3.14159 });

    const events = await collector.waitForEvents({
      table: "users",
      type: "INSERT",
      count: 1,
      timeout: 10_000,
    });

    expect(events.length).toBeGreaterThanOrEqual(1);
    const ev = events[0]!;
    expect(ev.after).not.toBeNull();
    const doubleCols = ev.after!.filter((c: ColumnValue) => c.type === "double");
    expect(doubleCols.length).toBeGreaterThan(0);
  });
});
