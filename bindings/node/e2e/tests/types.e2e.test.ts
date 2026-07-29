// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from "vitest";
import type { ClientConfig } from "../../src/types.js";
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
  readTimeoutS: 1,
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
    const gtid = await mysql.getCurrentGtid();
    collector = new StreamingCollector({ ...CLIENT_CONFIG, startGtid: gtid });
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
    const nullValues = Object.values(ev.after!).filter((v) => v === null);
    expect(nullValues.length).toBeGreaterThan(0);

    // Column key assertions (users table)
    expect(ev.after!.id).toBeDefined();
    expect(ev.after!.name).toBeDefined();
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
    expect(typeof ev.after!.value).toBe("number");

    // Column key assertions (items: id, name, value)
    expect(ev.after!.id).toBeDefined();
    expect(ev.after!.name).toBeDefined();
    expect(ev.after!.value).toBe(2147483647);
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
    expect(String(ev.after!.name)).toContain("\u65E5\u672C\u8A9E");

    // Column key assertions (items: id, name, value)
    expect(ev.after!.id).toBeDefined();
    expect(ev.after!.name).toBeDefined();
    expect(ev.after!.value).toBeDefined();
  });

  it("DOUBLE, DECIMAL, and temporal columns use their documented types", async () => {
    await mysql.insert("users", { name: "DoubleUser", balance: "1234.56", score: 3.14159 });

    const events = await collector.waitForEvents({
      table: "users",
      type: "INSERT",
      count: 1,
      timeout: 10_000,
    });

    expect(events.length).toBeGreaterThanOrEqual(1);
    const ev = events[0]!;
    expect(ev.after).not.toBeNull();
    expect(typeof ev.after!.score).toBe("number");
    expect(typeof ev.after!.balance).toBe("string");
    expect(typeof ev.after!.created_at).toBe("string");
    expect(typeof ev.after!.updated_at).toBe("string");

    // Column key assertions (users table)
    expect(ev.after!.id).toBeDefined();
    expect(ev.after!.name).toBeDefined();
    expect(ev.after!.score).toBeDefined();
  });

  it("distinguishes BINARY bytes from LONGTEXT", async () => {
    const binaryValue = Buffer.from([...Array(16).keys()]);
    await mysql.execute("DELETE FROM charset_values");
    await mysql.execute(
      "INSERT INTO charset_values (id, binary_value, text_value) VALUES (?, ?, ?)",
      [1, binaryValue, "charset metadata text"],
    );

    const events = await collector.waitForEvents({
      table: "charset_values",
      type: "INSERT",
      count: 1,
      timeout: 10_000,
    });
    const values = Object.values(events[0]!.after!);
    const bytes = values.find((value) => value instanceof Uint8Array);
    expect(bytes).toBeInstanceOf(Uint8Array);
    expect(Buffer.from(bytes as Uint8Array)).toEqual(binaryValue);
    expect(values).toContain("charset metadata text");
  });

  it("maps ENUM, SET, BIT, and overflowing unsigned BIGINT precisely", async () => {
    await mysql.execute("DELETE FROM type_mapping_values");
    await mysql.execute(
      "INSERT INTO type_mapping_values " +
        "(id, enum_value, set_value, bit_value, unsigned_value) " +
        "VALUES (1, 'second', 'a,c', b'10101010', 18446744073709551615)",
    );

    const events = await collector.waitForEvents({
      table: "type_mapping_values",
      type: "INSERT",
      count: 1,
      timeout: 10_000,
    });
    expect(events[0]!.after).toMatchObject({
      enum_value: 2,
      set_value: 5,
      bit_value: 170,
      unsigned_value: "18446744073709551615",
    });
  });
});
