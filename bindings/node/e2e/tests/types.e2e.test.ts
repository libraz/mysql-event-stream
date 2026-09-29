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
    const rowId = await mysql.insert("users", { name: "NullUser" });

    const events = await collector.waitForEvents({
      table: "users",
      type: "INSERT",
      count: 1,
      timeout: 10_000,
    });

    expect(events.length).toBeGreaterThanOrEqual(1);
    const ev = events[0]!;
    expect(ev.after).not.toBeNull();
    // Exactly the columns the INSERT left NULL decode as null; a shifted NULL
    // bitmap would null a written column or leave an unwritten one non-null.
    expect(ev.after!.id).toBe(rowId);
    expect(ev.after!.name).toBe("NullUser");
    expect(ev.after!.is_active).toBe(1);
    for (const column of ["email", "age", "balance", "score", "bio", "avatar"]) {
      expect(ev.after![column]).toBeNull();
    }
  });

  it("INT column values are decoded as integers", async () => {
    const intRowId = await mysql.insert("items", { name: "int_test", value: 2147483647 });

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

    expect(ev.after!.id).toBe(intRowId);
    expect(ev.after!.name).toBe("int_test");
    expect(ev.after!.value).toBe(2147483647);
  });

  it("UTF-8 strings including CJK characters are correctly decoded", async () => {
    const utf8RowId = await mysql.insert("items", {
      name: "\u65E5\u672C\u8A9E\u30C6\u30B9\u30C8",
      value: 1,
    });

    const events = await collector.waitForEvents({
      table: "items",
      type: "INSERT",
      count: 1,
      timeout: 10_000,
    });

    expect(events.length).toBeGreaterThanOrEqual(1);
    const ev = events[0]!;
    expect(ev.after).not.toBeNull();
    expect(ev.after!.id).toBe(utf8RowId);
    expect(ev.after!.name).toBe("\u65E5\u672C\u8A9E\u30C6\u30B9\u30C8");
    expect(ev.after!.value).toBe(1);
  });

  it("DOUBLE, DECIMAL, and temporal columns use their documented types", async () => {
    const doubleRowId = await mysql.insert("users", {
      name: "DoubleUser",
      balance: "1234.56",
      score: 3.14159,
    });

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

    expect(ev.after!.id).toBe(doubleRowId);
    expect(ev.after!.name).toBe("DoubleUser");
    expect(ev.after!.score).toBeCloseTo(3.14159);
    expect(ev.after!.balance).toBe("1234.56");
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
