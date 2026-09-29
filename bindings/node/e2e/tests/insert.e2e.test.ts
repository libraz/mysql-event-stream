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
  serverId: 101,
  startGtid: "",
  connectTimeoutS: 10,
  readTimeoutS: 1,
};

describe("INSERT events", () => {
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

  it("simple INSERT produces an INSERT ChangeEvent", async () => {
    const rowId = await mysql.insert("items", { name: "test_item", value: 42 });
    expect(rowId).toBeGreaterThan(0);

    const events = await collector.waitForEvents({
      table: "items",
      type: "INSERT",
      count: 1,
      timeout: 10_000,
    });

    expect(events.length).toBeGreaterThanOrEqual(1);
    const ev = events[0]!;
    expect(ev.database).toBe("mes_test");
    expect(ev.table).toBe("items");
    expect(ev.type).toBe("INSERT");
    expect(ev.before).toBeNull();
    expect(ev.after).not.toBeNull();
    // The after-image must equal the literal value the INSERT wrote --
    // toBeDefined() would pass for null or a value in the wrong column.
    expect(ev.after!.id).toBe(rowId);
    expect(ev.after!.name).toBe("test_item");
    expect(ev.after!.value).toBe(42);
  });

  it("multiple INSERTs produce multiple INSERT ChangeEvents, each carrying its own row", async () => {
    const rowIds = [
      await mysql.insert("items", { name: "item_a", value: 1 }),
      await mysql.insert("items", { name: "item_b", value: 2 }),
      await mysql.insert("items", { name: "item_c", value: 3 }),
    ];

    const events = await collector.waitForEvents({
      table: "items",
      type: "INSERT",
      count: 3,
      timeout: 10_000,
    });

    expect(events).toHaveLength(3);
    const rows = [
      { name: "item_a", value: 1 },
      { name: "item_b", value: 2 },
      { name: "item_c", value: 3 },
    ];
    events.forEach((ev, index) => {
      expect(ev.after!.id).toBe(rowIds[index]);
      expect(ev.after!.name).toBe(rows[index]!.name);
      expect(ev.after!.value).toBe(rows[index]!.value);
    });
  });

  it("INSERT with various column types", async () => {
    const rowId = await mysql.insert("users", {
      name: "Alice",
      email: "alice@example.com",
      age: 30,
      balance: "1234.56",
      score: 3.14,
      is_active: 1,
      bio: "Hello, world!",
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
    // Each column must equal the literal value the INSERT wrote.
    expect(ev.after!.id).toBe(rowId);
    expect(ev.after!.name).toBe("Alice");
    expect(ev.after!.email).toBe("alice@example.com");
    expect(ev.after!.age).toBe(30);
    expect(ev.after!.balance).toBe("1234.56");
    expect(ev.after!.score).toBe(3.14);
    expect(ev.after!.is_active).toBe(1);
    expect(ev.after!.bio).toBe("Hello, world!");
  });
});
