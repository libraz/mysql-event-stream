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
  serverId: 102,
  startGtid: "",
  connectTimeoutS: 10,
  readTimeoutS: 30,
};

describe("UPDATE events", () => {
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

  it("simple UPDATE produces an UPDATE ChangeEvent with before and after", async () => {
    const rowId = await mysql.insert("items", {
      name: "original",
      value: 10,
    });

    collector.clearEvents();
    await mysql.update("items", `id = ${rowId}`, {
      name: "updated",
      value: 20,
    });

    const events = await collector.waitForEvents({
      table: "items",
      type: "UPDATE",
      count: 1,
      timeout: 10_000,
    });

    expect(events.length).toBeGreaterThanOrEqual(1);
    const ev = events[0]!;
    expect(ev.database).toBe("mes_test");
    expect(ev.table).toBe("items");
    expect(ev.type).toBe("UPDATE");
    expect(ev.before).not.toBeNull();
    expect(ev.after).not.toBeNull();
  });

  it("UPDATE multiple columns in users table", async () => {
    await mysql.insert("users", {
      name: "Bob",
      email: "bob@example.com",
      age: 25,
      is_active: 1,
    });

    collector.clearEvents();
    await mysql.update("users", "name = 'Bob'", {
      email: "bob_new@example.com",
      age: 26,
    });

    const events = await collector.waitForEvents({
      table: "users",
      type: "UPDATE",
      count: 1,
      timeout: 10_000,
    });

    expect(events.length).toBeGreaterThanOrEqual(1);
    const ev = events[0]!;
    expect(ev.before).not.toBeNull();
    expect(ev.after).not.toBeNull();
    expect(ev.before!.length).toBe(ev.after!.length);
  });
});
