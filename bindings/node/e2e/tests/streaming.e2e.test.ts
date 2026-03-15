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
  serverId: 100,
  startGtid: "",
  connectTimeoutS: 10,
  readTimeoutS: 30,
};

describe("BinlogClient streaming", () => {
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

  it("captures INSERT events via BinlogClient", async () => {
    await mysql.insert("items", { name: "streamed_item", value: 42 });

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
  });

  it("captures UPDATE events via BinlogClient", async () => {
    const rowId = await mysql.insert("items", { name: "original", value: 10 });

    collector.clearEvents();
    await mysql.update("items", `id = ${rowId}`, { name: "updated", value: 20 });

    const events = await collector.waitForEvents({
      table: "items",
      type: "UPDATE",
      count: 1,
      timeout: 10_000,
    });

    expect(events.length).toBeGreaterThanOrEqual(1);
    const ev = events[0]!;
    expect(ev.type).toBe("UPDATE");
    expect(ev.before).not.toBeNull();
    expect(ev.after).not.toBeNull();
  });

  it("captures DELETE events via BinlogClient", async () => {
    const rowId = await mysql.insert("items", { name: "to_delete", value: 99 });

    collector.clearEvents();
    await mysql.delete("items", `id = ${rowId}`);

    const events = await collector.waitForEvents({
      table: "items",
      type: "DELETE",
      count: 1,
      timeout: 10_000,
    });

    expect(events.length).toBeGreaterThanOrEqual(1);
    const ev = events[0]!;
    expect(ev.type).toBe("DELETE");
    expect(ev.before).not.toBeNull();
    expect(ev.after).toBeNull();
  });
});
