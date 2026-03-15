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
  serverId: 103,
  startGtid: "",
  connectTimeoutS: 10,
  readTimeoutS: 30,
};

describe("DELETE events", () => {
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

  it("simple DELETE produces a DELETE ChangeEvent with before image only", async () => {
    const rowId = await mysql.insert("items", {
      name: "to_delete",
      value: 99,
    });

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
    expect(ev.database).toBe("mes_test");
    expect(ev.table).toBe("items");
    expect(ev.type).toBe("DELETE");
    expect(ev.before).not.toBeNull();
    expect(ev.after).toBeNull();
  });

  it("DELETE multiple rows produces one ChangeEvent per row", async () => {
    await mysql.insert("items", { name: "del_a", value: 1 });
    await mysql.insert("items", { name: "del_b", value: 2 });
    await mysql.insert("items", { name: "del_c", value: 3 });

    collector.clearEvents();
    await mysql.delete("items", "value IN (1, 2, 3)");

    const events = await collector.waitForEvents({
      table: "items",
      type: "DELETE",
      count: 3,
      timeout: 10_000,
    });

    expect(events).toHaveLength(3);
    for (const ev of events) {
      expect(ev.before).not.toBeNull();
      expect(ev.after).toBeNull();
    }
  });
});
