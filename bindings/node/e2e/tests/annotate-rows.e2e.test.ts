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
  serverId: 108,
  startGtid: "",
  connectTimeoutS: 10,
  readTimeoutS: 1,
};

const isMariaDB = process.env.DB_FLAVOR === "mariadb";

// ANNOTATE_ROWS is MariaDB-only, and the server withholds it unless the dump
// request asks for it. These tests are the surface proof that the request
// carries that flag: without it sourceSql is permanently empty.
describe.skipIf(!isMariaDB)("MariaDB ANNOTATE_ROWS source SQL", () => {
  let mysql: MysqlClient;
  let collector: StreamingCollector;

  beforeAll(async () => {
    mysql = new MysqlClient();
    await waitUntil(() => mysql.ping(), {
      timeout: 60_000,
      interval: 2_000,
      description: "MariaDB to be ready",
    });
  });

  beforeEach(async () => {
    await mysql.truncate("items");
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

  it("INSERT carries the originating statement", async () => {
    const sql = "INSERT INTO items (name, value) VALUES ('annotate_insert', 1)";
    await mysql.queryText(sql);

    const events = await collector.waitForEvents({
      table: "items",
      type: "INSERT",
      count: 1,
      timeout: 10_000,
    });

    expect(events.length).toBeGreaterThanOrEqual(1);
    expect(events[0]!.sourceSql).toBe(sql);
  });

  it("UPDATE carries the originating statement", async () => {
    await mysql.queryText("INSERT INTO items (name, value) VALUES ('annotate_update', 1)");
    const sql = "UPDATE items SET value = 2 WHERE name = 'annotate_update'";
    await mysql.queryText(sql);

    const events = await collector.waitForEvents({
      table: "items",
      type: "UPDATE",
      count: 1,
      timeout: 10_000,
    });

    expect(events.length).toBeGreaterThanOrEqual(1);
    expect(events[0]!.sourceSql).toBe(sql);
  });

  it("DELETE carries the originating statement", async () => {
    await mysql.queryText("INSERT INTO items (name, value) VALUES ('annotate_delete', 1)");
    const sql = "DELETE FROM items WHERE name = 'annotate_delete'";
    await mysql.queryText(sql);

    const events = await collector.waitForEvents({
      table: "items",
      type: "DELETE",
      count: 1,
      timeout: 10_000,
    });

    expect(events.length).toBeGreaterThanOrEqual(1);
    expect(events[0]!.sourceSql).toBe(sql);
  });
});
