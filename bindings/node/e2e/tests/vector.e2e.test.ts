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
  serverId: 111,
  startGtid: "",
  connectTimeoutS: 10,
  readTimeoutS: 1,
};

const isMariaDB = process.env.DB_FLAVOR === "mariadb";

// VECTOR reaches the Node surface as raw bytes, and TABLE_MAP counts it as a
// binary-charset character column. Both facts are decoded in the C++ core, so
// this is the binding-level proof that they survive the marshalling boundary.
describe.skipIf(isMariaDB)("VECTOR columns", () => {
  let mysql: MysqlClient;
  let collector: StreamingCollector;
  let supportsVector = false;

  beforeAll(async () => {
    mysql = new MysqlClient();
    await waitUntil(() => mysql.ping(), {
      timeout: 60_000,
      interval: 2_000,
      description: "MySQL to be ready",
    });
    supportsVector = Number.parseInt(await mysql.serverVersion(), 10) >= 9;
    if (supportsVector) {
      await mysql.queryText("DROP TABLE IF EXISTS vec_values");
      await mysql.queryText(
        "CREATE TABLE vec_values (" +
          "id INT NOT NULL PRIMARY KEY, " +
          "embedding VECTOR(3) NOT NULL, " +
          "label VARCHAR(64) CHARACTER SET utf8mb4 NOT NULL, " +
          "payload BLOB NOT NULL) ENGINE=InnoDB",
      );
    }
  });

  beforeEach(async () => {
    if (!supportsVector) return;
    await mysql.truncate("vec_values");
    const gtid = await mysql.getCurrentGtid();
    collector = new StreamingCollector({ ...CLIENT_CONFIG, startGtid: gtid });
    await collector.start();
  });

  afterEach(async () => {
    if (!supportsVector) return;
    await collector.stop();
  });

  afterAll(async () => {
    if (supportsVector) {
      await mysql.queryText("DROP TABLE IF EXISTS vec_values");
    }
    await mysql.close();
  });

  it("INSERT exposes the vector as bytes without shifting string charsets", async (ctx) => {
    if (!supportsVector) ctx.skip();

    await mysql.queryText(
      "INSERT INTO vec_values VALUES " +
        "(1, TO_VECTOR('[1.0, 2.0, 3.0]'), 'vector label', 'payload bytes')",
    );

    const events = await collector.waitForEvents({
      table: "vec_values",
      type: "INSERT",
      count: 1,
      timeout: 10_000,
    });

    expect(events.length).toBeGreaterThanOrEqual(1);
    const after = events[0]!.after!;
    expect(after.id).toBe(1);

    // 3 float32 elements, little-endian, exactly as the server stored them.
    expect(after.embedding).toBeInstanceOf(Uint8Array);
    const embedding = Buffer.from(after.embedding as Uint8Array);
    expect(embedding.length).toBe(12);
    expect(embedding.readFloatLE(0)).toBeCloseTo(1.0, 6);
    expect(embedding.readFloatLE(4)).toBeCloseTo(2.0, 6);
    expect(embedding.readFloatLE(8)).toBeCloseTo(3.0, 6);

    // The utf8mb4 exception in DEFAULT_CHARSET is indexed over character
    // columns, and VECTOR is one of them. Skipping it would land the exception
    // on `payload` and surface `label` as bytes instead of a string.
    expect(after.label).toBe("vector label");
    expect(after.payload).toBeInstanceOf(Uint8Array);
    expect(Buffer.from(after.payload as Uint8Array).toString()).toBe("payload bytes");
  });

  it("UPDATE reports both vector images", async (ctx) => {
    if (!supportsVector) ctx.skip();

    await mysql.queryText(
      "INSERT INTO vec_values VALUES " +
        "(1, TO_VECTOR('[1.0, 2.0, 3.0]'), 'before', 'payload bytes')",
    );
    await mysql.queryText(
      "UPDATE vec_values SET embedding = TO_VECTOR('[4.0, 5.0, 6.0]'), label = 'after' WHERE id = 1",
    );

    const events = await collector.waitForEvents({
      table: "vec_values",
      type: "UPDATE",
      count: 1,
      timeout: 10_000,
    });

    expect(events.length).toBeGreaterThanOrEqual(1);
    const before = events[0]!.before!;
    const after = events[0]!.after!;
    expect(Buffer.from(before.embedding as Uint8Array).readFloatLE(0)).toBeCloseTo(1.0, 6);
    expect(Buffer.from(after.embedding as Uint8Array).readFloatLE(0)).toBeCloseTo(4.0, 6);
    expect(before.label).toBe("before");
    expect(after.label).toBe("after");
  });
});
