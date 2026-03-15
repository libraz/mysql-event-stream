// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { CdcStream } from "../../src/stream.js";
import type { ChangeEvent, ClientConfig } from "../../src/types.js";
import { MysqlClient } from "../lib/mysql-client.js";
import { waitUntil } from "../lib/wait.js";

const CLIENT_CONFIG: ClientConfig = {
  host: "127.0.0.1",
  port: 13307,
  user: "root",
  password: "test_root_password",
  serverId: 110,
  startGtid: "",
  connectTimeoutS: 10,
  readTimeoutS: 1,
};

describe("CdcStream", () => {
  let mysql: MysqlClient;

  beforeAll(async () => {
    mysql = new MysqlClient();
    await waitUntil(() => mysql.ping(), {
      timeout: 60_000,
      interval: 2_000,
      description: "MySQL to be ready",
    });
  });

  let currentGtid: string;

  beforeEach(async () => {
    await mysql.truncate("items");
    await mysql.truncate("users");
    currentGtid = await mysql.getCurrentGtid();
  });

  afterAll(async () => {
    await mysql.close();
  });

  it("receives events via for-await", async () => {
    const stream = new CdcStream({ ...CLIENT_CONFIG, startGtid: currentGtid });
    const collected: ChangeEvent[] = [];

    // Insert in background after a short delay
    setTimeout(async () => {
      await mysql.insert("items", { name: "stream_test", value: 1 });
    }, 500);

    try {
      for await (const event of stream) {
        if (event.table === "items") {
          collected.push(event);
          break;
        }
      }
    } finally {
      await stream.close();
    }

    expect(collected.length).toBe(1);
    expect(collected[0]!.type).toBe("INSERT");
    expect(collected[0]!.table).toBe("items");
  });

  it("cleans up on break", async () => {
    const stream = new CdcStream({ ...CLIENT_CONFIG, startGtid: currentGtid });
    let count = 0;

    setTimeout(async () => {
      await mysql.insert("items", { name: "break_test", value: 1 });
    }, 500);

    for await (const event of stream) {
      if (event.table === "items") {
        count++;
        break; // Should trigger cleanup via generator return
      }
    }

    expect(count).toBe(1);
    // Stream should be cleaned up - calling close again should be safe
    await stream.close();
  });

  it("close() terminates iteration", async () => {
    const stream = new CdcStream({ ...CLIENT_CONFIG, startGtid: currentGtid });
    const collected: ChangeEvent[] = [];

    setTimeout(async () => {
      await mysql.insert("items", { name: "close_test", value: 1 });
    }, 500);

    // Close after collecting one event
    setTimeout(async () => {
      // Wait a bit for the event to be collected
      await new Promise((r) => setTimeout(r, 2000));
      await stream.close();
    }, 0);

    for await (const event of stream) {
      if (event.table === "items") {
        collected.push(event);
        await stream.close();
      }
    }

    expect(collected.length).toBeGreaterThanOrEqual(1);
  });

  it("exposes currentGtid", async () => {
    const stream = new CdcStream({ ...CLIENT_CONFIG, startGtid: currentGtid });

    setTimeout(async () => {
      await mysql.insert("items", { name: "gtid_test", value: 1 });
    }, 500);

    try {
      for await (const event of stream) {
        if (event.table === "items") {
          expect(stream.currentGtid).toContain(":");
          break;
        }
      }
    } finally {
      await stream.close();
    }
  });
});
