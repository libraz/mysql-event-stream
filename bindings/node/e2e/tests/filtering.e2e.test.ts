// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { CdcStream } from "../../src/stream.js";
import type { StreamConfig } from "../../src/types.js";
import { MysqlClient } from "../lib/mysql-client.js";
import { waitUntil } from "../lib/wait.js";

// A user of its own, so the forced reconnect kills only this test's dump thread.
const STREAM_USER = "mes_filter_e2e";
const STREAM_PASSWORD = "filter_e2e_password";

const STREAM_CONFIG: StreamConfig = {
  host: "127.0.0.1",
  port: 13307,
  user: STREAM_USER,
  password: STREAM_PASSWORD,
  allowPublicKeyRetrieval: true,
  serverId: 310,
  startGtid: "",
  connectTimeoutS: 10,
  readTimeoutS: 1,
  includeDatabases: ["mes_test"],
  // A qualified entry and a bare name; large_data is then removed again by a wildcard.
  includeTables: ["mes_test.items", "users", "large_data"],
  excludeTables: ["large_*"],
};

describe("stream filters", () => {
  let mysql: MysqlClient;

  beforeAll(async () => {
    mysql = new MysqlClient();
    await waitUntil(() => mysql.ping(), {
      timeout: 60_000,
      interval: 2_000,
      description: "MySQL to be ready",
    });
    await mysql.queryText(
      `CREATE USER IF NOT EXISTS '${STREAM_USER}'@'%' IDENTIFIED BY '${STREAM_PASSWORD}'`,
    );
    await mysql.queryText(
      `GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO '${STREAM_USER}'@'%'`,
    );
    await mysql.queryText(`GRANT SELECT ON mes_test.* TO '${STREAM_USER}'@'%'`);
  });

  afterAll(async () => {
    await mysql.close();
  });

  /** One write to each allowed and filtered table, ending on an allowed sentinel. */
  async function writeRound(tag: string): Promise<void> {
    await mysql.insert("large_data", { big_text: tag }); // excluded by wildcard
    await mysql.insert("items", { name: tag, value: 1 }); // qualified include
    await mysql.insert("ddl_test", { val: tag }); // matches no include entry
    await mysql.insert("users", { name: `${tag}-sentinel` }); // bare include
  }

  it("delivers exactly the allowed tables before and after a forced reconnect", async () => {
    await mysql.truncate("items");
    await mysql.truncate("users");
    const startGtid = await mysql.getCurrentGtid();
    const stream = new CdcStream({ ...STREAM_CONFIG, startGtid });
    const delivered: string[] = [];
    const killed: number[] = [];
    let round = 0;

    setTimeout(() => void writeRound("first"), 500);

    try {
      for await (const event of stream) {
        delivered.push(event.table);
        const name = String(event.after?.name ?? "");
        if (event.table !== "users" || !name.endsWith("-sentinel")) continue;
        round++;
        if (round === 2) break;

        const threads = await mysql.dumpThreadIds(STREAM_USER);
        expect(threads.length).toBeGreaterThan(0);
        for (const id of threads) await mysql.killThread(id);
        killed.push(...threads);
        await writeRound("second");
      }
    } finally {
      await stream.close();
    }

    expect(delivered).toEqual(["items", "users", "items", "users"]);
    expect(killed.length).toBeGreaterThan(0);
  });
});
