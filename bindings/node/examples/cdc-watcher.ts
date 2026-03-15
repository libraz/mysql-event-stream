#!/usr/bin/env npx tsx
/**
 * CDC Watcher - Monitor MySQL changes in real time.
 *
 * Connects to MySQL via CdcStream and prints change events
 * with colored output. Optionally filters by table name.
 *
 * Prerequisites:
 *   1. Docker MySQL running: cd e2e/docker && docker compose up -d
 *   2. Build native addon: yarn build
 *
 * Usage:
 *   # Watch all tables
 *   npx tsx examples/cdc-watcher.ts
 *
 *   # Watch specific table
 *   npx tsx examples/cdc-watcher.ts --table items
 *
 * Example output:
 *   mysql-event-stream CDC Watcher
 *     MySQL:  127.0.0.1:13307
 *
 *   Waiting for changes... (Ctrl+C to stop)
 *
 *   [INSERT] mes_test.items
 *     binlog: mysql-bin.000003:3265  ts: 1773584163
 *     after:  [id=8, name='DocTest_Widget', value=42]
 *
 *   [UPDATE] mes_test.items
 *     binlog: mysql-bin.000003:3611  ts: 1773584164
 *     before: [id=8, name='DocTest_Widget', value=42]
 *     after:  [id=8, name='DocTest_Widget', value=100]
 *
 *   [DELETE] mes_test.items
 *     binlog: mysql-bin.000003:3922  ts: 1773584164
 *     before: [id=8, name='DocTest_Widget', value=100]
 */

import { parseArgs } from "node:util";
import { CdcStream } from "../src/stream.js";
import type { ChangeEvent } from "../src/types.js";

// --- Display ---

const COLORS = {
  INSERT: "\x1b[32m",
  UPDATE: "\x1b[33m",
  DELETE: "\x1b[31m",
} as const;
const RESET = "\x1b[0m";
const DIM = "\x1b[2m";

function formatRow(row: Record<string, unknown>): string {
  return Object.entries(row)
    .map(([key, val]) => {
      if (val === null) return `${key}=NULL`;
      if (val instanceof Uint8Array) {
        const hex = [...val.slice(0, 16)].map((b) => b.toString(16).padStart(2, "0")).join("");
        return val.length > 16 ? `${key}=0x${hex}...` : `${key}=0x${hex}`;
      }
      if (typeof val === "string") return `${key}='${val}'`;
      return `${key}=${val}`;
    })
    .join(", ");
}

function printEvent(event: ChangeEvent): void {
  const color = COLORS[event.type];

  console.log(`\n${color}[${event.type}]${RESET} ${event.database}.${event.table}`);
  console.log(
    `  ${DIM}binlog: ${event.position.file}:${event.position.offset}  ts: ${event.timestamp}${RESET}`,
  );

  if (event.before !== null) {
    console.log(`  before: [${formatRow(event.before)}]`);
  }
  if (event.after !== null) {
    console.log(`  after:  [${formatRow(event.after)}]`);
  }
}

// --- Main ---

async function main(): Promise<void> {
  const { values } = parseArgs({
    options: {
      table: { type: "string" },
    },
    strict: true,
  });

  const tableFilter = values.table;

  const stream = new CdcStream({
    host: "127.0.0.1",
    port: 13307,
    user: "root",
    password: "test_root_password",
    serverId: 98,
    startGtid: "",
    connectTimeoutS: 10,
    readTimeoutS: 30,
  });

  let totalEvents = 0;
  let running = true;

  console.log("mysql-event-stream CDC Watcher");
  console.log("  MySQL:  127.0.0.1:13307");
  if (tableFilter) console.log(`  Filter: table=${tableFilter}`);
  console.log("\nWaiting for changes... (Ctrl+C to stop)\n");

  const shutdown = () => {
    running = false;
    console.log(`\n${DIM}Shutting down...${RESET}`);
    stream.close().catch(() => {});
  };
  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);

  try {
    for await (const event of stream) {
      if (!running) break;
      if (tableFilter && event.table !== tableFilter) continue;
      printEvent(event);
      totalEvents++;
    }
  } catch (err) {
    if (running) console.error("Stream error:", err);
  }

  console.log(`\n${DIM}Total events captured: ${totalEvents}${RESET}`);
}

main().catch((err) => {
  console.error("Fatal:", err);
  process.exit(1);
});
