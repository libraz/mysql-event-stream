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
 */

import { parseArgs } from "node:util";
import { CdcStream } from "../src/stream.js";
import type { ChangeEvent, ColumnValue } from "../src/types.js";

// --- Display ---

const COLORS = {
  INSERT: "\x1b[32m",
  UPDATE: "\x1b[33m",
  DELETE: "\x1b[31m",
} as const;
const RESET = "\x1b[0m";
const DIM = "\x1b[2m";

function formatColumnValue(col: ColumnValue): string {
  if (col.type === "null") return "NULL";
  if (col.type === "bytes") {
    const bytes = col.value as Uint8Array;
    const hex = [...bytes.slice(0, 16)].map((b) => b.toString(16).padStart(2, "0")).join("");
    return bytes.length > 16 ? `0x${hex}...` : `0x${hex}`;
  }
  if (col.type === "string") return `'${col.value}'`;
  return String(col.value);
}

function printEvent(event: ChangeEvent): void {
  const color = COLORS[event.type];

  console.log(`\n${color}[${event.type}]${RESET} ${event.database}.${event.table}`);
  console.log(
    `  ${DIM}binlog: ${event.position.file}:${event.position.offset}  ts: ${event.timestamp}${RESET}`,
  );

  if (event.before !== null) {
    const vals = event.before.map(formatColumnValue).join(", ");
    console.log(`  before: [${vals}]`);
  }
  if (event.after !== null) {
    const vals = event.after.map(formatColumnValue).join(", ");
    console.log(`  after:  [${vals}]`);
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
