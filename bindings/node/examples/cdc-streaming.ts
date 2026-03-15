#!/usr/bin/env npx tsx
/**
 * CDC Streaming Example for Node.js
 *
 * Connects to MySQL and streams binlog events using CdcStream.
 * Demonstrates the AsyncIterator pattern with `for await`.
 *
 * Prerequisites:
 *   1. Docker MySQL running: cd e2e/docker && docker compose up -d
 *   2. Build native addon: yarn build
 *
 * Usage:
 *   npx tsx examples/cdc-streaming.ts
 *   npx tsx examples/cdc-streaming.ts --gtid ""
 */

import { parseArgs } from "node:util";
import { CdcStream } from "../src/stream.js";
import type { ChangeEvent, ColumnValue } from "../src/types.js";

/** Format a column value for display. */
function formatValue(col: ColumnValue): string {
  if (col.type === "null") return "NULL";
  if (col.type === "bytes") {
    const bytes = col.value as Uint8Array;
    const hex = [...bytes.slice(0, 8)].map((b) => b.toString(16).padStart(2, "0")).join("");
    return bytes.length > 8 ? `0x${hex}...` : `0x${hex}`;
  }
  if (col.type === "string") return `'${col.value}'`;
  return String(col.value);
}

/** Print a change event to stdout. */
function printEvent(event: ChangeEvent): void {
  const label = { INSERT: "+", UPDATE: "~", DELETE: "-" }[event.type];
  const cols = (event.after ?? event.before ?? []).map(formatValue).join(", ");
  console.log(`  ${label} ${event.database}.${event.table} [${cols}]`);
}

async function main(): Promise<void> {
  const { values } = parseArgs({
    options: {
      gtid: { type: "string", default: "" },
    },
    strict: true,
  });

  console.log("mysql-event-stream CDC Streaming Example");
  console.log("  Using CdcStream (AsyncIterator)\n");

  const stream = new CdcStream({
    host: "127.0.0.1",
    port: 13307,
    user: "root",
    password: "test_root_password",
    serverId: 99,
    startGtid: values.gtid ?? "",
    connectTimeoutS: 10,
    readTimeoutS: 30,
  });

  let totalEvents = 0;
  let running = true;

  const shutdown = () => {
    running = false;
    console.log("\nShutting down...");
    stream.close().catch(() => {});
  };
  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);

  console.log("Streaming events:\n");

  try {
    for await (const event of stream) {
      if (!running) break;
      printEvent(event);
      totalEvents++;
    }
  } catch (err) {
    if (running) console.error("Stream error:", err);
  }

  console.log(`\nDone. ${totalEvents} event(s) captured.`);
}

main().catch((err) => {
  console.error("Fatal:", err);
  process.exit(1);
});
