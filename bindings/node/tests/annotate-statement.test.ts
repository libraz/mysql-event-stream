// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * The MariaDB ANNOTATE_ROWS statement shared by every row of one ROWS event.
 *
 * One ANNOTATE_ROWS event annotates the whole ROWS event that follows it, and
 * the core hands each of that event's rows to this binding as its own C event
 * pointing at the same statement storage. The addon therefore converts the
 * statement to a JS string once per ROWS event rather than once per row, which
 * `sourceSqlConversions` on the native engine reports: the sharing is counted
 * here rather than timed, and every assertion on a count is paired with one on
 * the value, since a cache with the right count and the wrong string is worse
 * than no cache.
 */

import { describe, expect, it } from "vitest";
import { loadNativeAddon } from "../src/native.js";
import type { ChangeEvent } from "../src/types.js";
import { buildEvent, buildTableMapBody, concat } from "./helpers.js";

const TABLE_MAP_EVENT = 19;
const WRITE_ROWS_EVENT = 30;
const XID_EVENT = 16;
const MARIADB_ANNOTATE_ROWS_EVENT = 160;

/**
 * The native engine, plus the conversion counter the JS `CdcEngine` wrapper
 * does not forward.
 */
interface NativeEngineForTest {
  feed(data: Uint8Array): number;
  nextEvent(): ChangeEvent | null;
  destroy(): void;
  readonly sourceSqlConversions: number;
}

const addon = loadNativeAddon<{ CdcEngine: new () => NativeEngineForTest }>();

const encoder = new TextEncoder();

function tableId48(tableId: number): number[] {
  return [
    tableId & 0xff,
    (tableId >> 8) & 0xff,
    (tableId >> 16) & 0xff,
    (tableId >> 24) & 0xff,
    0,
    0,
  ];
}

/**
 * Build a WRITE_ROWS_EVENT V2 body carrying one INT column over several rows.
 * `helpers.ts` builds the single-row form; a shared statement is only visible
 * across the rows of one event.
 */
function buildMultiRowWriteRowsBody(tableId: number, values: readonly number[]): Uint8Array {
  const parts: number[] = [
    ...tableId48(tableId),
    0,
    0, // flags
    2,
    0, // var_header_len (V2)
    1, // column_count (packed int)
    0x01, // columns_present bitmap
  ];
  for (const value of values) {
    parts.push(0x00); // null bitmap: the column carries a value
    parts.push(value & 0xff, (value >> 8) & 0xff, (value >> 16) & 0xff, (value >> 24) & 0xff);
  }
  return new Uint8Array(parts);
}

/** ANNOTATE_ROWS + TABLE_MAP + WRITE_ROWS for one statement over `values`. */
function annotatedRowsEvent(tableId: number, sql: string, values: readonly number[]): Uint8Array {
  return concat(
    buildEvent(MARIADB_ANNOTATE_ROWS_EVENT, 1000, encoder.encode(sql)),
    buildEvent(TABLE_MAP_EVENT, 1000, buildTableMapBody(tableId, "testdb", "users")),
    buildEvent(WRITE_ROWS_EVENT, 1000, buildMultiRowWriteRowsBody(tableId, values)),
  );
}

/** Drain `count` events, failing rather than returning a short list. */
function drain(engine: NativeEngineForTest, count: number): ChangeEvent[] {
  const events: ChangeEvent[] = [];
  for (let i = 0; i < count; i++) {
    const event = engine.nextEvent();
    if (event === null) throw new Error(`expected ${count} events, got ${events.length}`);
    events.push(event);
  }
  return events;
}

describe("ANNOTATE_ROWS statement conversion", () => {
  it("converts a multi-row event's statement once and carries it on every row", () => {
    const engine = new addon.CdcEngine();
    try {
      const sql = "INSERT INTO users VALUES (1),(2),(3),(4),(5)";
      engine.feed(annotatedRowsEvent(1, sql, [1, 2, 3, 4, 5]));

      const events = drain(engine, 5);
      expect(events.map((event) => event.after?.["0"])).toEqual([1, 2, 3, 4, 5]);
      for (const event of events) {
        expect(event.sourceSql).toBe(sql);
      }
      expect(engine.sourceSqlConversions).toBe(1);
    } finally {
      engine.destroy();
    }
  });

  it("converts the statement again for the next event that carries a different one", () => {
    const engine = new addon.CdcEngine();
    try {
      const first = "UPDATE users SET n = 1";
      const second = "UPDATE users SET n = 2";
      engine.feed(annotatedRowsEvent(1, first, [10, 11]));
      engine.feed(annotatedRowsEvent(2, second, [20, 21]));

      const events = drain(engine, 4);
      expect(events.map((event) => event.sourceSql)).toEqual([first, first, second, second]);
      expect(engine.sourceSqlConversions).toBe(2);
    } finally {
      engine.destroy();
    }
  });

  it("leaves an unannotated event's statement empty and converts nothing", () => {
    const engine = new addon.CdcEngine();
    try {
      engine.feed(
        concat(
          buildEvent(TABLE_MAP_EVENT, 1000, buildTableMapBody(1, "testdb", "users")),
          buildEvent(WRITE_ROWS_EVENT, 1000, buildMultiRowWriteRowsBody(1, [7, 8])),
        ),
      );

      const events = drain(engine, 2);
      expect(events.map((event) => event.sourceSql)).toEqual(["", ""]);
      expect(engine.sourceSqlConversions).toBe(0);
    } finally {
      engine.destroy();
    }
  });

  it("serves the current statement after the core reuses the storage of a released one", () => {
    // The statement of a drained event is released once the next event
    // replaces it, so the statement of a later event can land on the address
    // the released one held. Whether the allocator hands back that address is
    // not under this test's control; what is pinned either way is that the
    // value follows the event rather than the address, and that a statement
    // recognised as new is converted.
    const engine = new addon.CdcEngine();
    try {
      const first = "DELETE FROM users WHERE n = 1";
      const second = "DELETE FROM users WHERE n = 2";
      expect(second.length).toBe(first.length);

      engine.feed(annotatedRowsEvent(1, first, [1, 2]));
      expect(drain(engine, 2).map((event) => event.sourceSql)).toEqual([first, first]);

      // XID ends the transaction, dropping the engine's own reference to the
      // statement; draining the unannotated event that follows releases the
      // last one.
      engine.feed(
        concat(
          buildEvent(XID_EVENT, 1000, new Uint8Array(8)),
          buildEvent(TABLE_MAP_EVENT, 1000, buildTableMapBody(2, "testdb", "users")),
          buildEvent(WRITE_ROWS_EVENT, 1000, buildMultiRowWriteRowsBody(2, [3])),
        ),
      );
      expect(drain(engine, 1)[0]?.sourceSql).toBe("");

      engine.feed(annotatedRowsEvent(3, second, [4, 5]));
      expect(drain(engine, 2).map((event) => event.sourceSql)).toEqual([second, second]);
      expect(engine.sourceSqlConversions).toBe(2);
    } finally {
      engine.destroy();
    }
  });
});
