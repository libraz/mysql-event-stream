// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

import { afterEach, describe, expect, it } from "vitest";
import { CdcEngine } from "../src/engine.js";
import {
  buildDeleteRowsBody,
  buildEvent,
  buildRotateBody,
  buildTableMapBody,
  buildUpdateRowsBody,
  buildWriteRowsBody,
  concat,
} from "./helpers.js";

// Binlog event type codes (from MySQL 8.4)
const TABLE_MAP_EVENT = 19;
const WRITE_ROWS_EVENT = 30;
const UPDATE_ROWS_EVENT = 31;
const DELETE_ROWS_EVENT = 32;
const ROTATE_EVENT = 4;

describe("CdcEngine", () => {
  let engine: CdcEngine;

  afterEach(() => {
    engine?.destroy();
  });

  it("should create and destroy", async () => {
    engine = await CdcEngine.create();
    expect(engine).toBeDefined();
    expect(engine.hasEvents()).toBe(false);
  });

  it("should handle empty feed", async () => {
    engine = await CdcEngine.create();
    const consumed = engine.feed(new Uint8Array(0));
    expect(consumed).toBe(0);
  });

  it("should parse INSERT event", async () => {
    engine = await CdcEngine.create();

    const tmBody = buildTableMapBody(1, "testdb", "users");
    const tmEvent = buildEvent(TABLE_MAP_EVENT, 1000, tmBody);
    const wrBody = buildWriteRowsBody(1, 42);
    const wrEvent = buildEvent(WRITE_ROWS_EVENT, 1000, wrBody);

    engine.feed(concat(tmEvent, wrEvent));

    expect(engine.hasEvents()).toBe(true);
    const event = engine.nextEvent();
    expect(event).not.toBeNull();
    expect(event!.type).toBe("INSERT");
    expect(event!.database).toBe("testdb");
    expect(event!.table).toBe("users");
    expect(event!.before).toBeNull();
    expect(event!.after).not.toBeNull();
    expect(event!.after!["0"]).toBe(42);
    expect(event!.timestamp).toBe(1000);
  });

  it("should parse UPDATE event", async () => {
    engine = await CdcEngine.create();

    const tmBody = buildTableMapBody(1, "testdb", "users");
    const tmEvent = buildEvent(TABLE_MAP_EVENT, 2000, tmBody);
    const urBody = buildUpdateRowsBody(1, 10, 20);
    const urEvent = buildEvent(UPDATE_ROWS_EVENT, 2000, urBody);

    engine.feed(concat(tmEvent, urEvent));

    const event = engine.nextEvent();
    expect(event).not.toBeNull();
    expect(event!.type).toBe("UPDATE");
    expect(event!.before).not.toBeNull();
    expect(event!.before!["0"]).toBe(10);
    expect(event!.after).not.toBeNull();
    expect(event!.after!["0"]).toBe(20);
  });

  it("should parse DELETE event", async () => {
    engine = await CdcEngine.create();

    const tmBody = buildTableMapBody(1, "testdb", "users");
    const tmEvent = buildEvent(TABLE_MAP_EVENT, 3000, tmBody);
    const drBody = buildDeleteRowsBody(1, 99);
    const drEvent = buildEvent(DELETE_ROWS_EVENT, 3000, drBody);

    engine.feed(concat(tmEvent, drEvent));

    const event = engine.nextEvent();
    expect(event).not.toBeNull();
    expect(event!.type).toBe("DELETE");
    expect(event!.before).not.toBeNull();
    expect(event!.before!["0"]).toBe(99);
    expect(event!.after).toBeNull();
  });

  it("should handle ROTATE event", async () => {
    engine = await CdcEngine.create();

    const rotBody = buildRotateBody(4, "binlog.000002");
    const rotEvent = buildEvent(ROTATE_EVENT, 0, rotBody);

    engine.feed(rotEvent);

    const pos = engine.getPosition();
    expect(pos.file).toBe("binlog.000002");
    expect(pos.offset).toBe(4);
  });

  it("should return null when no events", async () => {
    engine = await CdcEngine.create();
    expect(engine.nextEvent()).toBeNull();
  });

  it("should handle reset", async () => {
    engine = await CdcEngine.create();

    const tmBody = buildTableMapBody(1, "testdb", "users");
    const tmEvent = buildEvent(TABLE_MAP_EVENT, 1000, tmBody);
    const wrBody = buildWriteRowsBody(1, 42);
    const wrEvent = buildEvent(WRITE_ROWS_EVENT, 1000, wrBody);

    engine.feed(concat(tmEvent, wrEvent));
    expect(engine.hasEvents()).toBe(true);

    engine.reset();
    expect(engine.hasEvents()).toBe(false);
  });

  it("should throw after destroy", async () => {
    engine = await CdcEngine.create();
    engine.destroy();
    expect(() => engine.feed(new Uint8Array([1, 2, 3]))).toThrow("destroyed");
  });

  it("should handle multiple events", async () => {
    engine = await CdcEngine.create();

    const tmBody = buildTableMapBody(1, "testdb", "users");

    // First event pair
    const tmEvent1 = buildEvent(TABLE_MAP_EVENT, 1000, tmBody);
    const wr1 = buildEvent(WRITE_ROWS_EVENT, 1000, buildWriteRowsBody(1, 10));
    engine.feed(concat(tmEvent1, wr1));

    // Second event pair (TABLE_MAP needed before each row event group)
    const tmEvent2 = buildEvent(TABLE_MAP_EVENT, 1001, tmBody);
    const wr2 = buildEvent(WRITE_ROWS_EVENT, 1001, buildWriteRowsBody(1, 20));
    engine.feed(concat(tmEvent2, wr2));

    const e1 = engine.nextEvent();
    const e2 = engine.nextEvent();
    expect(e1).not.toBeNull();
    expect(e2).not.toBeNull();
    expect(e1!.after!["0"]).toBe(10);
    expect(e2!.after!["0"]).toBe(20);
    expect(engine.nextEvent()).toBeNull();
  });
});
