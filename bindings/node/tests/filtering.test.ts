// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

import { afterEach, describe, expect, it } from "vitest";
import { CdcEngine } from "../src/engine.js";
import { LogLevel, setLogCallback } from "../src/logging.js";
import { buildEvent, buildTableMapBody, buildWriteRowsBody } from "./helpers.js";

const TABLE_MAP_EVENT = 19;
const WRITE_ROWS_EVENT = 30;

const tableMap = () => buildEvent(TABLE_MAP_EVENT, 1000, buildTableMapBody(1, "mydb", "users"));
const writeRow = (value: number) =>
  buildEvent(WRITE_ROWS_EVENT, 1000, buildWriteRowsBody(1, value));

describe("engine filter lifecycle", () => {
  let engine: CdcEngine;

  afterEach(() => {
    engine?.destroy();
    setLogCallback(null);
  });

  const setters: Array<{
    name: string;
    blocking: string[];
    apply: (e: CdcEngine, v: string[]) => void;
  }> = [
    { name: "includeDatabases", blocking: ["otherdb"], apply: (e, v) => e.setIncludeDatabases(v) },
    { name: "includeTables", blocking: ["mydb.other"], apply: (e, v) => e.setIncludeTables(v) },
    { name: "excludeTables", blocking: ["mydb.users"], apply: (e, v) => e.setExcludeTables(v) },
  ];

  it.each(setters)(
    "$name with an empty list lifts a restriction on a registered table",
    async ({ blocking, apply }) => {
      engine = await CdcEngine.create();
      engine.feed(tableMap());

      apply(engine, blocking);
      engine.feed(writeRow(10));
      expect(engine.hasEvents()).toBe(false);

      apply(engine, []);
      engine.feed(writeRow(20));
      const event = engine.nextEvent();
      expect(event).not.toBeNull();
      expect(event!.table).toBe("users");
      expect(engine.hasEvents()).toBe(false);
    },
  );

  it("warns once at reset and once at close when an include filter matched nothing", async () => {
    const messages: string[] = [];
    setLogCallback((_level, message) => messages.push(message), LogLevel.Warn);
    engine = await CdcEngine.create();
    engine.setIncludeTables(["mydb.missing"]);

    engine.feed(tableMap());
    engine.reset();
    engine.feed(tableMap());
    engine.destroy();
    // The handler is invoked from the event loop, not from the native thread.
    await new Promise<void>((resolve) => setTimeout(resolve, 50));

    expect(messages.filter((m) => m.includes("event=include_filter_matched_nothing"))).toHaveLength(
      2,
    );
  });
});
