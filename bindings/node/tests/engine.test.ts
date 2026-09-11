// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

import { readdirSync, readFileSync } from "node:fs";
import { afterEach, describe, expect, it } from "vitest";
import { CdcEngine } from "../src/engine.js";
import { MesErrorCode } from "../src/types.js";
import {
  buildColumnEvents,
  Collation,
  ColType,
  type ColumnFixture,
  lengthPrefixed,
} from "./column-fixture.js";
import {
  buildDeleteRowsBody,
  buildEvent,
  buildEventNoChecksum,
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
const MARIADB_ANNOTATE_ROWS_EVENT = 160;

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
    // Standalone mode (no metadata connection): the TABLE_MAP has no names.
    expect(event!.namesResolved).toBe(false);
  });

  it("exposes MariaDB ANNOTATE_ROWS SQL on row events", async () => {
    engine = await CdcEngine.create();
    const sql = "INSERT INTO users VALUES (42)";
    const annotate = buildEvent(MARIADB_ANNOTATE_ROWS_EVENT, 1000, new TextEncoder().encode(sql));
    const tableMap = buildEvent(TABLE_MAP_EVENT, 1000, buildTableMapBody(1, "testdb", "users"));
    const write = buildEvent(WRITE_ROWS_EVENT, 1000, buildWriteRowsBody(1, 42));
    engine.feed(concat(annotate, tableMap, write));

    expect(engine.nextEvent()?.sourceSql).toBe(sql);
  });

  it("keeps prototype-like column names as own data properties", async () => {
    engine = await CdcEngine.create();

    const names = ["__proto__", "constructor", "prototype", "toString"];
    for (const [index, columnName] of names.entries()) {
      const tableId = index + 10;
      const tm = buildEvent(
        TABLE_MAP_EVENT,
        1000,
        buildTableMapBody(tableId, "testdb", "special_names", columnName),
      );
      const wr = buildEvent(WRITE_ROWS_EVENT, 1000, buildWriteRowsBody(tableId, 40 + index));
      engine.feed(concat(tm, wr));

      const row = engine.nextEvent()?.after;
      expect(row).not.toBeNull();
      expect(Object.hasOwn(row, columnName)).toBe(true);
      expect(row?.[columnName]).toBe(40 + index);
      expect(Object.getPrototypeOf(row)).toBe(Object.prototype);
      // A data property, not the accessor these names reach on
      // Object.prototype: a column value must never arrive as a getter, and
      // "__proto__" must not have gone through the prototype setter.
      const descriptor = Object.getOwnPropertyDescriptor(row ?? {}, columnName);
      expect(descriptor).toEqual({
        value: 40 + index,
        writable: true,
        enumerable: true,
        configurable: true,
      });
    }
  });

  it("gives every column of a row a writable, enumerable, configurable data property", async () => {
    engine = await CdcEngine.create();

    const columns: ColumnFixture[] = [
      { type: ColType.Long, value: new Uint8Array([7, 0, 0, 0]), name: "id" },
      {
        type: ColType.Blob,
        meta: [4],
        collation: Collation.Utf8mb4,
        value: lengthPrefixed(new TextEncoder().encode("ok"), 4),
        name: "label",
      },
      { type: ColType.Long, value: null, name: "absent" },
    ];
    engine.feed(buildColumnEvents(20, "testdb", "wide", columns));

    const row = engine.nextEvent()?.after;
    expect(row).not.toBeNull();
    expect(Object.keys(row!)).toEqual(["id", "label", "absent"]);
    expect(Object.getPrototypeOf(row)).toBe(Object.prototype);
    for (const [name, value] of [
      ["id", 7],
      ["label", "ok"],
      ["absent", null],
    ] as const) {
      expect(Object.getOwnPropertyDescriptor(row!, name)).toEqual({
        value,
        writable: true,
        enumerable: true,
        configurable: true,
      });
    }
  });

  it("leaves the later column in place when two columns resolve to one key", async () => {
    engine = await CdcEngine.create();

    // An unnamed column keys on its index, which a named column is free to
    // spell. The row carries one property per distinct key, holding the value
    // of the last column that resolved to it.
    const columns: ColumnFixture[] = [
      { type: ColType.Long, value: new Uint8Array([1, 0, 0, 0]) },
      { type: ColType.Long, value: new Uint8Array([2, 0, 0, 0]), name: "0" },
    ];
    engine.feed(buildColumnEvents(21, "testdb", "collide", columns));

    const row = engine.nextEvent()?.after;
    expect(Object.keys(row!)).toEqual(["0"]);
    expect(row?.["0"]).toBe(2);
  });

  it("should parse checksum=NONE events after an explicit override", async () => {
    engine = await CdcEngine.create();
    engine.setChecksumEnabled(false);
    const tm = buildEventNoChecksum(TABLE_MAP_EVENT, 1000, buildTableMapBody(1, "db", "t"));
    const wr = buildEventNoChecksum(WRITE_ROWS_EVENT, 1000, buildWriteRowsBody(1, 73));
    engine.feed(concat(tm, wr));
    expect(engine.nextEvent()?.after?.["0"]).toBe(73);
  });

  it("should reject a corrupted CRC32 event", async () => {
    engine = await CdcEngine.create();
    const event = buildEvent(TABLE_MAP_EVENT, 1000, buildTableMapBody(1, "db", "t"));
    event[20] ^= 0x40;
    expect(() => engine.feed(event)).toThrow("checksum mismatch");
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
    // reset deliberately retains events decoded before the reset boundary.
    const event = engine.nextEvent();
    expect(event?.after?.["0"]).toBe(42);
    expect(engine.hasEvents()).toBe(false);
  });

  it("should reject non-Uint8Array typed arrays", async () => {
    const engine = await CdcEngine.create();
    // biome-ignore lint/suspicious/noExplicitAny: testing runtime type guard
    expect(() => engine.feed(new Float64Array([1.0]) as any)).toThrow(
      "Expected Buffer or Uint8Array",
    );
    engine.destroy();
  });

  it("should throw after destroy", async () => {
    engine = await CdcEngine.create();
    engine.destroy();
    try {
      engine.feed(new Uint8Array([1, 2, 3]));
    } catch (error) {
      expect(error).toMatchObject({
        code: MesErrorCode.InvalidArg,
        name: "MesError",
        message: "Engine has been destroyed",
      });
      return;
    }
    throw new Error("expected destroyed engine to reject feed");
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

  it("keeps a held event unchanged while later data is fed, decoded and reset", async () => {
    engine = await CdcEngine.create();

    // The native event a decode reads from is a single reusable slot pointing
    // at engine-owned storage, so the object handed to a caller has to own its
    // bytes outright. Binary and character payloads are both included: they
    // reach JS as a Uint8Array and a string through separate conversion arms.
    const heldBytes = new Uint8Array(64).fill(0xa1);
    const heldText = "held".padEnd(64, "y");

    engine.feed(buildHeldRowEvents(1, "held_db", "held_rows", heldBytes, heldText, 1000));
    const held = engine.nextEvent();
    expect(held).not.toBeNull();
    expect(Array.from(held!.after!.payload as Uint8Array)).toEqual(Array.from(heldBytes));
    expect(held!.after!.note).toBe(heldText);

    // Unrelated rows of the same shape: a payload of identical length can take
    // over the storage the held one used, which the held object must not follow.
    const laterBytes = new Uint8Array(64).fill(0xb2);
    const laterText = "later".padEnd(64, "z");
    engine.feed(buildHeldRowEvents(2, "other_db", "other_rows", laterBytes, laterText, 2000));
    const later = engine.nextEvent();
    expect(Array.from(later!.after!.payload as Uint8Array)).toEqual(Array.from(laterBytes));

    // A longer payload, so nothing about the held event's bytes surviving can
    // rest on the following decode happening to allocate elsewhere.
    const longBytes = new Uint8Array(512).fill(0xc3);
    engine.feed(
      buildHeldRowEvents(3, "third_db", "third_rows", longBytes, "third".padEnd(512, "w"), 3000),
    );
    expect(engine.nextEvent()).not.toBeNull();

    engine.reset();

    expect(held!.type).toBe("INSERT");
    expect(held!.database).toBe("held_db");
    expect(held!.table).toBe("held_rows");
    expect(held!.timestamp).toBe(1000);
    expect(held!.before).toBeNull();
    expect(Array.from(held!.after!.payload as Uint8Array)).toEqual(Array.from(heldBytes));
    expect(held!.after!.note).toBe(heldText);
    expect(Object.keys(held!.after!)).toEqual(["payload", "note"]);
    expect(held).not.toBe(later);
  });
});

/** Build the TABLE_MAP + WRITE_ROWS pair for one binary and one text column. */
function buildHeldRowEvents(
  tableId: number,
  db: string,
  table: string,
  payload: Uint8Array,
  note: string,
  timestamp: number,
): Uint8Array {
  const columns: ColumnFixture[] = [
    {
      type: ColType.Blob,
      meta: [4],
      collation: Collation.Binary,
      value: lengthPrefixed(payload, 4),
      name: "payload",
    },
    {
      type: ColType.Blob,
      meta: [4],
      collation: Collation.Utf8mb4,
      value: lengthPrefixed(new TextEncoder().encode(note), 4),
      name: "note",
    },
  ];
  return buildColumnEvents(tableId, db, table, columns, timestamp);
}

/** Call an engine method with a value its declared parameter type forbids. */
function callWithArgument(engine: CdcEngine, method: string, argument: unknown): void {
  const methods = engine as unknown as Record<string, (value: unknown) => unknown>;
  methods[method](argument);
}

/** Engine methods that take an argument and can therefore refuse one. */
const ARGUMENT_TAKING_METHODS = Object.getOwnPropertyNames(CdcEngine.prototype).filter((name) => {
  if (name === "constructor") return false;
  const member = (CdcEngine.prototype as unknown as Record<string, unknown>)[name];
  return typeof member === "function" && member.length >= 1;
});

/**
 * One refused argument per way an engine entry point can refuse one. The set of
 * methods covered is compared against the class's own argument-taking methods
 * below, so an entry point added without a coded rejection fails that check
 * rather than going unexercised here.
 */
const REFUSED_ARGUMENTS: Array<{ method: string; argument: unknown; label: string }> = [
  { method: "feed", argument: new Float64Array([1]), label: "a non-Uint8Array typed array" },
  { method: "feed", argument: "0102", label: "a string" },
  { method: "setMaxQueueSize", argument: "100", label: "a string" },
  { method: "setMaxQueueSize", argument: -1, label: "a negative count" },
  { method: "setMaxEventSize", argument: "100", label: "a string" },
  { method: "setMaxEventSize", argument: 2 ** 32, label: "a size past uint32" },
  { method: "setChecksumEnabled", argument: 1, label: "a number" },
  { method: "setIncludeDatabases", argument: "mydb", label: "a bare string" },
  { method: "setIncludeDatabases", argument: [1], label: "an array of non-strings" },
  { method: "setIncludeTables", argument: "mydb.users", label: "a bare string" },
  { method: "setIncludeTables", argument: [1], label: "an array of non-strings" },
  { method: "setExcludeTables", argument: "mydb.users", label: "a bare string" },
  { method: "setExcludeTables", argument: [1], label: "an array of non-strings" },
  { method: "enableMetadata", argument: 42, label: "a non-object config" },
  { method: "enableMetadata", argument: { port: "3306" }, label: "a wrongly-typed option" },
];

describe("CdcEngine error codes", () => {
  let engine: CdcEngine;

  afterEach(() => {
    engine?.destroy();
  });

  it("refuses an argument with a numeric error code on every entry point", async () => {
    for (const { method, argument, label } of REFUSED_ARGUMENTS) {
      engine = await CdcEngine.create();
      let thrown: unknown;
      try {
        callWithArgument(engine, method, argument);
      } catch (error) {
        thrown = error;
      }
      engine.destroy();

      const rejection = thrown as (Error & { code?: unknown }) | undefined;
      expect(rejection, `${method} refuses ${label}`).toBeDefined();
      expect(rejection?.code, `${method} codes its refusal of ${label}`).toBe(
        MesErrorCode.InvalidArg,
      );
      expect(rejection?.name, `${method} names its refusal of ${label}`).toBe("MesError");
    }
  });

  it("covers every engine method that takes an argument", () => {
    const covered = [...new Set(REFUSED_ARGUMENTS.map((refusal) => refusal.method))].sort();
    expect(covered).toEqual([...ARGUMENT_TAKING_METHODS].sort());
  });

  it("codes a decoded event whose type the addon does not know", async () => {
    engine = await CdcEngine.create();
    // The engine cannot be driven to produce an unknown type through the wire
    // format, so the value the addon branches on is what gets checked: the
    // refusal must be a decode failure, which the retry policy treats as
    // permanent, rather than an uncoded error it would retry.
    const source = readFileSync(new URL("../src/addon/engine_wrap.cpp", import.meta.url), "utf8");
    const throwSite = source.match(/"Unknown event type: " \+ std::to_string\(type_idx\),\s*(\w+)/);
    expect(throwSite, "the addon reports an unknown event type").not.toBeNull();
    expect((throwSite as RegExpMatchArray)[1]).toBe("MES_ERR_DECODE");
  });

  it("builds every addon error through the helper that attaches a code", () => {
    const addonUrl = new URL("../src/addon/", import.meta.url);
    // The one place a Napi error object is constructed; everywhere else goes
    // through it, so no exit path can reach JavaScript without a code.
    const helper = "mes_error_util.h";
    const bare: string[] = [];
    for (const entry of readdirSync(addonUrl)) {
      if (entry === helper) continue;
      const lines = readFileSync(new URL(entry, addonUrl), "utf8").split("\n");
      lines.forEach((line, index) => {
        if (/Napi::(?:Type|Range)?Error::New/.test(line)) bare.push(`${entry}:${index + 1}`);
      });
    }
    expect(bare, `every throw is built by MakeMesError (see ${helper})`).toEqual([]);
  });
});

/**
 * Read the doc comment `src/engine.ts` attaches to a method, as one line.
 *
 * Parsed rather than restated: a hand-copied expectation would be one more
 * copy free to drift. Every step that could stop matching throws instead of
 * returning an empty result an assertion would pass over.
 */
function loadMethodDoc(method: string): string {
  const lines = readFileSync(new URL("../src/engine.ts", import.meta.url), "utf8").split("\n");
  // Anchored inside the class: the native interface above it declares the same
  // method names without the doc comments a caller reads.
  const classStart = lines.findIndex((line) => line.startsWith("export class CdcEngine"));
  if (classStart < 0) throw new Error("engine.ts does not declare CdcEngine");
  const offset = lines.slice(classStart).findIndex((line) => line.trim().startsWith(`${method}(`));
  if (offset < 0) throw new Error(`CdcEngine does not declare ${method}`);
  const declaration = classStart + offset;

  const doc: string[] = [];
  for (let index = declaration - 1; index >= 0; index--) {
    const line = lines[index].trim();
    if (!line.startsWith("*") && !line.startsWith("/**")) break;
    doc.unshift(line.replace(/^\/\*\*|^\*\/|^\*/, "").trim());
    if (line.startsWith("/**")) break;
  }
  const text = doc.join(" ").trim();
  if (text === "") throw new Error(`engine.ts does not document ${method}`);
  return text;
}

/** What a method's doc says about 0, up to the end of that sentence. */
function zeroMeaning(method: string): string {
  const stated = loadMethodDoc(method).match(/`0`[^.]*/);
  expect(stated, `${method} documents the meaning of 0`).not.toBeNull();
  return (stated as RegExpMatchArray)[0];
}

describe("CdcEngine size limits", () => {
  // The two limits resolve 0 differently, so a reader who takes the meaning
  // from the neighbouring setter raises the per-event ceiling to 1 GiB while
  // expecting the 64 MiB default.
  it("documents that 0 restores the default queue size", () => {
    expect(zeroMeaning("setMaxQueueSize")).toContain("10,000");
  });

  it("documents that 0 resolves the event size to the hard cap", () => {
    expect(zeroMeaning("setMaxEventSize")).toContain("1 GiB");
  });

  it("fails rather than passing over a doc it cannot read", () => {
    expect(() => loadMethodDoc("noSuchMethod")).toThrow();
  });
});
