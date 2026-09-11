// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * Execution tests for the conversion rules the `ColumnValue` documentation in
 * `src/types.ts` declares.
 *
 * `type-mapping.test.ts` compares that table against the canonical one in
 * `core/include/mes.h`, which catches drift between the two documents but stays
 * green while the conversion itself is wrong. These cases instead run the addon
 * over synthetic binlog events built to produce a chosen `mes_column_t`, and
 * assert both the JavaScript type and the value for every arm the table
 * declares — including the `MAX_SAFE_INTEGER` boundary on the accepting side,
 * the empty-payload/SQL-NULL distinction and the UTF-8 replacement character.
 *
 * The cases are generated from an explicit parameter model rather than picked
 * by hand: the declared MySQL type, the value class, the collation class and
 * the signedness vary independently. A coverage test at the end requires the
 * generated set to name exactly the MySQL types the documentation table
 * declares, so an arm cannot be left out silently.
 */

import { readFileSync } from "node:fs";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { CdcEngine } from "../src/engine.js";
import type { ColumnValue } from "../src/types.js";
import {
  buildColumnEvents,
  Collation,
  ColType,
  type ColumnFixture,
  datetime2Value,
  dateValue,
  float32,
  float64,
  intLe,
  lengthPrefixed,
  ROTATE_EVENT,
  time2Value,
  timestamp2Value,
  uintBe,
} from "./column-fixture.js";
import { buildEvent, buildRotateBody } from "./helpers.js";

const MAX_SAFE = 9007199254740991n;
const INT64_MAX = 9223372036854775807n;
const UINT64_MAX = 18446744073709551615n;
/** The Unicode replacement character an undecodable byte sequence becomes. */
const REPLACEMENT = "\uFFFD";

function bytes(...values: number[]): Uint8Array {
  return new Uint8Array(values);
}

const utf8 = new TextEncoder();

/* ---- The parameter model ---- */

/**
 * How a value relates to the boundaries of its column type. Every case names
 * one, and a coverage test requires each to be exercised somewhere.
 */
const VALUE_CLASSES = [
  "null",
  "zero",
  "negative",
  "min",
  "max",
  "unsigned-max",
  "safe-boundary",
  "past-safe-boundary",
  "negative-zero",
  "subnormal",
  "infinity",
  "nan",
  "empty",
  "ascii",
  "invalid-utf8",
  "multibyte",
  "embedded-nul",
  "typical",
  "fractional",
] as const;

type ValueClass = (typeof VALUE_CLASSES)[number];

/** What TABLE_MAP says about the column's collation. */
const COLLATION_CLASSES = ["text", "binary", "absent", "n/a"] as const;

type CollationClass = (typeof COLLATION_CLASSES)[number];

/** What the SIGNEDNESS bitmap says about the column. */
const SIGNEDNESS_CLASSES = ["signed", "unsigned", "n/a"] as const;

type SignednessClass = (typeof SIGNEDNESS_CLASSES)[number];

/** The JavaScript type and value a case must observe. */
type Expected =
  | { kind: "null" }
  | { kind: "number"; value: number }
  | { kind: "bigint"; value: bigint }
  | { kind: "string"; value: string }
  | { kind: "bytes"; value: Uint8Array };

interface ColumnCase {
  /** The MySQL type name the documentation table lists this column under. */
  mysqlType: string;
  valueClass: ValueClass;
  collationClass: CollationClass;
  signedness: SignednessClass;
  /** Extra discriminator for cases sharing the three classes above. */
  detail?: string;
  column: ColumnFixture;
  expect: Expected;
}

const NULL_VALUE: Expected = { kind: "null" };

/**
 * The rule the documentation states for integers: a `number` while the value is
 * exactly representable as one, a `bigint` past that. Stated once here so the
 * generated cases derive from the rule; the boundary itself is additionally
 * pinned with literal expectations on both sides below.
 */
function expectInt(value: bigint): Expected {
  if (value > MAX_SAFE || value < -MAX_SAFE) return { kind: "bigint", value };
  return { kind: "number", value: Number(value) };
}

/** A BIGINT UNSIGNED, SET or BIT value past `int64_t` arrives as exact text. */
function expectWideUnsigned(value: bigint): Expected {
  if (value > INT64_MAX) return { kind: "string", value: value.toString() };
  return expectInt(value);
}

/* ---- Case generation ---- */

const cases: ColumnCase[] = [];

function addCase(testCase: ColumnCase): void {
  cases.push(testCase);
}

/** TINYINT through BIGINT: the fixed-width two's-complement family. */
const INT_COLUMNS: Array<{ mysqlType: string; type: number; width: number }> = [
  { mysqlType: "TINYINT", type: ColType.Tiny, width: 1 },
  { mysqlType: "SMALLINT", type: ColType.Short, width: 2 },
  { mysqlType: "MEDIUMINT", type: ColType.Int24, width: 3 },
  { mysqlType: "INT", type: ColType.Long, width: 4 },
  { mysqlType: "BIGINT", type: ColType.LongLong, width: 8 },
];

for (const { mysqlType, type, width } of INT_COLUMNS) {
  const bits = BigInt(width * 8);
  const signedMin = -(1n << (bits - 1n));
  const signedMax = (1n << (bits - 1n)) - 1n;
  const unsignedMax = (1n << bits) - 1n;

  const signedArms: Array<[ValueClass, bigint | null]> = [
    ["null", null],
    ["zero", 0n],
    ["negative", -1n],
    ["min", signedMin],
    ["max", signedMax],
  ];
  for (const [valueClass, value] of signedArms) {
    addCase({
      mysqlType,
      valueClass,
      collationClass: "n/a",
      signedness: "signed",
      column: { type, value: value === null ? null : intLe(value, width), name: "n" },
      expect: value === null ? NULL_VALUE : expectInt(value),
    });
  }

  const unsignedArms: Array<[ValueClass, bigint]> = [
    ["zero", 0n],
    ["unsigned-max", unsignedMax],
  ];
  for (const [valueClass, value] of unsignedArms) {
    addCase({
      mysqlType,
      valueClass,
      collationClass: "n/a",
      signedness: "unsigned",
      column: { type, value: intLe(value, width), unsigned: true, name: "n" },
      expect: expectWideUnsigned(value),
    });
  }
}

// The boundary between number and bigint, with literal expectations on both
// sides. The accepting side is the guard against widening everything to bigint,
// which a test asserting only the rejecting side would not notice.
const BIGINT_BOUNDARY: Array<[ValueClass, string, bigint, Expected]> = [
  [
    "safe-boundary",
    "at MAX_SAFE_INTEGER",
    9007199254740991n,
    { kind: "number", value: 9007199254740991 },
  ],
  [
    "past-safe-boundary",
    "one past MAX_SAFE_INTEGER",
    9007199254740992n,
    { kind: "bigint", value: 9007199254740992n },
  ],
  [
    "safe-boundary",
    "at -MAX_SAFE_INTEGER",
    -9007199254740991n,
    { kind: "number", value: -9007199254740991 },
  ],
  [
    "past-safe-boundary",
    "one past -MAX_SAFE_INTEGER",
    -9007199254740992n,
    { kind: "bigint", value: -9007199254740992n },
  ],
];

for (const [valueClass, detail, value, expected] of BIGINT_BOUNDARY) {
  addCase({
    mysqlType: "BIGINT",
    valueClass,
    collationClass: "n/a",
    signedness: "signed",
    detail,
    column: { type: ColType.LongLong, value: intLe(value, 8), name: "n" },
    expect: expected,
  });
}

// BIGINT UNSIGNED just past int64_t: the first value the core hands over as
// exact text because int_val cannot hold it.
addCase({
  mysqlType: "BIGINT",
  valueClass: "past-safe-boundary",
  collationClass: "n/a",
  signedness: "unsigned",
  detail: "one past INT64_MAX",
  column: { type: ColType.LongLong, value: intLe(INT64_MAX + 1n, 8), unsigned: true, name: "n" },
  expect: { kind: "string", value: "9223372036854775808" },
});

/** YEAR: one stored byte offset by 1900, with zero reserved for "no year". */
const YEAR_ARMS: Array<[ValueClass, string, number | null, Expected]> = [
  ["null", "", null, NULL_VALUE],
  ["zero", "the zero year", 0, { kind: "number", value: 0 }],
  ["typical", "", 126, { kind: "number", value: 2026 }],
  ["max", "", 255, { kind: "number", value: 2155 }],
];

for (const [valueClass, detail, stored, expected] of YEAR_ARMS) {
  addCase({
    mysqlType: "YEAR",
    valueClass,
    collationClass: "n/a",
    signedness: "signed",
    detail: detail || undefined,
    column: { type: ColType.Year, value: stored === null ? null : bytes(stored), name: "y" },
    expect: expected,
  });
}

/** BIT(64): eight stored bytes read big-endian, spanning the whole range. */
const BIT64_ARMS: Array<[ValueClass, string, bigint | null]> = [
  ["null", "", null],
  ["zero", "", 0n],
  ["safe-boundary", "at MAX_SAFE_INTEGER", MAX_SAFE],
  ["past-safe-boundary", "one past MAX_SAFE_INTEGER", MAX_SAFE + 1n],
  ["max", "at INT64_MAX", INT64_MAX],
  ["past-safe-boundary", "one past INT64_MAX", INT64_MAX + 1n],
  ["unsigned-max", "", UINT64_MAX],
];

for (const [valueClass, detail, value] of BIT64_ARMS) {
  addCase({
    mysqlType: "BIT",
    valueClass,
    collationClass: "n/a",
    signedness: "n/a",
    detail: detail || undefined,
    column: {
      type: ColType.Bit,
      meta: [0, 8],
      value: value === null ? null : uintBe(value, 8),
      name: "b",
    },
    expect: value === null ? NULL_VALUE : expectWideUnsigned(value),
  });
}

// BIT(3): the partial-byte width, pinning that the integer is the bit value.
addCase({
  mysqlType: "BIT",
  valueClass: "typical",
  collationClass: "n/a",
  signedness: "n/a",
  detail: "three bits",
  column: { type: ColType.Bit, meta: [3, 0], value: bytes(0b101), name: "b" },
  expect: { kind: "number", value: 5 },
});

/** ENUM: transmitted as MYSQL_TYPE_STRING, surfaced as its 1-based ordinal. */
const ENUM_ARMS: Array<[ValueClass, string, number, Uint8Array | null, Expected]> = [
  ["null", "", 1, null, NULL_VALUE],
  ["zero", "the empty-string member", 1, bytes(0), { kind: "number", value: 0 }],
  ["typical", "the first member", 1, bytes(1), { kind: "number", value: 1 }],
  ["max", "one stored byte", 1, bytes(255), { kind: "number", value: 255 }],
  ["max", "two stored bytes", 2, intLe(65535, 2), { kind: "number", value: 65535 }],
];

for (const [valueClass, detail, size, value, expected] of ENUM_ARMS) {
  addCase({
    mysqlType: "ENUM",
    valueClass,
    collationClass: "n/a",
    signedness: "n/a",
    detail: detail || undefined,
    column: { type: ColType.String, meta: [ColType.Enum, size], value, name: "e" },
    expect: expected,
  });
}

/** SET: transmitted as MYSQL_TYPE_STRING, surfaced as its member bitmask. */
const SET_ARMS: Array<[ValueClass, string, number, bigint | null]> = [
  ["null", "", 1, null],
  ["zero", "no members", 1, 0n],
  ["typical", "", 1, 0b1010n],
  ["safe-boundary", "at MAX_SAFE_INTEGER", 8, MAX_SAFE],
  ["past-safe-boundary", "one past MAX_SAFE_INTEGER", 8, MAX_SAFE + 1n],
  ["unsigned-max", "every member of a 64-member SET", 8, UINT64_MAX],
];

for (const [valueClass, detail, size, value] of SET_ARMS) {
  addCase({
    mysqlType: "SET",
    valueClass,
    collationClass: "n/a",
    signedness: "n/a",
    detail: detail || undefined,
    column: {
      type: ColType.String,
      meta: [ColType.Set, size],
      value: value === null ? null : intLe(value, size),
      name: "s",
    },
    expect: value === null ? NULL_VALUE : expectWideUnsigned(value),
  });
}

/** FLOAT and DOUBLE: always a number, including the non-finite values. */
const FLOAT_ARMS: Array<[ValueClass, string, number | null]> = [
  ["null", "", null],
  ["zero", "", 0],
  ["negative-zero", "", -0],
  ["negative", "", -1.5],
  ["typical", "", 3.5],
  ["infinity", "", Number.POSITIVE_INFINITY],
  ["nan", "", Number.NaN],
];

for (const [valueClass, detail, value] of FLOAT_ARMS) {
  addCase({
    mysqlType: "FLOAT",
    valueClass,
    collationClass: "n/a",
    signedness: "signed",
    detail: detail || undefined,
    column: {
      type: ColType.Float,
      meta: [4],
      value: value === null ? null : float32(value),
      name: "f",
    },
    expect: value === null ? NULL_VALUE : { kind: "number", value },
  });
}

const DOUBLE_ARMS: Array<[ValueClass, string, number | null]> = [
  ["null", "", null],
  ["zero", "", 0],
  ["negative-zero", "", -0],
  ["negative", "", -2.25e300],
  ["max", "", Number.MAX_VALUE],
  ["subnormal", "", 5e-324],
  ["infinity", "negative infinity", Number.NEGATIVE_INFINITY],
  ["nan", "", Number.NaN],
];

for (const [valueClass, detail, value] of DOUBLE_ARMS) {
  addCase({
    mysqlType: "DOUBLE",
    valueClass,
    collationClass: "n/a",
    signedness: "signed",
    detail: detail || undefined,
    column: {
      type: ColType.Double,
      meta: [8],
      value: value === null ? null : float64(value),
      name: "d",
    },
    expect: value === null ? NULL_VALUE : { kind: "number", value },
  });
}

/**
 * The character and BLOB families. Each text/binary pair shares one binlog type
 * byte, so which member of the pair a consumer sees is decided by the
 * collation: a text collation makes it text, the binary collation makes it
 * bytes, and an absent collation leaves the two indistinguishable, which is
 * why the payload then has to stay bytes.
 */
const CHARACTER_COLUMNS: Array<{
  textType: string;
  binaryType: string;
  type: number;
  meta: readonly number[];
  prefixWidth: number;
}> = [
  {
    textType: "CHAR",
    binaryType: "BINARY",
    type: ColType.String,
    meta: [ColType.String, 32],
    prefixWidth: 1,
  },
  {
    textType: "VARCHAR",
    binaryType: "VARBINARY",
    type: ColType.Varchar,
    meta: [40, 0],
    prefixWidth: 1,
  },
  { textType: "TEXT", binaryType: "BLOB", type: ColType.Blob, meta: [2], prefixWidth: 2 },
];

const CHARACTER_PAYLOADS: Array<{
  valueClass: ValueClass;
  payload: Uint8Array | null;
  text: string;
}> = [
  { valueClass: "null", payload: null, text: "" },
  { valueClass: "empty", payload: new Uint8Array(0), text: "" },
  { valueClass: "ascii", payload: utf8.encode("hello"), text: "hello" },
  // A truncated multi-byte sequence: one U+FFFD, with the valid prefix kept.
  {
    valueClass: "invalid-utf8",
    payload: bytes(0x63, 0x61, 0x66, 0xe9),
    text: `caf${REPLACEMENT}`,
  },
  { valueClass: "multibyte", payload: utf8.encode("日本語"), text: "日本語" },
  { valueClass: "embedded-nul", payload: bytes(0x61, 0x00, 0x62), text: "a\u0000b" },
];

const COLLATION_ARMS: Array<{ collationClass: CollationClass; collation: number | undefined }> = [
  { collationClass: "text", collation: Collation.Utf8mb4 },
  { collationClass: "binary", collation: Collation.Binary },
  { collationClass: "absent", collation: undefined },
];

for (const column of CHARACTER_COLUMNS) {
  for (const arm of COLLATION_ARMS) {
    for (const { valueClass, payload, text } of CHARACTER_PAYLOADS) {
      let expected: Expected;
      if (payload === null) {
        expected = NULL_VALUE;
      } else if (arm.collationClass === "text") {
        expected = { kind: "string", value: text };
      } else {
        expected = { kind: "bytes", value: payload };
      }
      addCase({
        mysqlType: arm.collationClass === "binary" ? column.binaryType : column.textType,
        valueClass,
        collationClass: arm.collationClass,
        signedness: "n/a",
        column: {
          type: column.type,
          meta: column.meta,
          collation: arm.collation,
          value: payload === null ? null : lengthPrefixed(payload, column.prefixWidth),
          name: "c",
        },
        expect: expected,
      });
    }
  }
}

// The length-prefix widths the character families use on the wire. Independent
// of the conversion, so covered once each rather than crossed with everything.
const PREFIX_WIDTH_ARMS: Array<[string, string, number, readonly number[], number]> = [
  ["VARCHAR", "a two-byte length prefix", ColType.Varchar, [0, 4], 2],
  ["TEXT", "a one-byte length prefix", ColType.Blob, [1], 1],
  ["TEXT", "a four-byte length prefix", ColType.Blob, [4], 4],
];

for (const [mysqlType, detail, type, meta, prefixWidth] of PREFIX_WIDTH_ARMS) {
  addCase({
    mysqlType,
    valueClass: "ascii",
    collationClass: "text",
    signedness: "n/a",
    detail,
    column: {
      type,
      meta,
      collation: Collation.Utf8mb4,
      value: lengthPrefixed(utf8.encode("hello"), prefixWidth),
      name: "c",
    },
    expect: { kind: "string", value: "hello" },
  });
}

/** DECIMAL: formatted by the core as exact text, never as a float. */
const DECIMAL_ARMS: Array<[ValueClass, Uint8Array | null, Expected]> = [
  ["null", null, NULL_VALUE],
  ["typical", bytes(0x8c, 0x22), { kind: "string", value: "12.34" }],
  ["negative", bytes(0x73, 0xdd), { kind: "string", value: "-12.34" }],
  ["zero", bytes(0x80, 0x00), { kind: "string", value: "0.00" }],
];

for (const [valueClass, value, expected] of DECIMAL_ARMS) {
  addCase({
    mysqlType: "DECIMAL",
    valueClass,
    collationClass: "n/a",
    signedness: "signed",
    column: { type: ColType.NewDecimal, meta: [4, 2], value, name: "amount" },
    expect: expected,
  });
}

/** The temporal family: text, formatted by the core. */
const TEMPORAL_ARMS: Array<[string, ValueClass, string, ColumnFixture, Expected]> = [
  ["DATE", "null", "", { type: ColType.Date, value: null }, NULL_VALUE],
  [
    "DATE",
    "typical",
    "",
    { type: ColType.Date, value: dateValue(2026, 9, 11) },
    { kind: "string", value: "2026-09-11" },
  ],
  [
    "DATE",
    "zero",
    "the zero date",
    { type: ColType.Date, value: dateValue(0, 0, 0) },
    { kind: "string", value: "0000-00-00" },
  ],
  ["TIME", "null", "", { type: ColType.Time2, meta: [0], value: null }, NULL_VALUE],
  [
    "TIME",
    "typical",
    "",
    { type: ColType.Time2, meta: [0], value: time2Value(12, 34, 56) },
    { kind: "string", value: "12:34:56" },
  ],
  [
    "TIME",
    "fractional",
    "",
    { type: ColType.Time2, meta: [6], value: time2Value(12, 34, 56, 123456, 6) },
    { kind: "string", value: "12:34:56.123456" },
  ],
  ["DATETIME", "null", "", { type: ColType.Datetime2, meta: [0], value: null }, NULL_VALUE],
  [
    "DATETIME",
    "typical",
    "",
    { type: ColType.Datetime2, meta: [0], value: datetime2Value(2026, 9, 11, 12, 34, 56) },
    { kind: "string", value: "2026-09-11 12:34:56" },
  ],
  [
    "DATETIME",
    "fractional",
    "",
    {
      type: ColType.Datetime2,
      meta: [3],
      value: datetime2Value(2026, 9, 11, 12, 34, 56, 123000, 3),
    },
    { kind: "string", value: "2026-09-11 12:34:56.123" },
  ],
  ["TIMESTAMP", "null", "", { type: ColType.Timestamp2, meta: [0], value: null }, NULL_VALUE],
  [
    "TIMESTAMP",
    "typical",
    "whole epoch seconds",
    { type: ColType.Timestamp2, meta: [0], value: timestamp2Value(1735689600) },
    { kind: "string", value: "1735689600" },
  ],
  [
    "TIMESTAMP",
    "fractional",
    "epoch seconds at the column's declared precision",
    { type: ColType.Timestamp2, meta: [6], value: timestamp2Value(1735689600, 123456, 6) },
    { kind: "string", value: "1735689600.123456" },
  ],
];

for (const [mysqlType, valueClass, detail, column, expected] of TEMPORAL_ARMS) {
  addCase({
    mysqlType,
    valueClass,
    collationClass: "n/a",
    signedness: "n/a",
    detail: detail || undefined,
    column: { ...column, name: "t" },
    expect: expected,
  });
}

/**
 * JSON and GEOMETRY are bytes whatever the charset metadata says; VECTOR
 * occupies a charset slot but always carries the binary collation. A
 * zero-length payload is still bytes — only SQL NULL is null.
 */
const BYTE_PAYLOAD = bytes(0x00, 0x01, 0xff, 0x89);

const BYTE_COLUMNS: Array<[string, number, number | undefined]> = [
  ["JSON", ColType.Json, undefined],
  ["GEOMETRY", ColType.Geometry, undefined],
  ["VECTOR", ColType.Vector, Collation.Binary],
  ["VECTOR", ColType.Vector, undefined],
];

const BYTE_PAYLOAD_ARMS: Array<[ValueClass, Uint8Array | null]> = [
  ["null", null],
  ["empty", new Uint8Array(0)],
  ["typical", BYTE_PAYLOAD],
];

for (const [mysqlType, type, collation] of BYTE_COLUMNS) {
  for (const [valueClass, payload] of BYTE_PAYLOAD_ARMS) {
    addCase({
      mysqlType,
      valueClass,
      collationClass: collation === undefined ? "absent" : "binary",
      signedness: "n/a",
      column: {
        type,
        meta: [4],
        collation,
        value: payload === null ? null : lengthPrefixed(payload, 4),
        name: "v",
      },
      expect: payload === null ? NULL_VALUE : { kind: "bytes", value: payload },
    });
  }
}

/* ---- Assertions ---- */

function describeCase(testCase: ColumnCase): string {
  const parts = [testCase.mysqlType, testCase.valueClass];
  if (testCase.collationClass !== "n/a") parts.push(`${testCase.collationClass} collation`);
  if (testCase.signedness === "unsigned") parts.push("unsigned");
  if (testCase.detail) parts.push(testCase.detail);
  return `${parts.join(", ")} -> ${testCase.expect.kind}`;
}

/** Assert both the JavaScript type and the value, never just the value. */
function assertColumnValue(actual: ColumnValue | undefined, expected: Expected): void {
  expect(actual, "the row carries the expected key").not.toBeUndefined();
  switch (expected.kind) {
    case "null":
      expect(actual).toBeNull();
      break;
    case "number":
      expect(typeof actual).toBe("number");
      // toBe compares with Object.is, which separates -0 from 0 and matches NaN.
      expect(actual).toBe(expected.value);
      break;
    case "bigint":
      expect(typeof actual).toBe("bigint");
      expect(actual).toBe(expected.value);
      break;
    case "string":
      expect(typeof actual).toBe("string");
      expect(actual).toBe(expected.value);
      break;
    case "bytes":
      expect(actual).toBeInstanceOf(Uint8Array);
      expect(Array.from(actual as Uint8Array)).toEqual(Array.from(expected.value));
      break;
  }
}

describe("ColumnValue marshalling", () => {
  let engine: CdcEngine;
  let tableId = 0;

  beforeEach(async () => {
    engine = await CdcEngine.create();
  });

  afterEach(() => {
    engine?.destroy();
  });

  /** Feed one row of `columns` and return the decoded after image. */
  function readRow(columns: readonly ColumnFixture[]): Record<string, ColumnValue> {
    tableId += 1;
    engine.feed(buildColumnEvents(tableId, "mes_test", "marshalling", columns));
    const event = engine.nextEvent();
    expect(event, "the fixture decodes to one INSERT event").not.toBeNull();
    expect(event?.after, "the INSERT carries an after image").not.toBeNull();
    return event?.after as Record<string, ColumnValue>;
  }

  describe("declared conversion rules", () => {
    it.each(cases.map((testCase) => [describeCase(testCase), testCase] as const))(
      "%s",
      (_title, testCase) => {
        const key = testCase.column.name as string;
        const row = readRow([testCase.column]);
        expect(Object.keys(row)).toEqual([key]);
        assertColumnValue(row[key], testCase.expect);
      },
    );
  });

  describe("empty payloads versus SQL NULL", () => {
    it("keeps a zero-length binary payload an empty Uint8Array", () => {
      const row = readRow([
        {
          type: ColType.Blob,
          meta: [2],
          value: lengthPrefixed(new Uint8Array(0), 2),
          name: "blob",
        },
        { type: ColType.Blob, meta: [2], value: null, name: "missing" },
      ]);
      expect(row.blob).toBeInstanceOf(Uint8Array);
      expect((row.blob as Uint8Array).length).toBe(0);
      expect(row.missing).toBeNull();
    });

    it("keeps a zero-length text payload an empty string", () => {
      const row = readRow([
        {
          type: ColType.Blob,
          meta: [2],
          collation: Collation.Utf8mb4,
          value: lengthPrefixed(new Uint8Array(0), 2),
          name: "note",
        },
        {
          type: ColType.Blob,
          meta: [2],
          collation: Collation.Utf8mb4,
          value: null,
          name: "missing",
        },
      ]);
      expect(row.note).toBe("");
      expect(row.missing).toBeNull();
    });
  });

  describe("invalid UTF-8 substitution", () => {
    const substitutions: Array<[string, Uint8Array, string]> = [
      ["a lone invalid byte", bytes(0xff), REPLACEMENT],
      ["an invalid byte between valid ones", bytes(0x61, 0xff, 0x62), `a${REPLACEMENT}b`],
      ["a truncated multi-byte sequence", bytes(0x63, 0x61, 0x66, 0xe9), `caf${REPLACEMENT}`],
    ];

    it.each(substitutions)("replaces %s in a text column", (_title, payload, expected) => {
      const row = readRow([
        {
          type: ColType.Varchar,
          meta: [40, 0],
          collation: Collation.Utf8mb4,
          value: lengthPrefixed(payload, 1),
          name: "text",
        },
      ]);
      expect(row.text).toBe(expected);
    });

    it("leaves the same bytes intact in a binary column", () => {
      const payload = bytes(0x63, 0x61, 0x66, 0xe9);
      const row = readRow([
        {
          type: ColType.Varchar,
          meta: [40, 0],
          collation: Collation.Binary,
          value: lengthPrefixed(payload, 1),
          name: "raw",
        },
      ]);
      expect(Array.from(row.raw as Uint8Array)).toEqual(Array.from(payload));
    });

    it("replaces an undecodable byte in a column name", () => {
      const row = readRow([
        { type: ColType.Long, value: intLe(7, 4), name: bytes(0x62, 0x61, 0x64, 0xff) },
      ]);
      expect(Object.keys(row)).toEqual([`bad${REPLACEMENT}`]);
      expect(row[`bad${REPLACEMENT}`]).toBe(7);
    });
  });

  describe("column keys", () => {
    it("falls back to the index for a column with no name", () => {
      tableId += 1;
      engine.feed(
        buildColumnEvents(tableId, "mes_test", "keys", [
          { type: ColType.Long, value: intLe(1, 4), name: "first" },
          { type: ColType.Long, value: intLe(2, 4) },
          { type: ColType.Long, value: intLe(3, 4), name: "third" },
        ]),
      );
      const event = engine.nextEvent();
      expect(event?.after).toEqual({ first: 1, "1": 2, third: 3 });
      // One unresolved name makes the whole event's keys untrustworthy.
      expect(event?.namesResolved).toBe(false);
    });

    it("reports resolved names when every column is named", () => {
      tableId += 1;
      engine.feed(
        buildColumnEvents(tableId, "mes_test", "keys", [
          { type: ColType.Long, value: intLe(1, 4), name: "first" },
          { type: ColType.Long, value: intLe(2, 4), name: "second" },
        ]),
      );
      expect(engine.nextEvent()?.namesResolved).toBe(true);
    });

    it("keeps a multi-byte column name", () => {
      const row = readRow([{ type: ColType.Long, value: intLe(1, 4), name: "名前" }]);
      expect(Object.keys(row)).toEqual(["名前"]);
    });

    it("keeps the last value for a duplicated column name", () => {
      const row = readRow([
        { type: ColType.Long, value: intLe(1, 4), name: "dup" },
        {
          type: ColType.Varchar,
          meta: [40, 0],
          collation: Collation.Utf8mb4,
          value: lengthPrefixed(utf8.encode("second"), 1),
          name: "dup",
        },
      ]);
      expect(row).toEqual({ dup: "second" });
    });
  });

  describe("the column name cache", () => {
    it("returns the same key for a repeated name", () => {
      const column: ColumnFixture = { type: ColType.Long, value: intLe(41, 4), name: "repeated" };
      const first = readRow([column]);
      const second = readRow([column]);
      expect(Object.keys(first)).toEqual(["repeated"]);
      expect(Object.keys(second)).toEqual(["repeated"]);
      expect(second.repeated).toBe(41);
    });

    it("keeps keys correct when a name's storage address is reused", () => {
      // Re-mapping the same table id frees the previous metadata, so the next
      // name of equal length is likely to land on an address the cache still
      // holds a key for. Only the byte comparison behind that address stops a
      // stale key from being handed back.
      for (let index = 0; index < 400; index++) {
        const name = `col_${String(index).padStart(6, "0")}`;
        engine.feed(
          buildColumnEvents(7, "mes_test", "churn", [
            { type: ColType.Long, value: intLe(index, 4), name },
          ]),
        );
        const row = engine.nextEvent()?.after;
        expect({ index, keys: Object.keys(row ?? {}), value: row?.[name] }).toEqual({
          index,
          keys: [name],
          value: index,
        });
      }
    });

    it("keeps keys correct past the cache's capacity", () => {
      // The cache holds one entry per distinct name address and clears itself
      // on reaching its bound. Enough distinct names to cross that bound must
      // not lose or confuse a key.
      for (let index = 0; index < 8300; index++) {
        const name = `wide_${String(index).padStart(6, "0")}`;
        engine.feed(
          buildColumnEvents(index + 1, "mes_test", "capacity", [
            { type: ColType.Long, value: intLe(index, 4), name },
          ]),
        );
        const row = engine.nextEvent()?.after;
        if (row == null || row[name] !== index || Object.keys(row).length !== 1) {
          // Report the first mismatch with its index rather than leaving a bare
          // failure among thousands of passing assertions.
          expect({ index, keys: Object.keys(row ?? {}), value: row?.[name] }).toEqual({
            index,
            keys: [name],
            value: index,
          });
        }
      }
    });
  });

  describe("binlog position", () => {
    const offsets: Array<[string, bigint, string, number | bigint]> = [
      ["an offset at MAX_SAFE_INTEGER as a number", MAX_SAFE, "number", 9007199254740991],
      [
        "an offset one past MAX_SAFE_INTEGER as a bigint",
        MAX_SAFE + 1n,
        "bigint",
        9007199254740992n,
      ],
    ];

    it.each(offsets)("reports %s", (_title, offset, kind, expected) => {
      // A ROTATE event carries the full eight-byte offset, so it is the only
      // way to place the stream past what a JS number holds exactly.
      engine.feed(buildEvent(ROTATE_EVENT, 0, buildRotateBody(offset, "binlog.000007")));
      tableId += 1;
      engine.feed(
        buildColumnEvents(tableId, "mes_test", "position", [
          { type: ColType.Long, value: intLe(1, 4), name: "n" },
        ]),
      );

      const position = engine.getPosition();
      expect(typeof position.offset).toBe(kind);
      expect(position.offset).toBe(expected);

      const event = engine.nextEvent();
      expect(event?.position.file).toBe("binlog.000007");
      expect(typeof event?.position.offset).toBe(kind);
      expect(event?.position.offset).toBe(expected);
    });
  });

  describe("coverage of the documented table", () => {
    /** The MySQL column types the `ColumnValue` documentation table declares. */
    function declaredColumnTypes(): string[] {
      const source = readFileSync(new URL("../src/types.ts", import.meta.url), "utf8");
      const declared = new Set<string>();
      for (const line of source.split("\n")) {
        const match = line.match(
          /^\s*\*\s+[A-Za-z][A-Za-z0-9_ |]*=>\s*([A-Z0-9]+(?: [A-Z0-9]+)*)\s*$/,
        );
        const row = match?.[1];
        if (row === undefined) continue;
        for (const mysqlType of row.split(" ")) declared.add(mysqlType);
      }
      return [...declared].sort();
    }

    it("executes a case for every MySQL type the table declares", () => {
      const exercised = [...new Set(cases.map((testCase) => testCase.mysqlType))].sort();
      expect(exercised).toEqual(declaredColumnTypes());
    });

    it("produces every member of the ColumnValue union", () => {
      const kinds = [...new Set(cases.map((testCase) => testCase.expect.kind))].sort();
      expect(kinds).toEqual(["bigint", "bytes", "null", "number", "string"]);
    });

    it("exercises every value class the parameter model names", () => {
      const exercised = new Set(cases.map((testCase) => testCase.valueClass));
      expect(VALUE_CLASSES.filter((valueClass) => !exercised.has(valueClass))).toEqual([]);
    });

    it("exercises every collation and signedness class the model names", () => {
      const collations = new Set(cases.map((testCase) => testCase.collationClass));
      const signedness = new Set(cases.map((testCase) => testCase.signedness));
      expect(COLLATION_CLASSES.filter((arm) => !collations.has(arm))).toEqual([]);
      expect(SIGNEDNESS_CLASSES.filter((arm) => !signedness.has(arm))).toEqual([]);
    });
  });
});
