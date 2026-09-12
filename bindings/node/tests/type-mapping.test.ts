// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";

/**
 * The column-type contract lives in three places: the canonical table in
 * `core/include/mes.h` and the table each binding restates for its own users.
 * All three are documentation comments, so nothing but a test stops them from
 * drifting apart — which is how TIMESTAMP came to be documented as a number
 * while the core produced a string.
 *
 * Each file writes its rows as `<runtime type> => <MYSQL TYPE> <MYSQL TYPE>...`
 * and this test asserts that all three cover the same MySQL types, list each
 * type exactly once, and agree on the runtime type it produces.
 */

const repoRoot = fileURLToPath(new URL("../../..", import.meta.url));

/** Runtime type each MES_COL_* value produces in each binding. */
const RUNTIME_TYPES = {
  MES_COL_INT: { node: "number | bigint", python: "int" },
  MES_COL_DOUBLE: { node: "number", python: "float" },
  MES_COL_STRING: { node: "string", python: "str" },
  MES_COL_BYTES: { node: "Uint8Array", python: "bytes" },
} as const;

type Target = keyof typeof RUNTIME_TYPES;

const ROW_PATTERN = /^[\s*]*([A-Za-z_][A-Za-z0-9_ |]*?)\s*=>\s*([A-Z0-9]+(?: [A-Z0-9]+)*)\s*$/;

const EXPECTED_COLUMN_TYPES = [
  "BIGINT",
  "BINARY",
  "BIT",
  "BLOB",
  "CHAR",
  "DATE",
  "DATETIME",
  "DECIMAL",
  "DOUBLE",
  "ENUM",
  "FLOAT",
  "GEOMETRY",
  "INT",
  "JSON",
  "MEDIUMINT",
  "SET",
  "SMALLINT",
  "TEXT",
  "TIME",
  "TIMESTAMP",
  "TINYINT",
  "VARBINARY",
  "VARCHAR",
  "VECTOR",
  "YEAR",
];

/** Parse the `<target> => <TYPES>` rows a documentation table declares. */
function parseTable(file: string, targets: readonly string[]): Map<string, string> {
  const source = readFileSync(new URL(file, `file://${repoRoot}`), "utf8");
  const mapping = new Map<string, string>();
  let rows = 0;
  for (const line of source.split("\n")) {
    const [, target, types] = line.match(ROW_PATTERN) ?? [];
    if (target === undefined || types === undefined) continue;
    if (!targets.includes(target)) continue;
    rows += 1;
    for (const mysqlType of types.split(" ")) {
      expect(mapping.has(mysqlType), `${file} lists ${mysqlType} more than once`).toBe(false);
      mapping.set(mysqlType, target);
    }
  }
  expect(rows, `${file} must declare one row per column type category`).toBe(targets.length);
  return mapping;
}

describe("column type mapping", () => {
  const canonical = parseTable("core/include/mes.h", Object.keys(RUNTIME_TYPES));

  it("covers the MySQL column types every binding has to represent", () => {
    expect([...canonical.keys()].sort()).toEqual(EXPECTED_COLUMN_TYPES);
  });

  it.each([
    ["node", "bindings/node/src/types.ts"] as const,
    ["python", "bindings/python/src/mysql_event_stream/types.py"] as const,
  ])("keeps the %s public doc in step with mes.h", (binding, file) => {
    const expected = new Map(
      [...canonical].map(([mysqlType, target]) => [
        mysqlType,
        RUNTIME_TYPES[target as Target][binding],
      ]),
    );
    const targets = Object.values(RUNTIME_TYPES).map((types) => types[binding]);
    expect(parseTable(file, targets)).toEqual(expected);
  });

  // Pinned individually because each has been documented wrongly before, and
  // because a table that agrees with itself on every surface can still agree
  // on the wrong answer. TIMESTAMP is text from the decoder; ENUM and SET are
  // ordinals kept out of the charset index space, so they can never be the
  // text of their labels; VECTOR is in that index space with the binary
  // collation, so it is always bytes.
  it.each([
    ["TIMESTAMP", "MES_COL_STRING"],
    ["ENUM", "MES_COL_INT"],
    ["SET", "MES_COL_INT"],
    ["VECTOR", "MES_COL_BYTES"],
  ])("documents %s as %s, matching the core", (mysqlType, expected) => {
    expect(canonical.get(mysqlType)).toBe(expected);
  });
});
