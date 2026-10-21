// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from "vitest";
import { BinlogClient } from "../src/client.js";
import { CdcEngine } from "../src/engine.js";
import { CdcStream } from "../src/stream.js";
import type { ClientConfig, StreamConfig } from "../src/types.js";
import { MesErrorCode } from "../src/types.js";
import {
  CLIENT_OPTION_TYPES,
  OPTION_TYPES,
  REFUSAL_ERROR_NAME,
  validateClientOptions,
  validateFilterEntries,
  validateStreamOptions,
} from "../src/validation.js";

const NUL_VALUES = ["a\u0000b", "\u0000"];

function expectNulRefusal(call: () => void, key: string): void {
  let thrown: (Error & { code?: unknown }) | undefined;
  try {
    call();
  } catch (error) {
    thrown = error as Error & { code?: unknown };
  }
  expect(thrown).toBeInstanceOf(RangeError);
  expect(thrown?.name).toBe(REFUSAL_ERROR_NAME);
  expect(thrown?.code).toBe(MesErrorCode.InvalidArg);
  expect(thrown?.message).toContain(key);
}

const stringOptions = (table: Record<string, string>) =>
  Object.entries(table)
    .filter(([, type]) => type === "string")
    .map(([key]) => key);

const FILTER_OPTIONS = ["includeDatabases", "includeTables", "excludeTables"] as const;
const FILTER_SETTERS = {
  includeDatabases: "setIncludeDatabases",
  includeTables: "setIncludeTables",
  excludeTables: "setExcludeTables",
} as const;

describe("embedded NUL in strings crossing the C ABI", () => {
  for (const key of stringOptions(CLIENT_OPTION_TYPES)) {
    it.each(NUL_VALUES)(`validateClientOptions refuses ${key} = %j`, (value) => {
      expectNulRefusal(() => validateClientOptions({ [key]: value } as Partial<ClientConfig>), key);
    });
  }

  for (const key of stringOptions(OPTION_TYPES)) {
    it.each(NUL_VALUES)(`validateStreamOptions refuses ${key} = %j`, (value) => {
      expectNulRefusal(() => validateStreamOptions({ [key]: value } as Partial<StreamConfig>), key);
    });
  }

  it("refuses a NUL-only startBinlogFile as a NUL, not as an empty name", () => {
    expectNulRefusal(() => validateClientOptions({ startBinlogFile: "\u0000" }), "startBinlogFile");
  });

  it.each(FILTER_OPTIONS)("refuses a NUL in a %s entry on a stream", (key) => {
    const config = { [key]: ["ok.t", "db\u0000x.t"] } as Partial<StreamConfig>;
    expectNulRefusal(() => validateStreamOptions(config), key);
    expectNulRefusal(() => new CdcStream(config as StreamConfig), key);
  });

  it("refuses a NUL string before BinlogClient reaches the native layer", () => {
    expectNulRefusal(() => new BinlogClient({ host: "h\u0000x" }), "host");
  });

  it.each(FILTER_OPTIONS)("refuses a NUL in a %s entry on CdcEngine", (key) => {
    const engine = new CdcEngine();
    try {
      expectNulRefusal(() => engine[FILTER_SETTERS[key]](["ok.t", "db\u0000x.t"]), key);
      expect(() => engine[FILTER_SETTERS[key]](["db.t"])).not.toThrow();
    } finally {
      engine.destroy();
    }
  });

  it("refuses a NUL in enableMetadata options", () => {
    const engine = new CdcEngine();
    try {
      expectNulRefusal(() => engine.enableMetadata({ password: "p\u0000w" }), "password");
    } finally {
      engine.destroy();
    }
  });

  it("still accepts ordinary strings", () => {
    expect(() =>
      validateStreamOptions({
        host: "db.example",
        user: "repl",
        password: "pässwörd",
        startGtid: "uuid:1-5",
        includeTables: ["db.t", "db2.*"],
      }),
    ).not.toThrow();
    expect(() => validateFilterEntries("includeTables", ["db.t"])).not.toThrow();
  });
});
