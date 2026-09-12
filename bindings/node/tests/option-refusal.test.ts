// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";
import { BinlogClient } from "../src/client.js";
import { CdcEngine } from "../src/engine.js";
import { CdcStream } from "../src/stream.js";
import type { ClientConfig, StreamConfig } from "../src/types.js";
import { MesErrorCode } from "../src/types.js";
import { OPTION_TYPES, type OptionType, REFUSAL_ERROR_NAME } from "../src/validation.js";
import { loadBindingContract } from "./contract-fixture.js";

/**
 * How a refused option value presents itself to a caller. These three are what
 * a caller can branch on — `instanceof`, `name`, `code` — so they are what has
 * to read alike whichever entry point received the value. The message is not
 * part of it: the layers word a refusal for the option at hand, not for the
 * surface that happened to see it.
 */
interface Presentation {
  className: "Error" | "TypeError" | "RangeError";
  name: string;
  code: unknown;
}

function classNameOf(error: Error): Presentation["className"] {
  if (error instanceof TypeError) return "TypeError";
  if (error instanceof RangeError) return "RangeError";
  return "Error";
}

/** Run `call` and describe how it refused, or return null if it did not. */
function refusalFrom(call: () => void): Presentation | null {
  try {
    call();
    return null;
  } catch (error) {
    const thrown = error as Error & { code?: unknown };
    return { className: classNameOf(thrown), name: thrown.name, code: thrown.code };
  }
}

/** The presentation every refused option value carries. */
function refusal(className: Presentation["className"]): Presentation {
  return { className, name: REFUSAL_ERROR_NAME, code: MesErrorCode.InvalidArg };
}

const contract = loadBindingContract();

/**
 * Options the native client config parser reads, taken from the table it
 * iterates rather than restated here: a hand-copied list could claim an option
 * reaches the addon when it no longer does, and the comparison below would then
 * be run entirely inside the TypeScript validator.
 */
function nativeConfigKeys(): Set<string> {
  const source = readFileSync(new URL("../src/addon/config_parser.h", import.meta.url), "utf8");
  const table = source.match(/kConfigOptions\[\] = \{([\s\S]*?)\n\};/);
  if (table === null) throw new Error("config_parser.h no longer declares kConfigOptions");
  const keys = [...(table[1] ?? "").matchAll(/\{"(\w+)",/g)].map((entry) => entry[1] ?? "");
  if (keys.length === 0) throw new Error("config_parser.h declares no parsed option");
  return new Set(keys);
}

const NATIVE_CONFIG_KEYS = nativeConfigKeys();

/** Engine setters that take one option as a positional argument. */
const ENGINE_SETTERS: Record<string, (engine: CdcEngine, value: unknown) => void> = {
  maxQueueSize: (engine, value) => engine.setMaxQueueSize(value as number),
  maxEventSize: (engine, value) => engine.setMaxEventSize(value as number),
  includeDatabases: (engine, value) => engine.setIncludeDatabases(value as string[]),
  includeTables: (engine, value) => engine.setIncludeTables(value as string[]),
  excludeTables: (engine, value) => engine.setExcludeTables(value as string[]),
};

/** No server listens here, so a config nothing refuses fails at the connection. */
const UNREACHABLE_PORT = 19999;
const BASE_CONFIG = { host: "127.0.0.1", port: UNREACHABLE_PORT };

interface EntryPoint {
  /** How a caller reaches it, as it reads in an assertion message. */
  readonly id: string;
  /** Whether an option can be supplied here at all. */
  readonly reaches: (key: string) => boolean;
  readonly apply: (key: string, options: Record<string, unknown>) => void;
}

/**
 * Every public way an option value is handed to this binding. A value refused
 * by one of them has to be refused the same way by each of the others that
 * sees it, so every case below runs through all of them and the presentations
 * are compared against each other rather than merely against "it threw".
 */
const ENTRY_POINTS: readonly EntryPoint[] = [
  {
    id: "stream constructor",
    reaches: () => true,
    apply: (_key, options) => {
      new CdcStream(options as StreamConfig);
    },
  },
  {
    id: "stream configure",
    reaches: () => true,
    apply: (_key, options) => {
      new CdcStream({ host: "127.0.0.1" }).configure(options as Partial<StreamConfig>);
    },
  },
  {
    id: "client constructor",
    reaches: (key) => NATIVE_CONFIG_KEYS.has(key),
    apply: (_key, options) => {
      new BinlogClient({ ...BASE_CONFIG, ...options } as ClientConfig).destroy();
    },
  },
  {
    id: "engine enableMetadata",
    reaches: (key) => NATIVE_CONFIG_KEYS.has(key),
    apply: (_key, options) => {
      const engine = new CdcEngine();
      try {
        engine.enableMetadata({ ...BASE_CONFIG, ...options } as ClientConfig);
      } finally {
        engine.destroy();
      }
    },
  },
  {
    id: "engine setter",
    reaches: (key) => Object.hasOwn(ENGINE_SETTERS, key),
    apply: (key, options) => {
      const setter = ENGINE_SETTERS[key];
      if (setter === undefined) throw new Error(`no engine setter for ${key}`);
      const engine = new CdcEngine();
      try {
        setter(engine, options[key]);
      } finally {
        engine.destroy();
      }
    },
  },
];

/** A value of the wrong runtime type for each type an option declares. */
const WRONG_TYPE_VALUES: Record<OptionType, unknown> = {
  integer: "text",
  string: 1,
  boolean: "yes",
  stringArray: "mydb",
  callback: 42,
};

/**
 * One value outside the window its option accepts, with the entry points that
 * check that window. The list is exact in both directions: an entry point named
 * here must refuse the value, and one that could carry the option but is not
 * named must not refuse it as an argument — the native metadata connection
 * parses a client config without applying the queue and start-position limits
 * a client applies at connect.
 */
interface RangeCase {
  key: string;
  value: number;
  /** Options supplied alongside so the check under test is the one that fires. */
  companions?: Record<string, unknown>;
  refusedBy: readonly string[];
}

const RANGE_CASES: readonly RangeCase[] = [
  {
    key: "port",
    value: 70_000,
    refusedBy: [
      "stream constructor",
      "stream configure",
      "client constructor",
      "engine enableMetadata",
    ],
  },
  {
    key: "sslMode",
    value: 9,
    refusedBy: [
      "stream constructor",
      "stream configure",
      "client constructor",
      "engine enableMetadata",
    ],
  },
  {
    key: "maxQueueSize",
    value: -1,
    refusedBy: ["stream constructor", "stream configure", "client constructor", "engine setter"],
  },
  {
    key: "maxEventSize",
    value: 2 ** 32,
    refusedBy: ["stream constructor", "stream configure", "client constructor", "engine setter"],
  },
  {
    key: "maxQueueBytes",
    value: -1,
    refusedBy: ["stream constructor", "stream configure", "client constructor"],
  },
  {
    key: "startBinlogPosition",
    value: 3,
    companions: { startBinlogFile: "binlog.000001" },
    refusedBy: ["stream constructor", "stream configure", "client constructor"],
  },
];

describe("refused option values", () => {
  it("presents a wrongly-typed value alike on every entry point that sees it", () => {
    for (const [key, type] of Object.entries(OPTION_TYPES) as Array<[string, OptionType]>) {
      const options = { [key]: WRONG_TYPE_VALUES[type] };
      for (const entry of ENTRY_POINTS) {
        if (!entry.reaches(key)) continue;
        const label = `${key} = ${String(WRONG_TYPE_VALUES[type])} via ${entry.id}`;
        expect(
          refusalFrom(() => entry.apply(key, options)),
          label,
        ).toEqual(refusal("TypeError"));
      }
    }
  });

  it("presents an out-of-range value alike on every entry point that checks it", () => {
    for (const testCase of RANGE_CASES) {
      const options = { ...testCase.companions, [testCase.key]: testCase.value };
      for (const entry of ENTRY_POINTS) {
        if (!entry.reaches(testCase.key)) continue;
        const label = `${testCase.key} = ${testCase.value} via ${entry.id}`;
        const observed = refusalFrom(() => entry.apply(testCase.key, options));
        if (testCase.refusedBy.includes(entry.id)) {
          expect(observed, label).toEqual(refusal("RangeError"));
        } else {
          // Reached the option without applying that window: whatever came back
          // is not an argument refusal, so no presentation is being compared.
          expect(observed?.code, `${label} is not refused as an argument`).not.toBe(
            MesErrorCode.InvalidArg,
          );
        }
      }
    }
  });

  it("names a value for every type an option can declare", () => {
    const declared = new Set(Object.values(OPTION_TYPES));
    for (const type of declared) {
      expect(WRONG_TYPE_VALUES[type], `a value of the wrong type for ${type}`).toBeDefined();
    }
  });

  it("routes every option the addon parses through a native entry point", () => {
    // Without this the cases above could run entirely inside the TypeScript
    // validator and prove nothing about the addon.
    const nativeIds = ["client constructor", "engine enableMetadata"];
    for (const key of NATIVE_CONFIG_KEYS) {
      expect(OPTION_TYPES, `${key} is a declared option`).toHaveProperty(key);
      const reached = ENTRY_POINTS.filter((entry) => entry.reaches(key)).map((entry) => entry.id);
      expect(
        reached.filter((id) => nativeIds.includes(id)),
        `${key} reaches the addon`,
      ).toEqual(nativeIds);
    }
    // Every option the shared contract declares but the stream-level retry
    // budget is parsed natively, so one added to the contract cannot skip the
    // addon unnoticed.
    for (const option of contract.options) {
      if (option.node === "maxReconnectAttempts") continue;
      expect(NATIVE_CONFIG_KEYS.has(option.node), `${option.node} is parsed natively`).toBe(true);
    }
  });

  it("checks at least one window the native layer owns alone", () => {
    // sslMode is refused by the addon on both native entry points, so reverting
    // either layer's presentation to the other's fails the comparison above.
    const nativeOnly = RANGE_CASES.filter(
      (testCase) =>
        testCase.refusedBy.includes("client constructor") ||
        testCase.refusedBy.includes("engine enableMetadata"),
    );
    expect(nativeOnly.map((testCase) => testCase.key)).toContain("sslMode");
  });
});
