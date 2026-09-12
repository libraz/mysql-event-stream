// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from "vitest";
import { BinlogClient } from "../src/client.js";
import { OPTION_RANGES } from "../src/contract.js";
import { CdcStream } from "../src/stream.js";
import type { ClientConfig, StreamConfig } from "../src/types.js";
import { MesErrorCode } from "../src/types.js";
import { OPTION_TYPES, type OptionType, REFUSAL_ERROR_NAME } from "../src/validation.js";
import { acceptedMinimum, companionOptions, loadBindingContract } from "./contract-fixture.js";

const contract = loadBindingContract();

/** What a caller can observe about a rejection, compared path against path. */
interface Rejection {
  /** Built-in class the refusal is an instance of, which `name` no longer says. */
  className: string;
  name: string;
  message: string;
  code: unknown;
}

function classNameOf(error: Error): string {
  if (error instanceof TypeError) return "TypeError";
  if (error instanceof RangeError) return "RangeError";
  return "Error";
}

/** Run `call` and describe how it failed, or return null if it succeeded. */
function rejectionFrom(call: () => void): Rejection | null {
  try {
    call();
    return null;
  } catch (error) {
    const thrown = error as Error & { code?: unknown };
    return {
      className: classNameOf(thrown),
      name: thrown.name,
      message: thrown.message,
      code: thrown.code,
    };
  }
}

/**
 * The ways a caller can hand options to a stream. Both must accept and reject
 * exactly the same input, so every case below runs through all of them and the
 * rejections are compared to each other rather than merely to "it threw".
 */
const ENTRY_PATHS = [
  {
    name: "constructor",
    apply: (options: Record<string, unknown>): void => {
      new CdcStream(options as StreamConfig);
    },
  },
  {
    name: "configure",
    apply: (options: Record<string, unknown>): void => {
      new CdcStream({ host: "127.0.0.1" }).configure(options as Partial<StreamConfig>);
    },
  },
] as const;

/**
 * One representative value per declared option type, plus two values no option
 * accepts. Crossed with the option table this enumerates every
 * option × wrong-typed-value pair instead of a hand-picked selection.
 */
const SAMPLE_VALUES: Array<{ label: string; value: unknown; satisfies: OptionType | null }> = [
  { label: "a string", value: "text", satisfies: "string" },
  { label: "an integer", value: 2, satisfies: "integer" },
  { label: "a boolean", value: true, satisfies: "boolean" },
  { label: "an array of strings", value: ["mydb"], satisfies: "stringArray" },
  { label: "a function", value: () => {}, satisfies: "callback" },
  { label: "null", value: null, satisfies: null },
  { label: "a non-integer number", value: 1.5, satisfies: null },
];

const OPTION_ENTRIES = Object.entries(OPTION_TYPES) as Array<[string, OptionType]>;

const WRONG_TYPE_CASES = OPTION_ENTRIES.flatMap(([key, type]) =>
  SAMPLE_VALUES.filter((sample) => sample.satisfies !== type).map((sample) => ({ key, sample })),
);

const DECLARED_TYPE_CASES = OPTION_ENTRIES.map(([key, type]) => ({
  key,
  sample: SAMPLE_VALUES.find((sample) => sample.satisfies === type),
}));

/**
 * A configuration supplying one option with a value it accepts: the sample of
 * its declared type raised to the minimum the option accepts once its
 * companions are there, plus those companions.
 */
function acceptableConfig(key: string, value: unknown): Record<string, unknown> {
  const minimum = acceptedMinimum(key);
  const accepted =
    minimum !== undefined && typeof value === "number" ? Math.max(value, minimum) : value;
  return { ...companionOptions(key), [key]: accepted };
}

const RANGE_CASES = Object.entries(OPTION_RANGES).flatMap(([key, range]) => [
  { key, accepted: range.min, rejected: range.min - 1, edge: "minimum" },
  ...(range.max === null
    ? []
    : [{ key, accepted: range.max, rejected: range.max + 1, edge: "maximum" }]),
]);

/**
 * Keys no option table entry matches. `constructor` and `toString` are here
 * because a prototype member is not a recognized option either, and
 * `__proto__` is reached through a computed key so that it becomes an own
 * property rather than the object's prototype.
 */
const UNKNOWN_KEYS = [
  "hots",
  "portt",
  "ssl_mode",
  "onMetadataErrors",
  "constructor",
  "toString",
  "__proto__",
];

const NON_OBJECT_CONFIGS: unknown[] = [null, undefined, 42, "host=127.0.0.1", true];

/** Assert both entry paths rejected the same input in exactly the same way. */
function expectSharedRejection(
  options: Record<string, unknown>,
  label: string,
  expected: { className: string; contains: string },
): void {
  const [first, ...rest] = ENTRY_PATHS.map((path) => ({
    path: path.name,
    rejection: rejectionFrom(() => path.apply(options)),
  }));
  expect(first?.rejection, `${label} via ${first?.path}`).not.toBeNull();
  expect(first?.rejection?.className, `${label} via ${first?.path}`).toBe(expected.className);
  // The category name is the same for every refusal, whichever layer decided
  // it; `tests/option-refusal.test.ts` compares the two layers against it.
  expect(first?.rejection?.name, `${label} via ${first?.path}`).toBe(REFUSAL_ERROR_NAME);
  expect(first?.rejection?.message, `${label} via ${first?.path}`).toContain(expected.contains);
  expect(first?.rejection?.code, `${label} via ${first?.path}`).toBe(MesErrorCode.InvalidArg);
  for (const other of rest) {
    expect(other.rejection, `${label}: ${other.path} matches ${first?.path}`).toEqual(
      first?.rejection,
    );
  }
}

describe("stream option validation", () => {
  it("covers every option in the table with every sample value", () => {
    // 23 options x 6 values of the wrong type, and one accepted value each.
    expect(WRONG_TYPE_CASES).toHaveLength(OPTION_ENTRIES.length * (SAMPLE_VALUES.length - 1));
    for (const testCase of DECLARED_TYPE_CASES) {
      expect(testCase.sample, `${testCase.key} has a sample of its declared type`).toBeDefined();
    }
  });

  it("rejects a wrongly-typed value the same way on every entry path", () => {
    for (const { key, sample } of WRONG_TYPE_CASES) {
      expectSharedRejection({ [key]: sample.value }, `${key} = ${sample.label}`, {
        className: "TypeError",
        contains: key,
      });
    }
  });

  it("accepts a value of the option's declared type on every entry path", () => {
    for (const { key, sample } of DECLARED_TYPE_CASES) {
      for (const path of ENTRY_PATHS) {
        expect(
          rejectionFrom(() => path.apply(acceptableConfig(key, sample?.value))),
          `${key} = ${sample?.label} via ${path.name}`,
        ).toBeNull();
      }
    }
  });

  it("treats an option explicitly set to undefined as unset", () => {
    for (const [key] of OPTION_ENTRIES) {
      for (const path of ENTRY_PATHS) {
        expect(
          rejectionFrom(() => path.apply({ [key]: undefined })),
          `${key} = undefined via ${path.name}`,
        ).toBeNull();
      }
    }
  });

  it("rejects an out-of-range integer the same way on every entry path", () => {
    for (const { key, rejected, edge } of RANGE_CASES) {
      expectSharedRejection({ [key]: rejected }, `${key} past its ${edge}`, {
        className: "RangeError",
        contains: key,
      });
    }
  });

  it("accepts both range bounds on every entry path", () => {
    for (const { key, accepted, edge } of RANGE_CASES) {
      for (const path of ENTRY_PATHS) {
        expect(
          rejectionFrom(() => path.apply(acceptableConfig(key, accepted))),
          `${key} at its ${edge} via ${path.name}`,
        ).toBeNull();
      }
    }
  });

  it("rejects an unrecognized key the same way on every entry path", () => {
    for (const key of UNKNOWN_KEYS) {
      expectSharedRejection({ [key]: 1 }, `unknown key ${key}`, {
        className: "TypeError",
        contains: `Unknown config key: ${key}`,
      });
    }
  });

  it("rejects a config that is not an object the same way on every entry path", () => {
    for (const config of NON_OBJECT_CONFIGS) {
      expectSharedRejection(config as Record<string, unknown>, `config = ${String(config)}`, {
        className: "TypeError",
        contains: "config must be an object",
      });
    }
  });

  it("types every option the shared contract declares", () => {
    for (const option of contract.options) {
      expect(OPTION_TYPES[option.node as keyof typeof OPTION_TYPES], option.node).toBe(option.type);
    }
  });

  it("range-checks exactly the options it declares as integers", () => {
    const integerKeys = OPTION_ENTRIES.filter(([, type]) => type === "integer")
      .map(([key]) => key)
      .sort();
    expect(integerKeys).toEqual(Object.keys(OPTION_RANGES).sort());
  });
});

describe("native config parsing", () => {
  /** A value of the wrong type for each type the contract declares. */
  const WRONG_VALUE_FOR: Record<string, unknown> = {
    integer: "text",
    string: 1,
    boolean: "yes",
  };

  it("rejects a wrongly-typed option before it reaches the server", () => {
    // maxReconnectAttempts is a stream-level retry budget: the client never
    // receives it, so it has no native counterpart to check.
    const clientOptions = contract.options.filter(
      (option) => option.node !== "maxReconnectAttempts",
    );
    for (const option of clientOptions) {
      // The port is closed, so a config that got as far as connecting would
      // fail with a connection error instead of one naming the option.
      const config = {
        host: "127.0.0.1",
        port: 19999,
        [option.node]: WRONG_VALUE_FOR[option.type],
      } as ClientConfig;
      const rejection = rejectionFrom(() => {
        new BinlogClient(config);
      });
      expect(rejection, option.node).not.toBeNull();
      expect(rejection?.message, option.node).toContain(option.node);
      expect(rejection?.code, option.node).toBe(MesErrorCode.InvalidArg);
    }
  });
});
