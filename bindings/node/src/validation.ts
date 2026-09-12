import {
  CONDITIONAL_OPTION_MINIMUMS,
  MUTUALLY_EXCLUSIVE_OPTIONS,
  OPTION_RANGES,
  POLL_BATCH,
  REQUIRED_TOGETHER_OPTIONS,
  STREAM_DEFAULTS,
} from "./contract.js";
import type { StreamConfig } from "./types.js";
import { MesErrorCode } from "./types.js";

export function invalidArgument(message: string): RangeError {
  const error = new RangeError(message) as RangeError & { code: number };
  error.code = MesErrorCode.InvalidArg;
  return error;
}

/** Build the error a wrongly-typed or unrecognized option raises. */
export function invalidType(message: string): TypeError {
  const error = new TypeError(message) as TypeError & { code: number };
  error.code = MesErrorCode.InvalidArg;
  return error;
}

/** The runtime shape an option's value must have to be accepted. */
export type OptionType = "integer" | "string" | "boolean" | "stringArray" | "callback";

/**
 * Declared runtime type of every recognized {@link StreamConfig} option.
 *
 * This table is the option set: a key missing from it is unrecognized, and a
 * key present in a supplied config must carry a value of the declared type.
 * The `satisfies` clause makes adding a `StreamConfig` field without declaring
 * its type here a compile error, every `integer` entry is range-checked against
 * {@link OPTION_RANGES}, and `tests/contract.test.ts` compares the table with
 * `core/contracts/bindings.json` — so none of the three can drift apart.
 */
export const OPTION_TYPES = {
  host: "string",
  port: "integer",
  user: "string",
  password: "string",
  serverId: "integer",
  startGtid: "string",
  startBinlogFile: "string",
  startBinlogPosition: "integer",
  connectTimeoutS: "integer",
  readTimeoutS: "integer",
  sslMode: "integer",
  sslCa: "string",
  sslCert: "string",
  sslKey: "string",
  allowPublicKeyRetrieval: "boolean",
  maxQueueSize: "integer",
  maxQueueBytes: "integer",
  maxEventSize: "integer",
  includeDatabases: "stringArray",
  includeTables: "stringArray",
  excludeTables: "stringArray",
  maxReconnectAttempts: "integer",
  onMetadataError: "callback",
} as const satisfies Record<keyof StreamConfig, OptionType>;

/** How each declared type is tested, and how it is named in the error. */
const TYPE_CHECKS: Record<OptionType, { describe: string; accepts(value: unknown): boolean }> = {
  integer: {
    describe: "an integer",
    accepts: (value) => typeof value === "number" && Number.isInteger(value),
  },
  string: { describe: "a string", accepts: (value) => typeof value === "string" },
  boolean: { describe: "a boolean", accepts: (value) => typeof value === "boolean" },
  stringArray: {
    describe: "an array of strings",
    accepts: (value) => Array.isArray(value) && value.every((item) => typeof item === "string"),
  },
  callback: { describe: "a function", accepts: (value) => typeof value === "function" },
};

/** Reject a value whose runtime type does not match the option's declared one. */
function validateOptionType(key: string, type: OptionType, value: unknown): void {
  const check = TYPE_CHECKS[type];
  if (!check.accepts(value)) {
    throw invalidType(`${key} must be ${check.describe}`);
  }
}

export function validatePort(port: number | undefined): void {
  if (port === undefined) return;
  validateOptionType("port", "integer", port);
  const { min, max } = OPTION_RANGES.port;
  if (port < min || port > max) {
    throw invalidArgument(`port must be ${min}-${max}, got ${port}`);
  }
}

/** Range-check an integer option against the window the contract declares. */
function validateIntegerRange(key: string, value: number): void {
  if (key === "port") {
    // Delegated so a rejected port reads the same whether it arrived through a
    // stream config or straight into BinlogClient.
    validatePort(value);
    return;
  }
  const range = OPTION_RANGES[key as keyof typeof OPTION_RANGES];
  if (value < range.min || (range.max !== null && value > range.max)) {
    const upper = range.max === null ? "unbounded" : String(range.max);
    throw invalidArgument(`${key} must be between ${range.min} and ${upper}, got ${value}`);
  }
}

/**
 * Reject a configuration that supplies one option of a required-together pair
 * without the other.
 *
 * The constraint is over a pair, so no per-key check can express it: this runs
 * once over the whole configuration an entry point produces.
 *
 * @param supplied Every option the configuration carries, unset keys included.
 */
function validateRequiredTogether(supplied: Record<string, unknown>): void {
  for (const pair of REQUIRED_TOGETHER_OPTIONS) {
    const missing = pair.filter((key) => supplied[key] === undefined);
    if (missing.length === 0 || missing.length === pair.length) continue;
    throw invalidArgument(
      `${pair[0]} and ${pair[1]} are required together, and ${missing[0]} is unset`,
    );
  }
}

/**
 * Reject a configuration that supplies both options of a mutually exclusive
 * pair.
 *
 * Another whole-configuration check: the competing option is a different key,
 * so no per-key validator can see it. Runs after the pair rule, which is what
 * lets one option stand for a start mode its companion is bound to.
 *
 * @param supplied Every option the configuration carries, unset keys included.
 */
function validateMutuallyExclusive(supplied: Record<string, unknown>): void {
  for (const pair of MUTUALLY_EXCLUSIVE_OPTIONS) {
    if (pair.some((key) => supplied[key] === undefined)) continue;
    throw invalidArgument(`${pair[0]} and ${pair[1]} cannot be combined`);
  }
}

/**
 * Apply the floors that hold only while a companion option is set.
 *
 * Also a whole-configuration check: the companion is another key, so the
 * per-key range check cannot see it. Enforcing it here is what keeps a value
 * this validator accepts from being refused later by the native layer.
 *
 * @param supplied Every option the configuration carries, unset keys included.
 */
function validateConditionalMinimums(supplied: Record<string, unknown>): void {
  for (const [key, floor] of Object.entries(CONDITIONAL_OPTION_MINIMUMS)) {
    if (supplied[floor.companion] === undefined) continue;
    const value = supplied[key];
    if (typeof value !== "number" || value >= floor.minimum) continue;
    const { max } = OPTION_RANGES[key as keyof typeof OPTION_RANGES];
    const upper = max === null ? "unbounded" : String(max);
    throw invalidArgument(
      `${key} must be ${floor.minimum} through ${upper} when ${floor.companion} is set, got ${value}`,
    );
  }
}

/**
 * Validate a supplied stream configuration in full: unrecognized keys, values
 * that do not match their declared type, integers outside their accepted range,
 * options of a pair supplied alone, options naming competing start modes, and a
 * value below the floor its companion brings into force are all rejected here.
 *
 * Every entry point into a stream's configuration runs this, so no key or value
 * can be refused by one of them and silently defaulted by another. A key whose
 * value is `undefined` counts as unset and takes its default; `null` is a type
 * error, because `undefined` is how this surface spells "unset".
 *
 * @param config Options exactly as supplied, before any default is filled in.
 * @param base Configuration `config` overrides, when it is a partial update.
 *   The checks that span two options hold over the configuration the update
 *   produces, not over the keys the update happens to name.
 */
export function validateStreamOptions(
  config: Partial<StreamConfig>,
  base?: Partial<StreamConfig>,
): void {
  if (config === null || typeof config !== "object") {
    throw invalidType("config must be an object");
  }
  const supplied = config as Record<string, unknown>;
  for (const key of Object.keys(supplied)) {
    if (!Object.hasOwn(OPTION_TYPES, key)) {
      throw invalidType(`Unknown config key: ${key}`);
    }
    const value = supplied[key];
    if (value === undefined) continue;
    const type = OPTION_TYPES[key as keyof typeof OPTION_TYPES];
    validateOptionType(key, type, value);
    if (type === "integer") {
      validateIntegerRange(key, value as number);
    }
  }
  const effective = { ...base, ...supplied } as Record<string, unknown>;
  validateRequiredTogether(effective);
  validateMutuallyExclusive(effective);
  validateConditionalMinimums(effective);
}

/**
 * Fill in the shared option defaults the caller left unset. Start-position
 * options are deliberately absent: they select a start mode rather than carry
 * a default, and materializing one would override the configured mode.
 */
export function withStreamDefaults(config: StreamConfig): StreamConfig {
  const merged = { ...config } as Record<string, unknown>;
  for (const [key, value] of Object.entries(STREAM_DEFAULTS)) {
    if (merged[key] === undefined) merged[key] = value;
  }
  return merged as StreamConfig;
}

export function validatePollBatchSize(maxEvents: number): void {
  const { minMaxEvents, maxMaxEvents } = POLL_BATCH;
  if (!Number.isInteger(maxEvents) || maxEvents < minMaxEvents || maxEvents > maxMaxEvents) {
    throw invalidArgument(
      `maxEvents must be an integer between ${minMaxEvents} and ${maxMaxEvents}`,
    );
  }
}
