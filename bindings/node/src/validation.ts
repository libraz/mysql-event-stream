import {
  CONDITIONAL_OPTION_MINIMUMS,
  MUTUALLY_EXCLUSIVE_OPTIONS,
  OPTION_RANGES,
  POLL_BATCH,
  REQUIRED_TOGETHER_OPTIONS,
  STREAM_DEFAULTS,
} from "./contract.js";
import type { ClientConfig, MesError, StreamConfig } from "./types.js";
import { MesErrorCode } from "./types.js";

/**
 * Category name a refused argument carries. `MesErrorName()` in
 * `src/addon/mes_error_util.h` resolves the C ABI's invalid-argument code to
 * the same string, so `name` reads alike whichever layer refused the value.
 */
export const REFUSAL_ERROR_NAME = "MesError";

/**
 * Present a refusal the way this surface presents every refusal: the built-in
 * subclass the kind of refusal calls for, kept so `instanceof RangeError` and
 * `instanceof TypeError` hold, plus the numeric C ABI code and the category
 * name. The addon states the same presentation once in `MakeMesError()`, and
 * `tests/option-refusal.test.ts` drives one refused value per option through
 * both layers so neither can be changed alone.
 *
 * The result satisfies {@link MesError}, which is the one declaration of what
 * an error from this package carries, so `isMesError()` narrows a refusal the
 * same way it narrows a failure the addon raised.
 */
function refused<E extends Error>(error: E): E & MesError {
  const presented = error as E & MesError;
  presented.code = MesErrorCode.InvalidArg;
  presented.name = REFUSAL_ERROR_NAME;
  return presented;
}

/** Build the error a value outside the window its option accepts raises. */
export function invalidArgument(message: string): RangeError {
  return refused(new RangeError(message));
}

/** Build the error a wrongly-typed or unrecognized option raises. */
export function invalidType(message: string): TypeError {
  return refused(new TypeError(message));
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

/**
 * Declared runtime type of every recognized {@link ClientConfig} option --
 * the subset of {@link OPTION_TYPES} a direct connection accepts, without the
 * stream-only keys (filters, the reconnect budget, the metadata-error
 * callback). {@link CdcEngine.enableMetadata} validates against this table
 * rather than {@link OPTION_TYPES}, since it opens a connection on its own
 * and never reaches a stream that would otherwise reject those keys.
 */
export const CLIENT_OPTION_TYPES = {
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
} as const satisfies Record<keyof ClientConfig, OptionType>;

/**
 * String options that, once supplied, must not be empty. A file/offset start
 * naming no file has no position a server could start from, so an empty
 * string is refused here rather than reaching the native layer's own check on
 * first iteration -- which is what the constructor's whole-config validation
 * promises.
 */
const NON_EMPTY_STRING_OPTIONS: ReadonlySet<string> = new Set(["startBinlogFile"]);

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
 * Validate a supplied configuration against a declared option table in full:
 * unrecognized keys, values that do not match their declared type, integers
 * outside their accepted range, options of a pair supplied alone, options
 * naming competing start modes, and a value below the floor its companion
 * brings into force are all rejected here.
 *
 * A key whose value is `undefined` counts as unset and takes its default;
 * `null` is a type error, because `undefined` is how this surface spells
 * "unset".
 *
 * @param table The recognized option set and the runtime type each entry
 *   must carry -- {@link OPTION_TYPES} for a stream, {@link CLIENT_OPTION_TYPES}
 *   for a direct connection.
 * @param config Options exactly as supplied, before any default is filled in.
 * @param base Configuration `config` overrides, when it is a partial update.
 *   The checks that span two options hold over the configuration the update
 *   produces, not over the keys the update happens to name.
 */
function validateOptionsAgainst(
  table: Record<string, OptionType>,
  config: Record<string, unknown>,
  base: Record<string, unknown>,
): void {
  if (config === null || typeof config !== "object") {
    throw invalidType("config must be an object");
  }
  for (const key of Object.keys(config)) {
    if (!Object.hasOwn(table, key)) {
      throw invalidType(`Unknown config key: ${key}`);
    }
    const value = config[key];
    if (value === undefined) continue;
    // hasOwn above guarantees an entry.
    const type = table[key]!;
    validateOptionType(key, type, value);
    if (type === "integer") {
      validateIntegerRange(key, value as number);
    } else if (value === "" && NON_EMPTY_STRING_OPTIONS.has(key)) {
      throw invalidArgument(`${key} must name a binlog file`);
    }
  }
  const effective = { ...base, ...config };
  validateRequiredTogether(effective);
  validateMutuallyExclusive(effective);
  validateConditionalMinimums(effective);
}

/**
 * Validate a supplied stream configuration against every option a stream
 * accepts, {@link OPTION_TYPES}. Every entry point into a stream's
 * configuration runs this, so no key or value can be refused by one of them
 * and silently defaulted by another.
 *
 * @param config Options exactly as supplied, before any default is filled in.
 * @param base Configuration `config` overrides, when it is a partial update.
 */
export function validateStreamOptions(
  config: Partial<StreamConfig>,
  base?: Partial<StreamConfig>,
): void {
  validateOptionsAgainst(
    OPTION_TYPES,
    config as Record<string, unknown>,
    (base ?? {}) as Record<string, unknown>,
  );
}

/**
 * Validate a supplied configuration against {@link CLIENT_OPTION_TYPES}: the
 * options a direct connection accepts, without a stream's filters, reconnect
 * budget, or metadata-error callback.
 *
 * @param config Options exactly as supplied, before any default is filled in.
 * @param base Configuration `config` overrides, when it is a partial update.
 */
export function validateClientOptions(
  config: Partial<ClientConfig>,
  base?: Partial<ClientConfig>,
): void {
  validateOptionsAgainst(
    CLIENT_OPTION_TYPES,
    config as Record<string, unknown>,
    (base ?? {}) as Record<string, unknown>,
  );
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
