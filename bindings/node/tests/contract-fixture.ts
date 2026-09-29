// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

/** Loader for the cross-binding contract table shared with the Python binding. */

import { readFileSync } from "node:fs";

const HEADER_URL = new URL("../../../core/include/mes.h", import.meta.url);
const CONFIG_PARSER_URL = new URL("../src/addon/config_parser.h", import.meta.url);
const CLIENT_WRAP_URL = new URL("../src/addon/client_wrap.cpp", import.meta.url);

export interface ContractOption {
  canonical: string;
  node: string;
  python: string;
  type: "string" | "integer" | "boolean";
  default?: string | number | boolean;
  min?: number;
  max?: number | null;
  /** Tighter floor that holds only while {@link fileOption} is set. */
  minWhenFileSet?: number;
  /** Companion option whose presence brings {@link minWhenFileSet} into force. */
  fileOption?: { node: string; python: string };
}

/** A pair of options that are supplied together or not at all. */
export interface ContractOptionPair {
  canonical: [string, string];
  node: [string, string];
  python: [string, string];
}

export interface StartPosition {
  startGtid: string | null;
  startBinlogFile: string | null;
  startBinlogPosition: number | null;
}

export interface BindingContract {
  nonRetryableErrorCodes: Array<{ name: string; code: number }>;
  retryableErrorCodes: Array<{ name: string; code: number }>;
  reconnect: {
    baseDelayMs: number;
    maxDelayMs: number;
    jitterMin: number;
    jitterMax: number;
    schedule: Array<{ attempt: number; undelayedMs: number }>;
  };
  checkpointResume: {
    cases: Array<{
      name: string;
      checkpoint: string;
      config: StartPosition;
      expect: StartPosition;
    }>;
  };
  checkpointRetention: { maxNativeReadsPerPollBatch: number };
  metadataError: { defaultBehaviour: string };
  iteration: { releaseContract: string };
  pollBatch: { defaultMaxEvents: number; minMaxEvents: number; maxMaxEvents: number };
  logLevel: { min: number; max: number; default: number };
  requiredTogether: {
    rule: string;
    pairs: ContractOptionPair[];
    unsetRule: string;
    unsetValues: { node: Record<string, number>; python: Record<string, number> };
  };
  mutuallyExclusive: {
    rule: string;
    pairs: ContractOptionPair[];
    orderRule: string;
  };
  options: ContractOption[];
}

/** Read `core/contracts/bindings.json`, the source of truth both bindings mirror. */
export function loadBindingContract(): BindingContract {
  const path = new URL("../../../core/contracts/bindings.json", import.meta.url);
  return JSON.parse(readFileSync(path, "utf8")) as BindingContract;
}

/**
 * Lowest value an option accepts once everything the contract pairs with it is
 * supplied: the conditional floor, where one is stated, otherwise the range
 * minimum.
 *
 * @param key Option as this surface spells it.
 * @returns The accepted minimum, or `undefined` for an option with no range.
 */
export function acceptedMinimum(key: string): number | undefined {
  const contract = loadBindingContract();
  const option = contract.options.find((entry) => entry.node === key);
  if (option === undefined || option.min === undefined) return undefined;
  const paired = contract.requiredTogether.pairs.some((pair) => pair.node.includes(key));
  return paired ? (option.minWhenFileSet ?? option.min) : option.min;
}

/**
 * Options that have to accompany `key`, each with a value it accepts, so that
 * probing one option is not refused over a pair constraint it is not about.
 *
 * @param key Option the caller is about to supply on its own.
 * @returns The companions to supply with it, empty when it has none.
 */
export function companionOptions(key: string): Record<string, unknown> {
  const contract = loadBindingContract();
  const companions: Record<string, unknown> = {};
  for (const pair of contract.requiredTogether.pairs) {
    if (!pair.node.includes(key)) continue;
    for (const companion of pair.node.filter((name) => name !== key)) {
      const option = contract.options.find((entry) => entry.node === companion);
      companions[companion] =
        option?.type === "integer" ? acceptedMinimum(companion) : "binlog.000001";
    }
  }
  return companions;
}

/**
 * Read the doc comment `core/include/mes.h` attaches to a config field.
 *
 * The header is the published claim a C caller reads, so a range the contract
 * states has to match the words shipped with the ABI. Parsed rather than
 * restated: a hand-copied expectation would be one more copy free to drift.
 * Every step that could stop matching throws instead of returning an empty
 * result a test would pass over.
 *
 * @param field Name of the struct field, as the header declares it.
 * @returns The comment block immediately above the declaration, as one line.
 */
export function loadHeaderFieldDoc(field: string): string {
  const lines = readFileSync(HEADER_URL, "utf8").split("\n");
  if (lines.length <= 1) throw new Error("core/include/mes.h is not readable");

  const declaration = lines.findIndex((line) => line.trim().endsWith(` ${field};`));
  if (declaration < 0) throw new Error(`mes.h does not declare ${field}`);

  const doc: string[] = [];
  for (const raw of lines.slice(0, declaration).reverse()) {
    const line = raw.trim();
    if (!line.startsWith("/**") && !line.startsWith("*")) break;
    doc.unshift(line);
    if (line.startsWith("/**")) break;
  }
  const text = doc.join(" ");
  if (text === "") throw new Error(`mes.h does not document ${field}`);
  return text;
}

/** Parse a numeric C++ literal such as `32u * 1024u * 1024u`, stripping the `u`/`U` unsigned suffix so it can be evaluated. */
function parseNumericLiteral(expression: string): number {
  const cleaned = expression.replace(/[uU]/g, "").trim();
  if (!/^[\d\s*+\-()]+$/.test(cleaned)) {
    throw new Error(`not a numeric literal: ${expression}`);
  }
  return Function(`"use strict"; return (${cleaned});`)() as number;
}

export interface DirectClientDefaults {
  port: number;
  serverId: number;
  connectTimeoutS: number;
  readTimeoutS: number;
  maxEventSize: number;
  maxQueueBytes: number;
}

/**
 * Default values the native addon's config parser materializes for options
 * left unset, read from source rather than restated. `config_parser.h` and
 * `client_wrap.cpp` are what the direct `BinlogClient` and
 * `CdcEngine.enableMetadata` entry points parse, so these are what has to
 * agree with `STREAM_DEFAULTS` for that path to stay in step with `CdcStream`.
 */
export function loadDirectClientDefaults(): DirectClientDefaults {
  const configParser = readFileSync(CONFIG_PARSER_URL, "utf8");
  const clientWrap = readFileSync(CLIENT_WRAP_URL, "utf8");
  const header = readFileSync(HEADER_URL, "utf8");

  const fromConfigParser = (name: string): number => {
    const match = configParser.match(new RegExp(`constexpr \\w+ ${name} = ([^;]+);`));
    if (match === null) throw new Error(`config_parser.h does not declare ${name}`);
    return parseNumericLiteral(match[1] as string);
  };

  const maxEventSizeMatch = clientWrap.match(/uint32_t max_event_size = ([^;]+);/);
  if (maxEventSizeMatch === null) {
    throw new Error("client_wrap.cpp does not declare a max_event_size default");
  }
  if (!clientWrap.includes("= MES_DEFAULT_QUEUE_BYTES;")) {
    throw new Error("client_wrap.cpp no longer defaults maxQueueBytes to MES_DEFAULT_QUEUE_BYTES");
  }
  const queueBytesMatch = header.match(/#define MES_DEFAULT_QUEUE_BYTES \(([^)]+)\)/);
  if (queueBytesMatch === null) throw new Error("mes.h does not define MES_DEFAULT_QUEUE_BYTES");

  return {
    port: fromConfigParser("kDefaultPort"),
    serverId: fromConfigParser("kDefaultServerId"),
    connectTimeoutS: fromConfigParser("kDefaultConnectTimeoutS"),
    readTimeoutS: fromConfigParser("kDefaultReadTimeoutS"),
    maxEventSize: parseNumericLiteral(maxEventSizeMatch[1] as string),
    maxQueueBytes: parseNumericLiteral(queueBytesMatch[1] as string),
  };
}
