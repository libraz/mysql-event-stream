// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

/** Loader for the cross-binding contract table shared with the Python binding. */

import { readFileSync } from "node:fs";

const HEADER_URL = new URL("../../../core/include/mes.h", import.meta.url);

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
  options: ContractOption[];
}

/** Read `core/contracts/bindings.json`, the source of truth both bindings mirror. */
export function loadBindingContract(): BindingContract {
  const path = new URL("../../../core/contracts/bindings.json", import.meta.url);
  return JSON.parse(readFileSync(path, "utf8")) as BindingContract;
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
  for (let index = declaration - 1; index >= 0; index--) {
    const line = lines[index].trim();
    if (!line.startsWith("/**") && !line.startsWith("*")) break;
    doc.unshift(line);
    if (line.startsWith("/**")) break;
  }
  const text = doc.join(" ");
  if (text === "") throw new Error(`mes.h does not document ${field}`);
  return text;
}
