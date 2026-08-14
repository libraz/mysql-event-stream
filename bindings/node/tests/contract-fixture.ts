// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

/** Loader for the cross-binding contract table shared with the Python binding. */

import { readFileSync } from "node:fs";

export interface ContractOption {
  canonical: string;
  node: string;
  python: string;
  type: "string" | "integer" | "boolean";
  default?: string | number | boolean;
  min?: number;
  max?: number | null;
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
