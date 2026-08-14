// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from "vitest";
import {
  backoffDelayMs,
  LOG_LEVEL_RANGE,
  METADATA_ERROR_DEFAULT,
  NON_RETRYABLE_ERROR_CODES,
  OPTION_RANGES,
  POLL_BATCH,
  RECONNECT_POLICY,
  STREAM_DEFAULTS,
} from "../src/contract.js";
import { setLogCallback } from "../src/logging.js";
import { CdcStream } from "../src/stream.js";
import type { StreamConfig } from "../src/types.js";
import { validatePollBatchSize, validateStreamOptions } from "../src/validation.js";
import { loadBindingContract, type StartPosition } from "./contract-fixture.js";

const contract = loadBindingContract();

/** Reduce a stream config to the canonical start-position triple. */
function startPositionOf(config: StreamConfig): StartPosition {
  return {
    startGtid: config.startGtid ?? null,
    startBinlogFile: config.startBinlogFile ?? null,
    startBinlogPosition: config.startBinlogPosition ?? null,
  };
}

/** Build the stream config a checkpoint-resume case starts from. */
function configFor(start: StartPosition): StreamConfig {
  const config: StreamConfig = { host: "127.0.0.1" };
  if (start.startGtid !== null) config.startGtid = start.startGtid;
  if (start.startBinlogFile !== null) config.startBinlogFile = start.startBinlogFile;
  if (start.startBinlogPosition !== null) config.startBinlogPosition = start.startBinlogPosition;
  return config;
}

/** Reach the private resume rule without going through a live connection. */
function resumeConfig(stream: CdcStream, checkpoint: string): StreamConfig {
  return (stream as unknown as { resumeConfig(checkpoint: string): StreamConfig }).resumeConfig(
    checkpoint,
  );
}

/** Attach a metadata connection that always fails to an unstarted stream. */
function withFailingMetadata(stream: CdcStream): () => void {
  const internals = stream as unknown as {
    engine: { enableMetadata(): void };
    enableMetadataSafe(): void;
  };
  internals.engine = {
    enableMetadata(): void {
      throw new Error("metadata connection refused");
    },
  };
  return () => internals.enableMetadataSafe();
}

/** Run `body` with stderr captured, so "writes nothing" can be asserted. */
function captureStderr(body: () => void): string[] {
  const written: string[] = [];
  const original = process.stderr.write;
  process.stderr.write = ((chunk: string | Uint8Array) => {
    written.push(String(chunk));
    return true;
  }) as typeof process.stderr.write;
  try {
    body();
  } finally {
    process.stderr.write = original;
  }
  return written;
}

describe("binding contract", () => {
  it("classifies exactly the contract's non-retryable error codes", () => {
    expect([...NON_RETRYABLE_ERROR_CODES].sort((a, b) => a - b)).toEqual(
      contract.nonRetryableErrorCodes.map((entry) => entry.code).sort((a, b) => a - b),
    );
  });

  it("never classifies a retryable error code as permanent", () => {
    for (const entry of contract.retryableErrorCodes) {
      expect(NON_RETRYABLE_ERROR_CODES.has(entry.code), entry.name).toBe(false);
    }
  });

  it("matches the contract's backoff window", () => {
    expect({ ...RECONNECT_POLICY }).toEqual({
      baseDelayMs: contract.reconnect.baseDelayMs,
      maxDelayMs: contract.reconnect.maxDelayMs,
      jitterMin: contract.reconnect.jitterMin,
      jitterMax: contract.reconnect.jitterMax,
    });
  });

  it("matches the contract's backoff schedule", () => {
    for (const step of contract.reconnect.schedule) {
      expect(backoffDelayMs(step.attempt, 0), `attempt ${step.attempt} lower bound`).toBe(
        step.undelayedMs * contract.reconnect.jitterMin,
      );
      expect(backoffDelayMs(step.attempt, 1), `attempt ${step.attempt} upper bound`).toBe(
        step.undelayedMs * contract.reconnect.jitterMax,
      );
    }
  });

  it("follows the contract's checkpoint resume cases", () => {
    for (const testCase of contract.checkpointResume.cases) {
      const stream = new CdcStream(configFor(testCase.config));
      expect(startPositionOf(resumeConfig(stream, testCase.checkpoint)), testCase.name).toEqual(
        testCase.expect,
      );
    }
  });

  it("stays silent when metadata fails and no handler is configured", () => {
    expect(METADATA_ERROR_DEFAULT).toBe(contract.metadataError.defaultBehaviour);

    const report = withFailingMetadata(new CdcStream({ host: "127.0.0.1" }));
    const written = captureStderr(() => {
      expect(() => report()).not.toThrow();
    });
    expect(written).toEqual([]);
  });

  it("hands a metadata failure to the configured handler", () => {
    const seen: Error[] = [];
    const report = withFailingMetadata(
      new CdcStream({ host: "127.0.0.1", onMetadataError: (error) => seen.push(error) }),
    );
    report();
    expect(seen.map((error) => error.message)).toEqual(["metadata connection refused"]);
  });

  it("exposes the contract's iteration release entry point", () => {
    expect(contract.iteration.releaseContract).toBe("aclose");
    // Node spells the same contract as the async-disposal protocol.
    const stream = new CdcStream({ host: "127.0.0.1" });
    expect(typeof stream[Symbol.asyncDispose]).toBe("function");
    expect(typeof stream.close).toBe("function");
  });

  it("materializes exactly the contract's shared option defaults", () => {
    const expected = Object.fromEntries(
      contract.options
        .filter((option) => option.default !== undefined)
        .map((option) => [option.node, option.default]),
    );
    expect({ ...STREAM_DEFAULTS }).toEqual(expected);
  });

  it("enforces the contract's shared option ranges", () => {
    const expected = Object.fromEntries(
      contract.options
        .filter((option) => option.type === "integer")
        .map((option) => [option.node, { min: option.min, max: option.max ?? null }]),
    );
    expect(OPTION_RANGES).toEqual(expected);

    for (const option of contract.options) {
      if (option.type !== "integer") continue;
      const minimum = option.min as number;
      expect(
        () => validateStreamOptions({ [option.node]: minimum - 1 } as Partial<StreamConfig>),
        `${option.node} below minimum`,
      ).toThrow();
      expect(
        () => validateStreamOptions({ [option.node]: minimum } as Partial<StreamConfig>),
        `${option.node} at minimum`,
      ).not.toThrow();
      if (option.max === null || option.max === undefined) continue;
      expect(
        () => validateStreamOptions({ [option.node]: option.max + 1 } as Partial<StreamConfig>),
        `${option.node} above maximum`,
      ).toThrow();
    }
  });

  it("enforces the contract's poll batch window", () => {
    expect({ ...POLL_BATCH }).toEqual(contract.pollBatch);
    expect(() => validatePollBatchSize(contract.pollBatch.minMaxEvents - 1)).toThrow();
    expect(() => validatePollBatchSize(contract.pollBatch.maxMaxEvents + 1)).toThrow();
    expect(() => validatePollBatchSize(contract.pollBatch.minMaxEvents)).not.toThrow();
    expect(() => validatePollBatchSize(contract.pollBatch.maxMaxEvents)).not.toThrow();
    expect(() => validatePollBatchSize(contract.pollBatch.defaultMaxEvents)).not.toThrow();
  });

  it("enforces the contract's log level window", () => {
    expect({ ...LOG_LEVEL_RANGE }).toEqual({
      min: contract.logLevel.min,
      max: contract.logLevel.max,
      default: contract.logLevel.default,
    });
    try {
      for (let level = contract.logLevel.min; level <= contract.logLevel.max; level++) {
        expect(() => setLogCallback(() => {}, level as never)).not.toThrow();
      }
      expect(() => setLogCallback(() => {}, (contract.logLevel.min - 1) as never)).toThrow();
      expect(() => setLogCallback(() => {}, (contract.logLevel.max + 1) as never)).toThrow();
      expect(() => setLogCallback(() => {}, 1.5 as never)).toThrow();
    } finally {
      setLogCallback(null);
    }
  });
});
