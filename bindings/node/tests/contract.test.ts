// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from "vitest";
import { BinlogClient } from "../src/client.js";
import {
  backoffDelayMs,
  CONDITIONAL_OPTION_MINIMUMS,
  LOG_LEVEL_RANGE,
  METADATA_ERROR_DEFAULT,
  NON_RETRYABLE_ERROR_CODES,
  OPTION_RANGES,
  POLL_BATCH,
  RECONNECT_POLICY,
  REQUIRED_TOGETHER_OPTIONS,
  STREAM_DEFAULTS,
} from "../src/contract.js";
import { setLogCallback } from "../src/logging.js";
import { CdcStream } from "../src/stream.js";
import { MesErrorCode, type StreamConfig } from "../src/types.js";
import { OPTION_TYPES, validatePollBatchSize, validateStreamOptions } from "../src/validation.js";
import {
  acceptedMinimum,
  type ContractOption,
  type ContractOptionPair,
  companionOptions,
  loadBindingContract,
  loadHeaderFieldDoc,
  type StartPosition,
} from "./contract-fixture.js";

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

/** Start-position offset, whose windows the contract states. */
const startPosition = contract.options.find((option) => option.canonical === "startBinlogPosition");

/** The offset and the file it points into, which the contract pairs. */
const startPositionPair = contract.requiredTogether.pairs.find(
  (pair) => pair.canonical[1] === "startBinlogPosition",
);

/**
 * How a supplied offset relates to the windows the contract states. Crossed
 * with the paired file option's two states and with both entry points that
 * accept the option, this enumerates the whole start-position surface instead
 * of sampling it.
 */
const POSITION_CLASSES = [
  "omitted",
  "rangeMinimum",
  "belowFloor",
  "atFloor",
  "atMaximum",
  "aboveMaximum",
] as const;

type PositionClass = (typeof POSITION_CLASSES)[number];

/** Offset each class stands for, derived from the contract's own numbers. */
function positionFor(option: ContractOption, positionClass: PositionClass): number | undefined {
  const floor = option.minWhenFileSet as number;
  const maximum = option.max as number;
  switch (positionClass) {
    case "omitted":
      return undefined;
    case "rangeMinimum":
      return option.min as number;
    case "belowFloor":
      return floor - 1;
    case "atFloor":
      return floor;
    case "atMaximum":
      return maximum;
    case "aboveMaximum":
      return maximum + 1;
  }
}

/**
 * The rule that refuses a combination, or `null` if it is accepted.
 *
 * One predicate serves both entry points: the shared option table and the
 * native config parse agree on every combination. Absence is the whole of this
 * surface's unset spelling, so every value it is given is a supplied one --
 * including the offset the contract records as the value the Python surface
 * passes when nothing was requested, which is refused here and accepted there.
 * The contract states that difference; neither surface treats a configuration
 * that names a position any differently.
 */
function refusalReason(
  option: ContractOption,
  fileSet: boolean,
  position: number | undefined,
): "range" | "pair" | "floor" | null {
  if (
    position !== undefined &&
    (position < (option.min as number) || position > (option.max as number))
  ) {
    return "range";
  }
  if ((position !== undefined) !== fileSet) return "pair";
  if (fileSet && position !== undefined && position < (option.minWhenFileSet as number)) {
    return "floor";
  }
  return null;
}

/** Build the option subset one case supplies, leaving everything else unset. */
function caseConfig(fileSet: boolean, position: number | undefined): Partial<StreamConfig> {
  const [fileOption, offsetOption] = (startPositionPair as ContractOptionPair).node;
  const config: Record<string, unknown> = {};
  if (fileSet) config[fileOption] = "binlog.000001";
  if (position !== undefined) config[offsetOption] = position;
  return config as Partial<StreamConfig>;
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
      // An option of a required-together pair cannot be probed on its own, and
      // once its companion is there the floor the companion brings into force
      // is what the accepted minimum becomes.
      const companions = companionOptions(option.node);
      const accepted = acceptedMinimum(option.node) as number;
      const probe = (value: number) =>
        validateStreamOptions({ ...companions, [option.node]: value });
      expect(() => probe(accepted - 1), `${option.node} below its accepted minimum`).toThrow();
      expect(() => probe(accepted), `${option.node} at its accepted minimum`).not.toThrow();
      const { max } = option;
      if (max === null || max === undefined) continue;
      expect(() => probe(max + 1), `${option.node} above maximum`).toThrow();
    }
  });

  it("mirrors the contract's required-together pairs", () => {
    expect(REQUIRED_TOGETHER_OPTIONS).toEqual(
      contract.requiredTogether.pairs.map((pair) => pair.node),
    );
    for (const pair of REQUIRED_TOGETHER_OPTIONS) {
      for (const key of pair) {
        // Recognized by this surface's option table, whether or not the shared
        // table states a range for it.
        expect(OPTION_TYPES, `${key} is a declared option`).toHaveProperty(key);
      }
    }
  });

  it("mirrors the contract's conditional start-position floor", () => {
    expect(startPosition, "startBinlogPosition declared in the contract options").toBeDefined();
    const option = startPosition as ContractOption;
    expect(typeof option.minWhenFileSet, "conditional floor stated in the contract").toBe("number");
    expect(option.fileOption?.node, "companion option named for this surface").toBe(
      "startBinlogFile",
    );
    // The floor is conditional, so the stated range keeps a lower minimum: the
    // surface that cannot spell an absent offset passes a value inside that
    // range to mean no file/offset start was requested.
    expect(option.min).toBeLessThan(option.minWhenFileSet as number);

    expect(CONDITIONAL_OPTION_MINIMUMS[option.node as "startBinlogPosition"]).toEqual({
      minimum: option.minWhenFileSet,
      companion: option.fileOption?.node,
    });
  });

  it("spells an unset start-position selector by absence alone", () => {
    const option = startPosition as ContractOption;
    // This surface can leave a key out, so the contract records no value for
    // it: a selector materializes no default, and an explicit undefined is the
    // same as saying nothing.
    expect(contract.requiredTogether.unsetValues.node).toEqual({});
    expect(STREAM_DEFAULTS).not.toHaveProperty(option.node);
    expect(() => validateStreamOptions({ [option.node]: undefined })).not.toThrow();
  });

  it("documents the contract's start-position floor in the C ABI header", () => {
    const option = startPosition as ContractOption;
    const documented = loadHeaderFieldDoc("binlog_position");
    const window = documented.match(/(\d+) through UINT32_MAX/);
    expect(window, `mes.h states an accepted offset window: ${documented}`).not.toBeNull();
    expect(Number((window as RegExpMatchArray)[1])).toBe(option.minWhenFileSet);
    // UINT32_MAX as the header spells the upper bound the contract states.
    expect(option.max).toBe(2 ** 32 - 1);
    // The header states the same trigger the contract does: the floor holds for
    // a file/offset start, not for every offset the field can carry.
    expect(documented).toContain("MES_START_AT_POSITION");
    // A parse that stops matching has to fail rather than hand back nothing
    // for the assertions above to pass over.
    expect(() => loadHeaderFieldDoc("no_such_field")).toThrow();
  });

  it("requires the offset and its file together at every entry point", () => {
    const option = startPosition as ContractOption;
    const [fileOption, offsetOption] = (startPositionPair as ContractOptionPair).node;
    // No server listens here, so a config the native parse accepts fails at the
    // connection instead — which is how acceptance is observed.
    const unreachablePort = 19999;

    for (const fileSet of [false, true]) {
      for (const positionClass of POSITION_CLASSES) {
        const position = positionFor(option, positionClass);
        const config = caseConfig(fileSet, position);
        const label = `${positionClass}, file ${fileSet ? "set" : "unset"}`;
        const reason = refusalReason(option, fileSet, position);

        const optionsCall = () => validateStreamOptions(config);
        let thrown: unknown;
        try {
          new BinlogClient({ host: "127.0.0.1", port: unreachablePort, ...config }).destroy();
        } catch (error) {
          thrown = error;
        }
        const code = (thrown as { code?: number } | undefined)?.code;

        if (reason === null) {
          expect(optionsCall, `shared options accept ${label}`).not.toThrow();
          expect(thrown, `client reaches the connection for ${label}`).toBeDefined();
          expect(code, `client accepts ${label}`).not.toBe(MesErrorCode.InvalidArg);
          continue;
        }

        expect(optionsCall, `shared options reject ${label}`).toThrow();
        let stated = "";
        try {
          optionsCall();
        } catch (error) {
          stated = String((error as Error).message);
        }
        if (reason === "pair") {
          // A refusal the pair decides names both options.
          expect(stated, `rejection cites the file option for ${label}`).toContain(fileOption);
          expect(stated, `rejection cites the offset option for ${label}`).toContain(offsetOption);
        } else {
          // A refusal a window decides names the option and the bound it
          // crossed.
          expect(stated, `rejection cites the option for ${label}`).toContain(offsetOption);
          const bound = reason === "floor" ? option.minWhenFileSet : option.max;
          expect(stated, `rejection cites the bound for ${label}`).toContain(String(bound));
        }
        expect(code, `client rejects ${label}`).toBe(MesErrorCode.InvalidArg);
      }
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
