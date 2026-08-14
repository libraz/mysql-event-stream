import { OPTION_RANGES, POLL_BATCH, STREAM_DEFAULTS } from "./contract.js";
import type { StreamConfig } from "./types.js";
import { MesErrorCode } from "./types.js";

export function invalidArgument(message: string): RangeError {
  const error = new RangeError(message) as RangeError & { code: number };
  error.code = MesErrorCode.InvalidArg;
  return error;
}

export function validatePort(port: number | undefined): void {
  const { min, max } = OPTION_RANGES.port;
  if (port !== undefined && (!Number.isInteger(port) || port < min || port > max)) {
    throw invalidArgument(`port must be ${min}-${max}, got ${port}`);
  }
}

/**
 * Range-check every shared option the caller supplied, using the accepted
 * ranges declared in the cross-binding contract.
 */
export function validateStreamOptions(config: Partial<StreamConfig>): void {
  const supplied = config as Record<string, unknown>;
  for (const [key, range] of Object.entries(OPTION_RANGES)) {
    const value = supplied[key];
    if (value === undefined) continue;
    if (key === "port") {
      validatePort(value as number);
      continue;
    }
    if (typeof value !== "number" || !Number.isInteger(value)) {
      throw invalidArgument(`${key} must be an integer`);
    }
    if (value < range.min || (range.max !== null && value > range.max)) {
      const upper = range.max === null ? "unbounded" : String(range.max);
      throw invalidArgument(`${key} must be between ${range.min} and ${upper}, got ${value}`);
    }
  }
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
