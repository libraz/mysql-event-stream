// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

/** Error thrown when a wait operation times out. */
export class WaitTimeoutError extends Error {
  constructor(
    public readonly description: string,
    public readonly timeout: number,
  ) {
    super(`Timed out waiting for ${description} after ${timeout}ms`);
    this.name = "WaitTimeoutError";
  }
}

/**
 * Poll until predicate returns true or timeout is reached.
 */
export async function waitUntil(
  predicate: () => Promise<boolean> | boolean,
  options: {
    timeout?: number;
    interval?: number;
    description?: string;
  } = {},
): Promise<void> {
  const { timeout = 30_000, interval = 1_000, description = "condition" } = options;
  const deadline = Date.now() + timeout;
  let lastError: Error | undefined;

  while (Date.now() < deadline) {
    try {
      if (await predicate()) return;
    } catch (e) {
      lastError = e instanceof Error ? e : new Error(String(e));
    }
    await new Promise((resolve) => setTimeout(resolve, interval));
  }

  const err = new WaitTimeoutError(description, timeout);
  if (lastError) err.cause = lastError;
  throw err;
}
