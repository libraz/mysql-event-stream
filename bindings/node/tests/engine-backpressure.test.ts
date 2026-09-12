// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from "vitest";
import { CdcEngine } from "../src/engine.js";
import type { ChangeEvent } from "../src/types.js";
import { TABLE_MAP_EVENT, WRITE_ROWS_EVENT } from "./column-fixture.js";
import { buildEvent, buildTableMapBody, buildWriteRowsBody, concat } from "./helpers.js";

/** What one call to `feed` was offered and how much of it the engine took. */
interface FeedStep {
  offered: number;
  consumed: number;
}

/** Everything one run of the re-feed loop observed. */
interface FeedRun {
  events: ChangeEvent[];
  steps: FeedStep[];
}

/** The bytes of one INSERT of `value`: its TABLE_MAP, then the ROWS event. */
function buildInsertPair(value: number): Uint8Array {
  return concat(
    buildEvent(TABLE_MAP_EVENT, 1000, buildTableMapBody(1, "testdb", "users")),
    buildEvent(WRITE_ROWS_EVENT, 1000, buildWriteRowsBody(1, value)),
  );
}

/** Move everything the engine currently holds onto `into`, in queue order. */
function drainInto(engine: CdcEngine, into: ChangeEvent[]): void {
  for (let event = engine.nextEvent(); event !== null; event = engine.nextEvent()) {
    into.push(event);
  }
}

/**
 * Offer `bytes` the way `CdcStream` does: feed what is pending, drain, and
 * re-offer the unconsumed tail as a subarray view starting where the feed
 * stopped — never the whole buffer again.
 *
 * Two ways the loop can fail to end are reported rather than hung on: a feed
 * that neither consumes a byte nor yields an event has stalled, and a loop
 * outlasting the bytes it was given is re-offering data it already handed over.
 */
function feedWithBackpressure(engine: CdcEngine, bytes: Uint8Array): FeedRun {
  const events: ChangeEvent[] = [];
  const steps: FeedStep[] = [];
  let pending = bytes;

  while (pending.length > 0) {
    if (steps.length >= bytes.length) {
      throw new Error(
        `feed loop did not finish: ${steps.length} feeds, ${events.length} events drained, ` +
          `${pending.length} of ${bytes.length} bytes still pending`,
      );
    }

    const offered = pending.length;
    const drainedBefore = events.length;
    const consumed = engine.feed(pending);
    steps.push({ offered, consumed });
    drainInto(engine, events);

    if (consumed === 0 && events.length === drainedBefore) {
      throw new Error(`feed made no progress with ${offered} bytes pending`);
    }
    pending = pending.subarray(consumed);
  }

  return { events, steps };
}

describe("CdcEngine backpressure", () => {
  // Feed() stops at the top of its loop whenever the queue is at capacity, and
  // capacity is either the entry count or the queue's byte budget. The engine
  // surface exposes only the entry count, so that is the lever here; the byte
  // budget keeps its default, which this fixture comes nowhere near.
  const values = [11, 22, 33, 44, 55, 66, 77, 88];
  const bytes = concat(...values.map(buildInsertPair));

  it("delivers every event once, in order, across the re-fed remainders", async () => {
    const control = await CdcEngine.create();
    const constrained = await CdcEngine.create();
    try {
      // The same bytes through an engine that never applies backpressure. What
      // it decodes is the expectation, so a repeated or lost event shows up as
      // a difference rather than as a hand-written count that could be wrong
      // in the same direction as the behaviour it is checking.
      expect(control.feed(bytes), "the unconstrained control takes the whole buffer").toBe(
        bytes.length,
      );
      const expected: ChangeEvent[] = [];
      drainInto(control, expected);
      expect(expected).toHaveLength(values.length);

      constrained.setMaxQueueSize(1);
      const { events, steps } = feedWithBackpressure(constrained, bytes);
      const report = steps.map((step) => `${step.consumed}/${step.offered}`).join(" ");

      // Without a feed that stops short there is no remainder, and the re-feed
      // path this exercises is never reached: the comparison below would then
      // hold trivially. Requiring more than one short feed also puts the loop
      // itself under test rather than a single leftover.
      const first = steps[0];
      if (first === undefined) throw new Error("the constrained engine was never fed");
      expect(first.consumed, `first feed consumed/offered: ${report}`).toBeLessThan(first.offered);
      const shortFeeds = steps.filter((step) => step.consumed < step.offered);
      expect(shortFeeds.length, `consumed/offered per feed: ${report}`).toBeGreaterThan(1);

      expect(events).toEqual(expected);
    } finally {
      control.destroy();
      constrained.destroy();
    }
  });
});
