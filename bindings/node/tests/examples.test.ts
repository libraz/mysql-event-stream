// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

import { readdirSync, readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";

/**
 * The shipped examples are copied into services, so a start position they
 * configure is a recommendation. An empty GTID set is the one value that turns
 * a live-tail example into a full replay of every binlog the server retains,
 * which is why an omitted `startGtid` (snapshot the current position) is what
 * each of them has to hand the stream.
 */

const EXAMPLES_URL = new URL("../examples/", import.meta.url);

/** An offset assignment resolving to the empty string, `??`/`||` fallbacks included. */
const EMPTY_START_GTID = /startGtid\s*:\s*(?:[^,\n]*(?:\?\?|\|\|)\s*)?(["'`])\1/;

function loadExamples(): Array<{ name: string; source: string }> {
  const names = readdirSync(EXAMPLES_URL).filter((name) => name.endsWith(".ts"));
  if (names.length === 0) throw new Error("no examples found to check");
  return names.map((name) => ({
    name,
    source: readFileSync(new URL(name, EXAMPLES_URL), "utf8"),
  }));
}

describe("shipped examples", () => {
  it("never configure the empty GTID set as a start position", () => {
    const requesting = loadExamples()
      .filter(({ source }) => EMPTY_START_GTID.test(source))
      .map(({ name }) => name);
    expect(requesting, "an empty startGtid asks the server for every retained binlog").toEqual([]);
  });
});
