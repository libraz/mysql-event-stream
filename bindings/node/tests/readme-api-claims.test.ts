// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";

import { BinlogClient } from "../src/client.js";
import { CdcEngine } from "../src/engine.js";
import { CdcStream } from "../src/stream.js";

/**
 * A method named in the documentation has to exist on the class named with it.
 *
 * Cancellation guidance is where this has gone wrong: the READMEs described
 * `stop()` as the any-thread cancellation path for `BinlogClient` *and*
 * `CdcStream`, while only `BinlogClient` has ever had the method. Nothing in
 * the suite read the documentation, so a reader following it called a method
 * that was not there. Matching only the qualified `Class.method()` spelling is
 * what makes the claim checkable — an unqualified `stop()` in prose carries no
 * machine-readable owner.
 */

const repoRoot = fileURLToPath(new URL("../../..", import.meta.url));

// Every README that documents the Node surface.
const READMES = [
  "README.md",
  "README_ja.md",
  "bindings/node/README.md",
  "bindings/node/README.npm.md",
];

// biome-ignore lint/complexity/noBannedTypes: prototype lookup needs the constructor type
const CLASSES: Record<string, Function> = {
  BinlogClient,
  CdcEngine,
  CdcStream,
};

const QUALIFIED_CALL = /`([A-Z][A-Za-z]*)\.([a-z_][A-Za-z0-9_]*)\(\)`/g;

interface Mention {
  document: string;
  className: string;
  method: string;
}

function collectMentions(): Mention[] {
  const mentions: Mention[] = [];
  for (const document of READMES) {
    const text = readFileSync(`${repoRoot}${document}`, "utf8");
    for (const [, className, method] of text.matchAll(QUALIFIED_CALL)) {
      if (className in CLASSES) {
        mentions.push({ document, className, method });
      }
    }
  }
  return mentions;
}

describe("documented method names", () => {
  it("resolve on the class they are attributed to", () => {
    const mentions = collectMentions();
    // Guards against a silent pass: a regex that stops matching, or a README
    // that loses its cancellation guidance, would otherwise assert nothing.
    expect(mentions.length).toBeGreaterThan(0);
    expect(mentions.some((m) => m.className === "BinlogClient" && m.method === "stop")).toBe(true);

    for (const { document, className, method } of mentions) {
      const prototype = CLASSES[className].prototype as Record<string, unknown>;
      expect(
        typeof prototype[method],
        `${document} documents ${className}.${method}(), which ${className} does not expose`,
      ).toBe("function");
    }
  });

  it("cover the cancellation entry point on every Node README", () => {
    // CdcStream cancels through close(); BinlogClient.stop() is the only
    // any-thread path. Both classes ship on this surface, so a README that
    // names neither has lost the guidance rather than been corrected.
    const documented = READMES.filter((document) => {
      const text = readFileSync(`${repoRoot}${document}`, "utf8");
      return text.includes("Thread Safety");
    });
    expect(documented.length).toBeGreaterThan(0);

    for (const document of documented) {
      const text = readFileSync(`${repoRoot}${document}`, "utf8");
      expect(text, `${document} must attribute stop() to the class that owns it`).toContain(
        "`BinlogClient.stop()`",
      );
    }
  });
});
