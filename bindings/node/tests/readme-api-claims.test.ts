// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";

import { BinlogClient } from "../src/client.js";
import { CdcEngine } from "../src/engine.js";
import { CdcStream } from "../src/stream.js";
import { MesErrorCode } from "../src/types.js";

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
  /** Prototype of the class the mention is attributed to. */
  prototype: Record<string, unknown>;
}

function collectMentions(): Mention[] {
  const mentions: Mention[] = [];
  for (const document of READMES) {
    const text = readFileSync(`${repoRoot}${document}`, "utf8");
    for (const [, className, method] of text.matchAll(QUALIFIED_CALL)) {
      if (className === undefined || method === undefined) continue;
      const target = CLASSES[className];
      if (target === undefined) continue;
      mentions.push({
        document,
        className,
        method,
        prototype: target.prototype as Record<string, unknown>,
      });
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

    for (const { document, className, method, prototype } of mentions) {
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

function read(document: string): string {
  return readFileSync(`${repoRoot}${document}`, "utf8");
}

/**
 * The body one of `headings` opens, up to the next heading of the same level.
 *
 * Headings are written with their own `#` prefix so a caller can mix levels,
 * and the first one present wins. Returns null when the document carries none
 * of them, which is how a document that never makes the claim is told apart
 * from one that makes it wrongly.
 */
function section(text: string, ...headings: string[]): string | null {
  const lines = text.split("\n");
  for (const heading of headings) {
    const start = lines.indexOf(heading);
    if (start === -1) continue;
    // Ends at the next heading of the same level or shallower, so a subsection
    // stays inside its parent but a sibling does not leak in. Fenced blocks are
    // skipped: a `# macOS` shell comment is not a heading, and treating one as
    // the terminator silently cut every section short of its own prose.
    const depth = (heading.match(/^#+/) as RegExpMatchArray)[0].length;
    const terminator = new RegExp(`^#{1,${depth}} `);
    let fenced = false;
    for (const [offset, line] of lines.slice(start + 1).entries()) {
      if (line.startsWith("```")) fenced = !fenced;
      else if (!fenced && terminator.test(line)) {
        return lines.slice(start, start + 1 + offset).join("\n");
      }
    }
    return lines.slice(start).join("\n");
  }
  return null;
}

/** Collect the contents of every fenced code block in a markdown passage. */
function fencedBlocks(text: string): string[] {
  const blocks: string[] = [];
  let current: string[] | null = null;
  for (const line of text.split("\n")) {
    if (line.startsWith("```")) {
      if (current === null) current = [];
      else {
        blocks.push(current.join("\n"));
        current = null;
      }
    } else if (current !== null) current.push(line);
  }
  return blocks;
}

describe("the Exports table", () => {
  it("lists every symbol the package entry point exports", () => {
    // Derived from the entry point rather than transcribed, so a new export
    // fails here instead of quietly going undocumented. LogHandler is how this
    // went wrong: a type-only export the table never grew a row for.
    const entry = readFileSync(`${repoRoot}bindings/node/src/index.ts`, "utf8");
    const exported = new Set<string>();
    for (const [, names] of entry.matchAll(/export(?:\s+type)?\s*\{([^}]*)\}/g)) {
      if (names === undefined) continue;
      for (const name of names.split(",")) {
        const symbol = name
          .trim()
          .split(/\s+as\s+/)
          .pop()
          ?.trim();
        if (symbol) exported.add(symbol);
      }
    }
    expect(exported.size).toBeGreaterThan(0);

    const table = section(read("bindings/node/README.md"), "## Exports");
    expect(table, "bindings/node/README.md must keep its Exports section").not.toBeNull();
    for (const symbol of exported) {
      expect(table, `the Exports table omits ${symbol}, which index.ts exports`).toContain(
        `\`${symbol}\``,
      );
    }
  });
});

describe("the error-code table", () => {
  /** Every integer the section names, with `a–b` spans expanded. */
  function codesIn(text: string): Set<number> {
    const flat = text.replace(/(\d),(\d)/g, "$1$2");
    const codes = new Set<number>();
    for (const [, low, high] of flat.matchAll(/(\d+)\s*[–—-]\s*(\d+)/g)) {
      for (let code = Number(low); code <= Number(high); code += 1) codes.add(code);
    }
    for (const [value] of flat.matchAll(/\d+/g)) codes.add(Number(value));
    return codes;
  }

  it("accounts for every value exported on MesErrorCode", () => {
    // A caller writing a retry policy enumerates the whole enum, so a value the
    // section never names is one they cannot classify. Codes with no producer
    // count as accounted for only because the section says they are reserved.
    const documented = READMES.filter(
      (document) => section(read(document), "## Error codes", "## エラーコード") !== null,
    );
    expect(documented.length).toBeGreaterThan(0);

    for (const document of documented) {
      const codes = codesIn(section(read(document), "## Error codes", "## エラーコード") as string);
      for (const [name, value] of Object.entries(MesErrorCode)) {
        if (value === MesErrorCode.Ok) continue;
        expect(
          codes.has(value),
          `${document} never names ${value} (MesErrorCode.${name}) in its error-code section`,
        ).toBe(true);
      }
    }
  });

  it("gives the two paths that emit 301 a row each", () => {
    // One code, two producers with incompatible remedies: the client event
    // queue has a configurable budget, the retained query result has a
    // compile-time cap and takes the connection down with it. A single row
    // cannot carry both without sending half its readers to a knob that does
    // not exist.
    for (const document of READMES) {
      const errors = section(read(document), "## Error codes", "## エラーコード");
      if (errors === null) continue;
      const rows = errors
        .split("\n")
        .filter((line) => line.startsWith("| ") && /(^|\|)[^|]*301/.test(line));
      expect(rows.length, `${document} does not separate the two causes of 301`).toBeGreaterThan(1);
      expect(
        rows.some((row) => /max_?[Qq]ueue_?[Bb]ytes/.test(row)),
        `${document} does not name the queue budget as the remedy for one of them`,
      ).toBe(true);
      expect(
        rows.some((row) => /reconnect|再接続/i.test(row)),
        `${document} does not say the query-result cap requires reconnecting`,
      ).toBe(true);
    }
  });

  it("names the resume API where it tells the reader to reconnect from a checkpoint", () => {
    // The table's remedy for 403-404 is a procedure, and a procedure with no
    // documented API is one a reader cannot carry out.
    const instructing = READMES.filter((document) => {
      const errors = section(read(document), "## Error codes", "## エラーコード");
      return errors !== null && /checkpoint/i.test(errors);
    });
    expect(instructing.length).toBeGreaterThan(0);

    for (const document of instructing) {
      const text = read(document);
      for (const parameter of ["startGtid", "start_gtid", "currentGtid", "current_gtid"]) {
        expect(
          text,
          `${document} instructs a checkpoint resume without naming ${parameter}`,
        ).toContain(parameter);
      }
    }
  });
});

describe("the column-names feature", () => {
  it("names the call a CdcEngine consumer has to make", () => {
    // CdcStream enables the metadata connection itself; CdcEngine does not, and
    // every README advertising the feature is read by CdcEngine users too.
    const advertising = READMES.filter((document) => /## Features|## 特徴/.test(read(document)));
    expect(advertising.length).toBeGreaterThan(0);

    for (const document of advertising) {
      const text = read(document);
      if (!/binlog_row_metadata=FULL` or a metadata connection|メタデータ接続/.test(text)) continue;
      expect(text, `${document} advertises column names without naming enableMetadata`).toContain(
        "enableMetadata",
      );
    }
    expect(typeof CdcEngine.prototype.enableMetadata).toBe("function");
  });
});

describe("the documented resume API", () => {
  it("exists on the classes the documentation attributes it to", () => {
    for (const target of [CdcStream, BinlogClient]) {
      const descriptor = Object.getOwnPropertyDescriptor(target.prototype, "currentGtid");
      expect(
        typeof descriptor?.get,
        `${target.name} must expose currentGtid for the documented checkpoint resume`,
      ).toBe("function");
    }
  });
});

describe("the table-filtering section", () => {
  it("says the database filter has no wildcard wherever it documents one", () => {
    // The sample sets a database filter and a table filter together, so a rule
    // stated once over both reads as applying to both. Only the table filters
    // go through the prefix matcher; the database filter is a hash-set lookup.
    const EXACTNESS = ["byte for byte", "no wildcard", "バイト単位", "ワイルドカードはありません"];
    let checked = 0;
    for (const document of READMES) {
      const filtering = section(
        read(document),
        "### Table Filtering",
        "### テーブルフィルタリング",
      );
      if (
        filtering === null ||
        !/\*` (is|prefix)|prefix ワイルドカード|prefix wildcard/.test(filtering)
      )
        continue;
      if (!/[Ii]ncludeDatabases|include_databases/.test(filtering)) continue;
      checked += 1;
      expect(
        EXACTNESS.some((phrase) => filtering.includes(phrase)),
        `${document} documents a wildcard next to a database filter without saying the database filter has none`,
      ).toBe(true);
    }
    expect(checked, "no filtering section documents both filter kinds").toBeGreaterThan(0);
  });
});

describe("the published npm README", () => {
  it("documents the client lifecycle choice mes.h requires of every binding", () => {
    // prepack swaps the README, so the in-repo development README is not what
    // an npm consumer reads. The published text is the one that has to carry
    // the connect-on-construct choice, the single-poll rule and the idempotent
    // teardown.
    const manifest = JSON.parse(readFileSync(`${repoRoot}bindings/node/package.json`, "utf8")) as {
      scripts?: Record<string, string>;
    };
    const prepack = manifest.scripts?.prepack ?? "";
    const source = prepack.match(/(README\.[A-Za-z.]*md)\s+README\.md/)?.[1];
    expect(source, "prepack no longer substitutes a published README").toBeTruthy();

    const published = section(read(`bindings/node/${source}`), "## Lifecycle");
    expect(published, `bindings/node/${source} must document the client lifecycle`).not.toBeNull();
    const text = published as string;
    for (const required of ["BinlogClient", "start()", "poll()", "destroy()", "idempotent"]) {
      expect(text, `the published lifecycle section never mentions ${required}`).toContain(
        required,
      );
    }
    expect(text, "the published lifecycle section must say the constructor connects").toMatch(
      /connects and validates/,
    );
  });

  it("cites the measurement behind every throughput figure it publishes", () => {
    // A rate with no stated layer reads as the package's. The recorded numbers
    // are the core's decode path; nothing measures the addon's marshalling.
    for (const document of READMES) {
      for (const line of read(document).split("\n")) {
        if (!/events\/sec/.test(line)) continue;
        expect(line, `${document} publishes a throughput figure with no measurement`).toContain(
          "BASELINE.md",
        );
      }
    }
  });
});

describe("the from-source build prerequisites", () => {
  it("list a package for every required dependency the addon's CMake resolves", () => {
    // Derived from CMakeLists so a newly required package cannot slip past the
    // prerequisites of a README that documents building from source.
    const cmake = readFileSync(`${repoRoot}bindings/node/CMakeLists.txt`, "utf8");
    const required = [...cmake.matchAll(/^\s*find_package\(\s*([A-Za-z0-9_]+)[^)]*\bREQUIRED\b/gm)]
      .map(([, name]) => name)
      .filter((name) => name !== undefined);
    expect(required.length).toBeGreaterThan(0);

    // Only the commands a reader runs count. Naming a library in the surrounding
    // prose does not get it onto their machine, and listing it as a bullet while
    // leaving it out of the install line is the same omission one step along: the
    // reader copies the command, not the bullet. So the haystack is the fenced
    // blocks of the prerequisites passage and nothing else.
    const PREREQUISITE_HEADINGS = ["### Prerequisites", "### 前提条件", "## Installation"];
    const prerequisites = READMES.map(
      (document) =>
        [document, section(read(document), ...PREREQUISITE_HEADINGS)] as [string, string | null],
    ).filter(([, body]) => body !== null);
    expect(prerequisites.length).toBeGreaterThan(0);

    // Where a document hands the reader a system package-manager command, that
    // command is what has to carry the dependency: listing a library as a bullet
    // while leaving it out of the line the reader copies is the same omission one
    // step along. Where a document only names the dependencies in prose -- the
    // npm-facing README does, because it installs a published addon rather than
    // building one -- there is no command to check, so the prose is the claim.
    const PACKAGE_MANAGERS =
      /\b(?:brew|apt|apt-get|dnf|yum|zypper|pacman|apk)\b[^\n]*\b(?:install|-S)\b/;
    for (const [document, body] of prerequisites) {
      const commands = fencedBlocks(body as string)
        .join("\n")
        .toLowerCase();
      const haystack = PACKAGE_MANAGERS.test(commands) ? commands : (body as string).toLowerCase();
      const where =
        haystack === commands
          ? "no install command in its prerequisites installs"
          : "its prerequisites never name";
      for (const name of required) {
        // Package spellings differ per manager, so it is enough that the passage
        // names the library: apt's zlib1g-dev carries "zlib", and OpenSSL is
        // named by the brew line even though apt spells it libssl-dev.
        expect(
          haystack.includes(name.toLowerCase()),
          `${document} documents a from-source build but ${where} ${name}`,
        ).toBe(true);
      }
    }
  });
});
