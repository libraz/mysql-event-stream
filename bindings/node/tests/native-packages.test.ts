// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";

interface PackageManifest {
  name: string;
  version: string;
  os?: string[];
  cpu?: string[];
  optionalDependencies?: Record<string, string>;
}

function readManifest(path: string): PackageManifest {
  return JSON.parse(readFileSync(new URL(path, import.meta.url), "utf8")) as PackageManifest;
}

describe("platform package manifests", () => {
  const main = readManifest("../package.json");
  const targets = [
    ["darwin-arm64", "darwin", "arm64"],
    ["darwin-x64", "darwin", "x64"],
    ["linux-arm64", "linux", "arm64"],
    ["linux-x64", "linux", "x64"],
  ] as const;

  it.each(targets)("keeps %s synchronized with the main package", (target, os, cpu) => {
    const platform = readManifest(`../npm/${target}/package.json`);
    expect(platform.version).toBe(main.version);
    expect(platform.os).toEqual([os]);
    expect(platform.cpu).toEqual([cpu]);
    expect(main.optionalDependencies?.[platform.name]).toBe(main.version);
  });
});
