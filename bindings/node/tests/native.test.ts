// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from "vitest";
import { nativePackageName } from "../src/native.js";

describe("nativePackageName", () => {
  it.each([
    ["darwin", "arm64", "@libraz/mysql-event-stream-darwin-arm64"],
    ["darwin", "x64", "@libraz/mysql-event-stream-darwin-x64"],
    ["linux", "arm64", "@libraz/mysql-event-stream-linux-arm64"],
    ["linux", "x64", "@libraz/mysql-event-stream-linux-x64"],
  ])("maps %s-%s to its optional package", (platform, arch, expected) => {
    expect(nativePackageName(platform, arch)).toBe(expected);
  });

  it("rejects a target with no published prebuild", () => {
    expect(() => nativePackageName("win32", "x64")).toThrow(/Unsupported native platform/);
  });
});
