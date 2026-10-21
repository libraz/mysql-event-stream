// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it, vi } from "vitest";

vi.mock("node:module", () => ({
  createRequire: () => () => {
    throw new Error("dlopen failed");
  },
}));

const { loadNativeAddon } = await import("../src/native.js");

describe("loadNativeAddon", () => {
  it("names the published addon's target and the from-source path when loading fails", () => {
    let message = "";
    try {
      loadNativeAddon();
    } catch (error) {
      message = (error as Error).message;
    }
    expect(message).toContain(`${process.platform}/${process.arch}: dlopen failed`);
    expect(message).toContain("linux/x64 with glibc 2.39 or newer");
    expect(message).toContain("getting-started.md#building-from-source");
  });
});
