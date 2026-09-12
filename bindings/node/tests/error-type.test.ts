// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from "vitest";
import { BinlogClient, CdcStream, isMesError, type MesError, MesErrorCode } from "../src/index.js";

/**
 * What a caller can do with an error this package throws.
 *
 * Everything here is imported from the package entry point and nothing is
 * cast, because that is the whole claim: a consumer who reads `code` off a
 * caught value has a declared type to reach it through. `catch` binds
 * `unknown` under `strict`, so without the guard none of this compiles — which
 * is why this file has to be type-checked and not merely run.
 */

/** No server listens here, so a connection to it fails rather than hanging. */
const UNREACHABLE_PORT = 19999;

/** Run `call` and return the error it threw, narrowed by the guard. */
function narrowedFailure(call: () => void): MesError | null {
  try {
    call();
  } catch (error: unknown) {
    return isMesError(error) ? error : null;
  }
  throw new Error("expected the call to throw");
}

describe("an error from this package", () => {
  it("narrows from an unknown catch binding to a code a caller can branch on", () => {
    // Written the way a consumer writes it, in full, rather than through the
    // helper above: the catch binding is `unknown`, the guard is the only thing
    // between it and `code`, and the annotation on `observed` is what makes the
    // property's declared type part of what the type-check covers.
    let observed: MesErrorCode | undefined;
    try {
      new BinlogClient({ host: "127.0.0.1", port: UNREACHABLE_PORT });
    } catch (error: unknown) {
      if (isMesError(error)) {
        observed = error.code;
      }
    }
    expect(observed).toBe(MesErrorCode.Connect);
  });

  it("carries the same declared shape whichever layer raised it", () => {
    // A failed connection is raised by the addon; an option outside its
    // accepted window is refused in TypeScript before the addon is reached.
    // One declaration covers both, so a caller needs one branch, not two.
    const native = narrowedFailure(() => {
      new BinlogClient({ host: "127.0.0.1", port: UNREACHABLE_PORT });
    });
    expect(native?.code).toBe(MesErrorCode.Connect);

    const refused = narrowedFailure(() => {
      new CdcStream({ host: "127.0.0.1", port: 70_000 });
    });
    expect(refused?.code).toBe(MesErrorCode.InvalidArg);
  });

  it("reports a code the exported enum names", () => {
    // The declared type is the enum rather than a bare number, so a code with
    // no member would be a value the caller cannot name in a comparison.
    const native = narrowedFailure(() => {
      new BinlogClient({ host: "127.0.0.1", port: UNREACHABLE_PORT });
    });
    expect(Object.values(MesErrorCode)).toContain(native?.code);
  });
});

describe("the guard", () => {
  it("rejects everything else a catch binding can hold", () => {
    // A `throw` takes any value at all, so the guard is what stands between a
    // caller and reading `code` off something that never came from here.
    expect(isMesError(new Error("plain"))).toBe(false);
    expect(isMesError(null)).toBe(false);
    expect(isMesError(undefined)).toBe(false);
    expect(isMesError("MesConnectError: connection refused")).toBe(false);
    expect(isMesError({ code: MesErrorCode.Auth, message: "not an Error" })).toBe(false);
  });

  it("rejects an Error whose code is not the numeric kind this package sets", () => {
    // Node's own system errors are Errors carrying a string `code`, and one
    // reaches a caller's catch block from the same call as ours. Accepting it
    // would hand them a `code` no MesErrorCode comparison can ever match.
    expect(isMesError(Object.assign(new Error("no such file"), { code: "ENOENT" }))).toBe(false);
  });
});
