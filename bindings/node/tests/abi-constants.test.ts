// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * Pins every numeric constant this binding re-declares from the C ABI against
 * `core/include/mes.h`. The header is the only authority; nothing here restates
 * a value, so a drift on either side fails rather than agreeing with a third
 * copy of the same number.
 */

import { describe, expect, it } from "vitest";
import { LogLevel } from "../src/logging.js";
import { MesErrorCode, ServerFlavor, SslMode } from "../src/types.js";
import { loadAbiEnums } from "./abi-fixture.js";

const abi = loadAbiEnums();

/** One TypeScript constant table and the C enum whose values it mirrors. */
interface MirroredEnum {
  /** How the table is spelled in this binding's public API. */
  label: string;
  /** Tag of the `typedef enum` in `core/include/mes.h`. */
  tag: string;
  /**
   * Enumerator prefixes to strip, tried in order. `mes_error_t` needs two,
   * because it spells success as `MES_OK` and failures as `MES_ERR_*`.
   */
  prefixes: string[];
  table: Record<string, number>;
}

const MIRRORED_ENUMS: MirroredEnum[] = [
  {
    label: "MesErrorCode",
    tag: "mes_error_t",
    prefixes: ["MES_ERR_", "MES_"],
    table: MesErrorCode,
  },
  { label: "SslMode", tag: "mes_ssl_mode_t", prefixes: ["MES_SSL_"], table: SslMode },
  {
    label: "ServerFlavor",
    tag: "mes_server_flavor_t",
    prefixes: ["MES_SERVER_FLAVOR_"],
    table: ServerFlavor,
  },
  { label: "LogLevel", tag: "mes_log_level_t", prefixes: ["MES_LOG_"], table: LogLevel },
];

/**
 * Reduce a name to the form that pairs a C enumerator with its TypeScript
 * member, so `MES_SERVER_FLAVOR_MARIADB` and `MariaDb` meet without either
 * side's casing convention being written down a second time.
 */
function pairingKey(name: string, prefixes: string[]): string {
  let bare = name;
  for (const prefix of prefixes) {
    if (bare.startsWith(prefix)) {
      bare = bare.slice(prefix.length);
      break;
    }
  }
  return bare.replaceAll("_", "").toLowerCase();
}

/**
 * Line up the header's enumerators with the binding's members.
 *
 * `expected` and `actual` are keyed by both names at once, so a value mismatch
 * reports which C constant and which TypeScript member disagree. A member the
 * header declares but the table omits is absent from `actual`; a member only the
 * table declares comes back in `unmirrored`.
 */
function compare(mirror: MirroredEnum): {
  expected: Record<string, number>;
  actual: Record<string, number>;
  unmirrored: string[];
} {
  const remaining = new Map<string, [string, number]>();
  for (const [name, value] of Object.entries(mirror.table)) {
    remaining.set(pairingKey(name, []), [name, value]);
  }

  const declared = abi[mirror.tag];
  if (declared === undefined) {
    throw new Error(`${mirror.tag} is not declared in core/include/mes.h`);
  }

  const expected: Record<string, number> = {};
  const actual: Record<string, number> = {};
  for (const [cName, cValue] of Object.entries(declared)) {
    const key = pairingKey(cName, mirror.prefixes);
    const member = remaining.get(key);
    const label = `${cName} (${mirror.label}.${member?.[0] ?? "<not declared>"})`;
    expected[label] = cValue;
    if (member !== undefined) {
      actual[label] = member[1];
      remaining.delete(key);
    }
  }

  return {
    expected,
    actual,
    unmirrored: [...remaining.values()].map(([name]) => `${mirror.label}.${name}`),
  };
}

describe("C ABI numeric constants", () => {
  it("finds every mirrored enum in the public header", () => {
    for (const mirror of MIRRORED_ENUMS) {
      expect(abi[mirror.tag], `${mirror.tag} declared in core/include/mes.h`).toBeDefined();
    }
  });

  for (const mirror of MIRRORED_ENUMS) {
    it(`declares ${mirror.label} exactly as the header declares ${mirror.tag}`, () => {
      const { expected, actual, unmirrored } = compare(mirror);
      // Both directions at once: a wrong value, and an enumerator the table
      // never mirrored, are the same failure here.
      expect(actual).toEqual(expected);
      expect(unmirrored, `${mirror.label} members ${mirror.tag} does not declare`).toEqual([]);
    });
  }
});
