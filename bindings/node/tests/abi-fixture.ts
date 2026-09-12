// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

/** Loader for the numeric constants the C ABI header declares. */

import { readFileSync } from "node:fs";

/** The enumerators of one C enum, keyed by the name the header spells. */
export type AbiEnum = Record<string, number>;

const HEADER_URL = new URL("../../../core/include/mes.h", import.meta.url);

const ENUM_CLOSE = /^}\s*(\w+)\s*;/;
const ENUM_MEMBER = /^(MES_\w+)\s*=\s*(-?\d+)\s*[,}]?/;

/**
 * Parse every `typedef enum { ... } tag;` block in `core/include/mes.h`.
 *
 * Enumerators are returned under their C names, keyed by enum tag, so a test can
 * compare a binding's mirror table against the header itself. A hand-copied
 * expectation would just be another copy able to drift.
 */
export function loadAbiEnums(): Record<string, AbiEnum> {
  const enums: Record<string, AbiEnum> = {};
  let current: AbiEnum | null = null;
  for (const raw of readFileSync(HEADER_URL, "utf8").split("\n")) {
    const line = raw.trim();
    if (line.startsWith("typedef enum")) {
      current = {};
      continue;
    }
    if (current === null) continue;
    const [, tag] = line.match(ENUM_CLOSE) ?? [];
    if (tag !== undefined) {
      enums[tag] = current;
      current = null;
      continue;
    }
    // Only explicitly valued enumerators are mirrored by a binding; an implicit
    // one would have no stable value to pin and is left out deliberately.
    const [, name, value] = line.match(ENUM_MEMBER) ?? [];
    if (name !== undefined && value !== undefined) current[name] = Number(value);
  }
  return enums;
}
