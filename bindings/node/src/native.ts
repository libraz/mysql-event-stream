// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

import { createRequire } from "node:module";

const require = createRequire(import.meta.url);

/** Load the native addon compiled into build/Release. */
export function loadNativeAddon<T>(): T {
  try {
    return require("../build/Release/mes-node.node") as T;
  } catch (error) {
    const detail = error instanceof Error ? `: ${error.message}` : "";
    throw new Error(
      `Unable to load @libraz/mysql-event-stream native addon for ${process.platform}/${process.arch}${detail}`,
      { cause: error },
    );
  }
}
