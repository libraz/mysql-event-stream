// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

import { createRequire } from "node:module";

const require = createRequire(import.meta.url);

/** Load the native addon compiled into build/Release. */
export function loadNativeAddon<T>(): T {
  return require("../build/Release/mes-node.node") as T;
}
