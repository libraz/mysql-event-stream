// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

import { createRequire } from "node:module";

const require = createRequire(import.meta.url);

/** Platform the addon in the published npm archive is built for. */
const PUBLISHED_ADDON_TARGET = "linux/x64 with glibc 2.39 or newer";
const BUILD_FROM_SOURCE_URL =
  "https://github.com/libraz/mysql-event-stream/blob/main/docs/en/getting-started.md#building-from-source";

/** Load the native addon compiled into build/Release. */
export function loadNativeAddon<T>(): T {
  try {
    return require("../build/Release/mes-node.node") as T;
  } catch (error) {
    const detail = error instanceof Error ? `: ${error.message}` : "";
    throw new Error(
      `Unable to load @libraz/mysql-event-stream native addon for ${process.platform}/${process.arch}${detail}. ` +
        `The published addon is built for ${PUBLISHED_ADDON_TARGET}; on any other platform, build it from source: ${BUILD_FROM_SOURCE_URL}`,
      { cause: error },
    );
  }
}
