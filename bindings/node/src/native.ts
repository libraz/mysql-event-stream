// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

import { createRequire } from "node:module";

const require = createRequire(import.meta.url);

const nativePackages: Readonly<Record<string, Readonly<Record<string, string>>>> = {
  darwin: {
    arm64: "@libraz/mysql-event-stream-darwin-arm64",
    x64: "@libraz/mysql-event-stream-darwin-x64",
  },
  linux: {
    arm64: "@libraz/mysql-event-stream-linux-arm64",
    x64: "@libraz/mysql-event-stream-linux-x64",
  },
};

export function nativePackageName(platform: string, arch: string): string {
  const packageName = nativePackages[platform]?.[arch];
  if (packageName !== undefined) return packageName;
  throw new Error(
    `Unsupported native platform ${platform}-${arch}; supported targets are ` +
      "darwin-arm64, darwin-x64, linux-arm64, and linux-x64",
  );
}

/** Load a local development build or the matching optional prebuild package. */
export function loadNativeAddon<T>(): T {
  try {
    return require("../build/Release/mes-node.node") as T;
  } catch (localError) {
    const packageName = nativePackageName(process.platform, process.arch);
    try {
      return require(packageName) as T;
    } catch (packageError) {
      throw new Error(
        `Unable to load the mysql-event-stream native addon for ${process.platform}-${process.arch}. ` +
          `Ensure optional dependency ${packageName} is installed and optional dependencies are enabled.`,
        { cause: packageError ?? localError },
      );
    }
  }
}
