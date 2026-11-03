# mysql-event-stream — Node.js binding

[![CI](https://img.shields.io/github/actions/workflow/status/libraz/mysql-event-stream/ci.yml?branch=main&label=CI)](https://github.com/libraz/mysql-event-stream/actions)
[![npm](https://img.shields.io/npm/v/@libraz/mysql-event-stream?logo=npm)](https://www.npmjs.com/package/@libraz/mysql-event-stream)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue)](https://github.com/libraz/mysql-event-stream/blob/main/LICENSE)
[![Node](https://img.shields.io/badge/node-%E2%89%A522-brightgreen?logo=node.js)](https://nodejs.org/)
[![MySQL](https://img.shields.io/badge/MySQL-8.4%2B-blue?logo=mysql)](https://dev.mysql.com/)
[![MariaDB](https://img.shields.io/badge/MariaDB-10.11%2B-003545?logo=mariadb)](https://mariadb.org/)

Native N-API binding for the mysql-event-stream CDC engine, built with cmake-js.

This file is for working on the binding. Users of the published package want the [npm README](README.npm.md), and the API is documented in [docs/en/node-api.md](../../docs/en/node-api.md).

## Prerequisites

- Node.js 22+
- Yarn (pinned by `packageManager` in `package.json`)
- CMake 3.20+
- C++17 compiler (GCC 9+ or Clang 10+)
- OpenSSL and zlib development libraries

```bash
# macOS
brew install cmake openssl zlib

# Ubuntu / Debian
sudo apt install cmake build-essential libssl-dev zlib1g-dev pkg-config
```

The first native build fetches the binlog codec, [libmybinlog](https://github.com/libraz/libmybinlog), from GitHub, so it needs network access. Set `MYBINLOG_SOURCE_DIR` to build against a local checkout.

## Build, test, lint

```bash
yarn install
yarn build          # native addon + TypeScript
yarn build:native   # native addon only
yarn build:release  # addon with OpenSSL, zlib and the C++ runtime linked statically
yarn check:linkage  # fail if the built addon depends on them dynamically

yarn test           # unit tests (Vitest)
yarn test:e2e       # E2E tests (requires the Docker MySQL from bindings/node/e2e/docker)
yarn typecheck

yarn check          # Biome
yarn check:fix
```

The E2E containers here (`mes_node_test_mysql` / `mes_node_test_mariadb`, host port 13307) are separate from the core tier's, so the two suites do not displace each other.

## Layout

```
src/
  addon/              # C++ N-API addon
    engine_wrap.cpp   #   CdcEngine wrapper
    client_wrap.cpp   #   BinlogClient wrapper
    addon.cpp         #   module registration
  index.ts            # package entry point
  engine.ts           # CdcEngine wrapper
  client.ts           # BinlogClient wrapper
  stream.ts           # CdcStream async iterator
  contract.ts         # shared defaults and the retry classification
  types.ts            # public type definitions
```

The addon compiles the C++ core — protocol layer, engine, client — into itself. `yarn build` links the system OpenSSL and zlib dynamically; `yarn build:release`, which the published addon comes from, links them and the GNU C++ runtime statically, and `yarn check:linkage` fails if the result still depends on any of them. The TypeScript layer adds typed wrappers, option validation, and the `CdcStream` async iterator.

## Publishing

Automated by GitHub Actions on a `v*.*.*` tag: build and test the core, build the pinned static OpenSSL and zlib, build the binding with `build:release`, check its linkage and test it, create a GitHub Release, publish to npm with `--provenance`. The `prepack` script swaps `README.md` for `README.npm.md` so the registry shows the user-facing page.

## Exports

| Export | Description |
|--------|-------------|
| `CdcStream` | High-level async iterator of `ChangeEvent` (recommended) |
| `BinlogClient` | Connection and raw event bytes |
| `CdcEngine` | Binlog byte decoder |
| `LogLevel`, `setLogCallback`, `LogHandler` | Structured logging API and its handler type |
| `MesErrorCode` | Stable native error-code enum |
| `MesError`, `isMesError` | Declared shape of a thrown error and the guard that narrows a caught value to it |
| `ServerFlavor`, `SslMode` | Server and TLS enums |
| `ChangeEvent`, `ClientConfig`, `ColumnValue`, `EventType`, `PollResult`, `StreamConfig` | Public TypeScript types |
