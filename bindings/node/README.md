# mysql-event-stream — Node.js Binding

[![CI](https://img.shields.io/github/actions/workflow/status/libraz/mysql-event-stream/ci.yml?branch=main&label=CI)](https://github.com/libraz/mysql-event-stream/actions)
[![npm](https://img.shields.io/npm/v/@libraz/mysql-event-stream?logo=npm)](https://www.npmjs.com/package/@libraz/mysql-event-stream)
[![License](https://img.shields.io/github/license/libraz/mysql-event-stream)](https://github.com/libraz/mysql-event-stream/blob/main/LICENSE)
[![Node](https://img.shields.io/badge/node-%E2%89%A522-brightgreen?logo=node.js)](https://nodejs.org/)
[![MySQL](https://img.shields.io/badge/MySQL-8.4%2B-blue?logo=mysql)](https://dev.mysql.com/)
[![MariaDB](https://img.shields.io/badge/MariaDB-10.11%2B-003545?logo=mariadb)](https://mariadb.org/)
[![Platform](https://img.shields.io/badge/platform-Linux%20%7C%20macOS-lightgrey)](https://github.com/libraz/mysql-event-stream)

Native N-API binding for the mysql-event-stream CDC engine (MySQL 8.4+ and MariaDB 10.11+). Wraps the C++ core as a Node.js addon using cmake-js.

> **npm users**: See the [npm README](README.npm.md) for installation and usage.

## Development

### Prerequisites

- Node.js 22+
- Yarn 4.9.1
- CMake 3.20+
- C++17 compiler (GCC 9+ or Clang 10+)
- OpenSSL development libraries

```bash
# macOS
brew install cmake openssl

# Ubuntu / Debian
sudo apt install cmake build-essential libssl-dev pkg-config
```

### Build

```bash
yarn install
yarn build          # Native addon + TypeScript
yarn build:native   # Native addon only
```

### Test

```bash
yarn test           # Unit tests (Vitest)
yarn test:e2e       # E2E tests (requires Docker MySQL)
```

### Lint

```bash
yarn check          # Biome check
yarn check:fix      # Auto-fix
```

## Architecture

```
src/
  addon/              # C++ N-API addon
    engine_wrap.cpp   #   CdcEngine wrapper
    client_wrap.cpp   #   BinlogClient wrapper
    addon.cpp         #   Module registration
  index.ts            # Package entry point
  engine.ts           # CdcEngine TypeScript wrapper
  client.ts           # BinlogClient TypeScript wrapper
  stream.ts           # CdcStream async iterator
  types.ts            # Public type definitions
```

The native addon statically links the C++ core (protocol layer, CDC engine, BinlogClient) and OpenSSL. The TypeScript layer provides typed wrappers and the `CdcStream` async iterator.

## Publishing

Publishing is automated via GitHub Actions on `v*.*.*` tags. The workflow:

1. Builds and tests the C++ core
2. Builds and tests the Node.js binding
3. Creates a GitHub Release
4. Publishes to npm with `--provenance`

The `prepack` script swaps `README.md` with `README.npm.md` so npm shows user-facing documentation.

## Exports

| Export | Description |
|--------|-------------|
| `CdcEngine` | Low-level binlog byte parser |
| `BinlogClient` | MySQL binlog replication client |
| `CdcStream` | High-level async iterator (recommended) |
| `ChangeEvent` | Event type definition |
| `ClientConfig` | Connection config type |
