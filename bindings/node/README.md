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
- Yarn 4.18.0 (pinned by `packageManager` in `package.json`)
- CMake 3.20+
- C++17 compiler (GCC 9+ or Clang 10+)
- OpenSSL development libraries
- zlib development libraries

```bash
# macOS
brew install cmake openssl zlib

# Ubuntu / Debian
sudo apt install cmake build-essential libssl-dev zlib1g-dev pkg-config
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

The native addon statically links the C++ core (protocol layer, CDC engine, BinlogClient) together with OpenSSL and zlib. The TypeScript layer provides typed wrappers and the `CdcStream` async iterator.

## Lifecycle

`new BinlogClient(config)` connects and validates configuration immediately;
connection errors are thrown by the constructor. Call `start()` before polling,
then call `destroy()` when finished. `destroy()` is idempotent. Only one
`poll()` may be in flight; use `BinlogClient.stop()` to cancel it before
changing connection lifecycle state. `CdcStream` owns this sequence and `await stream.close()` is
the corresponding idempotent cleanup operation.

## Thread Safety

`CdcEngine` instances are single-owner objects. Do not call `feed()`,
`nextEvent()`, `reset()`, or filter/configuration methods concurrently on the
same engine instance. Use one engine per worker/task or serialize access
externally.

`BinlogClient` / `CdcStream` use an internal reader thread. Polling/iteration and
connection lifecycle calls are single-owner operations. `BinlogClient.stop()` is
the any-thread cancellation path and may be called from another thread to unblock
a pending `poll()`. `CdcStream` has no `stop` method: cancel it with
`await stream.close()` from the task that owns the stream, which interrupts the
native poll first and then finalizes the iterator.

Each active stream has one blocking native poll worker. `pollBatch()` amortizes
that handoff by draining up to 64 queued events after the first result, but the
worker still occupies a libuv thread-pool slot while idle. Node defaults to four
slots; for more than four concurrently idle streams, set `UV_THREADPOOL_SIZE`
before starting Node (for example, `UV_THREADPOOL_SIZE=16 node app.mjs`).

## MySQL binlog configuration

The connection validator requires the following MySQL settings. Copy this into
your `my.cnf` (or its included configuration file) and restart MySQL after
changing it:

```ini
[mysqld]
log_bin=ON
gtid_mode=ON
binlog_format=ROW
binlog_row_image=FULL
binlog_transaction_compression=OFF
binlog_row_value_options=""
```

`binlog_row_value_options` must not contain `PARTIAL_JSON`. MariaDB is checked
for the equivalent required row format and rejects `log_bin_compress=ON`.

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
| `LogLevel`, `setLogCallback`, `LogHandler` | Structured logging API and its handler type |
| `MesErrorCode` | Stable native error-code enum |
| `MesError`, `isMesError` | Declared shape of a thrown error and the guard that narrows a caught value to it |
| `ServerFlavor`, `SslMode` | Server and TLS enums |
| `ChangeEvent`, `ClientConfig`, `ColumnValue`, `EventType`, `PollResult`, `StreamConfig` | Public TypeScript types |
