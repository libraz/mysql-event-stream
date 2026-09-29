# mysql-event-stream

An embeddable CDC engine for MySQL 8.4+ and MariaDB 10.11+, as a native N-API addon. It reads the binary log as a replica would and emits row-level change events. No libmysqlclient: the wire protocol is implemented directly, and OpenSSL and zlib are bundled.

[![CI](https://img.shields.io/github/actions/workflow/status/libraz/mysql-event-stream/ci.yml?branch=main&label=CI)](https://github.com/libraz/mysql-event-stream/actions)
[![npm](https://img.shields.io/npm/v/@libraz/mysql-event-stream?logo=npm)](https://www.npmjs.com/package/@libraz/mysql-event-stream)
[![PyPI](https://img.shields.io/pypi/v/mysql-event-stream?logo=python)](https://pypi.org/project/mysql-event-stream/)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue)](https://github.com/libraz/mysql-event-stream/blob/main/LICENSE)
[![Node](https://img.shields.io/badge/node-%E2%89%A522-brightgreen?logo=node.js)](https://nodejs.org/)
[![MySQL](https://img.shields.io/badge/MySQL-8.4%2B-blue?logo=mysql)](https://dev.mysql.com/)
[![MariaDB](https://img.shields.io/badge/MariaDB-10.11%2B-003545?logo=mariadb)](https://mariadb.org/)

## Install

```bash
npm install @libraz/mysql-event-stream
```

Node.js 22 or later. The package uses neither optional platform dependencies nor an install-time prebuild downloader: its archive contains the addon produced at publish time, so use it when that addon matches your Node runtime and platform. Otherwise build from the repository with Node.js 22+, CMake, OpenSSL, zlib and a C++17 compiler:

```bash
git clone https://github.com/libraz/mysql-event-stream.git
cd mysql-event-stream/bindings/node
yarn install
yarn build
```

## Usage

```typescript
import { CdcStream } from "@libraz/mysql-event-stream";

await using stream = new CdcStream({
  host: "mysql.example.com",
  user: "replicator",
  password: process.env.MYSQL_PASSWORD,
  serverId: 1001,
  includeDatabases: ["shop"],
  startGtid: await loadCheckpoint(),
});

for await (const event of stream) {
  console.log(`${event.type} ${event.database}.${event.table}`);
  await handle(event); // idempotent
  await saveCheckpoint(stream.currentGtid);
}
```

An `UPDATE` arrives with both images of the row:

```json
{
  "type": "UPDATE",
  "database": "shop",
  "table": "items",
  "before": { "id": 8, "name": "Widget", "value": 42 },
  "after": { "id": 8, "name": "Widget", "value": 100 },
  "timestamp": 1773584164,
  "position": { "file": "mysql-bin.000003", "offset": 3611 },
  "namesResolved": true
}
```

`CdcStream` is the surface most applications want. Under it, `BinlogClient` owns the connection and hands back raw event bytes, and `CdcEngine` decodes binlog bytes from any source with no socket and no thread of its own. Bytes must start at an event boundary; a raw binlog file opens with a 4-byte magic number that has to be skipped first:

```typescript
import { CdcEngine } from "@libraz/mysql-event-stream";

const engine = new CdcEngine();
try {
  let offset = 0;
  while (offset < chunk.length || engine.hasEvents()) {
    for (let e = engine.nextEvent(); e !== null; e = engine.nextEvent()) {
      console.log(e.type, e.database, e.table);
    }
    if (offset < chunk.length) {
      const consumed = engine.feed(chunk.subarray(offset));
      offset += consumed;
      // Nothing consumed and nothing queued: the tail is a partial event.
      // Keep chunk.subarray(offset) and prepend it to the next chunk.
      if (consumed === 0 && !engine.hasEvents()) break;
    }
  }
} finally {
  engine.destroy();
}
```

## Server requirements

MySQL 8.4+ or MariaDB 10.11+, logging in row format with `binlog_row_image=FULL` and GTIDs enabled, and an account with `REPLICATION SLAVE` and `REPLICATION CLIENT`. Column names need either `binlog_row_metadata=FULL` or a `SELECT` grant on the streamed tables.

[Server setup](https://github.com/libraz/mysql-event-stream/blob/main/docs/en/server-setup.md) has the full configuration, the grants, and the replica identity rule that two processes on defaults will violate.

## Documentation

- [Node.js API](https://github.com/libraz/mysql-event-stream/blob/main/docs/en/node-api.md) — every class, option and default.
- [Getting started](https://github.com/libraz/mysql-event-stream/blob/main/docs/en/getting-started.md) and [Examples](https://github.com/libraz/mysql-event-stream/blob/main/docs/en/examples.md).
- [Change events](https://github.com/libraz/mysql-event-stream/blob/main/docs/en/change-events.md) and [Column values](https://github.com/libraz/mysql-event-stream/blob/main/docs/en/column-values.md) — what arrives, and as what type.
- [Checkpoints and recovery](https://github.com/libraz/mysql-event-stream/blob/main/docs/en/checkpoints.md), [Errors](https://github.com/libraz/mysql-event-stream/blob/main/docs/en/errors.md), [Threading and lifecycle](https://github.com/libraz/mysql-event-stream/blob/main/docs/en/threading.md).
- [Introduction](https://github.com/libraz/mysql-event-stream/blob/main/docs/en/introduction.md) indexes the rest.

## Also available

```bash
pip install mysql-event-stream  # Python binding
```

## License

[Apache-2.0](https://github.com/libraz/mysql-event-stream/blob/main/LICENSE)
