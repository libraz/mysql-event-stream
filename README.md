# mysql-event-stream

An embeddable CDC engine for MySQL and MariaDB. It reads the binary log as a replica would and emits row-level change events, without linking a MySQL client library.

[![CI](https://img.shields.io/github/actions/workflow/status/libraz/mysql-event-stream/ci.yml?branch=main&label=CI)](https://github.com/libraz/mysql-event-stream/actions)
[![Version](https://img.shields.io/github/v/release/libraz/mysql-event-stream?label=version)](https://github.com/libraz/mysql-event-stream/releases)
[![npm](https://img.shields.io/npm/v/@libraz/mysql-event-stream?logo=npm)](https://www.npmjs.com/package/@libraz/mysql-event-stream)
[![PyPI](https://img.shields.io/pypi/v/mysql-event-stream?logo=python)](https://pypi.org/project/mysql-event-stream/)
[![codecov](https://codecov.io/gh/libraz/mysql-event-stream/branch/main/graph/badge.svg)](https://codecov.io/gh/libraz/mysql-event-stream)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue)](https://github.com/libraz/mysql-event-stream/blob/main/LICENSE)
[![C++17](https://img.shields.io/badge/C%2B%2B-17-blue?logo=c%2B%2B)](https://en.cppreference.com/w/cpp/17)
[![MySQL](https://img.shields.io/badge/MySQL-8.4%2B-blue?logo=mysql)](https://dev.mysql.com/)
[![MariaDB](https://img.shields.io/badge/MariaDB-10.11%2B-003545?logo=mariadb)](https://mariadb.org/)
[![Platform](https://img.shields.io/badge/platform-Linux%20%7C%20macOS-lightgrey)](https://github.com/libraz/mysql-event-stream)

![From a binlog stream to row-level change events](docs/images/pipeline.svg)

A C++17 core implements the MySQL wire protocol directly on OpenSSL — handshake, `caching_sha2_password`, `COM_BINLOG_DUMP_GTID` — and the binlog parser and row decoder above it. A C ABI publishes that core, and a Node.js N-API addon and a Python `ctypes` package sit on it. Release artifacts link OpenSSL and zlib statically and depend on nothing else.

## What it does

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
  await handle(event); // idempotent
  await saveCheckpoint(stream.currentGtid);
}
```

```python
async with CdcStream(
    host="mysql.example.com",
    user="replicator",
    password=os.environ["MYSQL_PASSWORD"],
    server_id=1001,
    include_databases=["shop"],
    start_gtid=await load_checkpoint(),
) as stream:
    async for event in stream:
        await handle(event)  # idempotent
        await save_checkpoint(stream.current_gtid)
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

`CdcStream` is the surface most applications want. Under it, `BinlogClient` owns the connection and hands back raw event bytes, and `CdcEngine` decodes binlog bytes from any source — a file, a queue, another process — with no socket and no thread of its own.

## Install

```sh
npm install @libraz/mysql-event-stream
```

```sh
pip install mysql-event-stream
```

Node.js 22 or later; Python 3.11 or later. A C or C++ program links `libmes` instead — see [Getting started](docs/en/getting-started.md#building-from-source).

The source must be MySQL 8.4+ or MariaDB 10.11+, logging in row format with `binlog_row_image=FULL`, and the account needs `REPLICATION SLAVE` and `REPLICATION CLIENT`. [Server setup](docs/en/server-setup.md) has the rest, including the replica identity rule two processes on defaults will violate.

## Documentation

Start at [Introduction](docs/en/introduction.md) and [Getting started](docs/en/getting-started.md); the introduction indexes every page. [Examples](docs/en/examples.md) has the worked shapes — a resumable consumer, cache invalidation, driving the client and the engine by hand, decoding a captured stream offline.

The reference pages are the [C API](docs/en/c-api.md), the [Node.js API](docs/en/node-api.md) and the [Python API](docs/en/python-api.md). Release notes live in [docs/releases](docs/releases/) and the summary history in [CHANGELOG.md](CHANGELOG.md).

## What it doesn't do

- **No snapshot.** It reads the log, not tables. The initial load of existing rows is yours; the stream resumes from the GTID taken before it.
- **No exactly-once delivery.** An event can be redelivered after a reconnect. Persist a checkpoint only after processing succeeded, and make the processing idempotent.
- **No DDL events.** Schema statements are not surfaced as change events.
- **No historical schema.** Column names resolved through a metadata connection describe the schema as it is now. For replay, use `binlog_row_metadata=FULL`, where the names travel with the event.
- **No transcoding.** Text columns are decoded as UTF-8; data stored in another character set is not converted.

## Origin

The binlog parsing and replication layer of [mygram-db](https://github.com/libraz/mygram-db), an in-memory full-text search engine with MySQL replication, extracted as a standalone engine. mygram-db is a search server; this is only the CDC part of it, shaped to embed in anything.

## License

[Apache-2.0](LICENSE)
