# @libraz/mysql-event-stream

[![CI](https://img.shields.io/github/actions/workflow/status/libraz/mysql-event-stream/ci.yml?branch=main&label=CI)](https://github.com/libraz/mysql-event-stream/actions)
[![codecov](https://codecov.io/gh/libraz/mysql-event-stream/branch/main/graph/badge.svg)](https://codecov.io/gh/libraz/mysql-event-stream)
[![npm](https://img.shields.io/npm/v/@libraz/mysql-event-stream)](https://www.npmjs.com/package/@libraz/mysql-event-stream)
[![License](https://img.shields.io/github/license/libraz/mysql-event-stream)](https://github.com/libraz/mysql-event-stream/blob/main/LICENSE)

A lightweight MySQL 8.4 CDC (Change Data Capture) engine for Node.js. Parses binlog replication streams and emits structured row-level change events (INSERT / UPDATE / DELETE).

Built as a native N-API addon on a C++ core for high throughput and low latency.

## Install

```bash
npm install @libraz/mysql-event-stream
```

> Requires Node.js 22+, CMake 3.20+, and a C++17 compiler. The native addon is compiled on install.

## Usage

```typescript
import { MesEngine } from "@libraz/mysql-event-stream";

const engine = new MesEngine();

// Feed raw binlog bytes from your replication stream
engine.feed(binlogChunk);

while (engine.hasEvents()) {
  const event = engine.nextEvent();
  console.log(event.type, event.database, event.table);
  console.log("before:", event.before);
  console.log("after:", event.after);
}
```

## Event Format

Each `ChangeEvent` contains the event type, database/table name, binlog position, and row data as a plain object keyed by column name:

```json
{
  "type": "UPDATE",
  "database": "mydb",
  "table": "users",
  "before": { "id": 1, "name": "Alice", "score": 42 },
  "after": { "id": 1, "name": "Alice", "score": 100 },
  "timestamp": 1773584164,
  "position": { "file": "mysql-bin.000003", "offset": 3611 }
}
```

## Features

- **Native performance** - C++ core with N-API binding, >100k events/sec
- **Streaming** - Process events incrementally as bytes arrive
- **MySQL 8.4** - Built for the latest MySQL LTS release
- **GTID support** - Native BinlogClient with GTID-based replication
- **Row-level events** - Full before/after column values for INSERT, UPDATE, DELETE
- **Column names** - Automatic column name resolution via metadata queries
- **Dict-based** - Row data as `Record<string, unknown>` for intuitive access

## MySQL Requirements

- Version: 8.4
- Binary log format: ROW (`binlog_format=ROW`)
- GTID mode enabled (for BinlogClient)
- Replication privileges: `REPLICATION SLAVE`, `REPLICATION CLIENT`

## License

[Apache-2.0](https://github.com/libraz/mysql-event-stream/blob/main/LICENSE)
