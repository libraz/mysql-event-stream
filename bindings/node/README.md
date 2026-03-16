# mysql-event-stream — Node.js Binding

[![CI](https://img.shields.io/github/actions/workflow/status/libraz/mysql-event-stream/ci.yml?branch=main&label=CI)](https://github.com/libraz/mysql-event-stream/actions)
[![License](https://img.shields.io/github/license/libraz/mysql-event-stream)](https://github.com/libraz/mysql-event-stream/blob/main/LICENSE)

A lightweight MySQL 8.4 CDC (Change Data Capture) engine for Node.js. Parses binlog replication streams and emits structured row-level change events (INSERT / UPDATE / DELETE).

Built as a native N-API addon on a C++ core for high throughput and low latency.

## Install

```bash
# Prerequisites
# macOS
brew install cmake mysql-client@8.4

# Ubuntu / Debian
sudo apt install cmake build-essential libmysqlclient-dev pkg-config

# Clone and build
git clone https://github.com/libraz/mysql-event-stream.git
cd mysql-event-stream/bindings/node
yarn install
yarn build
```

## Usage

```typescript
import { CdcEngine } from "@libraz/mysql-event-stream";

const engine = new CdcEngine();

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

- **Native performance** — C++ core with N-API binding, >100k events/sec
- **Streaming** — Process events incrementally as bytes arrive
- **MySQL 8.4** — Built for the latest MySQL LTS release
- **GTID support** — Native BinlogClient with GTID-based replication
- **Row-level events** — Full before/after column values for INSERT, UPDATE, DELETE
- **Column names** — Automatic column name resolution via metadata queries

## MySQL Requirements

- Version: 8.4
- Binary log format: ROW (`binlog_format=ROW`)
- GTID mode enabled (for BinlogClient)
- Replication privileges: `REPLICATION SLAVE`, `REPLICATION CLIENT`

## License

[Apache-2.0](https://github.com/libraz/mysql-event-stream/blob/main/LICENSE)
