# mysql-event-stream

[![CI](https://img.shields.io/github/actions/workflow/status/libraz/mysql-event-stream/ci.yml?branch=main&label=CI)](https://github.com/libraz/mysql-event-stream/actions)
[![npm](https://img.shields.io/npm/v/@libraz/mysql-event-stream)](https://www.npmjs.com/package/@libraz/mysql-event-stream)
[![License](https://img.shields.io/github/license/libraz/mysql-event-stream)](https://github.com/libraz/mysql-event-stream/blob/main/LICENSE)

Lightweight CDC (Change Data Capture) engine for Node.js supporting MySQL 8.4+ and MariaDB 10.11+. Native N-API addon -- no libmysqlclient required.

## Installation

```bash
npm install @libraz/mysql-event-stream
```

The npm package does not use optional platform dependencies or an install-time
prebuild downloader. Its archive contains the addon produced at publish time;
use it only when that addon matches your Node runtime and platform. For a
portable deployment, build from this repository with Node.js 22+, CMake,
OpenSSL, ZLIB, and a C++17 compiler:

```bash
git clone https://github.com/libraz/mysql-event-stream.git
cd mysql-event-stream/bindings/node
yarn install
yarn build
```

## Usage

### Streaming from MySQL

```typescript
import { CdcStream } from "@libraz/mysql-event-stream";

const stream = new CdcStream({
  host: "127.0.0.1",
  port: 3306,
  user: "replicator",
  password: "secret",
});

for await (const event of stream) {
  console.log(`${event.type} ${event.database}.${event.table}`);
  console.log("  before:", event.before);
  console.log("  after: ", event.after);
}
```

### Parsing binlog bytes

```typescript
import { CdcEngine } from "@libraz/mysql-event-stream";

const engine = new CdcEngine();
// Only needed when a checksum=NONE byte stream starts after its FDE:
// engine.setChecksumEnabled(false);
let offset = 0;
while (offset < binlogChunk.length) {
  const consumed = engine.feed(binlogChunk.subarray(offset));
  if (consumed === 0) break; // retain the remainder and drain backpressure
  offset += consumed;
}

while (engine.hasEvents()) {
  const event = engine.nextEvent();
  console.log(event.type, event.database, event.table);
}

engine.destroy();
```

### Structured logging

```typescript
import { LogLevel, setLogCallback } from "@libraz/mysql-event-stream";

setLogCallback((level, message) => console.error(level, message), LogLevel.Warn);
```

The callback is process-wide, but does not keep a process or Worker alive by
itself. Native delivery is bounded to 256 pending records. If the JavaScript
thread falls behind, excess diagnostics are dropped and the next delivered
record is preceded by `event=node_log_queue_overflow dropped=N`. Call
`setLogCallback(null)` when the handler is no longer needed.

### SSL/TLS

```typescript
const stream = new CdcStream({
  host: "mysql.example.com",
  user: "replicator",
  password: "secret",
  sslMode: 2,  // 0=disabled, 1=preferred, 2=required, 3=verify_ca, 4=verify_identity
  sslCa: "/path/to/ca.pem",
});
```

### Table Filtering

```typescript
const engine = new CdcEngine();
engine.setIncludeDatabases(["mydb"]);
engine.setExcludeTables(["mydb.audit_log"]);
```

Table filters are case-sensitive. Use an exact `database.table` or bare table
name, or a trailing-`*` prefix such as `mydb.audit_*`. A `*` elsewhere is
literal. If include filters see TABLE_MAP events but match none, the configured
log callback receives one `include_filter_matched_nothing` WARN when the engine
is reset or destroyed.

## Thread Safety

`CdcEngine` instances are single-owner objects. Do not call `feed()`,
`nextEvent()`, `reset()`, or filter/configuration methods concurrently on the
same engine instance. Use one engine per worker/task or serialize access
externally.

`BinlogClient` / `CdcStream` use an internal reader thread. Polling/iteration and
connection lifecycle calls are single-owner operations; `stop()` is the intended
any-thread cancellation path and may be used to unblock a pending poll/iterator.

Each active stream has one blocking native poll worker. `pollBatch()` drains up
to 64 already queued events after the first result, but an idle stream still uses
a libuv thread-pool slot. Node defaults to four slots; for more than four idle
streams, set `UV_THREADPOOL_SIZE` before Node starts, for example
`UV_THREADPOOL_SIZE=16 node app.mjs`.

## Event Format

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

- **Native performance** -- C++ core with N-API binding, >100k events/sec
- **No libmysqlclient** -- MySQL / MariaDB wire protocol implemented directly; OpenSSL and ZLIB are bundled
- **Streaming** -- Process events incrementally as bytes arrive
- **MySQL 8.4+ and MariaDB 10.11+** -- Auto-detects server flavor and negotiates the appropriate binlog protocol
- **GTID support** -- BinlogClient with GTID-based replication (MySQL `uuid:gno` and MariaDB `domain-server-seq` formats)
- **Row-level events** -- Full before/after column values for INSERT, UPDATE, DELETE
- **Column names** -- Automatic resolution with `binlog_row_metadata=FULL` or a metadata connection that has `SELECT`
- **SSL/TLS** -- Secure MySQL connections with certificate verification
- **Backpressure** -- Internal reader thread with bounded event queue (default 10,000)
- **Auto-reconnection** -- Linear backoff on connection loss (default 10 attempts)
- **Table filtering** -- Include/exclude databases and tables

## Server Requirements

**MySQL:**
- Version: 8.4+
- Binary log format: ROW (`binlog_format=ROW`)
- GTID mode enabled (for BinlogClient)
- Replication privileges: `REPLICATION SLAVE`, `REPLICATION CLIENT`
- For schema-derived column names, set `binlog_row_metadata=FULL` or also grant `SELECT`. Metadata queries use a separate connection with the same credentials.

**MariaDB:**
- Version: 10.11+ (tested against 10.11 and 11.4)
- GTID replication enabled (`log_bin` in ROW format)
- Replication privileges: `REPLICATION SLAVE`, `REPLICATION CLIENT`
- For schema-derived column names, set `binlog_row_metadata=FULL` or also grant `SELECT`. Metadata queries use a separate connection with the same credentials.

### MySQL binlog configuration

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

## Also available

```bash
pip install mysql-event-stream  # Python binding
```

## License

[Apache-2.0](https://github.com/libraz/mysql-event-stream/blob/main/LICENSE)
