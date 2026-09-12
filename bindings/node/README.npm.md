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

// feed() stops early once the event queue is full, so drain the queue and
// re-feed the unconsumed tail instead of dropping it.
let offset = 0;
while (offset < binlogChunk.length) {
  const consumed = engine.feed(binlogChunk.subarray(offset));
  offset += consumed;

  while (engine.hasEvents()) {
    const event = engine.nextEvent();
    if (event === null) break;
    console.log(event.type, event.database, event.table);
  }

  // Nothing consumed and nothing left to drain: the tail is a partial event.
  // Retain binlogChunk.subarray(offset) and prepend it to the next chunk.
  if (consumed === 0) break;
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
literal. `setIncludeDatabases` has no wildcard form: it compares the database
name byte for byte, so list every database you want rather than reaching for a
prefix. If include filters see TABLE_MAP events but match none, the configured
log callback receives one `include_filter_matched_nothing` WARN when the engine
is reset or destroyed.

### Column names

`binlog_row_metadata=FULL` puts column names in the `TABLE_MAP` event and needs
nothing from the binding. Otherwise the names come from a separate connection
that runs `SHOW COLUMNS`. `CdcStream` opens that connection from its own
config; an engine you feed yourself enables it explicitly.

```typescript
const engine = new CdcEngine();
engine.enableMetadata({
  host: "mysql.example.com",
  user: "replicator",
  password: "secret",
  readTimeoutS: 30,
});
```

The credentials need `SELECT` on the streamed tables. `TABLE_MAP` processing
then runs `SHOW COLUMNS` synchronously, bounded by `readTimeoutS` -- `0`
delegates the bound to the operating system and can block indefinitely. A
timeout leaves that one event's names unresolved and the connection is retried
once, so check `namesResolved` on every event rather than assuming resolution
succeeded. The connection reads the server's schema as it is now, not as it was
at the binlog position being decoded; for replay from an older position, use
`binlog_row_metadata=FULL` and keep the original `TABLE_MAP` metadata.

## Lifecycle

`new BinlogClient(config)` connects and validates the server configuration
immediately, so a bad host or a rejected credential is thrown by the
constructor rather than surfacing on the first poll. Call `start()` before
polling, then `poll()` for one event or `pollBatch()` for one event plus
whatever else is already queued, and `destroy()` when finished. `destroy()` is
idempotent.

```typescript
import { BinlogClient } from "@libraz/mysql-event-stream";

const client = new BinlogClient({
  host: "127.0.0.1",
  user: "replicator",
  password: "secret",
});

client.start();
try {
  const result = await client.poll();
  // A heartbeat is a healthy silent interval, not an event: data is null.
  if (!result.isHeartbeat && result.data !== null) {
    feedToEngine(result.data);
  }
} finally {
  client.destroy();
}
```

Only one `poll()` may be in flight at a time, and it blocks until an event
arrives or the stream stops. Cancel a pending one with `BinlogClient.stop()`
before changing connection lifecycle state. `CdcStream` owns this whole
sequence and `await stream.close()` is its corresponding idempotent cleanup, so
use the client directly only when you own the event loop it runs on.

## Error handling

Every error this package throws carries a numeric `code` from `MesErrorCode`,
which mirrors the C ABI and is what to branch on — message text is worded for
the failure at hand and is not part of the API. `catch` binds its value as
`unknown` under TypeScript's `strict`, so narrow it with `isMesError` to reach
the code:

```typescript
import { isMesError, MesErrorCode } from "@libraz/mysql-event-stream";

try {
  for await (const event of stream) {
    handle(event);
  }
} catch (error) {
  if (isMesError(error) && error.code === MesErrorCode.Disconnected) {
    await resumeFromCheckpoint();
  } else {
    throw error;
  }
}
```

`MesError` is the type the guard narrows to. It is an interface rather than a
class: errors cross from the native addon as plain `Error`, `TypeError` and
`RangeError` objects with `code` set on them, so `isMesError` is the check that
works — `instanceof` has no class of ours to test against. An argument this
binding refuses before the call reaches the addon carries the same shape, so
one branch covers both. The built-in subclass is kept for argument refusals:
`instanceof TypeError` holds for a wrongly-typed option and
`instanceof RangeError` for one outside its accepted window.

Errors also carry a `name` naming their category — `MesAuthError`,
`MesConnectError`, `MesDecodeError` and so on, `MesError` where a code has no
category of its own. It reads well in a log line, but a category groups several
codes, so `code` is the finer-grained value.

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

- **Native performance** -- C++ core with N-API binding. Core decode throughput is recorded above 100k row events/sec in [the measurement baseline](https://github.com/libraz/mysql-event-stream/blob/main/core/benchmarks/BASELINE.md); the addon's own marshalling cost is not in that figure and has no published measurement, so read it as the core's rate rather than the package's
- **No libmysqlclient** -- MySQL / MariaDB wire protocol implemented directly; OpenSSL and ZLIB are bundled
- **Streaming** -- Process events incrementally as bytes arrive
- **MySQL 8.4+ and MariaDB 10.11+** -- Auto-detects server flavor and negotiates the appropriate binlog protocol
- **GTID support** -- BinlogClient with GTID-based replication (MySQL `uuid:gno` and MariaDB `domain-server-seq` formats)
- **Row-level events** -- Full before/after column values for INSERT, UPDATE, DELETE
- **Column names** -- Automatic resolution with `binlog_row_metadata=FULL` or a metadata connection that has `SELECT`; `CdcStream` opens that connection itself, `CdcEngine` takes it from `enableMetadata()`
- **SSL/TLS** -- Secure MySQL connections with certificate verification
- **Backpressure** -- Internal reader thread with bounded event queue (default 10,000)
- **Auto-reconnection** -- Jittered linear backoff on connection loss (default 10 attempts)
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
