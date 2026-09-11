# mysql-event-stream — Python Binding

[![CI](https://img.shields.io/github/actions/workflow/status/libraz/mysql-event-stream/ci.yml?branch=main&label=CI)](https://github.com/libraz/mysql-event-stream/actions)
[![PyPI](https://img.shields.io/pypi/v/mysql-event-stream?logo=python)](https://pypi.org/project/mysql-event-stream/)
[![License](https://img.shields.io/github/license/libraz/mysql-event-stream)](https://github.com/libraz/mysql-event-stream/blob/main/LICENSE)
[![Python](https://img.shields.io/badge/python-%E2%89%A53.11-blue?logo=python)](https://python.org/)
[![MySQL](https://img.shields.io/badge/MySQL-8.4%2B-blue?logo=mysql)](https://dev.mysql.com/)
[![MariaDB](https://img.shields.io/badge/MariaDB-10.11%2B-003545?logo=mariadb)](https://mariadb.org/)
[![Platform](https://img.shields.io/badge/platform-Linux%20%7C%20macOS-lightgrey)](https://github.com/libraz/mysql-event-stream)

A lightweight CDC (Change Data Capture) engine for Python supporting MySQL 8.4+ and MariaDB 10.11+. Parses binlog replication streams and emits structured row-level change events (INSERT / UPDATE / DELETE).

Built on a self-contained C++ core using ctypes FFI for high throughput and low latency. No external MySQL client library (libmysqlclient) required.

## Install

```bash
pip install mysql-event-stream
```

Platform wheels are available for:

- Linux x86_64 and aarch64 (recommended for server deployments)
- macOS 15.0 or newer on x86_64 and arm64 (development use)

## Usage

### Parsing binlog bytes

```python
from mysql_event_stream import CdcEngine

# The engine holds native state, so scope it rather than waiting for the
# garbage collector to release it.
with CdcEngine() as engine:
    # Only needed when a checksum=NONE byte stream starts after its FDE:
    # engine.set_checksum_enabled(False)

    # Feed raw binlog bytes. feed() stops early once the event queue is full,
    # so drain the queue and re-feed the unconsumed tail instead of dropping it.
    offset = 0
    while offset < len(binlog_chunk):
        consumed = engine.feed(binlog_chunk[offset:])
        offset += consumed

        while (event := engine.next_event()) is not None:
            print(event.type, event.database, event.table)
            print("before:", event.before)
            print("after:", event.after)

        if consumed == 0:
            # Partial event at the tail: retain binlog_chunk[offset:] and
            # prepend it to the next chunk.
            break
```

### Column names

`binlog_row_metadata=FULL` puts column names in the `TABLE_MAP` event and needs
nothing from the binding. Otherwise the names come from a separate connection
that runs `SHOW COLUMNS`. `CdcStream` opens that connection from its own
settings; an engine you feed yourself enables it explicitly.

```python
with CdcEngine() as engine:
    engine.enable_metadata(
        host="mysql.example.com",
        user="replicator",
        password="secret",
        read_timeout_s=30,
    )
```

The credentials need `SELECT` on the streamed tables. `TABLE_MAP` processing
then runs `SHOW COLUMNS` synchronously, bounded by `read_timeout_s` — `0`
delegates the bound to the operating system and can block indefinitely. A
timeout leaves that one event's names unresolved and the connection is retried
once, so check `names_resolved` on every event rather than assuming resolution
succeeded. The connection reads the server's schema as it is now, not as it was
at the binlog position being decoded; for replay from an older position, use
`binlog_row_metadata=FULL` and keep the original `TABLE_MAP` metadata.

### Streaming from MySQL

```python
import asyncio
from mysql_event_stream import CdcStream


async def main():
    # `async for` never finalizes the iterator it borrows, so scope the stream
    # and let the context manager close it. Leaving the loop early otherwise
    # keeps the native client, its reader thread, and the socket alive.
    async with CdcStream(
        host="127.0.0.1",
        port=3306,
        user="replicator",
        password="secret",
    ) as stream:
        async for event in stream:
            print(f"{event.type.name} {event.database}.{event.table}")
            print(f"  before: {event.before}")
            print(f"  after:  {event.after}")

    # The last checkpoint survives the scope. Delivery is at-least-once, so
    # persist this only after your own processing has succeeded.
    print(f"checkpoint: {stream.current_gtid}")


asyncio.run(main())
```

### Low-level client

`BinlogClient` exposes explicit `connect()`, `start()`, `poll()`, `stop()`,
`disconnect()`, and `close()` calls for applications that own their own event
loop. `ClientConfig` and `SslMode` describe its connection settings; `PollResult`
contains packet data or a heartbeat. `CdcStream` is the higher-level async
iterator and is the usual choice.

```python
from mysql_event_stream import BinlogClient, SslMode

with BinlogClient(user="replicator", password="secret", ssl_mode=SslMode.REQUIRED) as client:
    client.connect()
    client.start()
    result = client.poll()
```

### Errors and logging

`ParseError`, `DecodeError`, and `ChecksumError` identify malformed binlog
input. Native failures also carry a stable `MesErrorCode`. Install a
process-wide structured log handler with `set_log_callback`; it can run on the
native reader thread, so keep it non-blocking and do not call client lifecycle
methods from the handler.

```python
from mysql_event_stream import LogLevel, set_log_callback

set_log_callback(lambda level, message: print(level.name, message), LogLevel.WARN)
```

### Loading a specific native library

Set `MES_LIB_PATH=/absolute/path/to/libmes.so` (or `.dylib`) before import, or
pass `lib_path=` to `CdcEngine`, `BinlogClient`, or `set_log_callback()` to
select the libmes instance to use.

## Event Format

Each `ChangeEvent` contains the event type, database/table name, binlog position, and row data as a plain dict keyed by column name:

```python
ChangeEvent(
    type=EventType.UPDATE,
    database="mydb",
    table="users",
    before={"id": 1, "name": "Alice", "score": 42},
    after={"id": 1, "name": "Alice", "score": 100},
    timestamp=1773584164,
    position=BinlogPosition(file="mysql-bin.000003", offset=3611),
    names_resolved=True,
)
```

## Lifecycle

`BinlogClient()` only allocates the native handle; call `connect()` explicitly,
then `start()` before polling. Prefer `with BinlogClient(...) as client:` so
`close()` runs on every exit path. `close()` is idempotent: it stops a pending
poll, waits for native access to finish, then disconnects and destroys the
handle. Calls to `poll()` are serialized by the binding.

## Table filtering

`CdcStream(include_tables=["mydb.audit_*"])` and the lower-level engine table
filters accept exact, case-sensitive `database.table` or bare table names. A
trailing `*` is a prefix wildcard; other `*` characters are literal. The
database filter `include_databases` has no wildcard form and compares the name
byte for byte, so list every database you want. If include filters see
TABLE_MAP events but none matches, the configured native log callback receives
one `include_filter_matched_nothing` WARN on reset or close.

## Thread Safety

`CdcEngine` instances are single-owner objects. Do not call `feed()`,
`next_event()`, `reset()`, or filter/configuration methods concurrently on the
same engine instance. Use one engine per thread/task or serialize access
externally.

`CdcStream` uses an internal reader thread through the native binlog client.
Iteration and connection lifecycle operations should be owned by one task.
`CdcStream` has no `stop` method: cancel it with `await stream.aclose()` (or
`close()`, which it forwards to) from the task that owns the stream. Only
`BinlogClient.stop()` is callable from another thread, and it is what unblocks a
pending `poll()`.

## Exports

| Export | Description |
|--------|-------------|
| `CdcEngine` | Low-level binlog byte parser |
| `BinlogClient` | MySQL binlog replication client |
| `CdcStream` | High-level async iterator (recommended) |
| `ClientConfig`, `SslMode` | Connection settings and TLS mode |
| `ChangeEvent`, `EventType`, `BinlogPosition`, `PollResult` | Event and poll payload types |
| `ServerFlavor` | Detected server flavor, as returned by `client.flavor` |
| `LogLevel`, `set_log_callback` | Structured logging API |
| `MesErrorCode` | Stable native error-code enum |
| `ParseError`, `DecodeError`, `ChecksumError` | Malformed binlog input |
| `ColumnType`, `ColumnValue` | Deprecated legacy helpers. No API returns them — `ChangeEvent` exposes columns as a plain dict — and they have no Node counterpart |

## Features

- **Native performance** — C++ core with ctypes FFI
- **Self-contained** — No libmysqlclient required; OpenSSL and zlib are statically linked into the wheel
- **Streaming** — Process events incrementally as bytes arrive
- **MySQL 8.4+** — Supports LTS and Innovation releases
- **MariaDB 10.11+** — Auto-detects flavor and handles MariaDB binlog protocol (GTID events type 162, ANNOTATE_ROWS SQL in `ChangeEvent.source_sql`, slave capability negotiation)
- **GTID support** — Native BinlogClient with GTID-based replication (MySQL `uuid:gno` and MariaDB `domain-server-seq` formats)
- **Row-level events** — Full before/after column values for INSERT, UPDATE, DELETE
- **VECTOR type** — Native support for MySQL 9.0+ VECTOR columns (decoded as raw bytes)
- **Column names** — Automatic resolution with `binlog_row_metadata=FULL` or a metadata connection that has `SELECT`; `CdcStream` opens that connection itself, `CdcEngine` takes it from `enable_metadata()`
- **SSL/TLS** — Full SSL/TLS support for secure MySQL connections
- **Backpressure** — Internal reader thread with bounded event queue (default 10,000)
- **Auto-reconnection** — Automatic reconnection with jittered linear backoff on connection loss

## Server Requirements

**MySQL:**
- Version: 8.4+
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

## License

[Apache-2.0](https://github.com/libraz/mysql-event-stream/blob/main/LICENSE)
