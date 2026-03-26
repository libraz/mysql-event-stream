# mysql-event-stream — Python Binding

[![CI](https://img.shields.io/github/actions/workflow/status/libraz/mysql-event-stream/ci.yml?branch=main&label=CI)](https://github.com/libraz/mysql-event-stream/actions)
[![License](https://img.shields.io/github/license/libraz/mysql-event-stream)](https://github.com/libraz/mysql-event-stream/blob/main/LICENSE)

A lightweight MySQL 8.4 CDC (Change Data Capture) engine for Python. Parses binlog replication streams and emits structured row-level change events (INSERT / UPDATE / DELETE).

Built on a self-contained C++ core using ctypes FFI for high throughput and low latency. No external MySQL client library (libmysqlclient) required.

## Install

```bash
pip install mysql-event-stream
```

Platform wheels are available for:
- Linux x86_64
- Linux aarch64
- macOS ARM64 (Apple Silicon)

## Usage

### Parsing binlog bytes

```python
from mysql_event_stream import CdcEngine

engine = CdcEngine()

# Feed raw binlog bytes
engine.feed(binlog_chunk)

while (event := engine.next_event()) is not None:
    print(event.type, event.database, event.table)
    print("before:", event.before)
    print("after:", event.after)
```

### Streaming from MySQL

```python
import asyncio
from mysql_event_stream import CdcStream

async def main():
    async for event in CdcStream(
        host="127.0.0.1",
        port=3306,
        user="replicator",
        password="secret",
    ):
        print(f"{event.type.name} {event.database}.{event.table}")
        print(f"  before: {event.before}")
        print(f"  after:  {event.after}")

asyncio.run(main())
```

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
)
```

## Features

- **Native performance** — C++ core with ctypes FFI, >100k events/sec
- **Zero native dependencies** — No libmysqlclient required; only OpenSSL
- **Streaming** — Process events incrementally as bytes arrive
- **MySQL 8.4** — Built for the latest MySQL LTS release
- **GTID support** — Native BinlogClient with GTID-based replication
- **Row-level events** — Full before/after column values for INSERT, UPDATE, DELETE
- **Column names** — Automatic column name resolution via metadata queries
- **SSL/TLS** — Full SSL/TLS support for secure MySQL connections
- **Backpressure** — Internal reader thread with bounded event queue (default 10,000)
- **Auto-reconnection** — Automatic reconnection with linear backoff on connection loss

## MySQL Requirements

- Version: 8.4
- Binary log format: ROW (`binlog_format=ROW`)
- GTID mode enabled (for BinlogClient)
- Replication privileges: `REPLICATION SLAVE`, `REPLICATION CLIENT`

## License

[Apache-2.0](https://github.com/libraz/mysql-event-stream/blob/main/LICENSE)
