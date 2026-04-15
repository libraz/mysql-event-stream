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
- **MySQL 8.4+** — Supports LTS and Innovation releases
- **MariaDB 10.11+** — Auto-detects flavor and handles MariaDB binlog protocol (GTID events type 162, ANNOTATE_ROWS, slave capability negotiation)
- **GTID support** — Native BinlogClient with GTID-based replication (MySQL `uuid:gno` and MariaDB `domain-server-seq` formats)
- **Row-level events** — Full before/after column values for INSERT, UPDATE, DELETE
- **VECTOR type** — Native support for MySQL 9.0+ VECTOR columns (decoded as raw bytes)
- **Column names** — Automatic column name resolution via metadata queries
- **SSL/TLS** — Full SSL/TLS support for secure MySQL connections
- **Backpressure** — Internal reader thread with bounded event queue (default 10,000)
- **Auto-reconnection** — Automatic reconnection with linear backoff on connection loss

## Server Requirements

**MySQL:**
- Version: 8.4+
- GTID mode enabled (for BinlogClient)
- Replication privileges: `REPLICATION SLAVE`, `REPLICATION CLIENT`

**MariaDB:**
- Version: 10.11+ (tested against 10.11 and 11.4)
- GTID replication enabled (`log_bin` in ROW format)
- Replication privileges: `REPLICATION SLAVE`, `REPLICATION CLIENT`

## License

[Apache-2.0](https://github.com/libraz/mysql-event-stream/blob/main/LICENSE)
