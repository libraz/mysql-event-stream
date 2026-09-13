# mysql-event-stream

An embeddable CDC engine for MySQL 8.4+ and MariaDB 10.11+, as a Python package over a `ctypes` FFI. It reads the binary log as a replica would and emits row-level change events. No libmysqlclient: the wire protocol is implemented directly in the bundled C++ core.

[![CI](https://img.shields.io/github/actions/workflow/status/libraz/mysql-event-stream/ci.yml?branch=main&label=CI)](https://github.com/libraz/mysql-event-stream/actions)
[![PyPI](https://img.shields.io/pypi/v/mysql-event-stream?logo=python)](https://pypi.org/project/mysql-event-stream/)
[![npm](https://img.shields.io/npm/v/@libraz/mysql-event-stream?logo=npm)](https://www.npmjs.com/package/@libraz/mysql-event-stream)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue)](https://github.com/libraz/mysql-event-stream/blob/main/LICENSE)
[![Python](https://img.shields.io/badge/python-%E2%89%A53.11-blue?logo=python)](https://python.org/)
[![MySQL](https://img.shields.io/badge/MySQL-8.4%2B-blue?logo=mysql)](https://dev.mysql.com/)
[![MariaDB](https://img.shields.io/badge/MariaDB-10.11%2B-003545?logo=mariadb)](https://mariadb.org/)
[![Platform](https://img.shields.io/badge/platform-Linux%20%7C%20macOS-lightgrey)](https://github.com/libraz/mysql-event-stream)

## Install

```bash
pip install mysql-event-stream
```

Python 3.11 or later, no runtime dependencies, typed. Platform wheels are published for Linux x86_64 and aarch64 (recommended for servers) and macOS 15.0 or newer on x86_64 and arm64.

## Usage

```python
import os

from mysql_event_stream import CdcStream


async def run() -> None:
    async with CdcStream(
        host="mysql.example.com",
        user="replicator",
        password=os.environ["MYSQL_PASSWORD"],
        server_id=1001,
        include_databases=["shop"],
        start_gtid=await load_checkpoint(),
    ) as stream:
        async for event in stream:
            print(event.type, f"{event.database}.{event.table}")
            await handle(event)  # idempotent
            await save_checkpoint(stream.current_gtid)
```

An `UPDATE` arrives with both images of the row:

```python
ChangeEvent(
    type="UPDATE",
    database="shop",
    table="items",
    before={"id": 8, "name": "Widget", "value": 42},
    after={"id": 8, "name": "Widget", "value": 100},
    timestamp=1773584164,
    position=BinlogPosition(file="mysql-bin.000003", offset=3611),
    names_resolved=True,
    source_sql="",
)
```

`CdcStream` is the surface most applications want. Under it, `BinlogClient` owns the connection and hands back raw event bytes, and `CdcEngine` decodes binlog bytes from any source with no socket and no thread of its own:

```python
from mysql_event_stream import CdcEngine

# The engine holds native state, so scope it rather than waiting for the
# garbage collector to release it.
with CdcEngine() as engine:
    offset = 0
    while offset < len(chunk):
        consumed = engine.feed(chunk[offset:])
        offset += consumed

        while (event := engine.next_event()) is not None:
            print(event.type, event.database, event.table)

        # Nothing consumed and nothing left to drain: the tail is a partial
        # event. Keep chunk[offset:] and prepend it to the next chunk.
        if consumed == 0:
            break
```

## Server requirements

MySQL 8.4+ or MariaDB 10.11+, logging in row format with `binlog_row_image=FULL` and GTIDs enabled, and an account with `REPLICATION SLAVE` and `REPLICATION CLIENT`. Column names need either `binlog_row_metadata=FULL` or a `SELECT` grant on the streamed tables.

[Server setup](https://github.com/libraz/mysql-event-stream/blob/main/docs/en/server-setup.md) has the full configuration, the grants, and the replica identity rule that two processes on defaults will violate.

## Documentation

- [Python API](https://github.com/libraz/mysql-event-stream/blob/main/docs/en/python-api.md) — every class, option and default.
- [Getting started](https://github.com/libraz/mysql-event-stream/blob/main/docs/en/getting-started.md) and [Examples](https://github.com/libraz/mysql-event-stream/blob/main/docs/en/examples.md).
- [Change events](https://github.com/libraz/mysql-event-stream/blob/main/docs/en/change-events.md) and [Column values](https://github.com/libraz/mysql-event-stream/blob/main/docs/en/column-values.md) — what arrives, and as what type.
- [Checkpoints and recovery](https://github.com/libraz/mysql-event-stream/blob/main/docs/en/checkpoints.md), [Errors](https://github.com/libraz/mysql-event-stream/blob/main/docs/en/errors.md), [Threading and lifecycle](https://github.com/libraz/mysql-event-stream/blob/main/docs/en/threading.md).
- [Introduction](https://github.com/libraz/mysql-event-stream/blob/main/docs/en/introduction.md) indexes the rest.

## Development

The binding is managed with [Rye](https://rye.astral.sh/); `requirements.lock` and `requirements-dev.lock` are the source of truth for the development environment.

```bash
cd bindings/python
rye sync
rye run pytest
```

`MES_LIB_PATH` selects a specific `libmes`, which is what a source checkout needs when several builds are present.

## Also available

```bash
npm install @libraz/mysql-event-stream  # Node.js binding
```

## License

[Apache-2.0](https://github.com/libraz/mysql-event-stream/blob/main/LICENSE)
