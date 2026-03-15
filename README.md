# mysql-event-stream

[![CI](https://img.shields.io/github/actions/workflow/status/libraz/mysql-event-stream/ci.yml?branch=main&label=CI)](https://github.com/libraz/mysql-event-stream/actions)
[![codecov](https://codecov.io/gh/libraz/mysql-event-stream/branch/main/graph/badge.svg)](https://codecov.io/gh/libraz/mysql-event-stream)
[![License](https://img.shields.io/github/license/libraz/mysql-event-stream)](https://github.com/libraz/mysql-event-stream/blob/main/LICENSE)
[![C++17](https://img.shields.io/badge/C%2B%2B-17-blue?logo=c%2B%2B)](https://en.cppreference.com/w/cpp/17)

A lightweight library that converts MySQL binlog replication events into a streaming API for applications.

Extracted from [mygram-db](https://github.com/libraz/mygram-db)'s replication layer as a standalone, embeddable CDC (Change Data Capture) engine.

## Overview

mysql-event-stream parses MySQL 8.4 binary log events and emits structured row-level change events (INSERT / UPDATE / DELETE). It provides a C ABI core with first-class bindings for Node.js and Python, making it easy to build real-time data pipelines, audit logs, cache invalidation, and event-driven architectures on top of MySQL.

## Architecture

```mermaid
graph TD
    MySQL[MySQL 8.4 Primary] -->|binlog stream / GTID| Core

    subgraph mysql-event-stream
        Core[C++ Core Library\nC ABI: libmes]
        Core -->|N-API| Node[Node.js Binding]
        Core -->|ctypes| Python[Python Binding]
    end

    Node --> App[Your Application]
    Python --> App
```

## Quick Start

### Node.js

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

### Python

```python
from mysql_event_stream import MesEngine

engine = MesEngine()

# Feed raw binlog bytes
engine.feed(binlog_chunk)

while engine.has_events():
    event = engine.next_event()
    print(event.type, event.database, event.table)
    print("before:", event.before)
    print("after:", event.after)
```

### C API

```c
#include "mes.h"

mes_engine_t* engine = mes_create();
size_t consumed;
mes_feed(engine, data, len, &consumed);

const mes_event_t* event;
while (mes_next_event(engine, &event) == MES_OK) {
    printf("%s.%s: type=%d\n", event->database, event->table, event->type);
}

mes_destroy(engine);
```

### Example Output

Each `ChangeEvent` contains the event type, database/table name, binlog position, and row data as a plain dictionary keyed by column name:

```
-- INSERT INTO items (name, value) VALUES ('Widget', 42)
{
  "type": "INSERT",
  "database": "mes_test",
  "table": "items",
  "before": null,
  "after": { "id": 8, "name": "Widget", "value": 42 },
  "timestamp": 1773584163,
  "position": { "file": "mysql-bin.000003", "offset": 3265 }
}

-- UPDATE items SET value = 100 WHERE name = 'Widget'
{
  "type": "UPDATE",
  "database": "mes_test",
  "table": "items",
  "before": { "id": 8, "name": "Widget", "value": 42 },
  "after": { "id": 8, "name": "Widget", "value": 100 },
  "timestamp": 1773584164,
  "position": { "file": "mysql-bin.000003", "offset": 3611 }
}

-- DELETE FROM items WHERE name = 'Widget'
{
  "type": "DELETE",
  "database": "mes_test",
  "table": "items",
  "before": { "id": 8, "name": "Widget", "value": 100 },
  "after": null,
  "timestamp": 1773584164,
  "position": { "file": "mysql-bin.000003", "offset": 3922 }
}
```

## Features

- **Lightweight** - Minimal dependencies, small binary size
- **Streaming** - Process events incrementally as bytes arrive
- **Multi-language** - C/C++, Node.js (N-API), and Python (ctypes) bindings
- **MySQL 8.4** - Built for the latest MySQL LTS release
- **GTID support** - Native BinlogClient with GTID-based replication
- **Row-level events** - Full before/after column values for INSERT, UPDATE, DELETE
- **Column Names** - Automatic column name resolution via metadata queries
- **Dict-based** - Row data as `Record<string, unknown>` / `dict[str, Any]` for intuitive access

## Building

### Prerequisites

- CMake 3.20+
- C++17 compiler (GCC 9+ or Clang 10+)
- Node.js 22+ and Yarn (for Node.js binding)
- Python 3.11+ (for Python binding)

### C++ Core

```bash
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --parallel
cd build && ctest --output-on-failure
```

### Node.js Binding

```bash
cd bindings/node
yarn install
yarn build
yarn test
```

### Python Binding

```bash
cd bindings/python
pip install -e ".[dev]"
pytest
```

## Project Structure

```
mysql-event-stream/
  core/                        # C++ core library
    include/mes.h              #   Public C ABI header
    src/                       #   Implementation
  bindings/
    node/                      # Node.js binding (N-API addon)
    python/                    # Python binding (ctypes)
  e2e/                         # E2E tests (pytest + Docker MySQL 8.4)
```

## Origin

This project extracts the binlog parsing and replication components from [mygram-db](https://github.com/libraz/mygram-db), an in-memory full-text search engine with MySQL replication. While mygram-db is a complete search server, mysql-event-stream focuses solely on CDC - making it easy to embed MySQL change event streaming into any application.

## Requirements

**MySQL:**
- Version: 8.4
- Binary log format: ROW (`binlog_format=ROW`)
- GTID mode enabled (for BinlogClient)
- Replication privileges: `REPLICATION SLAVE`, `REPLICATION CLIENT`

## License

[Apache License 2.0](LICENSE)

## Author

- libraz
