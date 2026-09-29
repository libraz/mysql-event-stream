# Getting started

## Install

```sh
npm install @libraz/mysql-event-stream
```

```sh
pip install mysql-event-stream
```

Node.js 22 or later; Python 3.11 or later. The Python package publishes platform wheels. The npm package bundles a prebuilt addon rather than selecting one through optional platform dependencies, so a runtime it was not built for needs a [build from source](#building-from-source).

A C or C++ program links the core library instead; see [Building from source](#building-from-source) and the [C API](c-api.md).

## Before the first run

The source has to be configured for row-based replication with GTIDs, and the account needs replication privileges. [Server setup](server-setup.md) has the settings, the grants, and the `serverId` rule that two processes sharing a default will violate.

## First stream

`CdcStream` connects, iterates, and cleans up with the scope.

```typescript
import { CdcStream } from "@libraz/mysql-event-stream";

await using stream = new CdcStream({
  host: "127.0.0.1",
  user: "replicator",
  password: "secret",
  serverId: 1001,
  includeDatabases: ["shop"],
});

for await (const event of stream) {
  console.log(event.type, `${event.database}.${event.table}`);
  console.log("before:", event.before);
  console.log("after:", event.after);
}
```

```python
import asyncio

from mysql_event_stream import CdcStream


async def main() -> None:
    async with CdcStream(
        host="127.0.0.1",
        user="replicator",
        password="secret",
        server_id=1001,
        include_databases=["shop"],
    ) as stream:
        async for event in stream:
            print(event.type, f"{event.database}.{event.table}")
            print("before:", event.before)
            print("after:", event.after)


asyncio.run(main())
```

An `UPDATE` on that database now prints both images of the row. `event.type` is the string `"UPDATE"` in Node; in Python it is the `EventType.UPDATE` enum member, not a string — compare it with `==`, not against `"UPDATE"`.

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

A stream opened without a start position begins at the server's current position, so changes committed before it connected are not delivered. [Checkpoints and recovery](checkpoints.md) covers resuming where the last run stopped.

If the keys come back as `"0"`, `"1"`, `"2"` instead of column names, the server is not logging column metadata and no metadata connection was available. [Column names](column-names.md) explains both routes to real names.

## Decoding bytes you already have

`CdcEngine` takes binlog bytes from anywhere, as long as they start at an event boundary — a raw binlog file opens with a 4-byte magic number that has to be skipped first. `feed()` returns how many bytes it consumed and stops early once its queue is full, so the loop drains the queue and re-feeds the unconsumed tail.

```typescript
import { CdcEngine } from "@libraz/mysql-event-stream";

const engine = new CdcEngine();
try {
  let offset = 0;
  while (offset < chunk.length || engine.hasEvents()) {
    for (let event = engine.nextEvent(); event !== null; event = engine.nextEvent()) {
      console.log(event.type, event.database, event.table);
    }

    if (offset < chunk.length) {
      const consumed = engine.feed(chunk.subarray(offset));
      offset += consumed;

      // Nothing consumed and nothing queued: the tail is a partial event.
      // Keep chunk.subarray(offset) and prepend it to the next chunk.
      if (consumed === 0 && !engine.hasEvents()) break;
    }
  }
} finally {
  engine.destroy();
}
```

```python
from mysql_event_stream import CdcEngine

with CdcEngine() as engine:
    offset = 0
    while offset < len(chunk) or engine.has_events():
        while (event := engine.next_event()) is not None:
            print(event.type, event.database, event.table)

        if offset < len(chunk):
            consumed = engine.feed(chunk[offset:])
            offset += consumed

            # Nothing consumed and nothing queued: the tail is a partial
            # event. Keep chunk[offset:] and prepend it to the next chunk.
            if consumed == 0 and not engine.has_events():
                break
```

Never re-feed from offset zero after a short feed. The engine holds the partial event it has already accepted, and replaying those bytes corrupts its state.

## Building from source

```sh
git clone https://github.com/libraz/mysql-event-stream.git
cd mysql-event-stream
```

Prerequisites: CMake 3.20 or later, a C++17 compiler (GCC 9+ or Clang 10+), and the OpenSSL and zlib development packages.

```sh
# macOS
brew install cmake openssl zlib

# Ubuntu / Debian
sudo apt install cmake build-essential libssl-dev zlib1g-dev pkg-config
```

The core builds and tests with `make`, and `make install` puts `libmes` and `mes.h` where a C or C++ project can find them.

```sh
make build
make test
sudo make install
```

Each binding builds against that core.

```sh
cd bindings/node && yarn install && yarn build && yarn test
```

```sh
cd bindings/python && rye sync && rye run pytest
```

The Python binding is managed with [Rye](https://rye.astral.sh/); `requirements.lock` and `requirements-dev.lock` are the source of truth for its development environment.

## Next

- [Change events](change-events.md) — what arrives, and what every field means.
- [The three surfaces](introduction.md#the-three-surfaces) — when `CdcStream` is not the right one.
- [Errors](errors.md) — which failures are worth a retry.
