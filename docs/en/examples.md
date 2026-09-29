# Examples

## A resumable consumer

The shape most applications want: process an event, then persist the checkpoint, so a crash redelivers rather than skips.

```typescript
import { CdcStream } from "@libraz/mysql-event-stream";

await using stream = new CdcStream({
  host: "mysql.example.com",
  user: "replicator",
  password: process.env.MYSQL_PASSWORD,
  serverId: 1001,
  startGtid: await loadCheckpoint(),
  includeDatabases: ["shop"],
  onMetadataError: (err) => logger.warn({ err }, "column names unavailable"),
});

for await (const event of stream) {
  await handle(event); // idempotent
  await saveCheckpoint(stream.currentGtid);
}
```

```python
import os

from mysql_event_stream import CdcStream


async def run() -> None:
    async with CdcStream(
        host="mysql.example.com",
        user="replicator",
        password=os.environ["MYSQL_PASSWORD"],
        server_id=1001,
        start_gtid=await load_checkpoint(),
        include_databases=["shop"],
        on_metadata_error=lambda err: logger.warning("column names unavailable: %s", err),
    ) as stream:
        async for event in stream:
            await handle(event)  # idempotent
            await save_checkpoint(stream.current_gtid)
```

`handle` runs before the checkpoint is written, so an event is at worst processed twice. See [Checkpoints and recovery](checkpoints.md).

## Cache invalidation

A row changed, so the cache entry keyed on it is stale. Both images are available, which matters when the key itself changed.

```typescript
for await (const event of stream) {
  if (event.table !== "products") continue;

  const keys = new Set<string>();
  if (event.before) keys.add(`product:${event.before.id}`);
  if (event.after) keys.add(`product:${event.after.id}`);

  await cache.del(...keys);
  await saveCheckpoint(stream.currentGtid);
}
```

A `DELETE` has no `after`, an `INSERT` has no `before`, and an `UPDATE` that moved a row between keys has two entries to drop. Reading both images covers all three without branching on the event type.

## Filtering down to the tables that matter

A busy server publishes far more than one consumer needs. Filters drop events before they are decoded.

```typescript
const stream = new CdcStream({
  host: "mysql.example.com",
  serverId: 1002,
  includeDatabases: ["shop"],
  includeTables: ["shop.orders", "shop.order_items"],
  excludeTables: ["shop.audit_*"],
});
```

Watch the log for `include_filter_matched_nothing`: a filter matching nothing looks exactly like a quiet database. See [Table filtering](filtering.md).

## Driving the client and the engine by hand

`CdcStream` wires these two together. Doing it yourself is what you want when the raw bytes go somewhere in between — a queue, a file, another process.

```typescript
import { BinlogClient, CdcEngine } from "@libraz/mysql-event-stream";

const client = new BinlogClient({ host: "mysql.example.com", serverId: 1003 });
const engine = new CdcEngine();
engine.enableMetadata({ host: "mysql.example.com", user: "replicator", password: secret });

client.start();
try {
  for (;;) {
    const result = await client.poll();
    if (result.isHeartbeat || result.data === null) continue;

    // Frame the engine from the result, not from client.checksumEnabled: a
    // FORMAT_DESCRIPTION_EVENT moves the client's framing while events read
    // under the previous one are still queued.
    engine.setChecksumEnabled(result.checksumEnabled);
    engine.feed(result.data);

    for (let e = engine.nextEvent(); e !== null; e = engine.nextEvent()) {
      await handle(e);
    }
  }
} finally {
  client.stop();
  client.destroy();
  engine.destroy();
}
```

`poll()` returns a heartbeat when the server had nothing to send — a healthy silent interval, useful for advancing a lag metric, not an error. `stop()` is callable from another thread and is how a blocked poll is cancelled; see [Threading and lifecycle](threading.md).

## Decoding bytes that arrived some other way

The engine does not care where its bytes came from, as long as they start at an event boundary. A raw binlog file opens with a 4-byte magic number that is not itself an event, so skip it before the first `feed()`.

```python
from mysql_event_stream import CdcEngine

with CdcEngine() as engine:
    engine.set_checksum_enabled(True)  # the framing these bytes were written under

    with open("captured.binlog", "rb") as fh:
        fh.read(4)  # the file's leading magic number, not an event
        pending = b""
        while chunk := fh.read(1 << 20):
            buffer = pending + chunk
            offset = 0
            while offset < len(buffer) or engine.has_events():
                while (event := engine.next_event()) is not None:
                    print(event.type, event.database, event.table)

                if offset < len(buffer):
                    consumed = engine.feed(buffer[offset:])
                    offset += consumed

                    if consumed == 0 and not engine.has_events():
                        break
            pending = buffer[offset:]
```

The tail that `feed()` would not consume is an incomplete event; keep it and prepend it to the next chunk. Never re-feed from offset zero.

A capture that does not start at a transaction boundary has row events whose `TABLE_MAP` is missing, and those cannot be decoded at all. See [Change events](change-events.md).

## Structured logging into your own logger

```typescript
import { LogLevel, setLogCallback } from "@libraz/mysql-event-stream";

setLogCallback((level, message) => {
  logger.info({ native: message }, "mes");
}, LogLevel.Info);
```

Node marshals every record onto the JS event loop thread, so this always runs there — but still hand the message to your logger and return rather than calling back into a client or engine from it. See [Logging](logging.md).
