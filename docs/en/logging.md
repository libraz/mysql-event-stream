# Logging

The core emits structured log records through a callback. It writes nothing to stderr on its own, so an application that installs no handler sees nothing.

Records are `key=value` pairs with the event name first:

```
event=mysql_connected host=127.0.0.1 port=3306
event=binlog_error type=event_exceeds_queue_byte_budget max_queue_bytes=50331648
```

## Levels

`ERROR` (0), `WARN` (1), `INFO` (2), `DEBUG` (3). A handler is installed with the maximum verbosity it wants, and anything more verbose is suppressed — `WARN` delivers `ERROR` and `WARN` only. The default is `WARN`.

## Installing a handler

```typescript
import { LogLevel, setLogCallback } from "@libraz/mysql-event-stream";

const levelNames = ["error", "warn", "info", "debug"];

setLogCallback((level, message) => {
  logger.log(levelNames[level] ?? "info", message);
}, LogLevel.Info);

setLogCallback(null); // remove
```

```python
from mysql_event_stream import LogLevel, set_log_callback

set_log_callback(lambda level, message: logger.info("%s", message), LogLevel.INFO)

set_log_callback(None)  # remove
```

```c
void my_log(mes_log_level_t level, const char* message, void* userdata) {
    fprintf(stderr, "[%d] %s\n", level, message);
}

mes_set_log_callback(my_log, MES_LOG_INFO, NULL);
```

## What a handler may do

The callback is process-wide, not per engine or per client — it matches the C ABI, where one callback serves the whole loaded library.

For the C ABI and Python, it can run on the native reader thread — do not call `stop()`, `close()`, `poll()` or any other client or engine operation from inside it; hand the message to your logger and return. Node marshals every record onto the JS event loop thread through a thread-safe function first, so a Node handler always runs there instead and does not need that restriction, though it should still return quickly.

An exception raised inside the callback is swallowed. A logging handler must never interrupt stream processing, so a broken handler costs log records rather than events.

## Records worth watching

- `include_filter_matched_nothing` — configured include filters saw `TABLE_MAP` events and matched none of them. Emitted once per connection window: at reset (a reconnect triggers one too) and again at stream close. See [Table filtering](filtering.md).
- `event_exceeds_queue_byte_budget` — a single event larger than `max_queue_bytes`. The connection is retired and the poll reports code 301. See [Backpressure and limits](backpressure.md).

A failure to enable the metadata connection does not travel this way: `CdcStream` reports it through `onMetadataError` / `on_metadata_error`. A lookup that fails once the connection is open does arrive here. See [Column names](column-names.md).
