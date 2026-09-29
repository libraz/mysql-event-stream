# Errors

Every failure carries a stable numeric code from the C ABI's `mes_error_t`. Branch on the code, not on message text.

## Code ranges

| Range | Module |
| --- | --- |
| 0 | Success |
| 1–99 | General — invalid argument, internal |
| 100–199 | Parse — header, event body, checksum |
| 200–299 | Decode — row data, column values |
| 300–399 | State — no event, queue full |
| 400–499 | Connection — connect, auth, validation, stream, disconnected |

## What to do about each

| Codes | Meaning | Retry guidance |
| --- | --- | --- |
| 1–2 | Invalid API argument | Fix the call; do not retry. |
| 100–101 | Parse or checksum failure | Reset or reconnect only after diagnosing the input. |
| 200–202 | Row decode failure | Do not retry unchanged input. |
| 301 (event queue) | An event larger than the whole queue byte budget | Raise `maxQueueBytes` / `max_queue_bytes`; do not retry unchanged input. |
| 301 (query result) | A side query — configuration validation, GTID lookup, column metadata — retained more than 100,000 rows or 64 MiB | The caps are compile-time constants and the failure closes the connection; reconnect before querying again. |
| 400 | Connection failure — the handshake, an ERR packet in place of the server's greeting, or a transport failure before authentication | Retry; usually transient. |
| 401 | Authentication failure — the server answered the credentials with an ERR packet | Fix the credentials; do not retry. |
| 402 | Server configuration validation failure — the server answered and a required setting is wrong or missing | Fix the [server configuration](server-setup.md); do not retry. A dropped connection during that same check surfaces as 403 instead, and is retryable. |
| 403–404 | Stream transport ended, including a connection dropped while `mes_client_connect()` was validating server configuration | Reconnect from the persisted [checkpoint](checkpoints.md). |
| 405 | The server confirmed the requested GTIDs were purged | Choose a new snapshot point; do not retry. A "fatal error reading binlog" the server does not attribute to a purge (a stale position, a missing log) surfaces as 403 instead. |

`CdcStream` applies this division itself: it reconnects on a transport failure and surfaces everything in the "do not retry" column immediately.

## Codes that reach no caller

Five values are exported for ABI stability and never arrive as an error.

`NoEvent` (300) is how the native layer reports an empty queue; both bindings translate it to `null` / `None` from `nextEvent()` / `next_event()`. `Internal` (99), `Decode` (200), `DecodeColumn` (201) and `GtidTaggedUnsupported` (406) have no producer in the current core — a row decode failure is reported as `DecodeRow` (202).

## Node.js

An error from the addon is a plain `Error`, `TypeError` or `RangeError` with `code` set on it, not an instance of a class this package owns. There is nothing for `instanceof` to test, so narrow a caught value with `isMesError()`:

```typescript
import { isMesError, MesErrorCode } from "@libraz/mysql-event-stream";

try {
  await handle(event);
} catch (err) {
  if (isMesError(err) && err.code === MesErrorCode.GtidPurged) {
    await takeFreshSnapshot();
  } else {
    throw err;
  }
}
```

`name` holds a category string — `MesAuthError`, `MesDecodeError`, `MesParseError`, or `MesError` for a code with no category of its own. It reads well in a log line, but `code` is the value to branch on: it mirrors `mes_error_t` exactly, while a category groups several codes under one string.

## Python

`MesError` subclasses `RuntimeError` and is the base for `ParseError`, `DecodeError` and `ChecksumError`. A failure to reach the server is a `MesConnectionError`, which subclasses the built-in `ConnectionError` and therefore `OSError`, matching what any other socket client raises.

The two hierarchies have no common base below `Exception`. `code` is the attribute they share:

```python
from mysql_event_stream import MesError, MesConnectionError, MesErrorCode

try:
    await handle(event)
except (MesError, MesConnectionError) as exc:
    if exc.code == MesErrorCode.GTID_PURGED:
        await take_fresh_snapshot()
    else:
        raise
```

An out-of-range or wrongly-typed option — an unknown config key, a negative queue size, a library that fails to load — is refused before it ever reaches the native layer, but still as a `TypeError` or `ValueError` (so `except TypeError` / `except ValueError` keep working) with `code` set to `MesErrorCode.INVALID_ARG`, the same code the C ABI itself returns for a bad argument.

Both bindings export `MesErrorCode`. In C, `mes_error_string()` returns the canonical short description for a numeric code.
