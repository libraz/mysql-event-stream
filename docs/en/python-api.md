# Python API

```sh
pip install mysql-event-stream
```

Python 3.11 or later, no runtime dependencies, typed (`py.typed`). The package publishes platform wheels containing the native library.

```python
from mysql_event_stream import (
    BinlogClient,
    BinlogPosition,
    CdcEngine,
    CdcStream,
    ChangeEvent,
    ChecksumError,
    ClientConfig,
    ColumnType,
    ColumnValue,
    DecodeError,
    EventType,
    LogLevel,
    MesConnectionError,
    MesError,
    MesErrorCode,
    ParseError,
    PollResult,
    ServerFlavor,
    SslMode,
    set_log_callback,
)
```

Every constructor takes keyword arguments only.

## CdcStream

An async iterator and an async context manager.

```python
async with CdcStream(host="...", server_id=1001) as stream:
    async for event in stream:
        ...
```

| Member | Description |
| --- | --- |
| `CdcStream(**options)` | Validates the options here rather than at first iteration. |
| `configure(**overrides)` | Replaces options before iteration starts; raises afterwards. |
| `await close()` / `await aclose()` | Interrupts the native poll, then finalizes the iterator. Idempotent. |
| `current_gtid: str` | The delivered, committed checkpoint. Survives `close()`. |

### Options

| Option | Default | Description |
| --- | --- | --- |
| `host` | `"127.0.0.1"` | |
| `port` | `3306` | |
| `user` | `"root"` | |
| `password` | `""` | |
| `server_id` | `1` | Replica identity. Must be unique per process — see [Server setup](server-setup.md#replica-identity). |
| `start_gtid` | `None` | `None` snapshots the server's current set; `""` starts from the empty set. |
| `start_binlog_file` | `None` | With `start_binlog_position`, an exact file/offset start. Cannot be combined with `start_gtid`. |
| `start_binlog_position` | `0` | 4 through `UINT32_MAX`. Requires `start_binlog_file`. |
| `connect_timeout_s` | `10` | |
| `read_timeout_s` | `30` | Bounds a single socket read, on the handshake and on the stream alike. |
| `ssl_mode` | `1` (preferred) | See [TLS and authentication](tls-and-authentication.md). |
| `ssl_ca`, `ssl_cert`, `ssl_key` | `""` | Certificate paths. An empty `ssl_ca` in a verification mode uses the OS trust store. |
| `allow_public_key_retrieval` | `False` | Opts into unauthenticated RSA key retrieval. Prefer verified TLS. |
| `max_queue_size` | `0` (10,000) | |
| `max_queue_bytes` | 48 MiB | |
| `max_event_size` | 32 MiB | `0` resolves to the 1 GiB hard cap. |
| `include_databases` | `None` | Exact, case-sensitive database names. |
| `include_tables` | `None` | `database.table`, a bare name, or a trailing `*`. |
| `exclude_tables` | `None` | Same forms; an exclude wins. |
| `max_reconnect_attempts` | `10` | `0` disables reconnection. |
| `on_metadata_error` | `None` | Called when the metadata connection fails. Unset means the failure is tolerated silently. |
| `lib_path` | `None` | Loads a specific `libmes` instead of the bundled one. |

## BinlogClient

Construction does not connect; `connect()` is explicit.

```python
with BinlogClient(host="mysql.example.com", server_id=1003) as client:
    client.connect()
    client.start()
    result = client.poll()
```

| Member | Description |
| --- | --- |
| `BinlogClient(*, config=None, **options)` | A `ClientConfig` in `config` supersedes the individual options, `lib_path` excepted. |
| `connect()` | Connects and validates the server configuration. |
| `start()` | Requests the binlog dump. |
| `poll() -> PollResult` | Blocks until an event arrives or the stream stops. One poll at a time. |
| `poll_batch(max_events=...) -> list[PollResult]` | Blocks for one event, then returns whatever else is already queued. |
| `stop()` | Callable from another thread; unblocks a pending `poll()`. |
| `disconnect()` | Closes the connection. |
| `close()` | Releases the native client. Idempotent. |

Read-only properties: `is_connected`, `is_streaming`, `current_gtid`, `last_error`, `flavor`, `checksum_enabled`, `queued_bytes`, `max_queue_bytes`, `max_event_size`, `crc_errors`.

`PollResult` carries `data: bytes | None`, `is_heartbeat: bool` and `checksum_enabled: bool`. Frame the engine from `checksum_enabled` on the result, not from `client.checksum_enabled`.

## CdcEngine

```python
with CdcEngine() as engine:
    consumed = engine.feed(chunk)
```

| Member | Description |
| --- | --- |
| `CdcEngine(lib_path=None)` | |
| `feed(data: bytes \| bytearray) -> int` | Returns bytes consumed. Stops early on a full queue. |
| `next_event() -> ChangeEvent \| None` | `None` when the queue is empty. |
| `has_events() -> bool` | |
| `get_position() -> BinlogPosition` | |
| `reset()` | Clears the buffered bytes and the `TABLE_MAP` registry. |
| `set_max_queue_size(n)`, `set_max_queue_bytes(n)`, `get_max_queue_bytes()` | |
| `set_max_event_size(n)`, `get_max_event_size()` | |
| `set_checksum_enabled(enabled)` | |
| `set_trailer_pre_verified(v)`, `get_trailer_pre_verified()` | Declares that the CRC32 was already verified upstream. Nothing checks the promise. |
| `set_include_databases(list)`, `set_include_tables(list)`, `set_exclude_tables(list)` | |
| `enable_metadata(**options)` | Opens the metadata connection for column names. |
| `close()` | Releases the native engine. Idempotent. |

The engine holds native state, so scope it with `with` rather than waiting for the garbage collector.

## ChangeEvent

A frozen dataclass: `type`, `database`, `table`, `before`, `after`, `timestamp`, `position`, `names_resolved`, `source_sql`. `before` and `after` are `dict[str, Any] | None`. See [Change events](change-events.md) and [Column values](column-values.md).

## Errors

`MesError` subclasses `RuntimeError` and is the base for `ParseError`, `DecodeError` and `ChecksumError`. A failure to reach the server is a `MesConnectionError`, which subclasses `ConnectionError` and therefore `OSError`. Both declare `code`. See [Errors](errors.md).

## Logging

```python
set_log_callback(callback, level=LogLevel.WARN, *, lib_path=None)
```

Process-wide, and the callback can run on the native reader thread. An exception raised inside it is swallowed. See [Logging](logging.md).

## Loading a specific native library

`lib_path` on `CdcEngine`, `BinlogClient` and `set_log_callback` selects a particular `libmes`. Without it, the resolver tries the `MES_LIB_PATH` environment variable, then the development build when imported from a source checkout, then the library shipped next to the package, then the system library path.

A `MES_LIB_PATH` naming a file that does not exist is an error rather than a fallback: resolving to a different library than the caller asked for would load a second image of `libmes` into the process, and two images share no state.
