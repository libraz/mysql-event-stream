# C API

The published surface is `core/include/mes.h`. A C or C++ program links `libmes`; the Node and Python bindings sit on this same header.

```c
#include "mes.h"
```

## Invariants

- `mes_engine_t` and `mes_client_t` are **not thread-safe**. `mes_client_stop()` is the only entry point callable from another thread.
- An event pointer from `mes_next_event()` is valid **only until** the next `mes_feed()`, `mes_next_event()` or `mes_reset()` on that engine. `mes_client_poll()` data is valid only until the next poll. Copy anything that has to outlive the call.
- Every `const char*` the library returns is non-NULL. An unknown value is `""`.

## Version

```c
const char* mes_version(void);   /* the library's release version */
uint32_t    mes_abi_version(void);
size_t      mes_sizeof_event(void);
size_t      mes_sizeof_column(void);
```

`mes_abi_version()` reports the ABI generation. The two `sizeof` functions let a binding verify the struct sizes it was compiled against — additions are laid out so a binary built against an older header keeps working, and the ABI version is what refuses the reverse.

## Errors

```c
typedef enum {
  MES_OK = 0,
  MES_ERR_NULL_ARG = 1,
  MES_ERR_INVALID_ARG = 2,
  MES_ERR_INTERNAL = 99,
  MES_ERR_PARSE = 100,
  MES_ERR_CHECKSUM = 101,
  MES_ERR_DECODE = 200,
  MES_ERR_DECODE_COLUMN = 201,
  MES_ERR_DECODE_ROW = 202,
  MES_ERR_NO_EVENT = 300,
  MES_ERR_QUEUE_FULL = 301,
  MES_ERR_CONNECT = 400,
  MES_ERR_AUTH = 401,
  MES_ERR_VALIDATION = 402,
  MES_ERR_STREAM = 403,
  MES_ERR_DISCONNECTED = 404,
  MES_ERR_GTID_PURGED = 405,
  MES_ERR_GTID_TAGGED_UNSUPPORTED = 406,
} mes_error_t;

const char* mes_error_string(mes_error_t error);
```

[Errors](errors.md) has the retry guidance and the codes that reach no caller.

## Logging

```c
typedef enum { MES_LOG_ERROR = 0, MES_LOG_WARN = 1, MES_LOG_INFO = 2, MES_LOG_DEBUG = 3 } mes_log_level_t;

typedef void (*mes_log_callback_t)(mes_log_level_t level, const char* message, void* userdata);

void mes_set_log_callback(mes_log_callback_t callback, mes_log_level_t log_level, void* userdata);
```

The callback is process-wide and can run on the reader thread. See [Logging](logging.md).

## Events

```c
typedef enum { MES_EVENT_INSERT = 0, MES_EVENT_UPDATE = 1, MES_EVENT_DELETE = 2 } mes_event_type_t;
typedef enum { MES_COL_NULL = 0, MES_COL_INT = 1, MES_COL_DOUBLE = 2,
               MES_COL_STRING = 3, MES_COL_BYTES = 4 } mes_col_type_t;

typedef struct {
  mes_col_type_t type;
  int64_t        int_val;    /* type == MES_COL_INT */
  double         double_val; /* type == MES_COL_DOUBLE */
  const char*    str_data;   /* type == MES_COL_STRING or MES_COL_BYTES */
  uint32_t       str_len;
  const char*    col_name;   /* "" when unknown */
} mes_column_t;

typedef struct {
  mes_event_type_t   type;
  const char*        database;
  const char*        table;
  const mes_column_t* before_columns;
  uint32_t           before_count;
  const mes_column_t* after_columns;
  uint32_t           after_count;
  uint32_t           timestamp;
  const char*        binlog_file;    /* "" until the first ROTATE event */
  uint64_t           binlog_offset;  /* offset of the next event; resume from this */
  int                names_resolved;
  const char*        source_sql;     /* MariaDB ANNOTATE_ROWS, or "" */
} mes_event_t;
```

`timestamp` mirrors MySQL's 4-byte header field and overflows in 2038; widening it needs a major ABI bump. `str_len` is 32-bit at this boundary to keep the bindings simple, and the 1 GiB ceiling on a single event keeps the clamp unreachable.

## Engine

```c
mes_engine_t* mes_create(void);
void          mes_destroy(mes_engine_t* engine);

mes_error_t mes_feed(mes_engine_t* engine, const uint8_t* data, size_t len, size_t* consumed);
mes_error_t mes_next_event(mes_engine_t* engine, const mes_event_t** event);
int         mes_has_events(mes_engine_t* engine);
mes_error_t mes_get_position(mes_engine_t* engine, const char** file, uint64_t* offset);
mes_error_t mes_reset(mes_engine_t* engine);
```

`mes_feed()` stops early once the queue is full and reports what it consumed. `mes_next_event()` returns `MES_ERR_NO_EVENT` on an empty queue. `mes_reset()` clears the buffered bytes and the `TABLE_MAP` registry together.

### Limits

```c
mes_error_t mes_set_max_queue_size(mes_engine_t* engine, size_t max_size);
mes_error_t mes_set_max_queue_bytes(mes_engine_t* engine, size_t max_queue_bytes);
size_t      mes_get_max_queue_bytes(mes_engine_t* engine);
mes_error_t mes_set_max_event_size(mes_engine_t* engine, uint32_t max_event_size);
uint32_t    mes_get_max_event_size(mes_engine_t* engine);
```

`0` restores the default in each case: `MES_DEFAULT_QUEUE_SIZE` (10,000), `MES_DEFAULT_QUEUE_BYTES` (48 MiB), and the 1 GiB hard cap for the event size. See [Backpressure and limits](backpressure.md).

### Framing

```c
mes_error_t mes_set_checksum_enabled(mes_engine_t* engine, int enabled);
mes_error_t mes_set_trailer_pre_verified(mes_engine_t* engine, int pre_verified);
int         mes_get_trailer_pre_verified(mes_engine_t* engine);
```

Set `mes_set_checksum_enabled()` from the `checksum_enabled` field on the poll result that produced the bytes, not from the client's current view — a `FORMAT_DESCRIPTION_EVENT` moves that view while events read under the previous one are still queued.

`mes_set_trailer_pre_verified()` declares that something upstream has already verified each event's CRC32, so the engine does not compute it twice. Nothing verifies the promise: set it on an unvalidated stream and a corrupt event is accepted in silence.

### Filters

```c
mes_error_t mes_set_include_databases(mes_engine_t* engine, const char** databases, size_t count);
mes_error_t mes_set_include_tables(mes_engine_t* engine, const char** tables, size_t count);
mes_error_t mes_set_exclude_tables(mes_engine_t* engine, const char** tables, size_t count);
```

See [Table filtering](filtering.md).

### Column names

```c
mes_error_t mes_engine_set_metadata_conn(mes_engine_t* engine, const mes_client_config_t* config);
```

Opens a second connection with the given credentials and resolves column names through `SHOW COLUMNS` during `TABLE_MAP` processing. Resolved and failed lookups share an 8,192-table cache, cleared as a whole on overflow. See [Column names](column-names.md).

## Client

```c
typedef enum { MES_SSL_DISABLED = 0, MES_SSL_PREFERRED = 1, MES_SSL_REQUIRED = 2,
               MES_SSL_VERIFY_CA = 3, MES_SSL_VERIFY_IDENTITY = 4 } mes_ssl_mode_t;
typedef enum { MES_SERVER_FLAVOR_MYSQL = 0, MES_SERVER_FLAVOR_MARIADB = 1 } mes_server_flavor_t;
typedef enum { MES_START_AT_CURRENT = 0, MES_START_AT_GTID = 1,
               MES_START_AT_POSITION = 2 } mes_start_position_mode_t;
```

`mes_client_config_t` carries `host`, `port`, `user`, `password`, `server_id`, `start_gtid`, `connect_timeout_s`, `read_timeout_s`, the four TLS fields, `max_queue_size`, `allow_public_key_retrieval`, `start_position_mode`, `binlog_file` and `binlog_position`. A zero-initialized config means the conventional defaults: TLS disabled, and a start at the server's current position.

```c
mes_client_t* mes_client_create(void);
void          mes_client_destroy(mes_client_t* client);

mes_error_t mes_client_connect(mes_client_t* client, const mes_client_config_t* config);
mes_error_t mes_client_start(mes_client_t* client);
void        mes_client_stop(mes_client_t* client);      /* callable from another thread */
void        mes_client_disconnect(mes_client_t* client);
```

`mes_client_connect()` validates the server's configuration and fails with `MES_ERR_VALIDATION` when a required setting is wrong. Destruction requests a stop and waits for an in-flight poll to finish.

### Polling

```c
typedef struct {
  mes_error_t    error;
  const uint8_t* data;            /* valid until the next poll; NULL on error */
  size_t         size;
  int            is_heartbeat;
  int            checksum_enabled;
} mes_poll_result_t;

mes_poll_result_t mes_client_poll(mes_client_t* client);
mes_error_t       mes_client_poll_batch(mes_client_t* client, mes_poll_result_t* results,
                                        size_t max_results, size_t* count);
```

`error` and `is_heartbeat` are orthogonal, and a single result has at most one of them set. A heartbeat is a healthy silent interval: the dump produced nothing, so the server said so. `checksum_enabled` is meaningful only while `data` is non-NULL.

### Introspection

```c
int                 mes_client_is_connected(mes_client_t* client);
int                 mes_client_is_streaming(mes_client_t* client);
mes_server_flavor_t mes_client_flavor(mes_client_t* client);
const char*         mes_client_last_error(mes_client_t* client);
const char*         mes_client_current_gtid(mes_client_t* client);
int                 mes_client_checksum_enabled(mes_client_t* client);
size_t              mes_client_queued_bytes(mes_client_t* client);
uint64_t            mes_client_crc_errors(mes_client_t* client);

mes_error_t mes_client_set_max_event_size(mes_client_t* client, uint32_t max_event_size);
uint32_t    mes_client_get_max_event_size(mes_client_t* client);
mes_error_t mes_client_set_max_queue_bytes(mes_client_t* client, size_t max_queue_bytes);
size_t      mes_client_get_max_queue_bytes(mes_client_t* client);
```

`mes_client_current_gtid()` is the checkpoint to persist; see [Checkpoints and recovery](checkpoints.md).

## A feed loop

```c
mes_engine_t* engine = mes_create();
size_t offset = 0;
while (offset < len) {
    size_t consumed = 0;
    if (mes_feed(engine, data + offset, len - offset, &consumed) != MES_OK) {
        /* call mes_reset(), then drain the events already decoded */
        break;
    }
    offset += consumed;

    const mes_event_t* event;
    while (mes_next_event(engine, &event) == MES_OK) {
        printf("%s.%s type=%d\n", event->database, event->table, event->type);
    }

    /* Nothing consumed and nothing left to drain: the tail is a partial event.
       Keep data + offset through data + len and re-feed it with the next
       chunk. Never re-feed from offset 0. */
    if (consumed == 0) break;
}
mes_destroy(engine);
```
