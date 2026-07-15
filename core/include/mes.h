// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * @file mes.h
 * @brief Public C ABI header for the mysql-event-stream CDC engine
 *
 * This header provides the C-compatible interface for creating and using
 * a mysql-event-stream CDC engine instance. It is safe to include from both C and C++.
 */

/**
 * @note Thread safety: CdcEngine (mes_engine_t) instances are NOT thread-safe.
 * All calls to a single engine instance must be serialized by the caller.
 * Different engine instances may be used concurrently from different threads.
 * BinlogClient (mes_client_t) is NOT thread-safe except for mes_client_stop(),
 * which may be called from any thread to interrupt a blocking mes_client_poll().
 */

#ifndef MES_H_
#define MES_H_

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ---- Visibility ---- */
#if defined(MES_BUILDING)
#if defined(_WIN32)
#define MES_API __declspec(dllexport)
#else
#define MES_API __attribute__((visibility("default")))
#endif
#else
#define MES_API
#endif

/* ---- Opaque handle ---- */
typedef struct mes_engine mes_engine_t;

/* ---- Error codes ---- */
typedef enum {
  MES_OK = 0,
  /* General errors (1-99) */
  MES_ERR_NULL_ARG = 1,
  MES_ERR_INVALID_ARG = 2,
  MES_ERR_INTERNAL = 99,
  /* Parse errors (100-199) */
  MES_ERR_PARSE = 100,
  MES_ERR_CHECKSUM = 101, /**< CRC32 checksum mismatch */
  /* Decode errors (200-299) */
  MES_ERR_DECODE = 200,        /**< General decode error */
  MES_ERR_DECODE_COLUMN = 201, /**< Column value decode error */
  MES_ERR_DECODE_ROW = 202,    /**< Row data decode error */
  /* State errors (300-399) */
  MES_ERR_NO_EVENT = 300,
  MES_ERR_QUEUE_FULL = 301,
  /* Connection errors (400-499) */
  MES_ERR_CONNECT = 400,
  MES_ERR_AUTH = 401,
  MES_ERR_VALIDATION = 402,
  MES_ERR_STREAM = 403,
  MES_ERR_DISCONNECTED = 404,
} mes_error_t;

/* ---- Log levels ---- */
typedef enum {
  MES_LOG_ERROR = 0,
  MES_LOG_WARN = 1,
  MES_LOG_INFO = 2,
  MES_LOG_DEBUG = 3,
} mes_log_level_t;

/** @brief Log callback function type.
 *  @param level Log level.
 *  @param message Log message (null-terminated).
 *  @param userdata User-provided context pointer.
 */
typedef void (*mes_log_callback_t)(mes_log_level_t level, const char* message, void* userdata);

/** @brief Set log callback and log verbosity level. Pass NULL to disable logging.
 *
 *  Messages with a level value greater than @p log_level are suppressed.
 *  For example, MES_LOG_WARN (1) shows ERROR and WARN only.
 */
MES_API void mes_set_log_callback(mes_log_callback_t callback, mes_log_level_t log_level,
                                  void* userdata);

/* ---- Event types ---- */
typedef enum {
  MES_EVENT_INSERT = 0,
  MES_EVENT_UPDATE = 1,
  MES_EVENT_DELETE = 2,
} mes_event_type_t;

/* ---- Column value types (simplified for C ABI) ---- */
typedef enum {
  MES_COL_NULL = 0,
  MES_COL_INT = 1,
  MES_COL_DOUBLE = 2,
  MES_COL_STRING = 3,
  MES_COL_BYTES = 4,
} mes_col_type_t;

/* ---- Column value ---- */
/**
 * @note str_len is intentionally 32-bit to keep the struct compact and
 *       to simplify the ctypes/N-API bindings. MySQL LONGBLOB/LONGTEXT
 *       payloads larger than 4 GiB are therefore truncated at the C ABI
 *       boundary; row_decoder asserts on payloads that exceed UINT32_MAX.
 *       This field will be widened to uint64_t in the next major release.
 */
typedef struct {
  mes_col_type_t type;
  int64_t int_val;      /**< Valid when type == MES_COL_INT */
  double double_val;    /**< Valid when type == MES_COL_DOUBLE */
  const char* str_data; /**< Valid when type == MES_COL_STRING or MES_COL_BYTES */
  uint32_t str_len;     /**< Length of str_data (truncated to 32 bits; see struct note) */
  const char* col_name; /**< Column name ("" if unknown, never NULL) */
} mes_column_t;

/* ---- Change event (read-only view into engine internals) ---- */
typedef struct {
  mes_event_type_t type;
  const char* database;
  const char* table;
  const mes_column_t* before_columns;
  uint32_t before_count;
  const mes_column_t* after_columns;
  uint32_t after_count;
  /** @brief Unix epoch seconds from the binlog event header.
   *
   * Mirrors MySQL's 4-byte `time_written` field. Will overflow on
   * 2038-01-19. Widening to uint64_t requires a major ABI version bump. */
  uint32_t timestamp;
  /** @brief Active binlog filename, or "" until the first ROTATE event is seen
   *  (ROTATE carries the filename). Until then only binlog_offset is meaningful. */
  const char* binlog_file;
  uint64_t binlog_offset;
  /** @brief 1 if column names were resolved for this event's table, 0 if not.
   *
   * When 0, column resolution was required but failed (e.g. the metadata
   * side-connection dropped or lost SELECT privilege), so every col_name in
   * before_columns / after_columns is the empty string. This lets a consumer
   * distinguish "no names available" from a genuine fetch failure rather than
   * silently receiving unnamed columns. */
  int names_resolved;
} mes_event_t;

/* ---- Engine lifecycle ---- */

/** @brief Create a new CDC engine instance. Returns NULL on allocation failure.
 *
 *  @threadsafety Thread-safe (creates an independent instance).
 */
MES_API mes_engine_t* mes_create(void);

/** @brief Destroy an engine instance and free all resources.
 *
 *  @threadsafety NOT thread-safe with respect to the instance being destroyed.
 *                Caller must ensure no other thread is using @p engine.
 */
MES_API void mes_destroy(mes_engine_t* engine);

/* ---- Data processing ---- */

/**
 * @brief Feed raw binlog bytes into the engine.
 *
 * On a partial success, @p consumed reflects the bytes accepted before the
 * stop; events already parsed from those bytes remain queued and retrievable
 * via mes_next_event(). Always re-feed from data + @p consumed, never from
 * offset 0, or already-queued change events will be delivered a second time.
 *
 * @warning Error-recovery contract: if this returns a non-OK code, the engine
 * is in an undefined parse state. The ONLY valid next operation is mes_reset()
 * (after which feeding may resume from a known binlog position). Re-feeding the
 * same or subsequent bytes without a reset is unsupported: it may duplicate
 * events, or — if no bytes are consumed and the call is retried in a loop —
 * spin in a zero-progress busy-loop. Do not retry mes_feed() on error.
 *
 * @param engine  Engine handle.
 * @param data    Pointer to binlog byte stream.
 * @param len     Number of bytes available.
 * @param consumed Output: number of bytes consumed.
 * @return MES_OK on success, error code otherwise.
 * @threadsafety NOT thread-safe. External synchronization required for all
 *               mes_engine_* calls on a given engine instance.
 */
MES_API mes_error_t mes_feed(mes_engine_t* engine, const uint8_t* data, size_t len,
                             size_t* consumed);

/**
 * @brief Get the next change event.
 *
 * Pointers in the returned event are valid until the next call to
 * mes_feed(), mes_next_event(), or mes_reset().
 *
 * @note The event's `binlog_file` is empty until the engine has seen the first
 *       ROTATE event in the fed stream, which is what carries the active binlog
 *       filename. Until then only `binlog_offset` is meaningful. When streaming
 *       via BinlogClient the server emits a ROTATE before any row events, so
 *       this is normally populated by the time row events arrive.
 *
 * @param engine Engine handle.
 * @param event  Output: pointer to the event.
 * @return MES_OK if event available, MES_ERR_NO_EVENT if empty.
 * @threadsafety NOT thread-safe.
 */
MES_API mes_error_t mes_next_event(mes_engine_t* engine, const mes_event_t** event);

/**
 * @brief Check if there are pending events.
 *
 * @param engine Engine handle.
 * @return 1 if events available, 0 otherwise (including null engine).
 * @threadsafety NOT thread-safe.
 */
MES_API int mes_has_events(mes_engine_t* engine);

/**
 * @brief Get current binlog position.
 *
 * @note The returned @p file is empty until the engine has seen the first
 *       ROTATE event in the fed stream (ROTATE is what carries the active
 *       binlog filename). Until then only @p offset is meaningful.
 *
 * @param engine Engine handle.
 * @param file   Output: binlog filename (points to internal memory).
 * @param offset Output: binlog offset.
 * @return MES_OK on success.
 * @threadsafety NOT thread-safe. The returned @p file pointer is valid until
 *               the next mutating engine call.
 */
MES_API mes_error_t mes_get_position(mes_engine_t* engine, const char** file, uint64_t* offset);

/**
 * @brief Set maximum event queue size for backpressure control.
 *
 * When the queue reaches this limit, mes_feed() will stop consuming
 * bytes early. The caller should drain events via mes_next_event()
 * then re-feed the remaining data.
 *
 * @warning The default is 0 (unlimited). With the default, feeding a large
 * binlog faster than mes_next_event() drains it lets the internal queue grow
 * without bound, which can exhaust memory. For streaming workloads with a
 * slow or bursty consumer, set a non-zero cap (e.g. 10000, matching the
 * BinlogClient default) and use the feed/drain/re-feed loop above. Because the
 * cap is rechecked per binlog event (not per row), a single multi-row event
 * may push the queue slightly past it.
 *
 * @param engine Engine handle.
 * @param max_size Maximum queue size. 0 means unlimited (the engine default).
 *        Note this differs from mes_client_config_t::max_queue_size, where 0
 *        selects the bounded default MES_DEFAULT_QUEUE_SIZE (10000) rather than
 *        unlimited: the engine is a synchronous feed/drain API the caller paces,
 *        whereas the client has an async reader thread that needs a bound.
 * @return MES_OK on success.
 * @threadsafety NOT thread-safe.
 */
MES_API mes_error_t mes_set_max_queue_size(mes_engine_t* engine, size_t max_size);

/**
 * @brief Reset the engine, clearing all state.
 *
 * @param engine Engine handle.
 * @return MES_OK on success.
 * @threadsafety NOT thread-safe.
 */
MES_API mes_error_t mes_reset(mes_engine_t* engine);

/**
 * @brief Override the maximum per-event size accepted by the engine.
 *
 * The default is 64 MiB, matching MySQL's default max_allowed_packet
 * for binlog events. Workloads with very large BLOB/JSON columns and
 * a raised max_allowed_packet on the server may need a larger ceiling.
 *
 * Values below the minimum (header + checksum) or above 1 GiB are
 * clamped to the nearest valid bound; the call always succeeds.
 *
 * @param engine Engine handle.
 * @param max_event_size Desired ceiling in bytes. 0 means "no limit",
 *                       consistent with max_queue_size == 0 elsewhere; it
 *                       resolves to the 1 GiB hard cap, which still guards
 *                       against unbounded per-event allocation. Pass a real
 *                       cap to reject oversized events sooner.
 * @return MES_OK on success, MES_ERR_NULL_ARG if @p engine is NULL.
 * @threadsafety NOT thread-safe.
 */
MES_API mes_error_t mes_set_max_event_size(mes_engine_t* engine, uint32_t max_event_size);

/**
 * @brief Get the currently configured maximum event size (bytes).
 *
 * @param engine Engine handle.
 * @return Configured ceiling, or 0 if @p engine is NULL.
 * @threadsafety NOT thread-safe.
 */
MES_API uint32_t mes_get_max_event_size(mes_engine_t* engine);

/**
 * @brief Set whether fed events carry a trailing 4-byte CRC32 checksum.
 *
 * Defaults to enabled (1), matching MySQL's default binlog_checksum=CRC32.
 * Set to 0 when feeding raw bytes from a stream produced with
 * binlog_checksum=NONE (e.g. MariaDB's historical default) when the stream
 * does not start with a FORMAT_DESCRIPTION_EVENT. When the fed stream
 * contains an FDE, the engine auto-detects the algorithm and this setting
 * is overridden.
 *
 * Misframing the checksum silently corrupts the last bytes of every event,
 * so this must match the stream.
 *
 * @param engine Engine handle.
 * @param enabled Non-zero to treat events as checksummed; 0 otherwise.
 * @return MES_OK on success, MES_ERR_NULL_ARG if @p engine is NULL.
 * @threadsafety NOT thread-safe.
 */
MES_API mes_error_t mes_set_checksum_enabled(mes_engine_t* engine, int enabled);

/* ---- ABI introspection ---- */

/**
 * @brief Size in bytes of mes_event_t as compiled into this library.
 *
 * Lets a foreign-function binding assert that its mirror of the struct matches
 * the loaded library exactly, catching ABI drift (e.g. an appended field) that
 * a loose range check would miss.
 *
 * @return sizeof(mes_event_t).
 * @threadsafety Thread-safe (pure constant).
 */
MES_API size_t mes_sizeof_event(void);

/**
 * @brief Size in bytes of mes_column_t as compiled into this library.
 *
 * @return sizeof(mes_column_t).
 * @threadsafety Thread-safe (pure constant).
 */
MES_API size_t mes_sizeof_column(void);

/* ---- Table filtering ---- */

/**
 * @brief Set database include filter. Only events from these databases are processed.
 *
 * @param engine    Engine handle.
 * @param databases Array of database name strings.
 * @param count     Number of entries in the array. 0 clears the filter (all databases).
 * @return MES_OK on success.
 */
MES_API mes_error_t mes_set_include_databases(mes_engine_t* engine, const char** databases,
                                              size_t count);

/**
 * @brief Set table include filter. Only events from these tables are processed.
 *
 * Each entry is "database.table" or just "table" (matches any database).
 *
 * @param engine Engine handle.
 * @param tables Array of table name strings.
 * @param count  Number of entries. 0 clears the filter (all tables).
 * @return MES_OK on success.
 */
MES_API mes_error_t mes_set_include_tables(mes_engine_t* engine, const char** tables, size_t count);

/**
 * @brief Set table exclude filter. Events from these tables are skipped.
 *
 * Each entry is "database.table" or just "table" (matches any database).
 *
 * @param engine Engine handle.
 * @param tables Array of table name strings.
 * @param count  Number of entries. 0 clears the filter.
 * @return MES_OK on success.
 */
MES_API mes_error_t mes_set_exclude_tables(mes_engine_t* engine, const char** tables, size_t count);

/* ---- SSL mode ---- */
/** @brief SSL connection mode.
 *
 * NOTE(abi): The underlying type of this C enum is implementation-defined.
 * All bindings assume sizeof(mes_ssl_mode_t) == sizeof(uint32_t), which
 * holds on all supported platforms (x86-64, ARM64). A future major version
 * may switch to typedef uint32_t + #define constants for strict portability.
 */
typedef enum {
  MES_SSL_DISABLED = 0,
  MES_SSL_PREFERRED = 1,
  MES_SSL_REQUIRED = 2,
  MES_SSL_VERIFY_CA = 3,
  MES_SSL_VERIFY_IDENTITY = 4,
} mes_ssl_mode_t;

/* ---- BinlogClient API ---- */

typedef struct mes_client mes_client_t;

/** @brief Default internal event queue size when max_queue_size is 0. */
#define MES_DEFAULT_QUEUE_SIZE 10000u
/** @brief Default total payload byte budget for the client event queue. */
#define MES_DEFAULT_QUEUE_BYTES (256u * 1024u * 1024u)

typedef struct {
  const char* host;
  uint16_t port;
  const char* user;
  const char* password;
  uint32_t server_id;
  const char* start_gtid; /**< Empty/NULL = snapshot current executed GTID set at stream start. */
  uint32_t connect_timeout_s;
  uint32_t read_timeout_s;
  /* SSL/TLS options */
  mes_ssl_mode_t ssl_mode; /**< SSL connection mode */
  const char* ssl_ca;      /**< Path to CA certificate file (NULL to skip) */
  const char* ssl_cert;    /**< Path to client certificate file (NULL to skip) */
  const char* ssl_key;     /**< Path to client private key file (NULL to skip) */
  /* Buffering */
  size_t max_queue_size; /**< @brief 0 = use MES_DEFAULT_QUEUE_SIZE */
  /** Allow fetching an unauthenticated RSA key for plaintext caching_sha2 auth.
   *  Disabled by default. Prefer verified TLS; opt-in remains MITM-sensitive. */
  int allow_public_key_retrieval;
} mes_client_config_t;

/**
 * @brief Result of a single poll operation.
 *
 * @note The `data` pointer is valid only until the next call to
 *       mes_client_poll(). Callers must copy the data if they need
 *       to retain it beyond that point.
 * @note `error` and `is_heartbeat` are orthogonal signals and both are
 *       retained deliberately. `error` reports a failure condition
 *       (connection loss, decode failure, etc.) whereas `is_heartbeat`
 *       indicates a healthy, silent interval from the server: the
 *       binlog dump produced no new events, so the server emitted a
 *       heartbeat instead. Callers typically use heartbeats to advance
 *       wall-clock timestamps or update lag metrics without treating
 *       the poll as a data event. A single poll result has at most one
 *       of these signals set.
 */
typedef struct {
  mes_error_t error;
  const uint8_t* data; /**< Event data, valid until next poll. NULL on error. */
  size_t size;
  int is_heartbeat; /**< 1 if this poll represents a server heartbeat (no data). */
} mes_poll_result_t;

/** @brief Create a new BinlogClient instance.
 *  @threadsafety Thread-safe (independent instance).
 */
MES_API mes_client_t* mes_client_create(void);

/** @brief Destroy a BinlogClient instance.
 *  @threadsafety Caller must ensure no other thread is using @p client.
 *                Any concurrent mes_client_poll() must have returned first;
 *                use mes_client_stop() to unblock a poll before destroying.
 */
MES_API void mes_client_destroy(mes_client_t* client);

/** @brief Connect to MySQL server with given configuration.
 *  @threadsafety Single-owner thread. Must not be called concurrently with
 *                any other mes_client_* function on the same client
 *                (except mes_client_stop()).
 */
MES_API mes_error_t mes_client_connect(mes_client_t* client, const mes_client_config_t* config);

/** @brief Start binlog streaming.
 *  @threadsafety Single-owner thread. See mes_client_connect().
 */
MES_API mes_error_t mes_client_start(mes_client_t* client);

/** @brief Poll for next binlog event (blocking).
 *  @threadsafety Single-owner thread. Only one thread may call poll on a
 *                given client at a time. mes_client_stop() may be called
 *                from a different thread to unblock this call.
 */
MES_API mes_poll_result_t mes_client_poll(mes_client_t* client);

/** @brief Synchronously stop a streaming client.
 *  @threadsafety May be called from a thread other than the poll/owner thread
 *                to unblock mes_client_poll(). The call acquires locks, shuts
 *                down the socket, and joins the reader thread, so it may block
 *                until that thread exits. It is NOT async-signal-safe and
 *                must not be called from a signal handler.
 */
MES_API void mes_client_stop(mes_client_t* client);

/** @brief Disconnect from MySQL server.
 *  @threadsafety Single-owner thread.
 */
MES_API void mes_client_disconnect(mes_client_t* client);

/** @brief Check whether the authenticated transport is still usable.
 *
 * Returns 0 after a terminal reader error, stop, or disconnect, even while a
 * queued terminal error remains available from mes_client_poll(). Use
 * mes_client_is_streaming() to decide whether Poll can still drain that state.
 *  @threadsafety Thread-safe.
 */
MES_API int mes_client_is_connected(mes_client_t* client);

/**
 * @brief Check whether mes_client_poll() can still yield data or a terminal error.
 *
 * False after stop/disconnect and after Poll consumes a terminal error. It can
 * briefly remain true with is_connected=false so the owner can drain the
 * queued error exactly once rather than busy-looping on MES_ERR_DISCONNECTED.
 * @threadsafety Thread-safe.
 */
MES_API int mes_client_is_streaming(mes_client_t* client);

/** @brief Get last error message. Returns empty string if no error.
 *  @threadsafety Single-owner thread. The returned pointer is valid until
 *                the next mes_client_* call on the same client.
 */
MES_API const char* mes_client_last_error(mes_client_t* client);

/** @brief Get the delivered, committed GTID checkpoint candidate.
 *
 *  The value advances only after a transaction commit event has been polled
 *  and the caller requests the following event. It never advances merely
 *  because the reader thread received or queued an event. This is an implicit
 *  delivery acknowledgement, not a durable/exactly-once acknowledgement;
 *  persist the checkpoint only after application processing succeeds.
 *  @note The returned pointer is valid until the next call to
 *        mes_client_current_gtid() on the same client instance, at which
 *        point the underlying buffer may be overwritten. Callers must copy
 *        the result (e.g. via strdup or std::string) if they need it to
 *        persist beyond the next call.
 *  @threadsafety Single-owner thread.
 */
MES_API const char* mes_client_current_gtid(mes_client_t* client);

/**
 * @brief Return whether the connected binlog stream carries CRC32 trailers.
 *
 * Call after mes_client_start(). Pass the result to
 * mes_set_checksum_enabled() on a raw engine consuming this client's poll
 * results. FORMAT_DESCRIPTION_EVENT can subsequently update both layers.
 *
 * @return 1 for CRC32, 0 for checksum=NONE or a NULL client.
 * @threadsafety Thread-safe.
 */
MES_API int mes_client_checksum_enabled(mes_client_t* client);

/**
 * @brief Set the maximum binlog event size accepted by the client reader.
 *
 * Keep this value aligned with mes_set_max_event_size() on the engine that
 * consumes mes_client_poll() results. The reader accounts for MySQL's one-byte
 * OK packet prefix separately, so an event exactly at the configured ceiling
 * is accepted. Values use the same normalization as the engine: 0 resolves to
 * the 1 GiB hard cap and other out-of-range values are clamped.
 *
 * Call before mes_client_start().
 * @threadsafety NOT thread-safe.
 */
MES_API mes_error_t mes_client_set_max_event_size(mes_client_t* client, uint32_t max_event_size);

/** @brief Get the client's normalized maximum binlog event size. */
MES_API uint32_t mes_client_get_max_event_size(mes_client_t* client);

/**
 * @brief Set the total payload byte budget for the client event queue.
 *
 * The producer blocks when either max_queue_size events or this many charged
 * bytes are queued. Charged bytes use the packet vector's allocated capacity
 * plus non-empty checkpoint storage. 0 restores MES_DEFAULT_QUEUE_BYTES.
 * The budget must be greater than the normalized max event size when
 * mes_client_start() is called, otherwise start returns MES_ERR_INVALID_ARG.
 *
 * Call before mes_client_start().
 * @threadsafety NOT thread-safe.
 */
MES_API mes_error_t mes_client_set_max_queue_bytes(mes_client_t* client, size_t max_queue_bytes);

/** @brief Get the configured total queue payload byte budget. */
MES_API size_t mes_client_get_max_queue_bytes(mes_client_t* client);

/**
 * @brief Get the current charged payload bytes waiting in the event queue.
 * @threadsafety Safe against the reader thread; do not race with stop/destroy.
 */
MES_API size_t mes_client_queued_bytes(mes_client_t* client);

/** @brief Enable metadata queries for column name resolution.
 *  Uses a separate MySQL connection with the same credentials. TABLE_MAP
 *  processing may synchronously execute SHOW COLUMNS; each network read is
 *  bounded by config->read_timeout_s (0 delegates to the operating system and
 *  can block indefinitely). A timeout leaves names unresolved for that event
 *  and the metadata connection is retried once with the same timeout.
 *  @threadsafety NOT thread-safe with respect to the engine instance.
 */
MES_API mes_error_t mes_engine_set_metadata_conn(mes_engine_t* engine,
                                                 const mes_client_config_t* config);

#ifdef __cplusplus
} /* extern "C" */
#endif

#endif /* MES_H_ */
