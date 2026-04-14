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
  uint32_t timestamp;
  const char* binlog_file;
  uint64_t binlog_offset;
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
 * mes_feed() or mes_next_event().
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
 * @param engine Engine handle.
 * @param max_size Maximum queue size. 0 means unlimited (default).
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
typedef enum {
  MES_SSL_DISABLED = 0,
  MES_SSL_PREFERRED = 1,
  MES_SSL_REQUIRED = 2,
  MES_SSL_VERIFY_CA = 3,
  MES_SSL_VERIFY_IDENTITY = 4,
} mes_ssl_mode_t;

/* ---- BinlogClient API ---- */

typedef struct mes_client mes_client_t;

typedef struct {
  const char* host;
  uint16_t port;
  const char* user;
  const char* password;
  uint32_t server_id;
  const char* start_gtid;
  uint32_t connect_timeout_s;
  uint32_t read_timeout_s;
  /* SSL/TLS options */
  mes_ssl_mode_t ssl_mode; /**< SSL connection mode */
  const char* ssl_ca;      /**< Path to CA certificate file (NULL to skip) */
  const char* ssl_cert;    /**< Path to client certificate file (NULL to skip) */
  const char* ssl_key;     /**< Path to client private key file (NULL to skip) */
  /* Buffering */
  size_t max_queue_size; /**< Max internal event queue size (0 = default 10000) */
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

/** @brief Request stop of a streaming client.
 *  @threadsafety Any-thread safe. Posts a stop signal and returns immediately;
 *                safe to invoke from a thread other than the poll/owner
 *                thread, including from a signal-safe context.
 */
MES_API void mes_client_stop(mes_client_t* client);

/** @brief Disconnect from MySQL server.
 *  @threadsafety Single-owner thread.
 */
MES_API void mes_client_disconnect(mes_client_t* client);

/** @brief Check if client is connected. Returns 1 if connected, 0 otherwise.
 *  @threadsafety Single-owner thread.
 */
MES_API int mes_client_is_connected(mes_client_t* client);

/** @brief Get last error message. Returns empty string if no error.
 *  @threadsafety Single-owner thread. The returned pointer is valid until
 *                the next mes_client_* call on the same client.
 */
MES_API const char* mes_client_last_error(mes_client_t* client);

/** @brief Get current GTID position. Returns empty string if unknown.
 *
 *  @note The returned pointer is valid until the next call to
 *        mes_client_current_gtid() on the same client instance, at which
 *        point the underlying buffer may be overwritten. Callers must copy
 *        the result (e.g. via strdup or std::string) if they need it to
 *        persist beyond the next call.
 *  @threadsafety Single-owner thread.
 */
MES_API const char* mes_client_current_gtid(mes_client_t* client);

/** @brief Enable metadata queries for column name resolution.
 *  Uses a separate MySQL connection with the same credentials.
 *  @threadsafety NOT thread-safe with respect to the engine instance.
 */
MES_API mes_error_t mes_engine_set_metadata_conn(mes_engine_t* engine,
                                                 const mes_client_config_t* config);

#ifdef __cplusplus
} /* extern "C" */
#endif

#endif /* MES_H_ */
