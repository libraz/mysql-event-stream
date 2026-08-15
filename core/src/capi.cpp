// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * @file capi.cpp
 * @brief C ABI implementation for the mysql-event-stream CDC engine
 *
 * Bridges the internal C++ CdcEngine to the public C API defined in mes.h.
 * All exported functions perform null-argument validation and translate
 * between internal C++ types and their C ABI equivalents.
 */

#include <cstdint>
#include <memory>
#include <new>
#include <string>
#include <type_traits>
#include <vector>

#include "cdc_engine.h"
#include "client/metadata_fetcher.h"
#include "logger.h"
#include "mes.h"
#include "types.h"

namespace mes {

/**
 * @brief Sole owner of the engine's metadata fetcher.
 *
 * CdcEngine holds a non-owning MetadataFetcher* and may dereference it at any
 * point at which that pointer is non-null, including from its own destructor.
 * The fetcher must therefore outlive the CdcEngine. Expressing the ownership
 * as a base class rather than a member makes that structural: a base subobject
 * is constructed before, and destroyed after, every member of the derived
 * class, whatever order the members happen to be declared in.
 */
struct MetadataFetcherOwner {
  std::unique_ptr<MetadataFetcher> metadata_fetcher;
};

}  // namespace mes

struct mes_engine : mes::MetadataFetcherOwner {
  mes::CdcEngine engine;

  // Buffers for the current event's C representation, valid until the
  // next call to mes_feed() or mes_next_event().
  mes::ChangeEvent current_event;
  mes_event_t c_event;
  std::vector<mes_column_t> before_cols;
  std::vector<mes_column_t> after_cols;
};

static_assert(std::is_base_of<mes::MetadataFetcherOwner, mes_engine>::value,
              "metadata_fetcher must stay in a base class of mes_engine so it is destroyed "
              "after CdcEngine, which points at it");

// Narrow a payload length to mes_column_t::str_len.
//
// mes.h documents a single reporting path for this boundary: the length is
// clamped to UINT32_MAX and a `column_data_truncated` WARN event is emitted.
// Every narrowing in ConvertColumn goes through here so no column type can
// truncate silently.
static uint32_t ClampPayloadLength(size_t size) {
  if (size > UINT32_MAX) {
    mes::StructuredLog()
        .Event("column_data_truncated")
        .Field("size", static_cast<uint64_t>(size))
        .Warn();
    return UINT32_MAX;
  }
  return static_cast<uint32_t>(size);
}

// Convert a single internal ColumnValue to its C ABI representation.
static mes_column_t ConvertColumn(const mes::ColumnValue& col) {
  mes_column_t c{};
  // Set col_name before the is_null early return. The mes.h contract states
  // col_name is never NULL ("" if unknown), so it must be set for all columns
  // including NULL-valued ones.
  c.col_name = col.name.empty() ? "" : col.name.data();
  if (col.is_null) {
    c.type = MES_COL_NULL;
    return c;
  }
  if (col.is_binary) {
    c.type = MES_COL_BYTES;
    c.str_data = reinterpret_cast<const char*>(col.bytes_data());
    c.str_len = ClampPayloadLength(col.bytes_size());
    return c;
  }
  switch (col.type) {
    case mes::ColumnType::kTiny:
    case mes::ColumnType::kShort:
    case mes::ColumnType::kLong:
    case mes::ColumnType::kLongLong:
    case mes::ColumnType::kInt24:
    case mes::ColumnType::kYear:
    case mes::ColumnType::kBit:
    case mes::ColumnType::kTimestamp:
    case mes::ColumnType::kEnum:
    case mes::ColumnType::kSet:
      // An UNSIGNED BIGINT whose value exceeds INT64_MAX is decoded into
      // string_val (a decimal string) to avoid signed overflow; int_val is 0
      // in that case. Surface it as a string so the binding does not report a
      // wrong 0. Normal integers leave string_val empty.
      if (!col.string_val.empty()) {
        c.type = MES_COL_STRING;
        c.str_data = col.string_val.c_str();
        c.str_len = ClampPayloadLength(col.string_val.size());
      } else {
        c.type = MES_COL_INT;
        c.int_val = col.int_val;
      }
      break;
    case mes::ColumnType::kFloat:
    case mes::ColumnType::kDouble:
      c.type = MES_COL_DOUBLE;
      c.double_val = col.real_val;
      break;
    case mes::ColumnType::kJson:
    case mes::ColumnType::kTypedArray:
    case mes::ColumnType::kGeometry:
    case mes::ColumnType::kVector:
      // BLOB/JSON/GEOMETRY payloads live in string_val alongside text
      // columns; see ColumnValue documentation. bytes_data()/bytes_size()
      // are thin wrappers that return the same pointer/length.
      c.type = MES_COL_BYTES;
      c.str_data = reinterpret_cast<const char*>(col.bytes_data());
      c.str_len = ClampPayloadLength(col.bytes_size());
      break;
    default:
      // All remaining types use string representation:
      // kVarchar, kVarString, kString, kDate, kTime, kDatetime,
      // kDatetime2, kTimestamp2, kTime2, kNewDecimal
      c.type = MES_COL_STRING;
      c.str_data = col.string_val.c_str();
      c.str_len = ClampPayloadLength(col.string_val.size());
      break;
  }
  return c;
}

// Map internal EventType to C ABI event type.
//
// Note: mes::EventType currently has exactly three values (see
// types.h). The switch below is exhaustive for the current enum. The
// fallback path exists only to satisfy compilers that do not treat the
// switch as total when the argument is an enum class; it logs at ERROR
// (not WARN) because reaching it implies an engine state corruption or a
// forward-incompatible enum addition that was not reflected here.
static mes_event_type_t ConvertEventType(mes::EventType t) {
  switch (t) {
    case mes::EventType::kInsert:
      return MES_EVENT_INSERT;
    case mes::EventType::kUpdate:
      return MES_EVENT_UPDATE;
    case mes::EventType::kDelete:
      return MES_EVENT_DELETE;
  }
  mes::StructuredLog()
      .Event("capi_unknown_event_type")
      .Field("value", static_cast<uint64_t>(static_cast<uint8_t>(t)))
      .Error();
  return MES_EVENT_INSERT;
}

// Copy a C string array into a filter list. Rejects a NULL element instead of
// skipping it: a partially collected list would silently narrow (or, for an
// all-NULL array, silently clear) the filter the caller asked for. Nothing is
// installed until the whole array validates, so a rejected call leaves the
// engine's current filter untouched.
static mes_error_t CollectFilterEntries(const char** values, size_t count,
                                        std::vector<std::string>* out) {
  if (values == nullptr && count > 0) {
    return MES_ERR_NULL_ARG;
  }
  out->reserve(count);
  for (size_t i = 0; i < count; i++) {
    if (values[i] == nullptr) {
      return MES_ERR_NULL_ARG;
    }
    out->emplace_back(values[i]);
  }
  return MES_OK;
}

namespace mes {

/**
 * @brief Install a metadata fetcher that has never been connected.
 *
 * mes_engine_set_metadata_conn() only publishes a fetcher after a successful
 * server handshake, so this is the only way to bring an engine into the
 * "metadata enabled" shape without a live server. Declared by the C ABI tests,
 * never by mes.h.
 */
void CapiInstallUnconnectedMetadataFetcher(mes_engine_t* engine) {
  engine->engine.SetMetadataFetcher(nullptr);
  engine->metadata_fetcher = std::make_unique<MetadataFetcher>();
  engine->engine.SetMetadataFetcher(engine->metadata_fetcher.get());
}

}  // namespace mes

/* ---- Exported C ABI functions ---- */

extern "C" {

/* Composed from the header macros so the reported version cannot drift from
 * the one callers compile against. CheckVersionConsistency.cmake ties those
 * macros to the CMake project version and to both binding manifests. */
#define MES_STRINGIFY_EXPANDED(value) #value
#define MES_STRINGIFY(value) MES_STRINGIFY_EXPANDED(value)

#define MES_VERSION_STRING         \
  MES_STRINGIFY(MES_VERSION_MAJOR) \
  "." MES_STRINGIFY(MES_VERSION_MINOR) "." MES_STRINGIFY(MES_VERSION_PATCH)

MES_API const char* mes_version(void) { return MES_VERSION_STRING; }

MES_API uint32_t mes_abi_version(void) { return MES_ABI_VERSION; }

MES_API const char* mes_error_string(mes_error_t error) {
  switch (error) {
    case MES_OK:
      return "success";
    case MES_ERR_NULL_ARG:
      return "null argument";
    case MES_ERR_INVALID_ARG:
      return "invalid argument";
    case MES_ERR_INTERNAL:
      return "internal error";
    case MES_ERR_PARSE:
      return "parse error";
    case MES_ERR_CHECKSUM:
      return "checksum mismatch";
    case MES_ERR_DECODE:
      return "decode error";
    case MES_ERR_DECODE_COLUMN:
      return "column decode error";
    case MES_ERR_DECODE_ROW:
      return "row decode error";
    case MES_ERR_NO_EVENT:
      return "no event available";
    case MES_ERR_QUEUE_FULL:
      return "queue full";
    case MES_ERR_CONNECT:
      return "connection error";
    case MES_ERR_AUTH:
      return "authentication error";
    case MES_ERR_VALIDATION:
      return "validation error";
    case MES_ERR_STREAM:
      return "stream error";
    case MES_ERR_DISCONNECTED:
      return "disconnected";
    case MES_ERR_GTID_PURGED:
      return "requested GTID position has been purged";
    case MES_ERR_GTID_TAGGED_UNSUPPORTED:
      return "legacy tagged GTID error";
  }
  return "unknown error";
}

/* ---- Engine lifecycle ---- */

MES_API mes_engine_t* mes_create(void) { return new (std::nothrow) mes_engine_t(); }

MES_API void mes_destroy(mes_engine_t* engine) { delete engine; }

/* ---- Data processing ---- */

MES_API mes_error_t mes_feed(mes_engine_t* engine, const uint8_t* data, size_t len,
                             size_t* consumed) {
  if (engine == nullptr || consumed == nullptr) {
    return MES_ERR_NULL_ARG;
  }
  if (data == nullptr && len > 0) {
    return MES_ERR_NULL_ARG;
  }
  *consumed = engine->engine.Feed(data, len);
  if (engine->engine.IsError()) {
    // Intentionally reset consumed to 0: the parse state is undefined once an
    // event fails, so no prefix of this buffer can be reported as safely
    // consumed. Per the mes_feed() contract in mes.h the only valid next
    // operation is mes_reset(), after which the events decoded before the
    // failure must be drained via mes_next_event() and the stream resumed from
    // a known binlog position. Re-feeding these bytes, or the ones that follow
    // them, without a reset is unsupported.
    *consumed = 0;
    return engine->engine.ErrorCode();
  }
  return MES_OK;
}

MES_API mes_error_t mes_next_event(mes_engine_t* engine, const mes_event_t** event) {
  if (engine == nullptr || event == nullptr) {
    return MES_ERR_NULL_ARG;
  }
  if (!engine->engine.NextEvent(&engine->current_event)) {
    return MES_ERR_NO_EVENT;
  }

  // Convert before columns
  engine->before_cols.clear();
  for (const auto& col : engine->current_event.before.columns) {
    engine->before_cols.push_back(ConvertColumn(col));
  }

  // Convert after columns
  engine->after_cols.clear();
  for (const auto& col : engine->current_event.after.columns) {
    engine->after_cols.push_back(ConvertColumn(col));
  }

  // Populate the C event struct
  mes_event_t& ce = engine->c_event;
  ce.type = ConvertEventType(engine->current_event.type);
  ce.database = engine->current_event.database.c_str();
  ce.table = engine->current_event.table.c_str();
  ce.before_columns = engine->before_cols.empty() ? nullptr : engine->before_cols.data();
  ce.before_count = static_cast<uint32_t>(engine->before_cols.size());
  ce.after_columns = engine->after_cols.empty() ? nullptr : engine->after_cols.data();
  ce.after_count = static_cast<uint32_t>(engine->after_cols.size());
  ce.timestamp = engine->current_event.timestamp;
  ce.binlog_file = engine->current_event.position.binlog_file.c_str();
  ce.binlog_offset = engine->current_event.position.offset;
  ce.names_resolved = engine->current_event.names_resolved ? 1 : 0;
  ce.source_sql = engine->current_event.SourceSql().c_str();

  *event = &engine->c_event;
  return MES_OK;
}

MES_API int mes_has_events(mes_engine_t* engine) {
  if (engine == nullptr) {
    return 0;
  }
  return engine->engine.HasEvents() ? 1 : 0;
}

MES_API mes_error_t mes_get_position(mes_engine_t* engine, const char** file, uint64_t* offset) {
  if (engine == nullptr) {
    return MES_ERR_NULL_ARG;
  }
  const mes::BinlogPosition& pos = engine->engine.CurrentPosition();
  if (file != nullptr) {
    *file = pos.binlog_file.c_str();
  }
  if (offset != nullptr) {
    *offset = pos.offset;
  }
  return MES_OK;
}

MES_API mes_error_t mes_set_max_queue_size(mes_engine_t* engine, size_t max_size) {
  if (engine == nullptr) return MES_ERR_NULL_ARG;
  engine->engine.SetMaxQueueSize(max_size);
  return MES_OK;
}

MES_API mes_error_t mes_reset(mes_engine_t* engine) {
  if (engine == nullptr) {
    return MES_ERR_NULL_ARG;
  }
  engine->engine.Reset();
  return MES_OK;
}

MES_API mes_error_t mes_set_max_event_size(mes_engine_t* engine, uint32_t max_event_size) {
  if (engine == nullptr) return MES_ERR_NULL_ARG;
  engine->engine.SetMaxEventSize(max_event_size);
  return MES_OK;
}

MES_API uint32_t mes_get_max_event_size(mes_engine_t* engine) {
  if (engine == nullptr) return 0;
  return engine->engine.MaxEventSize();
}

MES_API mes_error_t mes_set_checksum_enabled(mes_engine_t* engine, int enabled) {
  if (engine == nullptr) return MES_ERR_NULL_ARG;
  engine->engine.SetChecksumEnabled(enabled != 0);
  return MES_OK;
}

MES_API size_t mes_sizeof_event(void) { return sizeof(mes_event_t); }

MES_API size_t mes_sizeof_column(void) { return sizeof(mes_column_t); }

MES_API mes_error_t mes_set_include_databases(mes_engine_t* engine, const char** databases,
                                              size_t count) {
  if (engine == nullptr) return MES_ERR_NULL_ARG;
  std::vector<std::string> dbs;
  mes_error_t rc = CollectFilterEntries(databases, count, &dbs);
  if (rc != MES_OK) return rc;
  engine->engine.SetIncludeDatabases(dbs);
  return MES_OK;
}

MES_API mes_error_t mes_set_include_tables(mes_engine_t* engine, const char** tables,
                                           size_t count) {
  if (engine == nullptr) return MES_ERR_NULL_ARG;
  std::vector<std::string> tbs;
  mes_error_t rc = CollectFilterEntries(tables, count, &tbs);
  if (rc != MES_OK) return rc;
  engine->engine.SetIncludeTables(tbs);
  return MES_OK;
}

MES_API mes_error_t mes_set_exclude_tables(mes_engine_t* engine, const char** tables,
                                           size_t count) {
  if (engine == nullptr) return MES_ERR_NULL_ARG;
  std::vector<std::string> tbs;
  mes_error_t rc = CollectFilterEntries(tables, count, &tbs);
  if (rc != MES_OK) return rc;
  engine->engine.SetExcludeTables(tbs);
  return MES_OK;
}

MES_API void mes_set_log_callback(mes_log_callback_t callback, mes_log_level_t log_level,
                                  void* userdata) {
  mes::LogConfig::SetCallback(callback, log_level, userdata);
}

MES_API mes_error_t mes_engine_set_metadata_conn(mes_engine_t* engine,
                                                 const mes_client_config_t* config) {
  if (engine == nullptr || config == nullptr) {
    return MES_ERR_NULL_ARG;
  }
  if (config->ssl_mode < MES_SSL_DISABLED || config->ssl_mode > MES_SSL_VERIFY_IDENTITY) {
    return MES_ERR_INVALID_ARG;
  }
  auto fetcher = std::make_unique<mes::MetadataFetcher>();
  std::string host = config->host != nullptr ? config->host : "127.0.0.1";
  std::string user = config->user != nullptr ? config->user : "";
  std::string password = config->password != nullptr ? config->password : "";
  std::string ssl_ca = config->ssl_ca != nullptr ? config->ssl_ca : "";
  std::string ssl_cert = config->ssl_cert != nullptr ? config->ssl_cert : "";
  std::string ssl_key = config->ssl_key != nullptr ? config->ssl_key : "";
  auto rc = fetcher->Connect(host, config->port, user, password, config->connect_timeout_s,
                             config->read_timeout_s, config->ssl_mode, ssl_ca, ssl_cert, ssl_key,
                             config->allow_public_key_retrieval != 0);
  if (rc != MES_OK) {
    return rc;
  }
  // Note: the three-step swap below (null -> move -> set) is
  // intentional and MUST NOT be collapsed into
  //   engine->metadata_fetcher = std::move(fetcher);
  //   engine->engine.SetMetadataFetcher(engine->metadata_fetcher.get());
  // The unique_ptr assignment destroys the previous MetadataFetcher
  // BEFORE the new raw pointer is published, which would briefly leave
  // CdcEngine holding a dangling pointer to freed memory. Clearing the
  // engine's raw pointer first guarantees the CdcEngine never observes
  // a freed fetcher, even if a stray callback fires on the engine during
  // the swap (we do not hold any lock here; CdcEngine's single-owner
  // contract is the caller's responsibility).
  engine->engine.SetMetadataFetcher(nullptr);
  engine->metadata_fetcher = std::move(fetcher);
  engine->engine.SetMetadataFetcher(engine->metadata_fetcher.get());
  return MES_OK;
}

}  // extern "C"
