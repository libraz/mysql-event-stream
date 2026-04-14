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

#include <memory>
#include <new>
#include <vector>

#include "cdc_engine.h"
#include "client/metadata_fetcher.h"
#include "logger.h"
#include "mes.h"
#include "types.h"

struct mes_engine {
  mes::CdcEngine engine;

  // Buffers for the current event's C representation, valid until the
  // next call to mes_feed() or mes_next_event().
  mes::ChangeEvent current_event;
  mes_event_t c_event;
  std::vector<mes_column_t> before_cols;
  std::vector<mes_column_t> after_cols;

  std::unique_ptr<mes::MetadataFetcher> metadata_fetcher;
};

// Convert a single internal ColumnValue to its C ABI representation.
static mes_column_t ConvertColumn(const mes::ColumnValue& col) {
  mes_column_t c{};
  // Set col_name before the is_null early return. The mes.h contract states
  // col_name is never NULL ("" if unknown), so it must be set for all columns
  // including NULL-valued ones.
  c.col_name = col.name.c_str();
  if (col.is_null) {
    c.type = MES_COL_NULL;
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
      c.type = MES_COL_INT;
      c.int_val = col.int_val;
      break;
    case mes::ColumnType::kFloat:
    case mes::ColumnType::kDouble:
      c.type = MES_COL_DOUBLE;
      c.double_val = col.real_val;
      break;
    case mes::ColumnType::kJson:
    case mes::ColumnType::kBlob:
    case mes::ColumnType::kTinyBlob:
    case mes::ColumnType::kMediumBlob:
    case mes::ColumnType::kLongBlob:
    case mes::ColumnType::kGeometry:
    case mes::ColumnType::kVector:
      c.type = MES_COL_BYTES;
      c.str_data = reinterpret_cast<const char*>(col.bytes_val.data());
      c.str_len = static_cast<uint32_t>(col.bytes_val.size());
      break;
    default:
      // All remaining types use string representation:
      // kVarchar, kVarString, kString, kDate, kTime, kDatetime,
      // kDatetime2, kTimestamp2, kTime2, kNewDecimal
      c.type = MES_COL_STRING;
      c.str_data = col.string_val.c_str();
      c.str_len = static_cast<uint32_t>(col.string_val.size());
      break;
  }
  return c;
}

// Map internal EventType to C ABI event type.
static mes_event_type_t ConvertEventType(mes::EventType t) {
  switch (t) {
    case mes::EventType::kInsert:
      return MES_EVENT_INSERT;
    case mes::EventType::kUpdate:
      return MES_EVENT_UPDATE;
    case mes::EventType::kDelete:
      return MES_EVENT_DELETE;
  }
  return MES_EVENT_INSERT;
}

/* ---- Exported C ABI functions ---- */

extern "C" {

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
    // Intentionally reset consumed to 0: the engine's internal state is now
    // corrupt and must be reset via mes_reset(). By reporting 0 consumed,
    // the caller keeps their full input buffer intact. After mes_reset()
    // clears all internal state (including any buffered bytes), the caller
    // can re-feed from the start or seek to a known-good stream position.
    *consumed = 0;
    return MES_ERR_PARSE;
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

MES_API mes_error_t mes_set_include_databases(mes_engine_t* engine, const char** databases,
                                              size_t count) {
  if (engine == nullptr) return MES_ERR_NULL_ARG;
  std::vector<std::string> dbs;
  for (size_t i = 0; i < count; i++) {
    if (databases[i]) dbs.emplace_back(databases[i]);
  }
  engine->engine.SetIncludeDatabases(dbs);
  return MES_OK;
}

MES_API mes_error_t mes_set_include_tables(mes_engine_t* engine, const char** tables,
                                           size_t count) {
  if (engine == nullptr) return MES_ERR_NULL_ARG;
  std::vector<std::string> tbs;
  for (size_t i = 0; i < count; i++) {
    if (tables[i]) tbs.emplace_back(tables[i]);
  }
  engine->engine.SetIncludeTables(tbs);
  return MES_OK;
}

MES_API mes_error_t mes_set_exclude_tables(mes_engine_t* engine, const char** tables,
                                           size_t count) {
  if (engine == nullptr) return MES_ERR_NULL_ARG;
  std::vector<std::string> tbs;
  for (size_t i = 0; i < count; i++) {
    if (tables[i]) tbs.emplace_back(tables[i]);
  }
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
  auto fetcher = std::make_unique<mes::MetadataFetcher>();
  std::string host = config->host != nullptr ? config->host : "127.0.0.1";
  std::string user = config->user != nullptr ? config->user : "";
  std::string password = config->password != nullptr ? config->password : "";
  std::string ssl_ca = config->ssl_ca != nullptr ? config->ssl_ca : "";
  std::string ssl_cert = config->ssl_cert != nullptr ? config->ssl_cert : "";
  std::string ssl_key = config->ssl_key != nullptr ? config->ssl_key : "";
  auto rc = fetcher->Connect(host, config->port, user, password, config->connect_timeout_s,
                             config->ssl_mode, ssl_ca, ssl_cert, ssl_key);
  if (rc != MES_OK) {
    return rc;
  }
  // Clear the engine's pointer before replacing the unique_ptr to avoid a
  // dangling pointer if the old MetadataFetcher is destroyed first.
  engine->engine.SetMetadataFetcher(nullptr);
  engine->metadata_fetcher = std::move(fetcher);
  engine->engine.SetMetadataFetcher(engine->metadata_fetcher.get());
  return MES_OK;
}

}  // extern "C"
