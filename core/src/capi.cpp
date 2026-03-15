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

#include <new>
#include <vector>

#include "cdc_engine.h"
#include "mes.h"
#include "types.h"

#ifdef MES_HAS_MYSQL
#include <memory>

#include "client/metadata_fetcher.h"
#endif

struct mes_engine {
  mes::CdcEngine engine;

  // Buffers for the current event's C representation, valid until the
  // next call to mes_feed() or mes_next_event().
  mes::ChangeEvent current_event;
  mes_event_t c_event;
  std::vector<mes_column_t> before_cols;
  std::vector<mes_column_t> after_cols;

#ifdef MES_HAS_MYSQL
  std::unique_ptr<mes::MetadataFetcher> metadata_fetcher;
#endif
};

// Convert a single internal ColumnValue to its C ABI representation.
static mes_column_t ConvertColumn(const mes::ColumnValue& col) {
  mes_column_t c{};
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
      c.double_val = col.float_val;
      break;
    case mes::ColumnType::kBlob:
    case mes::ColumnType::kTinyBlob:
    case mes::ColumnType::kMediumBlob:
    case mes::ColumnType::kLongBlob:
    case mes::ColumnType::kGeometry:
      c.type = MES_COL_BYTES;
      c.str_data = reinterpret_cast<const char*>(col.bytes_val.data());
      c.str_len = static_cast<uint32_t>(col.bytes_val.size());
      break;
    default:
      // All remaining types use string representation:
      // kVarchar, kVarString, kString, kDate, kTime, kDatetime,
      // kDatetime2, kTimestamp2, kTime2, kNewDecimal, kJson
      c.type = MES_COL_STRING;
      c.str_data = col.string_val.c_str();
      c.str_len = static_cast<uint32_t>(col.string_val.size());
      break;
  }
  c.col_name = col.name.c_str();
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

/* ---- Engine lifecycle ---- */

mes_engine_t* mes_create(void) { return new (std::nothrow) mes_engine_t(); }

void mes_destroy(mes_engine_t* engine) { delete engine; }

/* ---- Data processing ---- */

mes_error_t mes_feed(mes_engine_t* engine, const uint8_t* data, size_t len, size_t* consumed) {
  if (engine == nullptr || consumed == nullptr) {
    return MES_ERR_NULL_ARG;
  }
  if (data == nullptr && len > 0) {
    return MES_ERR_NULL_ARG;
  }
  *consumed = engine->engine.Feed(data, len);
  return MES_OK;
}

mes_error_t mes_next_event(mes_engine_t* engine, const mes_event_t** event) {
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

int mes_has_events(mes_engine_t* engine) {
  if (engine == nullptr) {
    return -1;
  }
  return engine->engine.HasEvents() ? 1 : 0;
}

mes_error_t mes_get_position(mes_engine_t* engine, const char** file, uint64_t* offset) {
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

mes_error_t mes_reset(mes_engine_t* engine) {
  if (engine == nullptr) {
    return MES_ERR_NULL_ARG;
  }
  engine->engine.Reset();
  return MES_OK;
}

#ifdef MES_HAS_MYSQL
mes_error_t mes_engine_set_metadata_conn(mes_engine_t* engine, const mes_client_config_t* config) {
  if (engine == nullptr || config == nullptr) {
    return MES_ERR_NULL_ARG;
  }
  auto fetcher = std::make_unique<mes::MetadataFetcher>();
  std::string host = config->host != nullptr ? config->host : "127.0.0.1";
  std::string user = config->user != nullptr ? config->user : "";
  std::string password = config->password != nullptr ? config->password : "";
  auto rc = fetcher->Connect(host, config->port, user, password, config->connect_timeout_s);
  if (rc != MES_OK) {
    return rc;
  }
  engine->metadata_fetcher = std::move(fetcher);
  engine->engine.SetMetadataFetcher(engine->metadata_fetcher.get());
  return MES_OK;
}
#endif
