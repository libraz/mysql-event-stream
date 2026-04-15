// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#include "cdc_engine.h"

#include <algorithm>

#include "binary_util.h"
#include "client/metadata_fetcher.h"
#include "logger.h"
#include "rotate_event.h"

namespace mes {

static void AttachColumnNames(RowData& row, const TableMetadata& meta) {
  for (size_t i = 0; i < row.columns.size() && i < meta.columns.size(); i++) {
    row.columns[i].name = meta.columns[i].name;
  }
}

size_t CdcEngine::Feed(const uint8_t* data, size_t len) {
  size_t total_consumed = 0;

  while (total_consumed < len) {
    // Stop feeding if queue is full (backpressure)
    if (max_queue_size_ > 0 && event_queue_.size() >= max_queue_size_) {
      break;
    }

    size_t consumed = stream_parser_.Feed(data + total_consumed, len - total_consumed);
    if (consumed == 0 && !stream_parser_.HasEvent()) {
      break;
    }
    total_consumed += consumed;

    while (stream_parser_.HasEvent()) {
      // Note: the queue size check is per binlog event, not per row.
      // A single multi-row WRITE_ROWS/UPDATE_ROWS/DELETE_ROWS event may
      // push all its rows before the limit is rechecked. The queue can
      // temporarily exceed max_queue_size_ by (rows_per_event - 1) items.
      if (max_queue_size_ > 0 && event_queue_.size() >= max_queue_size_) {
        break;
      }
      const EventHeader& header = stream_parser_.CurrentHeader();
      const uint8_t* body = nullptr;
      size_t body_len = 0;
      stream_parser_.CurrentBody(&body, &body_len);
      ProcessEvent(header, body, body_len);
      stream_parser_.Advance();
    }
  }

  return total_consumed;
}

void CdcEngine::SetMaxQueueSize(size_t max_size) { max_queue_size_ = max_size; }

bool CdcEngine::NextEvent(ChangeEvent* event) {
  if (event_queue_.empty() || event == nullptr) {
    return false;
  }
  *event = std::move(event_queue_.front());
  event_queue_.pop();
  return true;
}

bool CdcEngine::HasEvents() const { return !event_queue_.empty(); }

const BinlogPosition& CdcEngine::CurrentPosition() const { return position_; }

void CdcEngine::Reset() {
  stream_parser_.Reset();
  table_registry_.Clear();
  position_ = BinlogPosition{};
  blocked_table_ids_.clear();
  last_error_ = MES_OK;
  // Clear the queue
  std::queue<ChangeEvent> empty;
  event_queue_.swap(empty);
  // NOTE(review): metadata_fetcher_ is intentionally NOT cleared. Reset()
  // is used on reconnect paths; the metadata connection is long-lived and
  // reusing it avoids a SHOW COLUMNS round-trip storm right after a
  // reconnect. The caller owns the fetcher's lifetime via
  // SetMetadataFetcher().
}

size_t CdcEngine::PendingEventCount() const { return event_queue_.size(); }

bool CdcEngine::IsError() const { return stream_parser_.GetState() == ParserState::kError; }

mes_error_t CdcEngine::ErrorCode() const {
  if (stream_parser_.GetState() != ParserState::kError) {
    return MES_OK;
  }
  // ProcessRowEvent refines last_error_ to a decode-specific code when it
  // can; otherwise the parser entered kError on its own and we report the
  // generic parse error. MES_OK is never surfaced from this branch because
  // the outer state check above already filtered non-error states.
  return last_error_ != MES_OK ? last_error_ : MES_ERR_PARSE;
}

void CdcEngine::SetIncludeDatabases(const std::vector<std::string>& databases) {
  include_databases_ = std::unordered_set<std::string>(databases.begin(), databases.end());
  blocked_table_ids_.clear();  // Force re-evaluation on next TABLE_MAP
}

void CdcEngine::SetIncludeTables(const std::vector<std::string>& tables) {
  include_tables_ = std::unordered_set<std::string>(tables.begin(), tables.end());
  blocked_table_ids_.clear();  // Force re-evaluation on next TABLE_MAP
}

void CdcEngine::SetExcludeTables(const std::vector<std::string>& tables) {
  exclude_tables_ = std::unordered_set<std::string>(tables.begin(), tables.end());
  blocked_table_ids_.clear();  // Force re-evaluation on next TABLE_MAP
}

bool CdcEngine::IsTableAllowed(const std::string& database, const std::string& table) const {
  // Check database filter
  if (!include_databases_.empty() &&
      include_databases_.find(database) == include_databases_.end()) {
    return false;
  }

  std::string qualified = database + "." + table;

  // Check exclude filter
  if (exclude_tables_.find(qualified) != exclude_tables_.end() ||
      exclude_tables_.find(table) != exclude_tables_.end()) {
    return false;
  }

  // Check include filter
  if (!include_tables_.empty()) {
    return include_tables_.find(qualified) != include_tables_.end() ||
           include_tables_.find(table) != include_tables_.end();
  }

  return true;
}

void CdcEngine::SetMetadataFetcher(MetadataFetcher* fetcher) { metadata_fetcher_ = fetcher; }

void CdcEngine::ProcessEvent(const EventHeader& header, const uint8_t* body, size_t body_len) {
  // Update position from next_position
  if (header.next_position > 0) {
    position_.offset = header.next_position;
  }

  switch (header.type_code) {
    case static_cast<uint8_t>(BinlogEventType::kTableMapEvent): {
      if (body_len < 6) break;
      uint64_t table_id = binary::ReadU48Le(body);
      if (!table_registry_.ProcessTableMapEvent(body, body_len)) {
        break;
      }
      auto* meta = table_registry_.MutableLookup(table_id);
      if (meta) {
        if (!IsTableAllowed(meta->database_name, meta->table_name)) {
          blocked_table_ids_.insert(table_id);
          break;
        }
        blocked_table_ids_.erase(table_id);
        bool needs_column_names =
            metadata_fetcher_ && !meta->columns.empty() &&
            std::any_of(meta->columns.begin(), meta->columns.end(),
                        [](const ColumnMetadata& c) { return c.name.empty(); });
        if (needs_column_names) {
          auto infos = metadata_fetcher_->FetchColumnInfo(meta->database_name, meta->table_name,
                                                          meta->columns.size());
          for (size_t i = 0; i < infos.size() && i < meta->columns.size(); i++) {
            meta->columns[i].name = infos[i].name;
            meta->columns[i].is_unsigned = infos[i].is_unsigned;
          }
        }
      }
      break;
    }

    case static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent):
    case static_cast<uint8_t>(BinlogEventType::kWriteRowsEventV1):
    case static_cast<uint8_t>(BinlogEventType::kUpdateRowsEvent):
    case static_cast<uint8_t>(BinlogEventType::kUpdateRowsEventV1):
    case static_cast<uint8_t>(BinlogEventType::kDeleteRowsEvent):
    case static_cast<uint8_t>(BinlogEventType::kDeleteRowsEventV1):
      ProcessRowEvent(header, body, body_len);
      break;

    case static_cast<uint8_t>(BinlogEventType::kRotateEvent): {
      RotateEventData rot;
      if (ParseRotateEvent(body, body_len, &rot)) {
        position_.binlog_file = std::move(rot.new_log_file);
        position_.offset = rot.position;
        StructuredLog().Event("binlog_rotate").Field("file", position_.binlog_file).Debug();
      }
      break;
    }

    // MariaDB-specific events: skip (CDC uses standard row events)
    case static_cast<uint8_t>(BinlogEventType::kMariaDBAnnotateRowsEvent):
    case static_cast<uint8_t>(BinlogEventType::kMariaDBBinlogCheckpointEvent):
    case static_cast<uint8_t>(BinlogEventType::kMariaDBGtidEvent):
    case static_cast<uint8_t>(BinlogEventType::kMariaDBGtidListEvent):
    case static_cast<uint8_t>(BinlogEventType::kMariaDBStartEncryptionEvent):
      break;

    default:
      // TODO(review): FORMAT_DESCRIPTION_EVENT is currently treated as a
      // no-op. Post-header sizes are assumed fixed for MySQL 5.6+/MariaDB
      // 10.0+ (which covers MySQL 8.4 and MariaDB 10.x, the target versions).
      // If supporting other versions, parse the post-header size array here
      // to dynamically determine event offsets.
      break;
  }
}

void CdcEngine::ProcessRowEvent(const EventHeader& header, const uint8_t* body, size_t body_len) {
  if (body == nullptr || body_len < 6) return;

  // Extract table_id from the first 6 bytes of the body
  uint64_t table_id = binary::ReadU48Le(body);
  if (blocked_table_ids_.find(table_id) != blocked_table_ids_.end()) return;
  const TableMetadata* meta = table_registry_.Lookup(table_id);
  if (meta == nullptr) {
    StructuredLog()
        .Event("rows_event_no_table_map")
        .Field("table_id", static_cast<uint64_t>(table_id))
        .Warn();
    return;
  }

  uint8_t type_code = header.type_code;
  bool is_v2 = (type_code == static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent) ||
                type_code == static_cast<uint8_t>(BinlogEventType::kUpdateRowsEvent) ||
                type_code == static_cast<uint8_t>(BinlogEventType::kDeleteRowsEvent));

  bool is_write = (type_code == static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent) ||
                   type_code == static_cast<uint8_t>(BinlogEventType::kWriteRowsEventV1));
  bool is_update = (type_code == static_cast<uint8_t>(BinlogEventType::kUpdateRowsEvent) ||
                    type_code == static_cast<uint8_t>(BinlogEventType::kUpdateRowsEventV1));

  if (is_write) {
    row_buf_.clear();
    if (DecodeWriteRows(body, body_len, *meta, is_v2, &row_buf_)) {
      for (auto& row : row_buf_) {
        ChangeEvent event;
        event.type = EventType::kInsert;
        event.database = meta->database_name;
        event.table = meta->table_name;
        event.after = std::move(row);
        AttachColumnNames(event.after, *meta);
        event.timestamp = header.timestamp;
        event.position = position_;
        event_queue_.push(std::move(event));
      }
    } else {
      LogRowDecodeFailure("write_rows", *meta);
    }
  } else if (is_update) {
    update_buf_.clear();
    if (DecodeUpdateRows(body, body_len, *meta, is_v2, &update_buf_)) {
      for (auto& pair : update_buf_) {
        ChangeEvent event;
        event.type = EventType::kUpdate;
        event.database = meta->database_name;
        event.table = meta->table_name;
        event.before = std::move(pair.before);
        event.after = std::move(pair.after);
        AttachColumnNames(event.before, *meta);
        AttachColumnNames(event.after, *meta);
        event.timestamp = header.timestamp;
        event.position = position_;
        event_queue_.push(std::move(event));
      }
    } else {
      LogRowDecodeFailure("update_rows", *meta);
    }
  } else {
    // DELETE
    row_buf_.clear();
    if (DecodeDeleteRows(body, body_len, *meta, is_v2, &row_buf_)) {
      for (auto& row : row_buf_) {
        ChangeEvent event;
        event.type = EventType::kDelete;
        event.database = meta->database_name;
        event.table = meta->table_name;
        event.before = std::move(row);
        AttachColumnNames(event.before, *meta);
        event.timestamp = header.timestamp;
        event.position = position_;
        event_queue_.push(std::move(event));
      }
    } else {
      LogRowDecodeFailure("delete_rows", *meta);
    }
  }
}

void CdcEngine::LogRowDecodeFailure(const char* kind, const TableMetadata& meta) {
  // Record a decode-specific error so ErrorCode() can distinguish from
  // pure parser errors. Row-event decode failures indicate per-row column
  // corruption, not a stream-level framing issue.
  last_error_ = MES_ERR_DECODE_ROW;
  StructuredLog()
      .Event("row_decode_failed")
      .Field("kind", kind)
      .Field("db", meta.database_name)
      .Field("table", meta.table_name)
      .Field("binlog_offset", static_cast<uint64_t>(position_.offset))
      .Warn();
}

}  // namespace mes
