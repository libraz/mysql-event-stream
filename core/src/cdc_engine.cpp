// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#include "cdc_engine.h"

#include <algorithm>
#include <array>
#include <cctype>
#include <cstring>
#include <string_view>

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

namespace {

bool IsSqlWhitespace(char ch) {
  return ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r' || ch == '\v' || ch == '\f';
}

/** Skip MySQL leading whitespace/comments and expose executable version comments. */
bool SkipLeadingSqlTrivia(const char* stmt, size_t stmt_len, size_t* pos) {
  for (;;) {
    while (*pos < stmt_len && IsSqlWhitespace(stmt[*pos])) ++(*pos);
    if (*pos >= stmt_len) return false;

    if (stmt[*pos] == '#') {
      while (*pos < stmt_len && stmt[*pos] != '\n' && stmt[*pos] != '\r') ++(*pos);
      continue;
    }

    if (stmt_len - *pos >= 2 && stmt[*pos] == '-' && stmt[*pos + 1] == '-' &&
        (stmt_len - *pos == 2 || IsSqlWhitespace(stmt[*pos + 2]))) {
      *pos += 2;
      while (*pos < stmt_len && stmt[*pos] != '\n' && stmt[*pos] != '\r') ++(*pos);
      continue;
    }

    if (stmt_len - *pos >= 2 && stmt[*pos] == '/' && stmt[*pos + 1] == '*') {
      const bool mysql_version_comment = stmt_len - *pos >= 3 && stmt[*pos + 2] == '!';
      const bool mariadb_version_comment = stmt_len - *pos >= 4 &&
                                           (stmt[*pos + 2] == 'M' || stmt[*pos + 2] == 'm') &&
                                           stmt[*pos + 3] == '!';
      if (mysql_version_comment || mariadb_version_comment) {
        *pos += mysql_version_comment ? 3 : 4;
        while (*pos < stmt_len && std::isdigit(static_cast<unsigned char>(stmt[*pos]))) ++(*pos);
        while (*pos < stmt_len && IsSqlWhitespace(stmt[*pos])) ++(*pos);
        return *pos < stmt_len;
      }

      const size_t comment_start = *pos;
      *pos += 2;
      bool closed = false;
      while (stmt_len - *pos >= 2) {
        if (stmt[*pos] == '*' && stmt[*pos + 1] == '/') {
          *pos += 2;
          closed = true;
          break;
        }
        ++(*pos);
      }
      if (!closed) {
        *pos = comment_start;
        return false;
      }
      continue;
    }

    return true;
  }
}

bool IsSqlIdentifierChar(char ch) {
  const auto uch = static_cast<unsigned char>(ch);
  return std::isalnum(uch) || ch == '_' || ch == '$';
}

}  // namespace

CdcEngine::~CdcEngine() { WarnIfIncludeFiltersMatchedNothing(); }

// Extract the SQL statement from a QUERY_EVENT body and decide whether it is a
// DDL statement that may alter table schema. QUERY_EVENT (v4) body layout:
//   [0..4)  thread_id, [4..8) exec_time, [8] db_len, [9..11) error_code,
//   [11..13) status_vars_len, status_vars(status_vars_len), db_name(db_len),
//   '\0', query statement (remainder).
bool IsDdlQueryEvent(const uint8_t* body, size_t body_len) {
  if (body == nullptr || body_len < 13) {
    return false;
  }
  uint8_t db_len = body[8];
  uint16_t status_vars_len = binary::ReadU16Le(body + 11);
  size_t stmt_offset = 13 + static_cast<size_t>(status_vars_len) + db_len + 1;
  if (stmt_offset >= body_len) {
    return false;
  }
  const char* stmt = reinterpret_cast<const char*>(body + stmt_offset);
  size_t stmt_len = body_len - stmt_offset;

  size_t pos = 0;
  if (!SkipLeadingSqlTrivia(stmt, stmt_len, &pos)) return false;

  // Compare the first keyword case-insensitively against known DDL verbs.
  static constexpr std::array<const char*, 5> kDdlKeywords = {"ALTER", "RENAME", "DROP", "CREATE",
                                                              "TRUNCATE"};
  for (const char* keyword : kDdlKeywords) {
    size_t keyword_len = std::strlen(keyword);
    if (stmt_len - pos < keyword_len) {
      continue;
    }
    bool match = true;
    for (size_t i = 0; i < keyword_len; ++i) {
      if (std::toupper(static_cast<unsigned char>(stmt[pos + i])) != keyword[i]) {
        match = false;
        break;
      }
    }
    if (match && (stmt_len - pos == keyword_len || !IsSqlIdentifierChar(stmt[pos + keyword_len]))) {
      return true;
    }
  }
  return false;
}

size_t CdcEngine::Feed(const uint8_t* data, size_t len) {
  if (IsError()) {
    return 0;
  }

  size_t total_consumed = 0;

  // Processing an already-buffered event can set an engine error after the
  // parser has accepted its bytes. EventStreamParser then remains in
  // kEventReady, so another Feed() call would make no progress. Stop this
  // call immediately instead of repeatedly processing the same event.
  while (total_consumed < len && !IsError()) {
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
      if (IsError()) {
        break;
      }
      stream_parser_.Advance();
    }
  }

  return total_consumed;
}

void CdcEngine::SetMaxQueueSize(size_t max_size) {
  max_queue_size_ = max_size == 0 ? MES_DEFAULT_QUEUE_SIZE : max_size;
}

size_t CdcEngine::MaxQueueSize() const { return max_queue_size_; }

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
  WarnIfIncludeFiltersMatchedNothing();
  stream_parser_.Reset();
  table_registry_.Clear();
  position_ = BinlogPosition{};
  pending_source_sql_.clear();
  blocked_table_ids_.clear();
  ResetIncludeFilterMatchState();
  last_error_ = MES_OK;
  // Keep already decoded events. A parse error can follow valid row events
  // in the same input buffer; discarding those events makes acknowledged data
  // unrecoverable. Callers may drain the queue after Reset() before resuming.
  // Note: metadata_fetcher_ is intentionally NOT cleared. Reset()
  // is used on reconnect paths; the metadata connection is long-lived and
  // reusing it avoids a SHOW COLUMNS round-trip storm right after a
  // reconnect. The caller owns the fetcher's lifetime via
  // SetMetadataFetcher().
}

size_t CdcEngine::PendingEventCount() const { return event_queue_.size(); }

bool CdcEngine::IsError() const {
  return last_error_ != MES_OK || stream_parser_.GetState() == ParserState::kError;
}

mes_error_t CdcEngine::ErrorCode() const {
  if (last_error_ != MES_OK) {
    return last_error_;
  }
  if (stream_parser_.GetState() != ParserState::kError) {
    return MES_OK;
  }
  return stream_parser_.ErrorCode();
}

void CdcEngine::SetIncludeDatabases(const std::vector<std::string>& databases) {
  include_databases_ = std::unordered_set<std::string>(databases.begin(), databases.end());
  ResetIncludeFilterMatchState();
  RebuildBlockedTableIds();
}

void CdcEngine::SetIncludeTables(const std::vector<std::string>& tables) {
  include_tables_ = std::unordered_set<std::string>(tables.begin(), tables.end());
  ResetIncludeFilterMatchState();
  RebuildBlockedTableIds();
}

void CdcEngine::SetExcludeTables(const std::vector<std::string>& tables) {
  exclude_tables_ = std::unordered_set<std::string>(tables.begin(), tables.end());
  RebuildBlockedTableIds();
}

bool CdcEngine::IsTableAllowed(const std::string& database, const std::string& table) const {
  if (!MatchesIncludeFilters(database, table)) return false;

  // Check exclude filter
  if (MatchesTableFilter(exclude_tables_, database, table)) {
    return false;
  }
  return true;
}

bool CdcEngine::HasIncludeFilters() const {
  return !include_databases_.empty() || !include_tables_.empty();
}

bool CdcEngine::MatchesTableFilter(const std::unordered_set<std::string>& filters,
                                   const std::string& database, const std::string& table) const {
  if (filters.empty()) return false;
  const std::string qualified = database + "." + table;
  for (const std::string& filter : filters) {
    if (filter == qualified || filter == table) return true;
    if (filter.empty() || filter.back() != '*') continue;
    const std::string_view prefix(filter.data(), filter.size() - 1);
    if (qualified.compare(0, prefix.size(), prefix.data(), prefix.size()) == 0 ||
        table.compare(0, prefix.size(), prefix.data(), prefix.size()) == 0) {
      return true;
    }
  }
  return false;
}

bool CdcEngine::MatchesIncludeFilters(const std::string& database, const std::string& table) const {
  if (!include_databases_.empty() &&
      include_databases_.find(database) == include_databases_.end()) {
    return false;
  }
  return include_tables_.empty() || MatchesTableFilter(include_tables_, database, table);
}

void CdcEngine::NoteTableMapForIncludeFilters(const TableMetadata& metadata) {
  if (!HasIncludeFilters()) return;
  include_filter_saw_table_map_ = true;
  include_filter_matched_ =
      include_filter_matched_ || MatchesIncludeFilters(metadata.database_name, metadata.table_name);
}

void CdcEngine::WarnIfIncludeFiltersMatchedNothing() {
  if (!HasIncludeFilters() || !include_filter_saw_table_map_ || include_filter_matched_) return;
  StructuredLog()
      .Event("include_filter_matched_nothing")
      .Field("include_database_count", static_cast<uint64_t>(include_databases_.size()))
      .Field("include_table_count", static_cast<uint64_t>(include_tables_.size()))
      .Warn();
}

void CdcEngine::ResetIncludeFilterMatchState() {
  include_filter_saw_table_map_ = false;
  include_filter_matched_ = false;
}

void CdcEngine::RebuildBlockedTableIds() {
  blocked_table_ids_.clear();
  table_registry_.ForEach([this](uint64_t table_id, const TableMetadata& metadata) {
    NoteTableMapForIncludeFilters(metadata);
    if (!IsTableAllowed(metadata.database_name, metadata.table_name)) {
      blocked_table_ids_.insert(table_id);
    }
  });
}

void CdcEngine::SetMetadataFetcher(MetadataFetcher* fetcher) { metadata_fetcher_ = fetcher; }

void CdcEngine::SetMaxEventSize(uint32_t max_event_size) {
  stream_parser_.SetMaxEventSize(max_event_size);
}

uint32_t CdcEngine::MaxEventSize() const { return stream_parser_.MaxEventSize(); }

void CdcEngine::SetChecksumEnabled(bool enabled) { stream_parser_.SetChecksumEnabled(enabled); }

void CdcEngine::ProcessEvent(const EventHeader& header, const uint8_t* body, size_t body_len) {
  // Advance the resume position to next_position so events emitted by this
  // call carry the offset to resume from after consuming them. The pre-event
  // position is saved so it can be restored if processing fails to decode,
  // ensuring a reconnect re-reads the offending event rather than skipping it.
  const BinlogPosition saved_position = position_;
  if (header.next_position > 0) {
    position_.offset = header.next_position;
  }

  switch (header.type_code) {
    case static_cast<uint8_t>(BinlogEventType::kTableMapEvent): {
      if (body == nullptr || body_len < 6) {
        last_error_ = MES_ERR_PARSE;
        StructuredLog().Event("table_map_parse_failed").Field("reason", "body_too_short").Error();
        break;
      }
      uint64_t table_id = binary::ReadU48Le(body);
      uint64_t evicted_table_id = UINT64_MAX;
      bool unchanged = false;
      if (!table_registry_.ProcessTableMapEvent(body, body_len, &evicted_table_id, &unchanged)) {
        last_error_ = MES_ERR_PARSE;
        StructuredLog()
            .Event("table_map_parse_failed")
            .Field("table_id", table_id)
            .Field("body_length", static_cast<uint64_t>(body_len))
            .Error();
        break;
      }
      if (evicted_table_id != UINT64_MAX) blocked_table_ids_.erase(evicted_table_id);
      if (unchanged) break;
      auto* meta = table_registry_.MutableLookup(table_id);
      if (meta) {
        NoteTableMapForIncludeFilters(*meta);
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
          // FetchColumnInfo returns an empty vector on any failure (connection
          // loss, lost SELECT privilege, column-count mismatch).
          for (size_t i = 0; i < infos.size() && i < meta->columns.size(); i++) {
            meta->columns[i].name = infos[i].name;
            // Binlog TABLE_MAP signedness (present in MINIMAL mode, the MySQL
            // default) is authoritative for the exact schema at this position.
            // Only fall back to the side-connection's signedness when the
            // binlog did not carry it (very old servers).
            if (!meta->signedness_from_binlog) {
              meta->columns[i].is_unsigned = infos[i].is_unsigned;
            }
          }
        }
        // A TABLE_MAP may carry names itself, or the metadata side-connection
        // may have supplied them above. The flag must describe the resulting
        // metadata, not whether a lookup was attempted.
        meta->names_resolved = std::none_of(meta->columns.begin(), meta->columns.end(),
                                            [](const ColumnMetadata& c) { return c.name.empty(); });
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

    case static_cast<uint8_t>(BinlogEventType::kQueryEvent): {
      pending_source_sql_.clear();
      // A DDL statement (ALTER/RENAME/DROP/CREATE/TRUNCATE) may change a
      // table's columns while preserving the column count, which the metadata
      // cache's count guard cannot detect. Invalidate the whole metadata cache
      // so the next row event re-fetches fresh column names and signedness.
      if (metadata_fetcher_ != nullptr && IsDdlQueryEvent(body, body_len)) {
        metadata_fetcher_->ClearCache();
        StructuredLog().Event("metadata_cache_invalidated_on_ddl").Debug();
      }
      break;
    }

    case static_cast<uint8_t>(BinlogEventType::kRotateEvent): {
      RotateEventData rot;
      if (ParseRotateEvent(body, body_len, &rot)) {
        // Empty filenames appear in artificial ROTATE events. They carry an
        // updated offset but must not erase the last usable resume filename.
        if (!rot.new_log_file.empty()) {
          position_.binlog_file = std::move(rot.new_log_file);
        }
        position_.offset = rot.position;
        // A new binlog file reassigns table_ids and re-sends TABLE_MAP events
        // before any row events, so the old registry is stale. Clear it (and
        // the derived filter cache) to avoid retaining metadata for table_ids
        // that no longer apply and to keep the registry from growing toward
        // its cap across many rotations.
        table_registry_.Clear();
        blocked_table_ids_.clear();
        StructuredLog().Event("binlog_rotate").Field("file", position_.binlog_file).Debug();
      } else {
        last_error_ = MES_ERR_PARSE;
        StructuredLog().Event("rotate_event_parse_failed").Error();
      }
      break;
    }

    case static_cast<uint8_t>(BinlogEventType::kMariaDBAnnotateRowsEvent):
      if (body == nullptr || body_len == 0) {
        last_error_ = MES_ERR_PARSE;
        StructuredLog().Event("mariadb_annotate_rows_parse_failed").Error();
      } else {
        pending_source_sql_.assign(reinterpret_cast<const char*>(body), body_len);
      }
      break;

    // MariaDB-specific events without a row-level representation.
    case static_cast<uint8_t>(BinlogEventType::kMariaDBBinlogCheckpointEvent):
    case static_cast<uint8_t>(BinlogEventType::kMariaDBGtidEvent):
    case static_cast<uint8_t>(BinlogEventType::kMariaDBGtidListEvent):
    case static_cast<uint8_t>(BinlogEventType::kMariaDBStartEncryptionEvent):
    case static_cast<uint8_t>(BinlogEventType::kHeartbeatLogEvent):
    case static_cast<uint8_t>(BinlogEventType::kIgnorableLogEvent):
    case static_cast<uint8_t>(BinlogEventType::kRowsQueryLogEvent):
    case static_cast<uint8_t>(BinlogEventType::kFormatDescriptionEvent):
    case static_cast<uint8_t>(BinlogEventType::kXidEvent):
      pending_source_sql_.clear();
      break;

    case static_cast<uint8_t>(BinlogEventType::kGtidLogEvent):
    case static_cast<uint8_t>(BinlogEventType::kGtidTaggedLogEvent):
    case static_cast<uint8_t>(BinlogEventType::kAnonymousGtidLogEvent):
    case static_cast<uint8_t>(BinlogEventType::kPreviousGtidsEvent):
    case static_cast<uint8_t>(BinlogEventType::kTransactionContextEvent):
    case static_cast<uint8_t>(BinlogEventType::kViewChangeEvent):
    case static_cast<uint8_t>(BinlogEventType::kHeartbeatLogEventV2):
      break;

    // PARTIAL_JSON and MariaDB log_bin_compress need decoders that preserve
    // full row values. Refuse them rather than silently advancing a CDC
    // checkpoint past changes we cannot represent.
    case static_cast<uint8_t>(BinlogEventType::kPartialUpdateRowsEvent):
    case static_cast<uint8_t>(BinlogEventType::kMariaDBQueryCompressedEvent):
    case static_cast<uint8_t>(BinlogEventType::kMariaDBWriteRowsCompressedEventV1):
    case static_cast<uint8_t>(BinlogEventType::kMariaDBUpdateRowsCompressedEventV1):
    case static_cast<uint8_t>(BinlogEventType::kMariaDBDeleteRowsCompressedEventV1):
    case static_cast<uint8_t>(BinlogEventType::kMariaDBWriteRowsCompressedEvent):
    case static_cast<uint8_t>(BinlogEventType::kMariaDBUpdateRowsCompressedEvent):
    case static_cast<uint8_t>(BinlogEventType::kMariaDBDeleteRowsCompressedEvent):
      last_error_ = MES_ERR_PARSE;
      StructuredLog()
          .Event("unsupported_binlog_event")
          .Field("type_code", static_cast<uint64_t>(header.type_code))
          .Error();
      break;

    default:
      // Do not silently skip a newly introduced event type: doing so advances
      // the caller's checkpoint while dropping an unknown change.
      last_error_ = MES_ERR_PARSE;
      StructuredLog()
          .Event("unknown_binlog_event")
          .Field("type_code", static_cast<uint64_t>(header.type_code))
          .Error();
      break;
  }

  // If this event failed to decode, restore the pre-event position so the
  // recorded resume offset does not advance past the offending event.
  if (last_error_ != MES_OK) {
    position_ = saved_position;
  }
}

void CdcEngine::ProcessRowEvent(const EventHeader& header, const uint8_t* body, size_t body_len) {
  if (body == nullptr || body_len < 6) {
    last_error_ = MES_ERR_DECODE_ROW;
    StructuredLog()
        .Event("row_decode_failed")
        .Field("kind", "row_event_header")
        .Field("reason", "body_too_short")
        .Warn();
    return;
  }

  // Extract table_id from the first 6 bytes of the body
  uint64_t table_id = binary::ReadU48Le(body);
  if (blocked_table_ids_.find(table_id) != blocked_table_ids_.end()) return;
  const auto meta = table_registry_.SharedLookup(table_id);
  if (!meta) {
    last_error_ = MES_ERR_DECODE_ROW;
    StructuredLog()
        .Event("rows_event_no_table_map")
        .Field("table_id", static_cast<uint64_t>(table_id))
        .Error();
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
        event.table_metadata = meta;
        event.timestamp = header.timestamp;
        event.position = position_;
        event.source_sql = pending_source_sql_;
        event.names_resolved = meta->names_resolved;
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
        event.table_metadata = meta;
        event.timestamp = header.timestamp;
        event.position = position_;
        event.source_sql = pending_source_sql_;
        event.names_resolved = meta->names_resolved;
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
        event.table_metadata = meta;
        event.timestamp = header.timestamp;
        event.position = position_;
        event.source_sql = pending_source_sql_;
        event.names_resolved = meta->names_resolved;
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
