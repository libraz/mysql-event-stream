// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#include "cdc_engine.h"

#include <algorithm>
#include <array>
#include <cctype>
#include <cstring>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "client/column_name_source.h"
#include "logger.h"
#include "mybinlog/disposition.h"
#include "mybinlog/event_types.h"
#include "mybinlog/format_description.h"
#include "mybinlog/mariadb_gtid.h"
#include "mybinlog/query_event.h"
#include "mybinlog/rotate.h"
#include "mybinlog/row_decoder.h"
#include "mybinlog/status.h"
#include "sql_statement.h"
#include "value_render.h"
#include "wire_util.h"

namespace mes {

static void AttachColumnNames(RowData& row, const TableMetadata& meta) {
  for (size_t i = 0; i < row.columns.size() && i < meta.columns.size(); i++) {
    row.columns[i].name = meta.columns[i].name;
  }
}

namespace {

// Row scratch capacity kept between events; one outsized event must not pin
// its row array for the life of the engine.
constexpr size_t kRetainedScratchRows = 1024;

// Floor under the per-event decode budget, so a small queue byte budget still
// decodes an ordinary row (a maximal-width row's column array alone is ~256 KiB)
// and applies backpressure instead of failing the event.
constexpr size_t kMinEventDecodeBudget = 1024U * 1024U;

/**
 * @brief Report that a table's numeric signedness is not established.
 *
 * With signedness known from neither the binlog TABLE_MAP nor a metadata
 * side-connection, every numeric column is decoded as signed, so an UNSIGNED
 * value above the signed range of its width reads as a negative number. The
 * report is made once per registered table definition, because the same
 * TABLE_MAP precedes every ROWS event.
 *
 * @param meta Registry entry for the table; marked as reported.
 */
void ReportUnknownSignedness(TableMetadata* meta) {
  if (meta->unknown_signedness_reported) return;
  if (std::none_of(
          meta->columns.begin(), meta->columns.end(),
          [](const mybinlog::TableMapColumn& c) { return mybinlog::HasSignednessBit(c.type); })) {
    return;
  }
  meta->unknown_signedness_reported = true;
  StructuredLog()
      .Event("table_map_missing_signedness")
      .Field("db", meta->schema)
      .Field("table", meta->table)
      .Warn();
}

/** True when @p a and @p b differ in no field the metadata resolution writes. */
bool SameResolution(const TableMetadata& a, const TableMetadata& b) {
  if (a.names_resolved != b.names_resolved ||
      a.unknown_signedness_reported != b.unknown_signedness_reported ||
      a.columns.size() != b.columns.size()) {
    return false;
  }
  for (size_t i = 0; i < a.columns.size(); i++) {
    if (a.columns[i].name != b.columns[i].name ||
        a.columns[i].is_unsigned != b.columns[i].is_unsigned) {
      return false;
    }
  }
  return true;
}

// Flavor an event is classified under. A raw feed may carry no
// FORMAT_DESCRIPTION_EVENT, and then each type is taken to come from the
// server family that defines it.
mybinlog::ServerFlavor ClassificationFlavor(const std::optional<mybinlog::ServerFlavor>& stream,
                                            uint8_t type_code) {
  if (stream.has_value()) return *stream;
  return type_code >= static_cast<uint8_t>(mybinlog::EventType::kMariaDBAnnotateRowsEvent)
             ? mybinlog::ServerFlavor::kMariaDB
             : mybinlog::ServerFlavor::kMySQL;
}

// A type no supported server writes is reported as unknown; a known type this
// engine cannot represent, as unsupported.
bool IsObsoleteOrUnknownType(uint8_t type_code) {
  switch (static_cast<mybinlog::EventType>(type_code)) {
    case mybinlog::EventType::kUnknown:
    case mybinlog::EventType::kStartEventV3:
    case mybinlog::EventType::kSlaveEvent:
    case mybinlog::EventType::kEnumEndEvent:
      return true;
    default:
      return std::strcmp(mybinlog::EventTypeName(type_code), "UNKNOWN") == 0;
  }
}

// Whether a skipped event ends the statement an ANNOTATE_ROWS opened. ROTATE
// and the MySQL position and Group Replication events carry no statement
// boundary of their own.
bool EndsAnnotatedStatement(uint8_t type_code) {
  switch (static_cast<mybinlog::EventType>(type_code)) {
    case mybinlog::EventType::kRotateEvent:
    case mybinlog::EventType::kGtidLogEvent:
    case mybinlog::EventType::kGtidTaggedLogEvent:
    case mybinlog::EventType::kPreviousGtidsEvent:
    case mybinlog::EventType::kTransactionContextEvent:
    case mybinlog::EventType::kViewChangeEvent:
    case mybinlog::EventType::kHeartbeatLogEventV2:
      return false;
    default:
      return true;
  }
}

}  // namespace

CdcEngine::~CdcEngine() { WarnIfIncludeFiltersMatchedNothing(); }

// Extract the SQL statement from a QUERY_EVENT body and decide whether it is a
// DDL statement that may alter table schema. QUERY_EVENT (v4) body layout:
//   [0..4)  thread_id, [4..8) exec_time, [8] db_len, [9..11) error_code,
//   [11..13) status_vars_len, status_vars(status_vars_len), db_name(db_len),
//   '\0', query statement (remainder).
bool IsDdlStatement(std::string_view statement) {
  const char* stmt = statement.data();
  const size_t stmt_len = statement.size();
  if (stmt_len == 0) return false;

  size_t pos = 0;
  if (!SkipLeadingSqlTrivia(stmt, stmt_len, &pos)) return false;

  // MariaDB logs `SET STATEMENT ... FOR <stmt>` verbatim; the statement that
  // follows FOR is what runs.
  size_t after_prefix = pos;
  if (SkipSetStatementPrefix(stmt, stmt_len, &after_prefix)) {
    pos = after_prefix;
    if (!SkipLeadingSqlTrivia(stmt, stmt_len, &pos)) return false;
  }

  // Compare the first keyword case-insensitively against known DDL verbs.
  static constexpr std::array<const char*, 5> kDdlKeywords = {"ALTER", "RENAME", "DROP", "CREATE",
                                                              "TRUNCATE"};
  for (const char* keyword : kDdlKeywords) {
    if (KeywordAt(stmt, stmt_len, pos, keyword)) return true;
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
    if (QueueAtCapacity()) {
      break;
    }

    size_t consumed = stream_parser_.Feed(data + total_consumed, len - total_consumed);
    if (consumed == 0 && !stream_parser_.HasEvent()) {
      break;
    }
    total_consumed += consumed;

    while (stream_parser_.HasEvent()) {
      // Note: both queue checks are per binlog event, not per row.
      // A single multi-row WRITE_ROWS/UPDATE_ROWS/DELETE_ROWS event may
      // push all its rows before the limits are rechecked. The queue can
      // temporarily exceed max_queue_size_ by (rows_per_event - 1) items, and
      // max_queue_bytes_ by that event's charge -- which its decode budget
      // bounds, so the overshoot stays a function of the configured limits.
      if (QueueAtCapacity()) {
        break;
      }
      const mybinlog::EventHeader& header = stream_parser_.CurrentHeader();
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

void CdcEngine::SetMaxQueueBytes(size_t max_queue_bytes) {
  max_queue_bytes_ = max_queue_bytes == 0 ? MES_DEFAULT_QUEUE_BYTES : max_queue_bytes;
}

size_t CdcEngine::MaxQueueBytes() const { return max_queue_bytes_; }

size_t CdcEngine::QueuedBytes() const { return queued_bytes_; }

bool CdcEngine::QueueAtCapacity() const {
  if (max_queue_size_ > 0 && event_queue_.size() >= max_queue_size_) return true;
  return queued_bytes_ >= max_queue_bytes_;
}

void CdcEngine::EnqueueEvent(ChangeEvent&& event) {
  // Charged before the move, while the event still owns its payloads, and
  // released again by the same function in NextEvent().
  queued_bytes_ += ChangeEventCharge(event);
  event_queue_.push(std::move(event));
}

bool CdcEngine::NextEvent(ChangeEvent* event) {
  if (event_queue_.empty() || event == nullptr) {
    return false;
  }
  // Recomputed rather than stored alongside the entry: nothing mutates a queued
  // event, so this is the same charge EnqueueEvent() applied. The subtraction is
  // floored regardless -- an underflow would wedge Feed() behind a queue that
  // looks permanently full instead of failing visibly.
  const size_t charge = ChangeEventCharge(event_queue_.front());
  *event = std::move(event_queue_.front());
  event_queue_.pop();
  queued_bytes_ -= std::min(charge, queued_bytes_);
  return true;
}

bool CdcEngine::HasEvents() const { return !event_queue_.empty(); }

const BinlogPosition& CdcEngine::CurrentPosition() const { return position_; }

void CdcEngine::Reset() {
  WarnIfIncludeFiltersMatchedNothing();
  stream_parser_.Reset();
  table_registry_.Clear();
  position_ = BinlogPosition{};
  position_binlog_file_.reset();
  pending_source_sql_.reset();
  blocked_table_ids_.clear();
  flavor_.reset();
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
  // Built on demand so a filter set of bare table names never pays for the
  // concatenation.
  std::string qualified;
  bool qualified_built = false;
  for (const std::string& filter : filters) {
    // An entry containing '.' names one "database.table"; an entry without one
    // names a bare table in any database. Comparing a bare entry against the
    // qualified name would let its prefix match the database name instead.
    const bool filter_is_qualified = filter.find('.') != std::string::npos;
    if (filter_is_qualified && !qualified_built) {
      qualified = database + "." + table;
      qualified_built = true;
    }
    const std::string& subject = filter_is_qualified ? qualified : table;
    if (filter == subject) return true;
    if (filter.empty() || filter.back() != '*') continue;
    const std::string_view prefix(filter.data(), filter.size() - 1);
    if (subject.compare(0, prefix.size(), prefix.data(), prefix.size()) == 0) {
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
      include_filter_matched_ || MatchesIncludeFilters(metadata.schema, metadata.table);
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

void CdcEngine::ResolveTableMetadata(uint64_t table_id, bool keep_if_same) {
  const TableMetadata* meta = table_registry_.Lookup(table_id);
  if (meta == nullptr) return;
  // Events that are already queued read their column names straight out
  // of the registered TableMetadata, so the resolution below is written
  // into a copy that then replaces the registry entry. Writing the names
  // into the registered object would change what an event decoded earlier
  // reports, and on reallocation would leave its views dangling.
  TableMetadata resolved = *meta;
  bool needs_column_names =
      metadata_fetcher_ && !resolved.columns.empty() &&
      std::any_of(resolved.columns.begin(), resolved.columns.end(),
                  [](const mybinlog::TableMapColumn& c) { return c.name.empty(); });
  bool signedness_from_metadata_conn = false;
  if (needs_column_names) {
    auto infos = metadata_fetcher_->FetchColumnInfo(resolved.schema, resolved.table,
                                                    resolved.columns.size());
    // FetchColumnInfo returns an empty vector on any failure (connection
    // loss, lost SELECT privilege, column-count mismatch).
    signedness_from_metadata_conn = !infos.empty();
    for (size_t i = 0; i < infos.size() && i < resolved.columns.size(); i++) {
      resolved.columns[i].name = infos[i].name;
      // Binlog TABLE_MAP signedness (present in MINIMAL mode, the MySQL
      // default) is authoritative for the exact schema at this position.
      // Only fall back to the side-connection's signedness when the
      // binlog did not carry it (very old servers).
      if (!resolved.has_signedness) {
        resolved.columns[i].is_unsigned = infos[i].is_unsigned;
      }
    }
  }
  // A TABLE_MAP may carry names itself, or the metadata side-connection
  // may have supplied them above. The flag must describe the resulting
  // metadata, not whether a lookup was attempted.
  resolved.names_resolved =
      std::none_of(resolved.columns.begin(), resolved.columns.end(),
                   [](const mybinlog::TableMapColumn& c) { return c.name.empty(); });
  if (!resolved.has_signedness && !signedness_from_metadata_conn) {
    ReportUnknownSignedness(&resolved);
  }
  // Resolution that yields nothing new keeps the registered object, so
  // queued events keep sharing one instance while the retry goes on.
  if (keep_if_same && SameResolution(*meta, resolved)) return;
  std::vector<uint64_t> evicted_table_ids;
  table_registry_.ReplaceMetadata(table_id, std::move(resolved), &evicted_table_ids);
  for (uint64_t evicted : evicted_table_ids) blocked_table_ids_.erase(evicted);
}

void CdcEngine::RebuildBlockedTableIds() {
  const std::unordered_set<uint64_t> previously_blocked = std::move(blocked_table_ids_);
  blocked_table_ids_.clear();
  std::vector<uint64_t> newly_allowed;
  table_registry_.ForEach([&](uint64_t table_id, const TableMetadata& metadata) {
    NoteTableMapForIncludeFilters(metadata);
    if (!IsTableAllowed(metadata.schema, metadata.table)) {
      blocked_table_ids_.insert(table_id);
    } else if (previously_blocked.count(table_id) != 0) {
      newly_allowed.push_back(table_id);
    }
  });
  // A table skipped before name resolution has no resolved names, and its next
  // TABLE_MAP may be byte-identical and exit early, so resolve it now.
  for (uint64_t table_id : newly_allowed) ResolveTableMetadata(table_id, /*keep_if_same=*/true);
}

void CdcEngine::SetMetadataFetcher(ColumnNameSource* fetcher) { metadata_fetcher_ = fetcher; }

void CdcEngine::SetMaxEventSize(uint32_t max_event_size) {
  stream_parser_.SetMaxEventSize(max_event_size);
}

uint32_t CdcEngine::MaxEventSize() const { return stream_parser_.MaxEventSize(); }

void CdcEngine::SetChecksumEnabled(bool enabled) { stream_parser_.SetChecksumEnabled(enabled); }

void CdcEngine::SetTrailerPreVerified(bool pre_verified) {
  stream_parser_.SetTrailerPreVerified(pre_verified);
}

bool CdcEngine::TrailerPreVerified() const { return stream_parser_.TrailerPreVerified(); }

void CdcEngine::ProcessEvent(const mybinlog::EventHeader& header, const uint8_t* body,
                             size_t body_len) {
  // Advance the resume position to next_position so events emitted by this
  // call carry the offset to resume from after consuming them. The pre-event
  // offset is saved so it can be restored if processing fails to decode,
  // ensuring a reconnect re-reads the offending event rather than skipping it.
  // The filename needs no saving: SetResumeBinlogFile() is reached only from
  // the ROTATE case after its body parsed, which leaves no error to roll back,
  // so restoring the offset restores the whole position.
  const uint64_t saved_offset = position_.offset;
  if (header.next_position > 0) {
    position_.offset = header.next_position;
  }

  // libmybinlog decides what each type code is; this engine additionally
  // refuses compressed events and PARTIAL_UPDATE_ROWS, whose rows it does not
  // decode, rather than advance a CDC checkpoint past changes it cannot
  // represent. Everything the codec fails closed on is refused as well.
  const mybinlog::EventDisposition disposition = mybinlog::ClassifyEvent(
      header.type_code, header.flags, ClassificationFlavor(flavor_, header.type_code));
  switch (disposition.disposition) {
    case mybinlog::Disposition::kTableMap: {
      if (body == nullptr || body_len < 6) {
        last_error_ = MES_ERR_PARSE;
        StructuredLog().Event("table_map_parse_failed").Field("reason", "body_too_short").Error();
        break;
      }
      uint64_t table_id = wire::ReadU48Le(body);
      // Empty unless the registration evicted, which is the rare case, so this
      // allocates nothing on the ordinary path.
      std::vector<uint64_t> evicted_table_ids;
      bool unchanged = false;
      if (!table_registry_.ProcessTableMapEvent(body, body_len,
                                                flavor_.value_or(mybinlog::ServerFlavor::kMySQL),
                                                &evicted_table_ids, &unchanged)) {
        last_error_ = MES_ERR_PARSE;
        StructuredLog()
            .Event("table_map_parse_failed")
            .Field("table_id", table_id)
            .Field("body_length", static_cast<uint64_t>(body_len))
            .Error();
        break;
      }
      for (uint64_t evicted : evicted_table_ids) blocked_table_ids_.erase(evicted);
      const TableMetadata* meta = table_registry_.Lookup(table_id);
      if (meta) {
        // A byte-identical TABLE_MAP re-registers no new schema, so the work
        // below can be skipped -- unless a column name is still empty while a
        // metadata side-connection is configured. The same TABLE_MAP precedes
        // every ROWS event, so treating it as nothing to do would make a
        // resolution that failed once fail for the rest of the binlog file.
        if (unchanged && (metadata_fetcher_ == nullptr || meta->names_resolved)) break;
        NoteTableMapForIncludeFilters(*meta);
        if (!IsTableAllowed(meta->schema, meta->table)) {
          blocked_table_ids_.insert(table_id);
          break;
        }
        blocked_table_ids_.erase(table_id);
        ResolveTableMetadata(table_id, unchanged);
      }
      break;
    }

    case mybinlog::Disposition::kRows: {
      if (header.type_code == static_cast<uint8_t>(mybinlog::EventType::kPartialUpdateRowsEvent)) {
        RefuseEvent(header, "unsupported_binlog_event",
                    "PARTIAL_JSON after images are not decoded");
        break;
      }
      ProcessRowEvent(header, body, body_len);
      // The statement's annotation ends with its last ROWS event, so a later
      // unannotated statement in the same transaction does not inherit it.
      mybinlog::RowsEventHeader rows_header;
      if (mybinlog::ParseRowsEventHeader(header, body, body_len, &rows_header) ==
              mybinlog::Status::kOk &&
          (rows_header.flags & mybinlog::kRowsStatementEndFlag) != 0) {
        pending_source_sql_.reset();
      }
      break;
    }

    case mybinlog::Disposition::kQuery: {
      pending_source_sql_.reset();
      // A DDL statement (ALTER/RENAME/DROP/CREATE/TRUNCATE) may change a
      // table's columns while preserving both the column count, which the
      // metadata cache's count guard cannot detect, and the TABLE_MAP body,
      // which carries no column names unless binlog_row_metadata is FULL. The
      // registry goes with the metadata cache: keeping an entry would let the
      // next TABLE_MAP be reported as unchanged and reuse names that describe
      // the pre-DDL schema. The server re-sends a TABLE_MAP before every ROWS
      // event, so dropping the registry costs a re-parse, not a decode.
      mybinlog::QueryEvent query;
      const mybinlog::Status query_status =
          mybinlog::ParseQueryEvent(header, body, body_len, &query);
      if (query_status != mybinlog::Status::kOk) {
        last_error_ = MES_ERR_PARSE;
        StructuredLog()
            .Event("query_event_parse_failed")
            .Field("status", mybinlog::StatusName(query_status))
            .Field("body_length", static_cast<uint64_t>(body_len))
            .Error();
        break;
      }
      if (IsDdlStatement(query.query)) {
        if (metadata_fetcher_ != nullptr) metadata_fetcher_->ClearCache();
        table_registry_.Clear();
        blocked_table_ids_.clear();
        StructuredLog().Event("metadata_cache_invalidated_on_ddl").Debug();
      }
      break;
    }

    case mybinlog::Disposition::kFraming: {
      if (header.type_code != static_cast<uint8_t>(mybinlog::EventType::kRotateEvent)) {
        // The parser already took the stream's framing from this FDE; only its
        // server version is read here, so a body that names none leaves the
        // flavor as it was.
        mybinlog::FormatDescriptionEvent fde;
        if (header.type_code ==
                static_cast<uint8_t>(mybinlog::EventType::kFormatDescriptionEvent) &&
            mybinlog::ParseFormatDescriptionEvent(header, body, body_len, &fde) ==
                mybinlog::Status::kOk) {
          flavor_ = mybinlog::DetectFlavorFromVersion(fde.server_version);
        }
        pending_source_sql_.reset();
        break;
      }
      mybinlog::RotateEvent rot;
      if (mybinlog::ParseRotateEvent(header, body, body_len, &rot) == mybinlog::Status::kOk) {
        // Empty filenames appear in artificial ROTATE events. They carry an
        // updated offset but must not erase the last usable resume filename.
        if (!rot.next_file.empty()) {
          SetResumeBinlogFile(std::move(rot.next_file));
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

    case mybinlog::Disposition::kDataNeutral:
      if (header.type_code ==
          static_cast<uint8_t>(mybinlog::EventType::kMariaDBAnnotateRowsEvent)) {
        // The server emits one ANNOTATE_ROWS per statement, not per ROWS event:
        // a statement whose row data exceeds the per-event size limit produces
        // several consecutive ROWS events under a single annotation. The
        // statement is therefore held past a ROWS event and ends at the ROWS
        // event flagged STMT_END_F, the next annotation or the next control
        // event. One allocation is shared by every row it annotates instead of
        // copied once per row.
        mybinlog::MariaDbAnnotateRowsEvent annotate;
        if (mybinlog::ParseMariaDbAnnotateRowsEvent(header, body, body_len, &annotate) !=
            mybinlog::Status::kOk) {
          last_error_ = MES_ERR_PARSE;
          StructuredLog().Event("mariadb_annotate_rows_parse_failed").Error();
          break;
        }
        pending_source_sql_ = std::make_shared<const std::string>(std::move(annotate.query));
        break;
      }
      // Carries no change this engine can surface, so the stream position
      // advances past it.
      if (EndsAnnotatedStatement(header.type_code)) pending_source_sql_.reset();
      break;

    case mybinlog::Disposition::kGtid:
    case mybinlog::Disposition::kTransactionBoundary:
      if (EndsAnnotatedStatement(header.type_code)) pending_source_sql_.reset();
      break;

    case mybinlog::Disposition::kCompressed:
    case mybinlog::Disposition::kFailClosed:
      RefuseEvent(header,
                  IsObsoleteOrUnknownType(header.type_code) ? "unknown_binlog_event"
                                                            : "unsupported_binlog_event",
                  disposition.reason);
      break;
  }

  // If this event failed to decode, restore the pre-event position so the
  // recorded resume offset does not advance past the offending event.
  if (last_error_ != MES_OK) {
    position_.offset = saved_offset;
  }
}

void CdcEngine::RefuseEvent(const mybinlog::EventHeader& header, const char* event,
                            const char* reason) {
  last_error_ = MES_ERR_PARSE;
  StructuredLog()
      .Event(event)
      .Field("type_code", static_cast<uint64_t>(header.type_code))
      .Field("reason", reason)
      .Error();
}

void CdcEngine::SetResumeBinlogFile(std::string binlog_file) {
  position_.binlog_file = std::move(binlog_file);
  position_binlog_file_ = std::make_shared<const std::string>(position_.binlog_file);
}

EventPosition CdcEngine::CurrentEventPosition() const {
  return EventPosition{position_binlog_file_, position_.offset};
}

void CdcEngine::ProcessRowEvent(const mybinlog::EventHeader& header, const uint8_t* body,
                                size_t body_len) {
  mybinlog::RowsEventHeader rows_header;
  if (mybinlog::ParseRowsEventHeader(header, body, body_len, &rows_header) !=
      mybinlog::Status::kOk) {
    last_error_ = MES_ERR_DECODE_ROW;
    StructuredLog()
        .Event("row_decode_failed")
        .Field("kind", "row_event_header")
        .Field("reason", "body_too_short")
        .Warn();
    return;
  }

  const uint64_t table_id = rows_header.table_id;
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

  const uint8_t type_code = header.type_code;
  const bool is_write = (type_code == static_cast<uint8_t>(mybinlog::EventType::kWriteRowsEvent) ||
                         type_code == static_cast<uint8_t>(mybinlog::EventType::kWriteRowsEventV1));
  const bool is_update =
      (type_code == static_cast<uint8_t>(mybinlog::EventType::kUpdateRowsEvent) ||
       type_code == static_cast<uint8_t>(mybinlog::EventType::kUpdateRowsEventV1));
  const char* kind = is_write ? "write_rows" : (is_update ? "update_rows" : "delete_rows");

  // Cap what one event may materialize at the queue's own byte budget rather
  // than at the decoder's standalone default, so the bytes a single event can
  // add to the queue stay a function of the engine's configured limits. The cap
  // is the whole budget, not what is left of it: a decode must not start failing
  // because the consumer happens to be behind. Every ColumnValue slot and every
  // queued entry is charged, so a NULL or a compressed value cannot buy more
  // than the budget either.
  mybinlog::DecodeBudget budget = mybinlog::DecodeBudget::ForEventBody(
      body_len, std::max(max_queue_bytes_, kMinEventDecodeBudget));
  mybinlog::DecodeOptions options;
  options.max_value_bytes = 0;
  options.max_decompressed_bytes = mybinlog::kMaxDecompressedColumnBytes;
  options.budget = &budget;
  options.per_value_overhead = sizeof(ColumnValue);
  options.per_row_overhead = sizeof(ChangeEvent);

  mybinlog::RowsEvent decoded;
  const mybinlog::Status status =
      mybinlog::DecodeRowsEvent(header, body, body_len, *meta, options, &decoded);
  if (status != mybinlog::Status::kOk) {
    LogRowDecodeFailure(kind, *meta, mybinlog::StatusName(status));
    return;
  }

  // Every row renders before any is queued, so a failure queues nothing.
  row_buf_.clear();
  before_buf_.clear();
  bool rendered = true;
  if (is_update) {
    row_buf_.resize(decoded.updates.size());
    before_buf_.resize(decoded.updates.size());
    for (size_t i = 0; rendered && i < decoded.updates.size(); ++i) {
      rendered = RenderRow(std::move(decoded.updates[i].first), &before_buf_[i], &budget) &&
                 RenderRow(std::move(decoded.updates[i].second), &row_buf_[i], &budget);
    }
  } else {
    row_buf_.resize(decoded.rows.size());
    for (size_t i = 0; rendered && i < decoded.rows.size(); ++i) {
      rendered = RenderRow(std::move(decoded.rows[i]), &row_buf_[i], &budget);
    }
  }

  if (rendered) {
    const EventType event_type =
        is_write ? EventType::kInsert : (is_update ? EventType::kUpdate : EventType::kDelete);
    for (size_t i = 0; i < row_buf_.size(); ++i) {
      ChangeEvent event;
      event.type = event_type;
      event.database = meta->schema;
      event.table = meta->table;
      if (is_update) {
        event.before = std::move(before_buf_[i]);
        AttachColumnNames(event.before, *meta);
      }
      if (event_type == EventType::kDelete) {
        event.before = std::move(row_buf_[i]);
        AttachColumnNames(event.before, *meta);
      } else {
        event.after = std::move(row_buf_[i]);
        AttachColumnNames(event.after, *meta);
      }
      event.table_metadata = meta;
      event.timestamp = header.timestamp;
      event.position = CurrentEventPosition();
      event.source_sql = pending_source_sql_;
      event.names_resolved = meta->names_resolved;
      EnqueueEvent(std::move(event));
    }
  } else {
    LogRowDecodeFailure(kind, *meta, "unrepresentable_value");
  }

  row_buf_.clear();
  before_buf_.clear();
  if (row_buf_.capacity() > kRetainedScratchRows) std::vector<RowData>().swap(row_buf_);
  if (before_buf_.capacity() > kRetainedScratchRows) std::vector<RowData>().swap(before_buf_);
}

void CdcEngine::LogRowDecodeFailure(const char* kind, const TableMetadata& meta,
                                    const char* reason) {
  // Record a decode-specific error so ErrorCode() can distinguish from
  // pure parser errors. Row-event decode failures indicate per-row column
  // corruption, not a stream-level framing issue.
  last_error_ = MES_ERR_DECODE_ROW;
  StructuredLog()
      .Event("row_decode_failed")
      .Field("kind", kind)
      .Field("db", meta.schema)
      .Field("table", meta.table)
      .Field("binlog_offset", static_cast<uint64_t>(position_.offset))
      .Field("reason", reason)
      .Warn();
}

}  // namespace mes
