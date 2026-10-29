// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#include "client/transaction_gtid_tracker.h"

#include <algorithm>
#include <cctype>
#include <string_view>
#include <utility>

#include "cdc_engine.h"
#include "logger.h"
#include "mybinlog/event_header.h"
#include "mybinlog/event_types.h"
#include "mybinlog/query_event.h"
#include "mybinlog/row_decoder.h"
#include "sql_statement.h"

namespace mes {
namespace {

/// QUERY_EVENT fixed post-header length.
constexpr size_t kQueryPostHeaderSize = 13;

bool IsStatementEndRowsEvent(const mybinlog::EventHeader& header, const uint8_t* body,
                             size_t body_size) {
  mybinlog::RowsEventHeader rows;
  if (mybinlog::ParseRowsEventHeader(header, body, body_size, &rows) != mybinlog::Status::kOk) {
    return false;
  }
  return (rows.flags & mybinlog::kRowsStatementEndFlag) != 0;
}

std::string FormatMySQLGtid(const mybinlog::Uuid& sid, const std::string& tag, uint64_t gno) {
  return mybinlog::FormatUuid(sid) + (tag.empty() ? ":" : ":" + tag + ":") + std::to_string(gno);
}

bool ExtractQueryKeywords(std::string_view statement, std::string* first, std::string* second) {
  size_t pos = 0;
  if (!SkipLeadingSqlTrivia(statement.data(), statement.size(), &pos)) return false;

  auto read_keyword = [&](std::string* keyword) {
    while (pos < statement.size() &&
           std::isspace(static_cast<unsigned char>(statement[pos])) != 0) {
      ++pos;
    }
    size_t end = pos;
    while (end < statement.size() &&
           std::isalpha(static_cast<unsigned char>(statement[end])) != 0) {
      ++end;
    }
    if (end == pos) return false;
    keyword->assign(statement.data() + pos, statement.data() + end);
    std::transform(keyword->begin(), keyword->end(), keyword->begin(),
                   [](unsigned char ch) { return static_cast<char>(std::toupper(ch)); });
    pos = end;
    return true;
  };

  if (!read_keyword(first)) return false;
  second->clear();
  (void)read_keyword(second);
  return true;
}

}  // namespace

void TransactionGtidTracker::Reset() {
  flavor_ = mybinlog::ServerFlavor::kMySQL;
  mysql_set_.Clear();
  mariadb_set_.clear();
  received_gtid_.clear();
  pending_gtid_ = {};
  transaction_open_ = false;
  baseline_known_ = true;
}

bool TransactionGtidTracker::Reset(const std::string& initial_gtid_set,
                                   mybinlog::ServerFlavor flavor) {
  Reset();
  flavor_ = flavor;

  if (flavor == mybinlog::ServerFlavor::kMariaDB) {
    mybinlog::MariaDbGtidPosition parsed;
    if (mybinlog::MariaDbGtidPosition::Parse(initial_gtid_set, &parsed) != mybinlog::Status::kOk) {
      return false;
    }
    for (const auto& gtid : parsed.gtids) MergeMariaDBGtid(&mariadb_set_, gtid);
    return true;
  }

  return MySQLSet::Parse(initial_gtid_set, &mysql_set_) == mybinlog::Status::kOk;
}

void TransactionGtidTracker::ResetAwaitingBaseline(mybinlog::ServerFlavor flavor) {
  Reset();
  flavor_ = flavor;
  baseline_known_ = false;
}

void TransactionGtidTracker::MergeMariaDBGtid(MariaDBSet* set, const mybinlog::MariaDbGtid& gtid) {
  auto it = set->find(gtid.domain_id);
  if (it == set->end() || gtid.sequence_no > it->second.sequence_no) {
    (*set)[gtid.domain_id] = gtid;
  }
}

std::string TransactionGtidTracker::FormatMySQLSet(const MySQLSet& set) { return set.ToString(); }

std::string TransactionGtidTracker::FormatMariaDBSet(const MariaDBSet& set) {
  mybinlog::MariaDbGtidPosition position;
  position.gtids.reserve(set.size());
  for (const auto& [domain, gtid] : set) {
    (void)domain;
    position.gtids.push_back(gtid);
  }
  return position.ToString();
}

std::string TransactionGtidTracker::FormatCurrentSet() const {
  return flavor_ == mybinlog::ServerFlavor::kMariaDB ? FormatMariaDBSet(mariadb_set_)
                                                     : FormatMySQLSet(mysql_set_);
}

std::string TransactionGtidTracker::CommitPending() {
  if (!pending_gtid_.present) return {};

  flavor_ = pending_gtid_.flavor;
  if (flavor_ == mybinlog::ServerFlavor::kMariaDB) {
    MergeMariaDBGtid(&mariadb_set_, pending_gtid_.mariadb);
  } else {
    if (mysql_set_.Add(pending_gtid_.sid, pending_gtid_.tag,
                       {pending_gtid_.sequence_no, pending_gtid_.sequence_no + 1}) !=
        mybinlog::Status::kOk) {
      // The committed GTID cannot join the set, so no checkpoint may advance.
      // Keep it pending and report it: dropping it silently would freeze the
      // checkpoint at an older position with no way to notice.
      StructuredLog()
          .Event("gtid_checkpoint_merge_failed")
          .Field("reason", "sid_interval_capacity_exhausted")
          .Field("gtid",
                 FormatMySQLGtid(pending_gtid_.sid, pending_gtid_.tag, pending_gtid_.sequence_no))
          .Field("max_intervals_per_sid", static_cast<uint64_t>(MySQLSet::kMaxIntervalsPerTsid))
          .Error();
      return {};
    }
  }
  pending_gtid_ = {};
  return baseline_known_ ? FormatCurrentSet() : std::string();
}

std::string TransactionGtidTracker::Observe(const uint8_t* data, size_t size, bool has_checksum) {
  if (data == nullptr) return {};

  mybinlog::EventHeader header;
  if (mybinlog::ParseEventHeader(data, size, &header) != mybinlog::Status::kOk ||
      header.event_length > size) {
    return {};
  }
  const size_t tail_size = has_checksum ? mybinlog::kChecksumSize : 0;
  if (header.event_length < mybinlog::kEventHeaderSize + tail_size) return {};
  const uint8_t* body = data + mybinlog::kEventHeaderSize;
  const size_t body_size = header.event_length - mybinlog::kEventHeaderSize - tail_size;
  const uint8_t event_type = header.type_code;

  if (event_type == static_cast<uint8_t>(mybinlog::EventType::kPreviousGtidsEvent)) {
    mybinlog::PreviousGtidsEvent baseline;
    if (mybinlog::ParsePreviousGtidsEvent(header, body, body_size, &baseline) !=
        mybinlog::Status::kOk) {
      return {};
    }
    MySQLSet merged = mysql_set_;
    if (merged.Merge(baseline.gtids) != mybinlog::Status::kOk) return {};
    mysql_set_ = std::move(merged);
    flavor_ = mybinlog::ServerFlavor::kMySQL;
    baseline_known_ = true;
    return FormatCurrentSet();
  }

  if (event_type == static_cast<uint8_t>(mybinlog::EventType::kMariaDBGtidListEvent)) {
    mybinlog::MariaDbGtidListEvent baseline;
    if (mybinlog::ParseMariaDbGtidListEvent(header, body, body_size, &baseline) !=
        mybinlog::Status::kOk) {
      return {};
    }
    MariaDBSet merged = mariadb_set_;
    for (const auto& gtid : baseline.gtids.gtids) MergeMariaDBGtid(&merged, gtid);
    mariadb_set_ = std::move(merged);
    flavor_ = mybinlog::ServerFlavor::kMariaDB;
    baseline_known_ = true;
    return FormatCurrentSet();
  }

  if (event_type == static_cast<uint8_t>(mybinlog::EventType::kGtidLogEvent) ||
      event_type == static_cast<uint8_t>(mybinlog::EventType::kGtidTaggedLogEvent)) {
    mybinlog::GtidEvent gtid;
    if (mybinlog::ParseGtidEvent(header, body, body_size, &gtid) != mybinlog::Status::kOk) {
      return {};
    }
    // Seeing the next GTID proves the preceding transaction group ended even
    // if it used a standalone event type we do not classify explicitly.
    std::string committed = CommitPending();
    transaction_open_ = false;
    flavor_ = mybinlog::ServerFlavor::kMySQL;
    received_gtid_ = gtid.ToString();
    pending_gtid_ = {true, mybinlog::ServerFlavor::kMySQL, gtid.uuid, std::move(gtid.tag), gtid.gno,
                     {}};
    return committed;
  }

  if (event_type == static_cast<uint8_t>(mybinlog::EventType::kMariaDBGtidEvent)) {
    mybinlog::MariaDbGtidEvent event;
    if (mybinlog::ParseMariaDbGtidEvent(header, body, body_size, &event) != mybinlog::Status::kOk) {
      return {};
    }
    const bool standalone = event.standalone();
    std::string committed = CommitPending();
    // MariaDB writes no BEGIN query: a group without FL_STANDALONE is open
    // from its GTID event until XID/COMMIT/ROLLBACK, even when its first
    // statement is DDL (CREATE TABLE ... SELECT).
    transaction_open_ = !standalone;
    flavor_ = mybinlog::ServerFlavor::kMariaDB;
    received_gtid_ = event.gtid.ToString();
    // FL_STANDALONE only says the group ends without a COMMIT/XID event; the
    // payload of the group still arrives after this event. Checkpointing here
    // would advertise a GTID whose rows the consumer has not seen, so the
    // group is closed by its terminating event instead.
    pending_gtid_ = {true, mybinlog::ServerFlavor::kMariaDB, {}, "", 0, event.gtid, standalone};
    return committed;
  }

  if (event_type == static_cast<uint8_t>(mybinlog::EventType::kAnonymousGtidLogEvent)) {
    std::string committed = CommitPending();
    transaction_open_ = false;
    received_gtid_.clear();
    pending_gtid_ = {};
    return committed;
  }

  if (event_type == static_cast<uint8_t>(mybinlog::EventType::kXidEvent)) {
    transaction_open_ = false;
    return CommitPending();
  }

  if (mybinlog::IsRowsEvent(event_type)) {
    // A standalone group has no COMMIT/XID, so its last ROWS_EVENT (the one
    // carrying STMT_END) is the point at which every event of the group has
    // been seen. Inside an open transaction STMT_END only ends a statement,
    // never the group.
    if (pending_gtid_.standalone && !transaction_open_ &&
        IsStatementEndRowsEvent(header, body, body_size)) {
      return CommitPending();
    }
    return {};
  }

  if (event_type != static_cast<uint8_t>(mybinlog::EventType::kQueryEvent)) return {};
  if (body_size < kQueryPostHeaderSize) return {};

  mybinlog::QueryEvent query;
  std::string first;
  std::string second;
  if (mybinlog::ParseQueryEvent(header, body, body_size, &query) == mybinlog::Status::kOk &&
      ExtractQueryKeywords(query.query, &first, &second)) {
    if (first == "BEGIN" || (first == "START" && second == "TRANSACTION")) {
      transaction_open_ = true;
      return {};
    }
    if (first == "COMMIT" || (first == "ROLLBACK" && second != "TO")) {
      transaction_open_ = false;
      return CommitPending();
    }
  }
  // DDL normally has an implicit transaction boundary, but never promote a
  // pending checkpoint merely because a DDL word appears inside an explicitly
  // open transaction group. In particular, ROLLBACK TO SAVEPOINT is not a
  // transaction rollback and must leave its GTID pending until COMMIT/XID.
  // A standalone group carries exactly one statement, so this QUERY is its
  // terminating event whether or not the statement is DDL.
  // DDL is classified by the engine's own rule, so both see through the same
  // comments and MariaDB `SET STATEMENT ... FOR` prefixes.
  if (transaction_open_) return {};
  if (pending_gtid_.standalone || IsDdlQueryEvent(body, body_size)) return CommitPending();
  return {};
}

}  // namespace mes
