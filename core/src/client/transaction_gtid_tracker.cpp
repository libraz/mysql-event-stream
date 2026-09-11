// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#include "client/transaction_gtid_tracker.h"

#include <algorithm>
#include <cctype>
#include <cstdio>
#include <limits>
#include <utility>

#include "binary_util.h"
#include "event_header.h"
#include "logger.h"
#include "mariadb_event_parser.h"

namespace mes {
namespace {

constexpr size_t kMySQLGtidBodySize = 25;

/// ROWS_EVENT post-header: 6-byte table id, then 2 flag bytes.
constexpr size_t kRowsEventFlagsOffset = 6;
/// Set on the last ROWS_EVENT of a statement.
constexpr uint16_t kRowsEventStmtEndFlag = 0x0001;

bool IsStatementEndRowsEvent(const uint8_t* data, size_t content_size) {
  if (content_size < kEventHeaderSize + kRowsEventFlagsOffset + sizeof(uint16_t)) return false;
  const uint16_t flags = binary::ReadU16Le(data + kEventHeaderSize + kRowsEventFlagsOffset);
  return (flags & kRowsEventStmtEndFlag) != 0;
}

bool ReadVarUInt(const uint8_t* data, size_t size, size_t* offset, uint64_t* value) {
  if (data == nullptr || offset == nullptr || value == nullptr || *offset >= size) return false;
  const uint8_t first = data[*offset];
  size_t bytes = 1;
  while (bytes <= 8 && (first & (uint8_t{1} << (bytes - 1))) != 0) ++bytes;
  if (bytes > size - *offset) return false;

  uint64_t decoded = first >> bytes;
  if (bytes > 1) {
    uint64_t trailing = 0;
    for (size_t i = 1; i < bytes; ++i) trailing |= uint64_t{data[*offset + i]} << (8 * (i - 1));
    decoded |= trailing << (bytes == 9 ? 0 : 8 - bytes);
  }
  *offset += bytes;
  *value = decoded;
  return true;
}

bool ReadVarInt(const uint8_t* data, size_t size, size_t* offset, int64_t* value) {
  uint64_t encoded = 0;
  if (!ReadVarUInt(data, size, offset, &encoded) || encoded > uint64_t{INT64_MAX} * 2 + 1) {
    return false;
  }
  *value = static_cast<int64_t>((encoded >> 1) ^ (0 - (encoded & 1)));
  return true;
}

bool ReadTaggedFieldId(const uint8_t* data, size_t size, size_t* offset, uint64_t expected) {
  uint64_t actual = 0;
  return ReadVarUInt(data, size, offset, &actual) && actual == expected;
}

std::string FormatMySQLGtid(const std::array<uint8_t, 16>& sid, const std::string& tag,
                            uint64_t gno) {
  char uuid[37];
  std::snprintf(uuid, sizeof(uuid),
                "%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x", sid[0],
                sid[1], sid[2], sid[3], sid[4], sid[5], sid[6], sid[7], sid[8], sid[9], sid[10],
                sid[11], sid[12], sid[13], sid[14], sid[15]);
  return std::string(uuid) + (tag.empty() ? ":" : ":" + tag + ":") + std::to_string(gno);
}

bool ExtractMySQLGtid(const uint8_t* data, size_t size, std::array<uint8_t, 16>* sid, uint64_t* gno,
                      std::string* formatted) {
  EventHeader header;
  if (!ParseEventHeader(data, size, &header) || header.event_length > size ||
      header.event_length < kEventHeaderSize + kMySQLGtidBodySize) {
    return false;
  }

  const uint8_t* body = data + kEventHeaderSize;
  std::copy_n(body + 1, sid->size(), sid->begin());
  *gno = binary::ReadU64Le(body + 17);
  if (*gno == 0 || *gno > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
    return false;
  }

  *formatted = FormatMySQLGtid(*sid, "", *gno);
  return true;
}

bool ExtractTaggedMySQLGtid(const uint8_t* data, size_t size, bool has_checksum,
                            std::array<uint8_t, 16>* sid, std::string* tag, uint64_t* gno,
                            std::string* formatted) {
  EventHeader header;
  if (!ParseEventHeader(data, size, &header) || header.event_length > size) return false;
  const size_t checksum_size = has_checksum ? kChecksumSize : 0;
  if (header.event_length < kEventHeaderSize + checksum_size) return false;
  const uint8_t* body = data + kEventHeaderSize;
  const size_t body_size = header.event_length - kEventHeaderSize - checksum_size;
  size_t offset = 0;
  uint64_t encoded_size = 0;
  uint64_t last_non_ignorable_field = 0;
  if (!ReadVarUInt(body, body_size, &offset, &encoded_size) || encoded_size != body_size ||
      !ReadVarUInt(body, body_size, &offset, &last_non_ignorable_field) ||
      last_non_ignorable_field < 4 || !ReadTaggedFieldId(body, body_size, &offset, 0)) {
    return false;
  }
  uint64_t flags = 0;
  if (!ReadVarUInt(body, body_size, &offset, &flags) || flags > UINT8_MAX ||
      !ReadTaggedFieldId(body, body_size, &offset, 1)) {
    return false;
  }
  for (uint8_t& byte : *sid) {
    uint64_t value = 0;
    if (!ReadVarUInt(body, body_size, &offset, &value) || value > UINT8_MAX) return false;
    byte = static_cast<uint8_t>(value);
  }
  int64_t signed_gno = 0;
  if (!ReadTaggedFieldId(body, body_size, &offset, 2) ||
      !ReadVarInt(body, body_size, &offset, &signed_gno) || signed_gno <= 0) {
    return false;
  }
  uint64_t tag_size = 0;
  if (!ReadTaggedFieldId(body, body_size, &offset, 3) ||
      !ReadVarUInt(body, body_size, &offset, &tag_size) || tag_size == 0 || tag_size > 32 ||
      tag_size > body_size - offset) {
    return false;
  }
  tag->assign(reinterpret_cast<const char*>(body + offset), static_cast<size_t>(tag_size));
  for (size_t i = 0; i < tag->size(); ++i) {
    const char ch = (*tag)[i];
    if (!((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '_' ||
          ((ch >= '0' && ch <= '9') && i > 0))) {
      return false;
    }
    (*tag)[i] = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
  }
  *gno = static_cast<uint64_t>(signed_gno);
  *formatted = FormatMySQLGtid(*sid, *tag, *gno);
  return true;
}

bool ExtractQueryKeywords(const uint8_t* data, size_t size, bool has_checksum, std::string* first,
                          std::string* second) {
  EventHeader header;
  if (!ParseEventHeader(data, size, &header) || header.event_length > size) return false;
  size_t tail_size = has_checksum ? kChecksumSize : 0;
  if (header.event_length < kEventHeaderSize + 13 + tail_size) return false;

  const uint8_t* body = data + kEventHeaderSize;
  size_t body_len = header.event_length - kEventHeaderSize - tail_size;
  uint8_t db_len = body[8];
  uint16_t status_vars_len = binary::ReadU16Le(body + 11);
  size_t stmt_offset = 13 + static_cast<size_t>(status_vars_len) + db_len + 1;
  if (stmt_offset >= body_len) return false;

  const char* statement = reinterpret_cast<const char*>(body + stmt_offset);
  size_t statement_len = body_len - stmt_offset;
  size_t pos = 0;
  for (;;) {
    while (pos < statement_len && std::isspace(static_cast<unsigned char>(statement[pos])) != 0) {
      ++pos;
    }
    if (pos + 1 < statement_len && statement[pos] == '/' && statement[pos + 1] == '*') {
      size_t end = pos + 2;
      while (end + 1 < statement_len && !(statement[end] == '*' && statement[end + 1] == '/')) {
        ++end;
      }
      if (end + 1 >= statement_len) return false;
      pos = end + 2;
      continue;
    }
    if (pos < statement_len && statement[pos] == '#') {
      while (pos < statement_len && statement[pos] != '\n') ++pos;
      continue;
    }
    if (pos + 1 < statement_len && statement[pos] == '-' && statement[pos + 1] == '-') {
      while (pos < statement_len && statement[pos] != '\n') ++pos;
      continue;
    }
    break;
  }

  auto read_keyword = [&](std::string* keyword) {
    while (pos < statement_len && std::isspace(static_cast<unsigned char>(statement[pos])) != 0) {
      ++pos;
    }
    size_t end = pos;
    while (end < statement_len && std::isalpha(static_cast<unsigned char>(statement[end])) != 0) {
      ++end;
    }
    if (end == pos) return false;
    keyword->assign(statement + pos, statement + end);
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

bool IsDdlKeyword(const std::string& keyword) {
  return keyword == "ALTER" || keyword == "RENAME" || keyword == "DROP" || keyword == "CREATE" ||
         keyword == "TRUNCATE";
}

}  // namespace

void TransactionGtidTracker::Reset() {
  flavor_ = ServerFlavor::kMySQL;
  mysql_set_.Clear();
  mariadb_set_.clear();
  received_gtid_.clear();
  pending_gtid_ = {};
  transaction_open_ = false;
}

bool TransactionGtidTracker::Reset(const std::string& initial_gtid_set, ServerFlavor flavor) {
  Reset();
  flavor_ = flavor;

  if (flavor == ServerFlavor::kMariaDB) {
    std::vector<MariaDBGtid> parsed;
    if (MariaDBGtid::ParseSet(initial_gtid_set, &parsed) != MES_OK) return false;
    // Yielding no GTID is only legitimate for a genuinely empty position. Text
    // of separators or whitespace alone would otherwise seed an empty high-water
    // set, which requests every binlog the server still retains.
    if (parsed.empty() && !initial_gtid_set.empty()) return false;
    for (const auto& gtid : parsed) MergeMariaDBGtid(&mariadb_set_, gtid);
    return true;
  }

  return GtidSet::Parse(initial_gtid_set, &mysql_set_) == MES_OK;
}

void TransactionGtidTracker::MergeMariaDBGtid(MariaDBSet* set, const MariaDBGtid& gtid) {
  auto it = set->find(gtid.domain_id);
  if (it == set->end() || gtid.sequence_no > it->second.sequence_no) {
    (*set)[gtid.domain_id] = gtid;
  }
}

std::string TransactionGtidTracker::FormatMySQLSet(const MySQLSet& set) { return set.ToString(); }

std::string TransactionGtidTracker::FormatMariaDBSet(const MariaDBSet& set) {
  std::vector<MariaDBGtid> gtids;
  gtids.reserve(set.size());
  for (const auto& [domain, gtid] : set) {
    (void)domain;
    gtids.push_back(gtid);
  }
  return MariaDBGtid::SetToString(gtids);
}

std::string TransactionGtidTracker::FormatCurrentSet() const {
  return flavor_ == ServerFlavor::kMariaDB ? FormatMariaDBSet(mariadb_set_)
                                           : FormatMySQLSet(mysql_set_);
}

std::string TransactionGtidTracker::CommitPending() {
  if (!pending_gtid_.present) return {};

  flavor_ = pending_gtid_.flavor;
  if (flavor_ == ServerFlavor::kMariaDB) {
    MergeMariaDBGtid(&mariadb_set_, pending_gtid_.mariadb);
  } else {
    if (!mysql_set_.Add(pending_gtid_.sid, pending_gtid_.tag,
                        {pending_gtid_.sequence_no, pending_gtid_.sequence_no + 1})) {
      // The committed GTID cannot join the set, so no checkpoint may advance.
      // Keep it pending and report it: dropping it silently would freeze the
      // checkpoint at an older position with no way to notice.
      StructuredLog()
          .Event("gtid_checkpoint_merge_failed")
          .Field("reason", "sid_interval_capacity_exhausted")
          .Field("gtid",
                 FormatMySQLGtid(pending_gtid_.sid, pending_gtid_.tag, pending_gtid_.sequence_no))
          .Field("max_intervals_per_sid", static_cast<uint64_t>(GtidSet::kMaxIntervalsPerSid))
          .Error();
      return {};
    }
  }
  pending_gtid_ = {};
  return FormatCurrentSet();
}

std::string TransactionGtidTracker::Observe(const uint8_t* data, size_t size, bool has_checksum) {
  if (data == nullptr || size < kEventHeaderSize) return {};

  EventHeader header;
  if (!ParseEventHeader(data, size, &header) || header.event_length > size) return {};
  const size_t tail_size = has_checksum ? kChecksumSize : 0;
  if (header.event_length < kEventHeaderSize + tail_size) return {};
  const size_t content_size = header.event_length - tail_size;
  const uint8_t event_type = header.type_code;

  if (event_type == static_cast<uint8_t>(BinlogEventType::kPreviousGtidsEvent)) {
    const size_t payload_size = content_size - kEventHeaderSize;
    MySQLSet baseline;
    if (!GtidSet::DecodeBinary(data + kEventHeaderSize, payload_size, &baseline)) return {};
    MySQLSet merged = mysql_set_;
    if (!merged.Merge(baseline)) return {};
    mysql_set_ = std::move(merged);
    flavor_ = ServerFlavor::kMySQL;
    return FormatCurrentSet();
  }

  if (event_type == static_cast<uint8_t>(BinlogEventType::kMariaDBGtidListEvent)) {
    std::vector<MariaDBGtid> baseline;
    if (MariaDBEventParser::ParseGtidList(data, content_size, &baseline) != MES_OK) {
      return {};
    }
    MariaDBSet merged = mariadb_set_;
    for (const auto& gtid : baseline) MergeMariaDBGtid(&merged, gtid);
    mariadb_set_ = std::move(merged);
    flavor_ = ServerFlavor::kMariaDB;
    return FormatCurrentSet();
  }

  if (event_type == static_cast<uint8_t>(BinlogEventType::kGtidLogEvent)) {
    Sid sid{};
    uint64_t gno = 0;
    std::string gtid;
    if (!ExtractMySQLGtid(data, size, &sid, &gno, &gtid)) return {};
    // Seeing the next GTID proves the preceding transaction group ended even
    // if it used a standalone event type we do not classify explicitly.
    std::string committed = CommitPending();
    transaction_open_ = false;
    flavor_ = ServerFlavor::kMySQL;
    received_gtid_ = std::move(gtid);
    pending_gtid_ = {true, ServerFlavor::kMySQL, sid, "", gno, {}};
    return committed;
  }

  if (event_type == static_cast<uint8_t>(BinlogEventType::kGtidTaggedLogEvent)) {
    Sid sid{};
    std::string tag;
    uint64_t gno = 0;
    std::string gtid;
    if (!ExtractTaggedMySQLGtid(data, size, has_checksum, &sid, &tag, &gno, &gtid)) return {};
    std::string committed = CommitPending();
    transaction_open_ = false;
    flavor_ = ServerFlavor::kMySQL;
    received_gtid_ = std::move(gtid);
    pending_gtid_ = {true, ServerFlavor::kMySQL, sid, std::move(tag), gno, {}};
    return committed;
  }

  if (event_type == static_cast<uint8_t>(BinlogEventType::kMariaDBGtidEvent)) {
    std::string gtid_string;
    bool standalone = false;
    if (MariaDBEventParser::ExtractGtid(data, content_size, &gtid_string, &standalone) != MES_OK) {
      return {};
    }
    MariaDBGtid gtid;
    if (MariaDBGtid::Parse(gtid_string, &gtid) != MES_OK) return {};
    std::string committed = CommitPending();
    transaction_open_ = false;
    flavor_ = ServerFlavor::kMariaDB;
    received_gtid_ = std::move(gtid_string);
    // FL_STANDALONE only says the group ends without a COMMIT/XID event; the
    // payload of the group still arrives after this event. Checkpointing here
    // would advertise a GTID whose rows the consumer has not seen, so the
    // group is closed by its terminating event instead.
    pending_gtid_ = {true, ServerFlavor::kMariaDB, {}, "", 0, gtid, standalone};
    return committed;
  }

  if (event_type == static_cast<uint8_t>(BinlogEventType::kAnonymousGtidLogEvent)) {
    std::string committed = CommitPending();
    transaction_open_ = false;
    received_gtid_.clear();
    pending_gtid_ = {};
    return committed;
  }

  if (event_type == static_cast<uint8_t>(BinlogEventType::kXidEvent)) {
    transaction_open_ = false;
    return CommitPending();
  }

  if (IsRowEvent(event_type)) {
    // A standalone group has no COMMIT/XID, so its last ROWS_EVENT (the one
    // carrying STMT_END) is the point at which every event of the group has
    // been seen. Inside an open transaction STMT_END only ends a statement,
    // never the group.
    if (pending_gtid_.standalone && !transaction_open_ &&
        IsStatementEndRowsEvent(data, content_size)) {
      return CommitPending();
    }
    return {};
  }

  if (event_type != static_cast<uint8_t>(BinlogEventType::kQueryEvent)) return {};

  std::string first;
  std::string second;
  if (!ExtractQueryKeywords(data, size, has_checksum, &first, &second)) return {};
  if (first == "BEGIN" || (first == "START" && second == "TRANSACTION")) {
    transaction_open_ = true;
    return {};
  }
  if (first == "COMMIT" || (first == "ROLLBACK" && second != "TO")) {
    transaction_open_ = false;
    return CommitPending();
  }
  // DDL normally has an implicit transaction boundary, but never promote a
  // pending checkpoint merely because a DDL word appears inside an explicitly
  // open transaction group. In particular, ROLLBACK TO SAVEPOINT is not a
  // transaction rollback and must leave its GTID pending until COMMIT/XID.
  // A standalone group carries exactly one statement, so this QUERY is its
  // terminating event whether or not the statement is DDL.
  if (transaction_open_) return {};
  if (pending_gtid_.standalone || IsDdlKeyword(first)) return CommitPending();
  return {};
}

}  // namespace mes
