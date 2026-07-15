// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#include "client/transaction_gtid_tracker.h"

#include <algorithm>
#include <cctype>
#include <cstdio>
#include <limits>
#include <utility>

#include "binary_util.h"
#include "client/gtid_encoder.h"
#include "event_header.h"
#include "mariadb_event_parser.h"

namespace mes {
namespace {

constexpr size_t kMySQLGtidBodySize = 25;

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

  char uuid[37];
  std::snprintf(
      uuid, sizeof(uuid), "%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
      (*sid)[0], (*sid)[1], (*sid)[2], (*sid)[3], (*sid)[4], (*sid)[5], (*sid)[6], (*sid)[7],
      (*sid)[8], (*sid)[9], (*sid)[10], (*sid)[11], (*sid)[12], (*sid)[13], (*sid)[14], (*sid)[15]);
  *formatted = std::string(uuid) + ":" + std::to_string(*gno);
  return true;
}

bool ExtractQueryKeyword(const uint8_t* data, size_t size, bool has_checksum,
                         std::string* keyword) {
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

  size_t end = pos;
  while (end < statement_len && std::isalpha(static_cast<unsigned char>(statement[end])) != 0) {
    ++end;
  }
  if (end == pos) return false;
  keyword->assign(statement + pos, statement + end);
  std::transform(keyword->begin(), keyword->end(), keyword->begin(),
                 [](unsigned char ch) { return static_cast<char>(std::toupper(ch)); });
  return true;
}

bool IsQueryCommitBoundary(const uint8_t* data, size_t size, bool has_checksum) {
  std::string keyword;
  if (!ExtractQueryKeyword(data, size, has_checksum, &keyword)) return false;
  return keyword == "COMMIT" || keyword == "ROLLBACK" || keyword == "ALTER" ||
         keyword == "RENAME" || keyword == "DROP" || keyword == "CREATE" || keyword == "TRUNCATE";
}

}  // namespace

void TransactionGtidTracker::Reset() {
  flavor_ = ServerFlavor::kMySQL;
  mysql_set_.clear();
  mariadb_set_.clear();
  received_gtid_.clear();
  pending_gtid_ = {};
}

bool TransactionGtidTracker::Reset(const std::string& initial_gtid_set, ServerFlavor flavor) {
  Reset();
  flavor_ = flavor;

  if (flavor == ServerFlavor::kMariaDB) {
    std::vector<MariaDBGtid> parsed;
    if (MariaDBGtid::ParseSet(initial_gtid_set, &parsed) != MES_OK) return false;
    for (const auto& gtid : parsed) MergeMariaDBGtid(&mariadb_set_, gtid);
    return true;
  }

  std::vector<uint8_t> encoded;
  if (GtidEncoder::Encode(initial_gtid_set.c_str(), &encoded) != MES_OK) return false;
  return DecodeMySQLSet(encoded.data(), encoded.size(), &mysql_set_);
}

bool TransactionGtidTracker::DecodeMySQLSet(const uint8_t* data, size_t size, MySQLSet* out) {
  if (data == nullptr || out == nullptr || size < sizeof(uint64_t)) return false;

  size_t offset = 0;
  const uint64_t sid_count = binary::ReadU64Le(data);
  offset += sizeof(uint64_t);
  // Every SID needs a UUID, an interval count, and at least one interval.
  constexpr size_t kMinSidSize = 16 + sizeof(uint64_t) + 2 * sizeof(uint64_t);
  if (sid_count > (size - offset) / kMinSidSize) return false;

  MySQLSet decoded;
  for (uint64_t sid_index = 0; sid_index < sid_count; ++sid_index) {
    if (size - offset < 16 + sizeof(uint64_t)) return false;
    Sid sid{};
    std::copy_n(data + offset, sid.size(), sid.begin());
    offset += sid.size();

    const uint64_t interval_count = binary::ReadU64Le(data + offset);
    offset += sizeof(uint64_t);
    constexpr size_t kIntervalSize = 2 * sizeof(uint64_t);
    if (interval_count == 0 || interval_count > (size - offset) / kIntervalSize) {
      return false;
    }

    auto& intervals = decoded[sid];
    for (uint64_t interval_index = 0; interval_index < interval_count; ++interval_index) {
      const uint64_t start = binary::ReadU64Le(data + offset);
      const uint64_t end = binary::ReadU64Le(data + offset + sizeof(uint64_t));
      offset += kIntervalSize;
      if (start == 0 || end <= start) return false;
      MergeInterval(&intervals, {start, end});
    }
  }

  if (offset != size) return false;
  *out = std::move(decoded);
  return true;
}

void TransactionGtidTracker::MergeInterval(std::vector<Interval>* intervals, Interval interval) {
  intervals->push_back(interval);
  std::sort(intervals->begin(), intervals->end(), [](const Interval& lhs, const Interval& rhs) {
    return lhs.start < rhs.start || (lhs.start == rhs.start && lhs.end < rhs.end);
  });

  size_t write = 0;
  for (const auto& candidate : *intervals) {
    if (write == 0 || candidate.start > (*intervals)[write - 1].end) {
      (*intervals)[write++] = candidate;
    } else if (candidate.end > (*intervals)[write - 1].end) {
      (*intervals)[write - 1].end = candidate.end;
    }
  }
  intervals->resize(write);
}

void TransactionGtidTracker::MergeMariaDBGtid(MariaDBSet* set, const MariaDBGtid& gtid) {
  auto it = set->find(gtid.domain_id);
  if (it == set->end() || gtid.sequence_no > it->second.sequence_no) {
    (*set)[gtid.domain_id] = gtid;
  }
}

std::string TransactionGtidTracker::FormatMySQLSet(const MySQLSet& set) {
  std::string result;
  for (const auto& [sid, intervals] : set) {
    if (intervals.empty()) continue;
    char uuid[37];
    std::snprintf(uuid, sizeof(uuid),
                  "%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x", sid[0],
                  sid[1], sid[2], sid[3], sid[4], sid[5], sid[6], sid[7], sid[8], sid[9], sid[10],
                  sid[11], sid[12], sid[13], sid[14], sid[15]);
    if (!result.empty()) result += ',';
    result += uuid;
    for (const auto& interval : intervals) {
      result += ':';
      result += std::to_string(interval.start);
      if (interval.end - interval.start != 1) {
        result += '-';
        result += std::to_string(interval.end - 1);
      }
    }
  }
  return result;
}

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
    MergeInterval(&mysql_set_[pending_gtid_.sid],
                  {pending_gtid_.sequence_no, pending_gtid_.sequence_no + 1});
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
    if (!DecodeMySQLSet(data + kEventHeaderSize, payload_size, &baseline)) return {};
    MySQLSet merged = mysql_set_;
    for (const auto& [sid, intervals] : baseline) {
      for (const auto& interval : intervals) MergeInterval(&merged[sid], interval);
    }
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
    flavor_ = ServerFlavor::kMySQL;
    received_gtid_ = std::move(gtid);
    pending_gtid_ = {true, ServerFlavor::kMySQL, sid, gno, {}};
    return committed;
  }

  if (event_type == static_cast<uint8_t>(BinlogEventType::kMariaDBGtidEvent)) {
    std::string gtid_string;
    if (MariaDBEventParser::ExtractGtid(data, content_size, &gtid_string) != MES_OK) return {};
    MariaDBGtid gtid;
    if (MariaDBGtid::Parse(gtid_string, &gtid) != MES_OK) return {};
    std::string committed = CommitPending();
    flavor_ = ServerFlavor::kMariaDB;
    received_gtid_ = std::move(gtid_string);
    pending_gtid_ = {true, ServerFlavor::kMariaDB, {}, 0, gtid};
    return committed;
  }

  if (event_type == static_cast<uint8_t>(BinlogEventType::kAnonymousGtidLogEvent)) {
    std::string committed = CommitPending();
    received_gtid_.clear();
    pending_gtid_ = {};
    return committed;
  }

  const bool commit_boundary = event_type == static_cast<uint8_t>(BinlogEventType::kXidEvent) ||
                               (event_type == static_cast<uint8_t>(BinlogEventType::kQueryEvent) &&
                                IsQueryCommitBoundary(data, size, has_checksum));
  return commit_boundary ? CommitPending() : std::string{};
}

}  // namespace mes
