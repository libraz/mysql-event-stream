// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#include "client/gtid_encoder.h"

#include <algorithm>
#include <cerrno>
#include <climits>
#include <cstdlib>
#include <cstring>
#include <map>
#include <string>
#include <vector>

namespace mes {

mes_error_t GtidEncoder::Encode(const char* gtid_set,
                                  std::vector<uint8_t>* out) {
  if (out == nullptr) {
    return MES_ERR_NULL_ARG;
  }
  if (gtid_set == nullptr) {
    return MES_ERR_NULL_ARG;
  }

  out->clear();

  // Empty GTID set: 8 zero bytes (n_sids = 0)
  if (gtid_set[0] == '\0') {
    out->resize(8, 0);
    return MES_OK;
  }

  std::vector<Sid> sids;

  // Split by comma for multiple SIDs
  std::string input(gtid_set);
  size_t pos = 0;
  while (pos < input.size()) {
    // Find next comma
    size_t comma_pos = input.find(',', pos);
    std::string sid_part;
    if (comma_pos == std::string::npos) {
      sid_part = input.substr(pos);
      pos = input.size();
    } else {
      sid_part = input.substr(pos, comma_pos - pos);
      pos = comma_pos + 1;
    }

    // Trim whitespace
    size_t start = sid_part.find_first_not_of(" \t\n\r");
    size_t end = sid_part.find_last_not_of(" \t\n\r");
    if (start == std::string::npos) {
      continue;
    }
    sid_part = sid_part.substr(start, end - start + 1);

    // Find the colon separating UUID from intervals
    size_t colon_pos = sid_part.find(':');
    if (colon_pos == std::string::npos) {
      return MES_ERR_INVALID_ARG;
    }

    Sid sid;
    std::string uuid_str = sid_part.substr(0, colon_pos);
    std::string intervals_str = sid_part.substr(colon_pos + 1);

    // Parse UUID
    mes_error_t err = ParseUuid(uuid_str.c_str(), sid.uuid.data());
    if (err != MES_OK) {
      return err;
    }

    // Parse intervals (colon-separated, e.g. "1-3:5-7:9")
    size_t ipos = 0;
    while (ipos < intervals_str.size()) {
      size_t next_colon = intervals_str.find(':', ipos);
      std::string interval_str;
      if (next_colon == std::string::npos) {
        interval_str = intervals_str.substr(ipos);
        ipos = intervals_str.size();
      } else {
        interval_str = intervals_str.substr(ipos, next_colon - ipos);
        ipos = next_colon + 1;
      }

      if (!interval_str.empty()) {
        Interval interval{};
        err = ParseInterval(interval_str.c_str(), &interval);
        if (err != MES_OK) {
          return err;
        }
        sid.intervals.push_back(interval);
      }
    }

    // SID must have at least one interval
    if (sid.intervals.empty()) {
      return MES_ERR_INVALID_ARG;
    }

    MergeIntervals(sid.intervals);
    sids.push_back(std::move(sid));
  }

  // Merge SIDs with identical UUIDs
  if (sids.size() > 1) {
    std::map<std::array<uint8_t, 16>, std::vector<Interval>> merged_map;
    for (auto& sid : sids) {
      auto& intervals = merged_map[sid.uuid];
      intervals.insert(intervals.end(), sid.intervals.begin(),
                       sid.intervals.end());
    }

    sids.clear();
    for (auto& entry : merged_map) {
      Sid merged_sid;
      merged_sid.uuid = entry.first;
      merged_sid.intervals = std::move(entry.second);
      MergeIntervals(merged_sid.intervals);
      sids.push_back(std::move(merged_sid));
    }
  }

  // Calculate total size
  size_t total_size = 8;  // n_sids
  for (const auto& sid : sids) {
    total_size += 16;                              // UUID
    total_size += 8;                               // n_intervals
    total_size += 16 * sid.intervals.size();       // intervals (start+end)
  }

  // Encode to binary
  out->reserve(total_size);

  // Number of SIDs
  StoreInt64Le(*out, sids.size());

  // Each SID
  for (const auto& sid : sids) {
    // UUID (16 bytes)
    out->insert(out->end(), sid.uuid.begin(), sid.uuid.end());

    // Number of intervals
    StoreInt64Le(*out, sid.intervals.size());

    // Each interval
    for (const auto& interval : sid.intervals) {
      StoreInt64Le(*out, static_cast<uint64_t>(interval.start));
      StoreInt64Le(*out, static_cast<uint64_t>(interval.end));
    }
  }

  return MES_OK;
}

std::string GtidEncoder::ConvertSingleGtidToRange(const std::string& gtid) {
  if (gtid.empty()) {
    return gtid;
  }

  // Multi-UUID (contains comma) - pass through
  if (gtid.find(',') != std::string::npos) {
    return gtid;
  }

  // Find the colon separating UUID from transaction number
  size_t colon_pos = gtid.find(':');
  if (colon_pos == std::string::npos) {
    return gtid;
  }

  std::string after_colon = gtid.substr(colon_pos + 1);

  // Already a range (contains dash) - pass through
  if (after_colon.find('-') != std::string::npos) {
    return gtid;
  }

  // Tagged GTID or multiple intervals (contains another colon) - pass through
  if (after_colon.find(':') != std::string::npos) {
    return gtid;
  }

  // Single GTID "uuid:N" -> "uuid:1-N"
  std::string uuid = gtid.substr(0, colon_pos);
  return uuid + ":1-" + after_colon;
}

mes_error_t GtidEncoder::ParseUuid(const char* str, uint8_t* out) {
  // Expected format: "61d5b289-bccc-11f0-b921-cabbb4ee51f6" (36 chars)
  size_t len = std::strlen(str);
  if (len != 36) {
    return MES_ERR_INVALID_ARG;
  }

  // Remove dashes and collect hex digits
  char hex[33];
  size_t hex_len = 0;
  for (size_t i = 0; i < 36; ++i) {
    if (str[i] != '-') {
      if (hex_len >= 32) {
        return MES_ERR_INVALID_ARG;
      }
      hex[hex_len++] = str[i];
    }
  }
  hex[hex_len] = '\0';

  if (hex_len != 32) {
    return MES_ERR_INVALID_ARG;
  }

  // Convert hex string to bytes
  for (size_t i = 0; i < 16; ++i) {
    char byte_str[3] = {hex[i * 2], hex[i * 2 + 1], '\0'};
    char* end = nullptr;
    unsigned long byte_val = std::strtoul(byte_str, &end, 16);
    if (end != byte_str + 2) {
      return MES_ERR_INVALID_ARG;
    }
    out[i] = static_cast<uint8_t>(byte_val);
  }

  return MES_OK;
}

mes_error_t GtidEncoder::ParseInterval(const char* str, Interval* out) {
  // Trim whitespace
  std::string trimmed(str);
  size_t start_pos = trimmed.find_first_not_of(" \t\n\r");
  size_t end_pos = trimmed.find_last_not_of(" \t\n\r");
  if (start_pos == std::string::npos) {
    return MES_ERR_INVALID_ARG;
  }
  trimmed = trimmed.substr(start_pos, end_pos - start_pos + 1);

  size_t dash_pos = trimmed.find('-');

  if (dash_pos == std::string::npos) {
    // Single transaction number (e.g., "5")
    char* end = nullptr;
    errno = 0;
    long long val = std::strtoll(trimmed.c_str(), &end, 10);
    if (end == trimmed.c_str() || *end != '\0' || errno == ERANGE) {
      return MES_ERR_INVALID_ARG;
    }
    if (val <= 0) {
      return MES_ERR_INVALID_ARG;
    }
    // Check overflow before adding 1
    if (val >= INT64_MAX) {
      return MES_ERR_INVALID_ARG;
    }
    out->start = val;
    out->end = val + 1;  // exclusive
  } else {
    // Range (e.g., "1-3")
    std::string start_str = trimmed.substr(0, dash_pos);
    std::string end_str = trimmed.substr(dash_pos + 1);

    char* endp = nullptr;
    errno = 0;
    long long start_val = std::strtoll(start_str.c_str(), &endp, 10);
    if (endp == start_str.c_str() || *endp != '\0' || errno == ERANGE) {
      return MES_ERR_INVALID_ARG;
    }

    endp = nullptr;
    errno = 0;
    long long end_val = std::strtoll(end_str.c_str(), &endp, 10);
    if (endp == end_str.c_str() || *endp != '\0' || errno == ERANGE) {
      return MES_ERR_INVALID_ARG;
    }

    if (start_val <= 0) {
      return MES_ERR_INVALID_ARG;
    }
    // Check overflow before adding 1
    if (end_val >= INT64_MAX) {
      return MES_ERR_INVALID_ARG;
    }

    out->start = start_val;
    out->end = end_val + 1;  // convert to exclusive

    if (out->end <= out->start) {
      return MES_ERR_INVALID_ARG;
    }
  }

  return MES_OK;
}

void GtidEncoder::MergeIntervals(std::vector<Interval>& intervals) {
  if (intervals.size() <= 1) return;
  std::sort(intervals.begin(), intervals.end(),
            [](const Interval& a, const Interval& b) {
              return a.start < b.start;
            });
  std::vector<Interval> merged;
  merged.push_back(intervals[0]);
  for (size_t i = 1; i < intervals.size(); ++i) {
    if (intervals[i].start <= merged.back().end) {
      merged.back().end = std::max(merged.back().end, intervals[i].end);
    } else {
      merged.push_back(intervals[i]);
    }
  }
  intervals = std::move(merged);
}

void GtidEncoder::StoreInt64Le(std::vector<uint8_t>& buf, uint64_t val) {
  buf.push_back(static_cast<uint8_t>(val & 0xFF));
  buf.push_back(static_cast<uint8_t>((val >> 8) & 0xFF));
  buf.push_back(static_cast<uint8_t>((val >> 16) & 0xFF));
  buf.push_back(static_cast<uint8_t>((val >> 24) & 0xFF));
  buf.push_back(static_cast<uint8_t>((val >> 32) & 0xFF));
  buf.push_back(static_cast<uint8_t>((val >> 40) & 0xFF));
  buf.push_back(static_cast<uint8_t>((val >> 48) & 0xFF));
  buf.push_back(static_cast<uint8_t>((val >> 56) & 0xFF));
}

}  // namespace mes
