// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * @file mariadb_gtid.cpp
 * @brief MariaDB GTID parsing and representation implementation
 */

#include "mariadb_gtid.h"

#include <algorithm>
#include <charconv>
#include <string_view>

namespace mes {

namespace {

/** @brief Trim whitespace from both ends of a string_view. */
std::string_view Trim(std::string_view sv) {
  while (!sv.empty() &&
         (sv.front() == ' ' || sv.front() == '\t' || sv.front() == '\n' || sv.front() == '\r')) {
    sv.remove_prefix(1);
  }
  while (!sv.empty() &&
         (sv.back() == ' ' || sv.back() == '\t' || sv.back() == '\n' || sv.back() == '\r')) {
    sv.remove_suffix(1);
  }
  return sv;
}

/** @brief Count occurrences of a character in a string_view. */
size_t CountChar(std::string_view sv, char ch) {
  size_t count = 0;
  for (char c : sv) {
    if (c == ch) {
      ++count;
    }
  }
  return count;
}

/** @brief Check if all characters in a string_view are digits. */
bool AllDigits(std::string_view sv) {
  return !sv.empty() &&
         std::all_of(sv.begin(), sv.end(), [](char c) { return c >= '0' && c <= '9'; });
}

}  // namespace

mes_error_t MariaDBGtid::Parse(const std::string& gtid_str, MariaDBGtid* out) {
  if (out == nullptr) {
    return MES_ERR_NULL_ARG;
  }

  std::string_view sv = Trim(std::string_view(gtid_str));

  if (sv.empty()) {
    return MES_ERR_INVALID_ARG;
  }

  // MariaDB GTID format: "domain_id-server_id-sequence_no"
  // Must have exactly 2 dashes
  if (CountChar(sv, '-') != 2) {
    return MES_ERR_INVALID_ARG;
  }

  // Split by dashes
  auto first_dash = sv.find('-');
  auto second_dash = sv.find('-', first_dash + 1);

  std::string_view domain_str = sv.substr(0, first_dash);
  std::string_view server_str = sv.substr(first_dash + 1, second_dash - first_dash - 1);
  std::string_view seq_str = sv.substr(second_dash + 1);

  // Validate all segments are numeric
  if (!AllDigits(domain_str) || !AllDigits(server_str) || !AllDigits(seq_str)) {
    return MES_ERR_INVALID_ARG;
  }

  MariaDBGtid gtid;

  // Parse domain_id (uint32_t)
  auto [ptr1, ec1] =
      std::from_chars(domain_str.data(), domain_str.data() + domain_str.size(), gtid.domain_id);
  if (ec1 != std::errc{}) {
    return MES_ERR_INVALID_ARG;
  }

  // Parse server_id (uint32_t)
  auto [ptr2, ec2] =
      std::from_chars(server_str.data(), server_str.data() + server_str.size(), gtid.server_id);
  if (ec2 != std::errc{}) {
    return MES_ERR_INVALID_ARG;
  }

  // Parse sequence_no (uint64_t)
  auto [ptr3, ec3] =
      std::from_chars(seq_str.data(), seq_str.data() + seq_str.size(), gtid.sequence_no);
  if (ec3 != std::errc{}) {
    return MES_ERR_INVALID_ARG;
  }

  *out = gtid;
  return MES_OK;
}

mes_error_t MariaDBGtid::ParseSet(const std::string& gtid_set_str, std::vector<MariaDBGtid>* out) {
  if (out == nullptr) {
    return MES_ERR_NULL_ARG;
  }

  std::string_view sv = Trim(std::string_view(gtid_set_str));

  if (sv.empty()) {
    out->clear();
    return MES_OK;
  }

  std::vector<MariaDBGtid> result;

  // Split by commas
  size_t start = 0;
  while (start < sv.size()) {
    size_t comma_pos = sv.find(',', start);
    std::string_view entry;
    if (comma_pos == std::string_view::npos) {
      entry = sv.substr(start);
      start = sv.size();
    } else {
      entry = sv.substr(start, comma_pos - start);
      start = comma_pos + 1;
    }

    entry = Trim(entry);
    if (entry.empty()) {
      continue;
    }

    MariaDBGtid gtid;
    mes_error_t err = Parse(std::string(entry), &gtid);
    if (err != MES_OK) {
      return err;
    }
    result.push_back(gtid);
  }

  *out = std::move(result);
  return MES_OK;
}

std::string MariaDBGtid::ToString() const {
  return std::to_string(domain_id) + "-" + std::to_string(server_id) + "-" +
         std::to_string(sequence_no);
}

std::string MariaDBGtid::SetToString(const std::vector<MariaDBGtid>& gtids) {
  std::string result;
  for (size_t i = 0; i < gtids.size(); ++i) {
    if (i > 0) {
      result += ",";
    }
    result += gtids[i].ToString();
  }
  return result;
}

bool MariaDBGtid::IsMariaDBGtidFormat(const std::string& gtid_str) {
  std::string_view sv = Trim(std::string_view(gtid_str));
  if (sv.empty()) {
    return false;
  }

  // MariaDB GTID: exactly 2 dashes, all segments numeric
  // MySQL GTID: 4 dashes (UUID format), hex segments
  if (CountChar(sv, '-') != 2) {
    return false;
  }

  auto first_dash = sv.find('-');
  auto second_dash = sv.find('-', first_dash + 1);

  std::string_view domain_str = sv.substr(0, first_dash);
  std::string_view server_str = sv.substr(first_dash + 1, second_dash - first_dash - 1);
  std::string_view seq_str = sv.substr(second_dash + 1);

  return AllDigits(domain_str) && AllDigits(server_str) && AllDigits(seq_str);
}

}  // namespace mes
