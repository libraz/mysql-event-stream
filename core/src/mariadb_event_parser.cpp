// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * @file mariadb_event_parser.cpp
 * @brief MariaDB-specific binlog event parsing implementation
 */

#include "mariadb_event_parser.h"

#include "binary_util.h"

namespace mes {

/// Minimum size of MARIADB_GTID_EVENT post-header: seq_no(8) + domain_id(4) + flags(1)
static constexpr size_t kMariaDBGtidEventMinPostHeader = 13;

/// Size of each entry in GTID_LIST_EVENT: domain_id(4) + server_id(4) + seq_no(8)
static constexpr size_t kGtidListEntrySize = 16;

/// Mask for extracting entry count from GTID_LIST count_and_flags field (lower 28 bits)
static constexpr uint32_t kGtidListCountMask = 0x0FFFFFFFu;

mes_error_t MariaDBEventParser::ExtractGtid(const uint8_t* buffer, size_t length,
                                            std::string* out) {
  if (buffer == nullptr || out == nullptr) {
    return MES_ERR_NULL_ARG;
  }

  // Need at least header + seq_no(8) + domain_id(4) + flags(1)
  if (length < kEventHeaderSize + kMariaDBGtidEventMinPostHeader) {
    return MES_ERR_PARSE;
  }

  // Extract server_id from event header (bytes 5-8, little-endian)
  uint32_t server_id = binary::ReadU32Le(buffer + 5);

  // Post-header starts after 19-byte event header
  const uint8_t* post_header = buffer + kEventHeaderSize;

  // seq_no: 8 bytes at offset 0 (little-endian uint64)
  uint64_t seq_no = binary::ReadU64Le(post_header);

  // domain_id: 4 bytes at offset 8 (little-endian uint32)
  uint32_t domain_id = binary::ReadU32Le(post_header + 8);

  // Construct "domain-server-seq" format
  MariaDBGtid gtid;
  gtid.domain_id = domain_id;
  gtid.server_id = server_id;
  gtid.sequence_no = seq_no;
  *out = gtid.ToString();
  return MES_OK;
}

mes_error_t MariaDBEventParser::ParseGtidList(const uint8_t* buffer, size_t length,
                                              std::vector<MariaDBGtid>* out) {
  if (buffer == nullptr || out == nullptr) {
    return MES_ERR_NULL_ARG;
  }

  // Need at least header + count_and_flags(4)
  if (length < kEventHeaderSize + 4) {
    return MES_ERR_PARSE;
  }

  const uint8_t* post_header = buffer + kEventHeaderSize;

  // count_and_flags: 4 bytes (lower 28 bits = count)
  uint32_t count_and_flags = binary::ReadU32Le(post_header);
  uint32_t count = count_and_flags & kGtidListCountMask;

  // Guard against size_t overflow on 32-bit platforms (e.g., WASM)
  static constexpr uint32_t kMaxGtidListCount = 1000000;
  if (count > kMaxGtidListCount) {
    return MES_ERR_PARSE;
  }

  // Validate we have enough data for all entries
  size_t entries_offset = kEventHeaderSize + 4;
  size_t required_size = entries_offset + (static_cast<size_t>(count) * kGtidListEntrySize);
  if (length < required_size) {
    return MES_ERR_PARSE;
  }

  std::vector<MariaDBGtid> result;
  result.reserve(count);

  const uint8_t* entry_ptr = buffer + entries_offset;
  for (uint32_t i = 0; i < count; ++i) {
    MariaDBGtid gtid;
    gtid.domain_id = binary::ReadU32Le(entry_ptr);
    gtid.server_id = binary::ReadU32Le(entry_ptr + 4);
    gtid.sequence_no = binary::ReadU64Le(entry_ptr + 8);
    result.push_back(gtid);
    entry_ptr += kGtidListEntrySize;
  }

  *out = std::move(result);
  return MES_OK;
}

mes_error_t MariaDBEventParser::ExtractAnnotateRows(const uint8_t* buffer, size_t length,
                                                    bool has_checksum, std::string* out) {
  if (buffer == nullptr || out == nullptr) {
    return MES_ERR_NULL_ARG;
  }

  size_t tail_size = has_checksum ? kChecksumSize : 0;

  // Need at least header + 1 byte of text + optional CRC32
  if (length <= kEventHeaderSize + tail_size) {
    return MES_ERR_PARSE;
  }

  size_t text_length = length - kEventHeaderSize - tail_size;

  if (text_length == 0) {
    return MES_ERR_PARSE;
  }

  // NOTE(review): ANNOTATE_ROWS payload is the original SQL text in the
  // server's character set (typically UTF-8 on modern MariaDB). Storing
  // it as a std::string preserves the raw bytes verbatim; downstream
  // consumers must handle potential non-UTF-8 bytes if the server
  // character_set_client differs.
  *out = std::string(reinterpret_cast<const char*>(buffer + kEventHeaderSize), text_length);
  return MES_OK;
}

}  // namespace mes
