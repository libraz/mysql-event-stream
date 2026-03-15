// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * @file test_helpers.h
 * @brief Shared test utilities for building binlog events
 *
 * Provides an EventBuilder class and helper functions for constructing
 * synthetic binlog events in unit tests.
 */

#ifndef MES_CORE_TESTS_TEST_HELPERS_H_
#define MES_CORE_TESTS_TEST_HELPERS_H_

#include <cstdint>
#include <string>
#include <vector>

#include "event_header.h"

namespace mes {
namespace test {

// Helper to write little-endian values for event construction.
class EventBuilder {
 public:
  void WriteU8(uint8_t v) { buf_.push_back(v); }
  void WriteU16Le(uint16_t v) {
    buf_.push_back(static_cast<uint8_t>(v));
    buf_.push_back(static_cast<uint8_t>(v >> 8));
  }
  void WriteU24Le(uint32_t v) {
    buf_.push_back(static_cast<uint8_t>(v));
    buf_.push_back(static_cast<uint8_t>(v >> 8));
    buf_.push_back(static_cast<uint8_t>(v >> 16));
  }
  void WriteU32Le(uint32_t v) {
    buf_.push_back(static_cast<uint8_t>(v));
    buf_.push_back(static_cast<uint8_t>(v >> 8));
    buf_.push_back(static_cast<uint8_t>(v >> 16));
    buf_.push_back(static_cast<uint8_t>(v >> 24));
  }
  void WriteU48Le(uint64_t v) {
    WriteU32Le(static_cast<uint32_t>(v));
    WriteU16Le(static_cast<uint16_t>(v >> 32));
  }
  void WriteU64Le(uint64_t v) {
    WriteU32Le(static_cast<uint32_t>(v));
    WriteU32Le(static_cast<uint32_t>(v >> 32));
  }
  void WriteString(const std::string& s) {
    buf_.insert(buf_.end(), s.begin(), s.end());
  }
  void WriteBytes(const std::vector<uint8_t>& b) {
    buf_.insert(buf_.end(), b.begin(), b.end());
  }

  const std::vector<uint8_t>& Data() const { return buf_; }
  size_t Size() const { return buf_.size(); }
  void Clear() { buf_.clear(); }

 private:
  std::vector<uint8_t> buf_;
};

// Build a complete binlog event with header, body, and checksum.
inline std::vector<uint8_t> BuildEvent(uint8_t type_code, uint32_t timestamp,
                                       uint32_t next_position,
                                       const std::vector<uint8_t>& body) {
  EventBuilder b;
  uint32_t event_length =
      static_cast<uint32_t>(kEventHeaderSize + body.size() + kChecksumSize);

  // 19-byte header
  b.WriteU32Le(timestamp);
  b.WriteU8(type_code);
  b.WriteU32Le(1);  // server_id
  b.WriteU32Le(event_length);
  b.WriteU32Le(next_position);
  b.WriteU16Le(0);  // flags

  // Body
  b.WriteBytes(body);

  // Fake checksum (4 zero bytes)
  b.WriteU32Le(0);

  return b.Data();
}

// Build a TABLE_MAP_EVENT body for a simple schema: single INT column.
inline std::vector<uint8_t> BuildTableMapBody(uint64_t table_id,
                                              const std::string& db,
                                              const std::string& table) {
  EventBuilder b;
  // table_id: 6 bytes
  b.WriteU48Le(table_id);
  // flags: 2 bytes
  b.WriteU16Le(0);
  // database_name
  b.WriteU8(static_cast<uint8_t>(db.size()));
  b.WriteString(db);
  b.WriteU8(0);  // null terminator
  // table_name
  b.WriteU8(static_cast<uint8_t>(table.size()));
  b.WriteString(table);
  b.WriteU8(0);  // null terminator
  // column_count = 1
  b.WriteU8(1);
  // column type: INT (0x03)
  b.WriteU8(0x03);
  // metadata length = 0
  b.WriteU8(0);
  // null bitmap: nullable = 0x01
  b.WriteU8(0x01);
  return b.Data();
}

// Build a WRITE_ROWS_EVENT V2 body for a single INT row.
inline std::vector<uint8_t> BuildWriteRowsBody(uint64_t table_id,
                                               int32_t value) {
  EventBuilder b;
  // table_id
  b.WriteU48Le(table_id);
  // flags
  b.WriteU16Le(0);
  // V2 var_header_len = 2
  b.WriteU16Le(2);
  // column_count = 1
  b.WriteU8(1);
  // columns_present = 0x01
  b.WriteU8(0x01);
  // null_bitmap = 0x00 (not null)
  b.WriteU8(0x00);
  // INT value
  b.WriteU32Le(static_cast<uint32_t>(value));
  return b.Data();
}

// Build an UPDATE_ROWS_EVENT V2 body for a single INT row.
inline std::vector<uint8_t> BuildUpdateRowsBody(uint64_t table_id,
                                                int32_t before_val,
                                                int32_t after_val) {
  EventBuilder b;
  b.WriteU48Le(table_id);
  b.WriteU16Le(0);
  b.WriteU16Le(2);
  b.WriteU8(1);     // column_count
  b.WriteU8(0x01);  // columns_present (before)
  b.WriteU8(0x01);  // columns_present_update (after)
  // Before row
  b.WriteU8(0x00);
  b.WriteU32Le(static_cast<uint32_t>(before_val));
  // After row
  b.WriteU8(0x00);
  b.WriteU32Le(static_cast<uint32_t>(after_val));
  return b.Data();
}

// Build a DELETE_ROWS_EVENT V2 body for a single INT row.
inline std::vector<uint8_t> BuildDeleteRowsBody(uint64_t table_id,
                                                int32_t value) {
  EventBuilder b;
  b.WriteU48Le(table_id);
  b.WriteU16Le(0);
  b.WriteU16Le(2);
  b.WriteU8(1);
  b.WriteU8(0x01);
  b.WriteU8(0x00);
  b.WriteU32Le(static_cast<uint32_t>(value));
  return b.Data();
}

// Build a ROTATE_EVENT body.
inline std::vector<uint8_t> BuildRotateBody(uint64_t position,
                                            const std::string& filename) {
  EventBuilder b;
  b.WriteU64Le(position);
  b.WriteString(filename);
  return b.Data();
}

}  // namespace test
}  // namespace mes

#endif  // MES_CORE_TESTS_TEST_HELPERS_H_
