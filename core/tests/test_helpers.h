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
#include <cstring>
#include <string>
#include <vector>

#include "crc32.h"
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
  void WriteU16Be(uint16_t v) {
    buf_.push_back(static_cast<uint8_t>(v >> 8));
    buf_.push_back(static_cast<uint8_t>(v));
  }
  void WriteU24Be(uint32_t v) {
    buf_.push_back(static_cast<uint8_t>(v >> 16));
    buf_.push_back(static_cast<uint8_t>(v >> 8));
    buf_.push_back(static_cast<uint8_t>(v));
  }
  void WriteU32Be(uint32_t v) {
    buf_.push_back(static_cast<uint8_t>(v >> 24));
    buf_.push_back(static_cast<uint8_t>(v >> 16));
    buf_.push_back(static_cast<uint8_t>(v >> 8));
    buf_.push_back(static_cast<uint8_t>(v));
  }
  void WriteFloat(float v) {
    uint8_t tmp[4];
    std::memcpy(tmp, &v, 4);
    buf_.insert(buf_.end(), tmp, tmp + 4);
  }
  void WriteDouble(double v) {
    uint8_t tmp[8];
    std::memcpy(tmp, &v, 8);
    buf_.insert(buf_.end(), tmp, tmp + 8);
  }
  void WriteString(const std::string& s) { buf_.insert(buf_.end(), s.begin(), s.end()); }
  void WriteBytes(const std::vector<uint8_t>& b) { buf_.insert(buf_.end(), b.begin(), b.end()); }
  void WriteBytes(const uint8_t* d, size_t n) { buf_.insert(buf_.end(), d, d + n); }

  const std::vector<uint8_t>& Data() const { return buf_; }
  const uint8_t* DataPtr() const { return buf_.data(); }
  std::vector<uint8_t>& Buffer() { return buf_; }
  size_t Size() const { return buf_.size(); }
  void Clear() { buf_.clear(); }

 private:
  std::vector<uint8_t> buf_;
};

// Build a complete binlog event with header, body, and checksum.
inline std::vector<uint8_t> BuildEvent(uint8_t type_code, uint32_t timestamp,
                                       uint32_t next_position, const std::vector<uint8_t>& body) {
  EventBuilder b;
  uint32_t event_length = static_cast<uint32_t>(kEventHeaderSize + body.size() + kChecksumSize);

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
inline std::vector<uint8_t> BuildTableMapBody(uint64_t table_id, const std::string& db,
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
inline std::vector<uint8_t> BuildWriteRowsBody(uint64_t table_id, int32_t value) {
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
inline std::vector<uint8_t> BuildUpdateRowsBody(uint64_t table_id, int32_t before_val,
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
inline std::vector<uint8_t> BuildDeleteRowsBody(uint64_t table_id, int32_t value) {
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
inline std::vector<uint8_t> BuildRotateBody(uint64_t position, const std::string& filename) {
  EventBuilder b;
  b.WriteU64Le(position);
  b.WriteString(filename);
  return b.Data();
}

/// @brief Compute CRC32 over all bytes except the trailing 4-byte checksum
///        and write the result into the last 4 bytes (little-endian).
inline void FixChecksum(std::vector<uint8_t>& buf) {
  if (buf.size() < kChecksumSize) {
    return;
  }
  size_t data_len = buf.size() - kChecksumSize;
  uint32_t crc = ComputeCRC32(buf.data(), data_len);
  std::memcpy(buf.data() + data_len, &crc, sizeof(crc));
}

/// @brief Build a complete binlog event with a valid CRC32 checksum.
inline std::vector<uint8_t> BuildValidEvent(uint8_t type_code, const std::vector<uint8_t>& body,
                                            uint32_t timestamp = 0, uint32_t server_id = 1) {
  auto event = BuildEvent(type_code, timestamp, 0, body);
  // Patch server_id into the header (bytes 5-8, little-endian)
  event[5] = static_cast<uint8_t>(server_id);
  event[6] = static_cast<uint8_t>(server_id >> 8);
  event[7] = static_cast<uint8_t>(server_id >> 16);
  event[8] = static_cast<uint8_t>(server_id >> 24);
  FixChecksum(event);
  return event;
}

/// @brief Pack a DATETIME2 value into a BinaryWriter (5 bytes, no frac).
///
/// Encodes the date/time as MySQL's internal DATETIME2 packed format:
///   intpart = ((year*13 + month) << 5 | day) << 17 | (hour << 12 | min << 6 | sec)
///   packed  = intpart + 0x8000000000
///   Written as 5 bytes big-endian.
template <typename Writer>
inline void WriteDatetime2(Writer& w, int year, int month, int day, int hour, int min, int sec) {
  int64_t ym = year * 13 + month;
  int64_t ymd = (ym << 5) | day;
  int64_t hms = (static_cast<int64_t>(hour) << 12) | (min << 6) | sec;
  int64_t intpart = (ymd << 17) | hms;
  int64_t packed = intpart + 0x8000000000LL;
  w.WriteU8(static_cast<uint8_t>((packed >> 32) & 0xFF));
  w.WriteU8(static_cast<uint8_t>((packed >> 24) & 0xFF));
  w.WriteU8(static_cast<uint8_t>((packed >> 16) & 0xFF));
  w.WriteU8(static_cast<uint8_t>((packed >> 8) & 0xFF));
  w.WriteU8(static_cast<uint8_t>(packed & 0xFF));
}

}  // namespace test
}  // namespace mes

#endif  // MES_CORE_TESTS_TEST_HELPERS_H_
