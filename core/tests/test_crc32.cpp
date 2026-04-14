// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#include <gtest/gtest.h>
#include <zlib.h>

#include <cstring>
#include <vector>

#include "crc32.h"
#include "event_header.h"
#include "test_helpers.h"

namespace mes {
namespace {

TEST(CRC32, ComputeMatchesZlib) {
  const char* data = "hello";
  uint32_t expected = static_cast<uint32_t>(crc32(0L, reinterpret_cast<const Bytef*>(data), 5));
  EXPECT_EQ(ComputeCRC32(data, 5), expected);
  EXPECT_EQ(expected, 0x3610A686u);
}

TEST(CRC32, EmptyData) {
  EXPECT_EQ(ComputeCRC32(nullptr, 0), static_cast<uint32_t>(crc32(0L, Z_NULL, 0)));
}

TEST(CRC32, Deterministic) {
  std::vector<uint8_t> data = {0x01, 0x02, 0x03, 0x04, 0x05};
  uint32_t first = ComputeCRC32(data.data(), data.size());
  uint32_t second = ComputeCRC32(data.data(), data.size());
  EXPECT_EQ(first, second);
}

TEST(CRC32, ValidChecksumPasses) {
  auto event =
      test::BuildEvent(static_cast<uint8_t>(BinlogEventType::kXidEvent), 1000, 100, {0x42});
  test::FixChecksum(event);

  size_t data_len = event.size() - kChecksumSize;
  uint32_t computed = ComputeCRC32(event.data(), data_len);
  uint32_t stored = 0;
  std::memcpy(&stored, event.data() + data_len, sizeof(stored));
  EXPECT_EQ(computed, stored);
}

TEST(CRC32, CorruptedDataFails) {
  auto event =
      test::BuildEvent(static_cast<uint8_t>(BinlogEventType::kXidEvent), 1000, 100, {0x42});
  test::FixChecksum(event);

  // Corrupt a byte in the payload
  event[kEventHeaderSize] ^= 0xFF;

  size_t data_len = event.size() - kChecksumSize;
  uint32_t computed = ComputeCRC32(event.data(), data_len);
  uint32_t stored = 0;
  std::memcpy(&stored, event.data() + data_len, sizeof(stored));
  EXPECT_NE(computed, stored);
}

TEST(CRC32, CorruptedChecksumFails) {
  auto event =
      test::BuildEvent(static_cast<uint8_t>(BinlogEventType::kXidEvent), 1000, 100, {0x42});
  test::FixChecksum(event);

  // Corrupt the checksum itself
  event[event.size() - 1] ^= 0xFF;

  size_t data_len = event.size() - kChecksumSize;
  uint32_t computed = ComputeCRC32(event.data(), data_len);
  uint32_t stored = 0;
  std::memcpy(&stored, event.data() + data_len, sizeof(stored));
  EXPECT_NE(computed, stored);
}

TEST(CRC32, MinimalEventWithChecksum) {
  auto event = test::BuildEvent(static_cast<uint8_t>(BinlogEventType::kXidEvent), 0, 0, {});
  test::FixChecksum(event);
  EXPECT_EQ(event.size(), kEventHeaderSize + kChecksumSize);

  size_t data_len = event.size() - kChecksumSize;
  uint32_t computed = ComputeCRC32(event.data(), data_len);
  uint32_t stored = 0;
  std::memcpy(&stored, event.data() + data_len, sizeof(stored));
  EXPECT_EQ(computed, stored);
}

}  // namespace
}  // namespace mes
