// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#include "rotate_event.h"

#include <cstring>
#include <vector>

#include <gtest/gtest.h>

namespace mes {
namespace {

// Build a ROTATE_EVENT body: 8-byte position (LE) + filename
std::vector<uint8_t> BuildRotateBody(uint64_t position, const std::string& filename) {
  std::vector<uint8_t> buf(8 + filename.size());
  buf[0] = static_cast<uint8_t>(position);
  buf[1] = static_cast<uint8_t>(position >> 8);
  buf[2] = static_cast<uint8_t>(position >> 16);
  buf[3] = static_cast<uint8_t>(position >> 24);
  buf[4] = static_cast<uint8_t>(position >> 32);
  buf[5] = static_cast<uint8_t>(position >> 40);
  buf[6] = static_cast<uint8_t>(position >> 48);
  buf[7] = static_cast<uint8_t>(position >> 56);
  std::memcpy(buf.data() + 8, filename.data(), filename.size());
  return buf;
}

TEST(RotateEventTest, ParseValid) {
  auto body = BuildRotateBody(4, "mysql-bin.000002");

  RotateEventData result;
  ASSERT_TRUE(ParseRotateEvent(body.data(), body.size(), &result));
  EXPECT_EQ(result.position, 4u);
  EXPECT_EQ(result.new_log_file, "mysql-bin.000002");
}

TEST(RotateEventTest, ParseLargePosition) {
  auto body = BuildRotateBody(123456789012345ULL, "binlog.000100");

  RotateEventData result;
  ASSERT_TRUE(ParseRotateEvent(body.data(), body.size(), &result));
  EXPECT_EQ(result.position, 123456789012345ULL);
  EXPECT_EQ(result.new_log_file, "binlog.000100");
}

TEST(RotateEventTest, ParsePositionOnly) {
  auto body = BuildRotateBody(4, "");

  RotateEventData result;
  ASSERT_TRUE(ParseRotateEvent(body.data(), body.size(), &result));
  EXPECT_EQ(result.position, 4u);
  EXPECT_TRUE(result.new_log_file.empty());
}

TEST(RotateEventTest, ParseTooShort) {
  uint8_t buf[7] = {0};
  RotateEventData result;
  EXPECT_FALSE(ParseRotateEvent(buf, sizeof(buf), &result));
}

TEST(RotateEventTest, ParseNullData) {
  RotateEventData result;
  EXPECT_FALSE(ParseRotateEvent(nullptr, 100, &result));
}

TEST(RotateEventTest, ParseNullOutput) {
  uint8_t buf[16] = {0};
  EXPECT_FALSE(ParseRotateEvent(buf, sizeof(buf), nullptr));
}

}  // namespace
}  // namespace mes
