// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#include <gtest/gtest.h>

#include <cstring>

#include "event_header.h"

namespace mes {
namespace {

// Helper to build a 19-byte event header in the buffer.
// Fields are written in little-endian format matching MySQL binlog v4.
void BuildHeader(uint8_t* buf, uint32_t timestamp, uint8_t type_code, uint32_t server_id,
                 uint32_t event_length, uint32_t next_position, uint16_t flags) {
  // timestamp: 4 bytes LE
  buf[0] = static_cast<uint8_t>(timestamp);
  buf[1] = static_cast<uint8_t>(timestamp >> 8);
  buf[2] = static_cast<uint8_t>(timestamp >> 16);
  buf[3] = static_cast<uint8_t>(timestamp >> 24);
  // type_code: 1 byte
  buf[4] = type_code;
  // server_id: 4 bytes LE
  buf[5] = static_cast<uint8_t>(server_id);
  buf[6] = static_cast<uint8_t>(server_id >> 8);
  buf[7] = static_cast<uint8_t>(server_id >> 16);
  buf[8] = static_cast<uint8_t>(server_id >> 24);
  // event_length: 4 bytes LE
  buf[9] = static_cast<uint8_t>(event_length);
  buf[10] = static_cast<uint8_t>(event_length >> 8);
  buf[11] = static_cast<uint8_t>(event_length >> 16);
  buf[12] = static_cast<uint8_t>(event_length >> 24);
  // next_position: 4 bytes LE
  buf[13] = static_cast<uint8_t>(next_position);
  buf[14] = static_cast<uint8_t>(next_position >> 8);
  buf[15] = static_cast<uint8_t>(next_position >> 16);
  buf[16] = static_cast<uint8_t>(next_position >> 24);
  // flags: 2 bytes LE
  buf[17] = static_cast<uint8_t>(flags);
  buf[18] = static_cast<uint8_t>(flags >> 8);
}

TEST(EventHeaderTest, ParseValidHeader) {
  uint8_t buf[19];
  BuildHeader(buf, 1700000000, 30, 1, 100, 200, 0x0001);

  EventHeader header;
  ASSERT_TRUE(ParseEventHeader(buf, sizeof(buf), &header));
  EXPECT_EQ(header.timestamp, 1700000000u);
  EXPECT_EQ(header.type_code, 30);
  EXPECT_EQ(header.server_id, 1u);
  EXPECT_EQ(header.event_length, 100u);
  EXPECT_EQ(header.next_position, 200u);
  EXPECT_EQ(header.flags, 0x0001);
}

TEST(EventHeaderTest, ParseWithLargerBuffer) {
  uint8_t buf[32];
  std::memset(buf, 0, sizeof(buf));
  BuildHeader(buf, 12345, 19, 42, 50, 73, 0);

  EventHeader header;
  ASSERT_TRUE(ParseEventHeader(buf, sizeof(buf), &header));
  EXPECT_EQ(header.timestamp, 12345u);
  EXPECT_EQ(header.type_code, 19);
  EXPECT_EQ(header.server_id, 42u);
  EXPECT_EQ(header.event_length, 50u);
  EXPECT_EQ(header.next_position, 73u);
  EXPECT_EQ(header.flags, 0);
}

TEST(EventHeaderTest, ParseTooShort) {
  uint8_t buf[18];
  std::memset(buf, 0, sizeof(buf));

  EventHeader header;
  EXPECT_FALSE(ParseEventHeader(buf, sizeof(buf), &header));
}

TEST(EventHeaderTest, ParseNullData) {
  EventHeader header;
  EXPECT_FALSE(ParseEventHeader(nullptr, 19, &header));
}

TEST(EventHeaderTest, ParseNullHeader) {
  uint8_t buf[19];
  std::memset(buf, 0, sizeof(buf));
  EXPECT_FALSE(ParseEventHeader(buf, sizeof(buf), nullptr));
}

TEST(EventHeaderTest, EventBodySizeNormal) {
  EventHeader header;
  header.event_length = 100;
  // body = 100 - 19 (header) - 4 (checksum) = 77
  EXPECT_EQ(EventBodySize(header), 77u);
}

TEST(EventHeaderTest, EventBodySizeMinimal) {
  EventHeader header;
  // Minimum valid: header(19) + checksum(4) = 23, body = 0
  header.event_length = 23;
  EXPECT_EQ(EventBodySize(header), 0u);
}

TEST(EventHeaderTest, EventBodySizeTooSmall) {
  EventHeader header;
  header.event_length = 22;  // Less than header + checksum
  EXPECT_EQ(EventBodySize(header), 0u);
}

TEST(EventHeaderTest, EventBodySizeZero) {
  EventHeader header;
  header.event_length = 0;
  EXPECT_EQ(EventBodySize(header), 0u);
}

TEST(EventHeaderTest, IsRowEventWriteV1) {
  EXPECT_TRUE(IsRowEvent(BinlogEventType::kWriteRowsEventV1));
  EXPECT_TRUE(IsRowEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEventV1)));
}

TEST(EventHeaderTest, IsRowEventUpdateV1) {
  EXPECT_TRUE(IsRowEvent(BinlogEventType::kUpdateRowsEventV1));
  EXPECT_TRUE(IsRowEvent(static_cast<uint8_t>(BinlogEventType::kUpdateRowsEventV1)));
}

TEST(EventHeaderTest, IsRowEventDeleteV1) {
  EXPECT_TRUE(IsRowEvent(BinlogEventType::kDeleteRowsEventV1));
  EXPECT_TRUE(IsRowEvent(static_cast<uint8_t>(BinlogEventType::kDeleteRowsEventV1)));
}

TEST(EventHeaderTest, IsRowEventWriteV2) {
  EXPECT_TRUE(IsRowEvent(BinlogEventType::kWriteRowsEvent));
  EXPECT_TRUE(IsRowEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent)));
}

TEST(EventHeaderTest, IsRowEventUpdateV2) {
  EXPECT_TRUE(IsRowEvent(BinlogEventType::kUpdateRowsEvent));
  EXPECT_TRUE(IsRowEvent(static_cast<uint8_t>(BinlogEventType::kUpdateRowsEvent)));
}

TEST(EventHeaderTest, IsRowEventDeleteV2) {
  EXPECT_TRUE(IsRowEvent(BinlogEventType::kDeleteRowsEvent));
  EXPECT_TRUE(IsRowEvent(static_cast<uint8_t>(BinlogEventType::kDeleteRowsEvent)));
}

TEST(EventHeaderTest, IsRowEventNonRow) {
  EXPECT_FALSE(IsRowEvent(BinlogEventType::kTableMapEvent));
  EXPECT_FALSE(IsRowEvent(BinlogEventType::kRotateEvent));
  EXPECT_FALSE(IsRowEvent(BinlogEventType::kFormatDescriptionEvent));
  EXPECT_FALSE(IsRowEvent(BinlogEventType::kXidEvent));
  EXPECT_FALSE(IsRowEvent(BinlogEventType::kGtidLogEvent));
  EXPECT_FALSE(IsRowEvent(BinlogEventType::kUnknown));
  EXPECT_FALSE(IsRowEvent(static_cast<uint8_t>(0)));
  EXPECT_FALSE(IsRowEvent(static_cast<uint8_t>(99)));
}

TEST(EventHeaderTest, BinlogEventTypeNameKnown) {
  EXPECT_STREQ(BinlogEventTypeName(4), "ROTATE_EVENT");
  EXPECT_STREQ(BinlogEventTypeName(15), "FORMAT_DESCRIPTION_EVENT");
  EXPECT_STREQ(BinlogEventTypeName(16), "XID_EVENT");
  EXPECT_STREQ(BinlogEventTypeName(19), "TABLE_MAP_EVENT");
  EXPECT_STREQ(BinlogEventTypeName(23), "WRITE_ROWS_EVENT_V1");
  EXPECT_STREQ(BinlogEventTypeName(24), "UPDATE_ROWS_EVENT_V1");
  EXPECT_STREQ(BinlogEventTypeName(25), "DELETE_ROWS_EVENT_V1");
  EXPECT_STREQ(BinlogEventTypeName(30), "WRITE_ROWS_EVENT");
  EXPECT_STREQ(BinlogEventTypeName(31), "UPDATE_ROWS_EVENT");
  EXPECT_STREQ(BinlogEventTypeName(32), "DELETE_ROWS_EVENT");
  EXPECT_STREQ(BinlogEventTypeName(33), "GTID_LOG_EVENT");
  EXPECT_STREQ(BinlogEventTypeName(34), "ANONYMOUS_GTID_LOG_EVENT");
}

TEST(EventHeaderTest, BinlogEventTypeNameUnknown) {
  EXPECT_STREQ(BinlogEventTypeName(0), "UNKNOWN");
  EXPECT_STREQ(BinlogEventTypeName(1), "UNKNOWN");
  EXPECT_STREQ(BinlogEventTypeName(99), "UNKNOWN");
  EXPECT_STREQ(BinlogEventTypeName(255), "UNKNOWN");
}

}  // namespace
}  // namespace mes
