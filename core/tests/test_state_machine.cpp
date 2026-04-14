// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#include <gtest/gtest.h>

#include <cstring>
#include <vector>

#include "crc32.h"
#include "state_machine.h"
#include "test_helpers.h"

namespace mes {
namespace {

// Convenience wrapper matching the old local signature (server_id=1, next_position=0)
std::vector<uint8_t> BuildEvent(uint8_t type_code, const std::vector<uint8_t>& body) {
  return test::BuildEvent(type_code, 1000, 0, body);
}

TEST(StateMachineTest, InitialState) {
  EventStreamParser parser;
  EXPECT_EQ(parser.GetState(), ParserState::kWaitingHeader);
  EXPECT_FALSE(parser.HasEvent());
}

TEST(StateMachineTest, FeedCompleteEventAtOnce) {
  std::vector<uint8_t> body = {0x01, 0x02, 0x03, 0x04, 0x05};
  auto event = BuildEvent(30, body);

  EventStreamParser parser;
  size_t consumed = parser.Feed(event.data(), event.size());
  EXPECT_EQ(consumed, event.size());
  EXPECT_TRUE(parser.HasEvent());
  EXPECT_EQ(parser.GetState(), ParserState::kEventReady);

  EXPECT_EQ(parser.CurrentHeader().type_code, 30);
  EXPECT_EQ(parser.CurrentHeader().timestamp, 1000u);
  EXPECT_EQ(parser.CurrentHeader().server_id, 1u);

  const uint8_t* body_data = nullptr;
  size_t body_len = 0;
  parser.CurrentBody(&body_data, &body_len);
  ASSERT_NE(body_data, nullptr);
  EXPECT_EQ(body_len, 5u);
  EXPECT_EQ(body_data[0], 0x01);
  EXPECT_EQ(body_data[4], 0x05);

  EXPECT_EQ(parser.RawSize(), event.size());
}

TEST(StateMachineTest, FeedByteByByte) {
  std::vector<uint8_t> body = {0xAA, 0xBB};
  auto event = BuildEvent(19, body);

  EventStreamParser parser;

  // Feed one byte at a time
  for (size_t i = 0; i < event.size() - 1; i++) {
    size_t consumed = parser.Feed(&event[i], 1);
    EXPECT_EQ(consumed, 1u);
    EXPECT_FALSE(parser.HasEvent()) << "Should not have event at byte " << i;
  }

  // Feed the last byte
  size_t consumed = parser.Feed(&event[event.size() - 1], 1);
  EXPECT_EQ(consumed, 1u);
  EXPECT_TRUE(parser.HasEvent());
  EXPECT_EQ(parser.CurrentHeader().type_code, 19);
}

TEST(StateMachineTest, FeedPartialHeader) {
  std::vector<uint8_t> body = {0x01};
  auto event = BuildEvent(30, body);

  EventStreamParser parser;

  // Feed 10 bytes (partial header)
  size_t consumed = parser.Feed(event.data(), 10);
  EXPECT_EQ(consumed, 10u);
  EXPECT_FALSE(parser.HasEvent());
  EXPECT_EQ(parser.GetState(), ParserState::kWaitingHeader);

  // Feed the rest
  consumed = parser.Feed(event.data() + 10, event.size() - 10);
  EXPECT_EQ(consumed, event.size() - 10);
  EXPECT_TRUE(parser.HasEvent());
}

TEST(StateMachineTest, FeedMultipleEventsSequentially) {
  std::vector<uint8_t> body1 = {0x01, 0x02};
  std::vector<uint8_t> body2 = {0x03, 0x04, 0x05};
  auto event1 = BuildEvent(30, body1);
  auto event2 = BuildEvent(31, body2);

  EventStreamParser parser;

  // Feed first event
  size_t consumed = parser.Feed(event1.data(), event1.size());
  EXPECT_EQ(consumed, event1.size());
  EXPECT_TRUE(parser.HasEvent());
  EXPECT_EQ(parser.CurrentHeader().type_code, 30);

  // Cannot feed more while event is ready
  consumed = parser.Feed(event2.data(), event2.size());
  EXPECT_EQ(consumed, 0u);

  // Advance
  parser.Advance();
  EXPECT_FALSE(parser.HasEvent());
  EXPECT_EQ(parser.GetState(), ParserState::kWaitingHeader);

  // Feed second event
  consumed = parser.Feed(event2.data(), event2.size());
  EXPECT_EQ(consumed, event2.size());
  EXPECT_TRUE(parser.HasEvent());
  EXPECT_EQ(parser.CurrentHeader().type_code, 31);
}

TEST(StateMachineTest, ErrorOnTinyEventLength) {
  // Build a header where event_length is too small (< 23)
  std::vector<uint8_t> header(kEventHeaderSize, 0);
  header[4] = 30;  // type_code
  // event_length = 10 (too small: must be >= 23)
  header[9] = 10;

  EventStreamParser parser;
  size_t consumed = parser.Feed(header.data(), header.size());
  EXPECT_EQ(consumed, header.size());
  EXPECT_EQ(parser.GetState(), ParserState::kError);
  EXPECT_FALSE(parser.HasEvent());

  // Feed returns 0 in error state
  consumed = parser.Feed(header.data(), header.size());
  EXPECT_EQ(consumed, 0u);
}

TEST(StateMachineTest, ResetFromError) {
  // Trigger error
  std::vector<uint8_t> header(kEventHeaderSize, 0);
  header[4] = 30;
  header[9] = 5;  // event_length too small

  EventStreamParser parser;
  parser.Feed(header.data(), header.size());
  EXPECT_EQ(parser.GetState(), ParserState::kError);

  // Reset
  parser.Reset();
  EXPECT_EQ(parser.GetState(), ParserState::kWaitingHeader);
  EXPECT_FALSE(parser.HasEvent());

  // Can feed again after reset
  std::vector<uint8_t> body = {0x01};
  auto event = BuildEvent(19, body);
  size_t consumed = parser.Feed(event.data(), event.size());
  EXPECT_EQ(consumed, event.size());
  EXPECT_TRUE(parser.HasEvent());
}

TEST(StateMachineTest, AdvanceResetsForNextEvent) {
  std::vector<uint8_t> body = {0x01};
  auto event = BuildEvent(30, body);

  EventStreamParser parser;
  parser.Feed(event.data(), event.size());
  ASSERT_TRUE(parser.HasEvent());

  parser.Advance();
  EXPECT_EQ(parser.GetState(), ParserState::kWaitingHeader);
  EXPECT_FALSE(parser.HasEvent());
}

TEST(StateMachineTest, FeedNullData) {
  EventStreamParser parser;
  EXPECT_EQ(parser.Feed(nullptr, 10), 0u);
}

TEST(StateMachineTest, FeedZeroLength) {
  uint8_t buf[1] = {0};
  EventStreamParser parser;
  EXPECT_EQ(parser.Feed(buf, 0), 0u);
}

TEST(StateMachineTest, MinimalEventNoBody) {
  // Event with zero body bytes: length = 19 (header) + 4 (checksum) = 23
  auto event = BuildEvent(16, {});  // XID event with no body

  EventStreamParser parser;
  size_t consumed = parser.Feed(event.data(), event.size());
  EXPECT_EQ(consumed, event.size());
  EXPECT_TRUE(parser.HasEvent());

  const uint8_t* body_data = nullptr;
  size_t body_len = 0;
  parser.CurrentBody(&body_data, &body_len);
  EXPECT_EQ(body_len, 0u);
}

TEST(StateMachineTest, RawDataMatchesInput) {
  std::vector<uint8_t> body = {0xDE, 0xAD, 0xBE, 0xEF};
  auto event = BuildEvent(30, body);

  EventStreamParser parser;
  parser.Feed(event.data(), event.size());
  ASSERT_TRUE(parser.HasEvent());

  EXPECT_EQ(parser.RawSize(), event.size());
  EXPECT_EQ(std::memcmp(parser.RawData(), event.data(), event.size()), 0);
}

TEST(StateMachineTest, HeaderParseFailure) {
  EventStreamParser parser;
  // Feed garbage that looks like a header but has impossible values
  // Event length of 0 (less than header + checksum = 23)
  uint8_t bad_header[19];
  memset(bad_header, 0, sizeof(bad_header));
  // Set event_length to a too-small value (e.g., 10)
  bad_header[9] = 10;  // event_length LE byte 0
  bad_header[10] = 0;
  bad_header[11] = 0;
  bad_header[12] = 0;
  size_t consumed = parser.Feed(bad_header, 19);
  EXPECT_EQ(consumed, 19u);
  EXPECT_EQ(parser.GetState(), ParserState::kError);
}

TEST(StateMachineTest, FeedWhileReady) {
  EventStreamParser parser;
  // Build a valid tiny event
  auto event = test::BuildEvent(0x04, 1000, 100, {});
  parser.Feed(event.data(), event.size());
  EXPECT_EQ(parser.GetState(), ParserState::kEventReady);
  // Feed again while ready should return 0
  uint8_t more[] = {1, 2, 3};
  EXPECT_EQ(parser.Feed(more, 3), 0u);
}

TEST(StateMachineTest, FeedWhileError) {
  EventStreamParser parser;
  // Cause error
  uint8_t bad_header[19] = {};
  bad_header[9] = 10;
  parser.Feed(bad_header, 19);
  EXPECT_EQ(parser.GetState(), ParserState::kError);
  // Feed again while error should return 0
  uint8_t more[] = {1, 2, 3};
  EXPECT_EQ(parser.Feed(more, 3), 0u);
}

TEST(StateMachineTest, CurrentBodyAndRawData) {
  EventStreamParser parser;
  std::vector<uint8_t> body = {0xAA, 0xBB, 0xCC};
  auto event = test::BuildEvent(0x04, 1000, 100, body);
  parser.Feed(event.data(), event.size());
  EXPECT_TRUE(parser.HasEvent());

  const uint8_t* body_data = nullptr;
  size_t body_len = 0;
  parser.CurrentBody(&body_data, &body_len);
  EXPECT_NE(body_data, nullptr);
  EXPECT_EQ(body_len, 3u);
  EXPECT_EQ(body_data[0], 0xAA);

  EXPECT_EQ(parser.RawSize(), event.size());
  EXPECT_NE(parser.RawData(), nullptr);
}

// --- Error recovery tests ---

TEST(StateMachineTest, ErrorRecoveryViaResetThenFeedValid) {
  EventStreamParser parser;

  // Feed invalid header to trigger kError
  uint8_t bad_header[kEventHeaderSize];
  memset(bad_header, 0, sizeof(bad_header));
  bad_header[4] = 30;  // type_code
  bad_header[9] = 5;   // event_length too small (< 23)
  parser.Feed(bad_header, kEventHeaderSize);
  EXPECT_EQ(parser.GetState(), ParserState::kError);

  // Reset should recover
  parser.Reset();
  EXPECT_EQ(parser.GetState(), ParserState::kWaitingHeader);

  // Feed valid data and verify it processes correctly
  std::vector<uint8_t> body = {0xAA, 0xBB, 0xCC};
  auto event = BuildEvent(30, body);
  size_t consumed = parser.Feed(event.data(), event.size());
  EXPECT_EQ(consumed, event.size());
  EXPECT_TRUE(parser.HasEvent());
  EXPECT_EQ(parser.GetState(), ParserState::kEventReady);
  EXPECT_EQ(parser.CurrentHeader().type_code, 30);

  const uint8_t* body_data = nullptr;
  size_t body_len = 0;
  parser.CurrentBody(&body_data, &body_len);
  ASSERT_NE(body_data, nullptr);
  EXPECT_EQ(body_len, 3u);
  EXPECT_EQ(body_data[0], 0xAA);
}

TEST(StateMachineTest, PartialHeaderThenInvalidLength) {
  EventStreamParser parser;

  // Build a header manually where the first 10 bytes are valid-looking
  // but the full 19 bytes reveal an invalid event_length
  uint8_t header[kEventHeaderSize];
  memset(header, 0, sizeof(header));
  header[0] = 0xE8;  // timestamp byte 0
  header[1] = 0x03;  // timestamp byte 1
  header[4] = 30;    // type_code
  header[5] = 0x01;  // server_id
  // event_length = 10 (invalid: < 23)
  header[9] = 10;
  header[10] = 0;
  header[11] = 0;
  header[12] = 0;

  // Feed partial header (10 bytes)
  size_t consumed = parser.Feed(header, 10);
  EXPECT_EQ(consumed, 10u);
  EXPECT_EQ(parser.GetState(), ParserState::kWaitingHeader);
  EXPECT_FALSE(parser.HasEvent());

  // Feed remaining 9 bytes to complete the invalid header
  consumed = parser.Feed(header + 10, kEventHeaderSize - 10);
  EXPECT_EQ(consumed, kEventHeaderSize - 10);
  EXPECT_EQ(parser.GetState(), ParserState::kError);
  EXPECT_FALSE(parser.HasEvent());
}

TEST(StateMachineTest, ErrorOnEventTooLargeMax) {
  // Build a header where event_length = 0xFFFFFFFF (way over 64 MB limit)
  uint8_t header[kEventHeaderSize];
  memset(header, 0, sizeof(header));
  header[4] = 30;  // type_code
  // event_length = 0xFFFFFFFF (LE)
  header[9] = 0xFF;
  header[10] = 0xFF;
  header[11] = 0xFF;
  header[12] = 0xFF;

  EventStreamParser parser;
  size_t consumed = parser.Feed(header, kEventHeaderSize);
  EXPECT_EQ(consumed, kEventHeaderSize);
  EXPECT_EQ(parser.GetState(), ParserState::kError);
  EXPECT_FALSE(parser.HasEvent());
}

TEST(StateMachineTest, ErrorOnEventTooLargeBoundary) {
  // Build a header where event_length = 64*1024*1024 + 1 (just over 64 MB limit)
  uint32_t too_large = 64 * 1024 * 1024 + 1;
  uint8_t header[kEventHeaderSize];
  memset(header, 0, sizeof(header));
  header[4] = 30;  // type_code
  header[9] = static_cast<uint8_t>(too_large);
  header[10] = static_cast<uint8_t>(too_large >> 8);
  header[11] = static_cast<uint8_t>(too_large >> 16);
  header[12] = static_cast<uint8_t>(too_large >> 24);

  EventStreamParser parser;
  size_t consumed = parser.Feed(header, kEventHeaderSize);
  EXPECT_EQ(consumed, kEventHeaderSize);
  EXPECT_EQ(parser.GetState(), ParserState::kError);
  EXPECT_FALSE(parser.HasEvent());
}

TEST(StateMachineTest, ValidChecksumRoundTrip) {
  // Build an event with a valid CRC32 checksum
  std::vector<uint8_t> body = {0xDE, 0xAD, 0xBE, 0xEF};
  auto event = test::BuildValidEvent(30, body, 1000);

  // Verify the checksum is correct
  size_t data_len = event.size() - kChecksumSize;
  uint32_t computed = ComputeCRC32(event.data(), data_len);
  uint32_t stored = 0;
  std::memcpy(&stored, event.data() + data_len, sizeof(stored));
  EXPECT_EQ(computed, stored);

  // Parser should accept the event normally
  EventStreamParser parser;
  size_t consumed = parser.Feed(event.data(), event.size());
  EXPECT_EQ(consumed, event.size());
  EXPECT_TRUE(parser.HasEvent());
}

TEST(StateMachineTest, CorruptedChecksumDetected) {
  // Build an event with a valid CRC32 checksum
  std::vector<uint8_t> body = {0xDE, 0xAD, 0xBE, 0xEF};
  auto event = test::BuildValidEvent(30, body, 1000);

  // Corrupt one body byte
  event[kEventHeaderSize + 1] ^= 0xFF;

  // Verify the checksum is now invalid
  size_t data_len = event.size() - kChecksumSize;
  uint32_t computed = ComputeCRC32(event.data(), data_len);
  uint32_t stored = 0;
  std::memcpy(&stored, event.data() + data_len, sizeof(stored));
  EXPECT_NE(computed, stored) << "Corrupted event should have mismatched checksum";
}

}  // namespace
}  // namespace mes
