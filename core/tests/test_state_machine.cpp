// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#include <gtest/gtest.h>

#include <algorithm>
#include <cstring>
#include <string>
#include <vector>

#include "crc32.h"
#include "state_machine.h"
#include "test_helpers.h"

namespace mes {

struct EventStreamParserTestAccess {
  static size_t BufferCapacity(const EventStreamParser& parser) {
    return parser.buffer_.capacity();
  }

  static size_t RetainedBufferLimit() { return EventStreamParser::kRetainedBufferLimit; }

  static size_t MaxEagerReserve() { return EventStreamParser::kMaxEagerReserve; }

  static uint64_t Crc32Passes(const EventStreamParser& parser) { return parser.crc32_passes_; }
};

namespace {

// Convenience wrapper matching the old local signature (server_id=1, next_position=0)
std::vector<uint8_t> BuildEvent(uint8_t type_code, const std::vector<uint8_t>& body) {
  return test::BuildEvent(type_code, 1000, 0, body);
}

// MARIADB_GTID_EVENT body: seq_no (8) + domain_id (4) + flags (1). The server_id
// half of a MariaDB GTID comes from the standard event header.
std::vector<uint8_t> BuildMariaDbGtidBody(uint64_t seq_no, uint32_t domain_id, uint8_t flags) {
  test::EventBuilder b;
  b.WriteU64Le(seq_no);
  b.WriteU32Le(domain_id);
  b.WriteU8(flags);
  return b.Data();
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

// MariaDB streams two events MySQL never emits: a domain-scoped GTID event and
// an ANNOTATE_ROWS event whose body is bare SQL text carrying no length of its
// own. Feed a whole transaction group one byte at a time — partial byte-stream
// buffering is a recurring defect area, and it has only ever been checked
// against MySQL-shaped events.
TEST(StateMachineTest, FramesMariaDbTransactionGroupFedOneByteAtATime) {
  const std::string annotation = "INSERT INTO users VALUES (42)";
  struct ExpectedEvent {
    uint8_t type_code;
    std::vector<uint8_t> body;
  };
  const std::vector<ExpectedEvent> expected = {
      {static_cast<uint8_t>(BinlogEventType::kMariaDBGtidEvent),
       BuildMariaDbGtidBody(42, 7001, 0x08)},
      {static_cast<uint8_t>(BinlogEventType::kMariaDBAnnotateRowsEvent),
       std::vector<uint8_t>(annotation.begin(), annotation.end())},
      {static_cast<uint8_t>(BinlogEventType::kTableMapEvent),
       test::BuildTableMapBody(42, "testdb", "users")},
      {static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), test::BuildWriteRowsBody(42, 42)},
      {static_cast<uint8_t>(BinlogEventType::kXidEvent), {}},
  };

  std::vector<uint8_t> stream;
  std::vector<size_t> event_ends;
  for (const auto& e : expected) {
    const auto encoded = BuildEvent(e.type_code, e.body);
    stream.insert(stream.end(), encoded.begin(), encoded.end());
    event_ends.push_back(stream.size());
  }

  EventStreamParser parser;
  size_t next_event = 0;
  for (size_t i = 0; i < stream.size(); ++i) {
    ASSERT_EQ(parser.Feed(&stream[i], 1), 1u) << "byte " << i;
    const bool at_boundary = next_event < event_ends.size() && i + 1 == event_ends[next_event];
    ASSERT_EQ(parser.HasEvent(), at_boundary) << "byte " << i;
    if (!at_boundary) continue;

    EXPECT_EQ(parser.CurrentHeader().type_code, expected[next_event].type_code);
    const uint8_t* body = nullptr;
    size_t body_len = 0;
    parser.CurrentBody(&body, &body_len);
    ASSERT_EQ(body_len, expected[next_event].body.size()) << "event " << next_event;
    if (body_len > 0) {
      ASSERT_NE(body, nullptr);
      EXPECT_EQ(std::vector<uint8_t>(body, body + body_len), expected[next_event].body)
          << "event " << next_event;
    }
    parser.Advance();
    ++next_event;
  }

  EXPECT_EQ(next_event, expected.size());
  EXPECT_EQ(parser.GetState(), ParserState::kWaitingHeader);
  EXPECT_FALSE(parser.HasEvent());
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

TEST(StateMachineTest, AdvanceReleasesBufferAboveRetentionHighWaterMark) {
  EventStreamParser parser;
  const size_t payload_size = EventStreamParserTestAccess::RetainedBufferLimit() + 1;
  std::vector<uint8_t> body(payload_size, 0xA5);
  auto event = BuildEvent(30, body);

  ASSERT_EQ(parser.Feed(event.data(), event.size()), event.size());
  ASSERT_TRUE(parser.HasEvent());
  ASSERT_GT(EventStreamParserTestAccess::BufferCapacity(parser),
            EventStreamParserTestAccess::RetainedBufferLimit());

  parser.Advance();
  EXPECT_EQ(EventStreamParserTestAccess::BufferCapacity(parser), 0u);

  auto small_event = BuildEvent(30, {0x01});
  EXPECT_EQ(parser.Feed(small_event.data(), small_event.size()), small_event.size());
  EXPECT_TRUE(parser.HasEvent());
  EXPECT_LE(EventStreamParserTestAccess::BufferCapacity(parser),
            EventStreamParserTestAccess::RetainedBufferLimit());
}

TEST(StateMachineTest, ResetAlwaysReleasesRetainedBuffer) {
  EventStreamParser parser;
  std::vector<uint8_t> body(1024 * 1024, 0x5A);
  auto event = BuildEvent(30, body);

  ASSERT_EQ(parser.Feed(event.data(), event.size()), event.size());
  ASSERT_TRUE(parser.HasEvent());
  ASSERT_GT(EventStreamParserTestAccess::BufferCapacity(parser), 0u);
  ASSERT_LE(EventStreamParserTestAccess::BufferCapacity(parser),
            EventStreamParserTestAccess::RetainedBufferLimit());

  parser.Reset();
  EXPECT_EQ(EventStreamParserTestAccess::BufferCapacity(parser), 0u);
  EXPECT_EQ(parser.GetState(), ParserState::kWaitingHeader);
}

// A 19-byte header that declares @p event_length but carries no body bytes.
std::vector<uint8_t> BuildHeaderDeclaring(uint32_t event_length) {
  test::EventBuilder b;
  b.WriteU32Le(0);  // timestamp
  b.WriteU8(30);    // type_code
  b.WriteU32Le(1);  // server_id
  b.WriteU32Le(event_length);
  b.WriteU32Le(0);  // next_position
  b.WriteU16Le(0);  // flags
  return b.Data();
}

TEST(StateMachineTest, HeaderDeclaringHugeEventDoesNotCommitTheAllocation) {
  EventStreamParser parser;
  ASSERT_GT(parser.MaxEventSize(), EventStreamParserTestAccess::MaxEagerReserve());

  // Right at the accepted ceiling, so the header passes validation and the
  // parser waits for a body that never arrives.
  auto header = BuildHeaderDeclaring(parser.MaxEventSize());
  ASSERT_EQ(parser.Feed(header.data(), header.size()), header.size());
  ASSERT_EQ(parser.GetState(), ParserState::kWaitingBody);

  // The reserve is bounded rather than removed: it still covers the appends
  // that follow, it just stops scaling with the declared length.
  const size_t capacity = EventStreamParserTestAccess::BufferCapacity(parser);
  EXPECT_GE(capacity, EventStreamParserTestAccess::MaxEagerReserve());
  EXPECT_LE(capacity, EventStreamParserTestAccess::MaxEagerReserve());
}

TEST(StateMachineTest, HeaderBeyondTheCeilingFailsWithoutAllocating) {
  EventStreamParser parser;
  parser.SetMaxEventSize(4096);

  auto header = BuildHeaderDeclaring(parser.MaxEventSize() + 1);
  EXPECT_EQ(parser.Feed(header.data(), header.size()), header.size());
  EXPECT_EQ(parser.GetState(), ParserState::kError);
  EXPECT_EQ(parser.ErrorCode(), MES_ERR_PARSE);
  EXPECT_FALSE(parser.HasEvent());
  EXPECT_LE(EventStreamParserTestAccess::BufferCapacity(parser),
            EventStreamParserTestAccess::MaxEagerReserve());
}

TEST(StateMachineTest, EventLargerThanTheEagerReserveStillAssembles) {
  // The buffer now grows from the bytes that arrive rather than from the
  // declared length, so an event well past the reserve bound must still
  // reassemble byte for byte across chunk boundaries.
  const size_t payload_size = EventStreamParserTestAccess::MaxEagerReserve() * 3;
  std::vector<uint8_t> body(payload_size, 0xC3);
  for (size_t i = 0; i < body.size(); i += 4096) {
    body[i] = static_cast<uint8_t>(i / 4096);
  }
  auto event = BuildEvent(30, body);

  EventStreamParser parser;
  const size_t chunk = 7919;  // deliberately not a divisor of the event size
  size_t offset = 0;
  while (offset < event.size()) {
    const size_t len = std::min(chunk, event.size() - offset);
    offset += parser.Feed(event.data() + offset, len);
  }

  ASSERT_TRUE(parser.HasEvent());
  ASSERT_EQ(parser.RawSize(), event.size());
  EXPECT_EQ(std::memcmp(parser.RawData(), event.data(), event.size()), 0);
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

  EventStreamParser parser;
  EXPECT_EQ(parser.Feed(event.data(), event.size()), event.size());
  EXPECT_EQ(parser.GetState(), ParserState::kError);
  EXPECT_EQ(parser.ErrorCode(), MES_ERR_CHECKSUM);
  EXPECT_FALSE(parser.HasEvent());

  parser.Reset();
  EXPECT_EQ(parser.ErrorCode(), MES_OK);
}

TEST(StateMachineTest, MaxEventSizeDefaultIs64MiB) {
  EventStreamParser parser;
  EXPECT_EQ(parser.MaxEventSize(), kDefaultMaxEventSize);
}

TEST(StateMachineTest, SetMaxEventSizeClampsToMinimum) {
  EventStreamParser parser;
  // The smallest non-zero value clamps up to header + checksum so the parser
  // can always accept at least an empty event.
  parser.SetMaxEventSize(1);
  EXPECT_EQ(parser.MaxEventSize(), static_cast<uint32_t>(kEventHeaderSize + kChecksumSize));
}

TEST(StateMachineTest, SetMaxEventSizeZeroMeansNoLimit) {
  EventStreamParser parser;
  parser.SetMaxEventSize(0);
  // 0 means "no limit": resolves to the absolute hard cap rather than the
  // minimum (which would reject every real event).
  EXPECT_EQ(parser.MaxEventSize(), kAbsoluteMaxEventSize);
}

TEST(StateMachineTest, SetMaxEventSizeZeroDoesNotRejectEvents) {
  EventStreamParser parser;
  parser.SetMaxEventSize(0);

  // A normal event must be accepted, not rejected as "too large".
  std::vector<uint8_t> body(64, 0xCD);
  auto event = test::BuildEvent(30, 1000, 0, body);
  size_t consumed = parser.Feed(event.data(), event.size());
  EXPECT_EQ(consumed, event.size());
  EXPECT_EQ(parser.GetState(), ParserState::kEventReady);
}

TEST(StateMachineTest, SetMaxEventSizeClampsToAbsoluteMax) {
  EventStreamParser parser;
  parser.SetMaxEventSize(UINT32_MAX);
  EXPECT_EQ(parser.MaxEventSize(), kAbsoluteMaxEventSize);
}

TEST(StateMachineTest, SetMaxEventSizeRejectsOversizedEvent) {
  // Build a header whose event_length is just beyond the configured cap.
  EventStreamParser parser;
  const uint32_t cap = 1024;  // 1 KiB cap
  parser.SetMaxEventSize(cap);

  // Craft a header claiming event_length = cap + 1; body content doesn't
  // matter because the parser should reject based on event_length alone.
  std::vector<uint8_t> body(cap + 1 - kEventHeaderSize - kChecksumSize, 0);
  auto event = test::BuildEvent(30, 1000, 0, body);
  // BuildEvent fills event_length correctly, so size == cap + 1.
  ASSERT_EQ(event.size(), cap + 1);

  size_t consumed = parser.Feed(event.data(), event.size());
  // Parser stops at header-size bytes because the size check fires
  // immediately after the header is parsed.
  EXPECT_EQ(consumed, kEventHeaderSize);
  EXPECT_EQ(parser.GetState(), ParserState::kError);
}

TEST(StateMachineTest, SetMaxEventSizeAcceptsEventAtLimit) {
  EventStreamParser parser;
  const uint32_t cap = kEventHeaderSize + kChecksumSize + 32;
  parser.SetMaxEventSize(cap);

  std::vector<uint8_t> body(32, 0xAB);
  auto event = test::BuildEvent(30, 1000, 0, body);
  ASSERT_EQ(event.size(), cap);

  size_t consumed = parser.Feed(event.data(), event.size());
  EXPECT_EQ(consumed, event.size());
  EXPECT_EQ(parser.GetState(), ParserState::kEventReady);
}

// --- Checksum handling and FDE auto-detection ---

// Build a minimal FORMAT_DESCRIPTION_EVENT body whose final byte is the given
// checksum algorithm descriptor.
std::vector<uint8_t> BuildFdeBody(uint8_t checksum_alg) {
  test::EventBuilder fde;
  fde.WriteU16Le(4);  // binlog_version
  for (int i = 0; i < 50; ++i) {
    fde.WriteU8(0);  // server_version (50 bytes)
  }
  fde.WriteU32Le(0);  // create_timestamp
  fde.WriteU8(19);    // event_header_length
  fde.WriteU8(0);     // one event-type header-length entry
  fde.WriteU8(checksum_alg);
  return fde.Data();
}

TEST(StateMachineChecksumTest, DetectsCrc32Fde) {
  EventStreamParser parser;
  // BuildEvent appends a 4-byte checksum, placing the alg byte at size-5.
  auto fde = test::BuildEvent(static_cast<uint8_t>(BinlogEventType::kFormatDescriptionEvent), 1000,
                              0, BuildFdeBody(kBinlogChecksumAlgCrc32));
  parser.Feed(fde.data(), fde.size());
  EXPECT_TRUE(parser.ChecksumEnabled());
}

TEST(StateMachineChecksumTest, DetectsOffFde) {
  EventStreamParser parser;
  // No trailing checksum; the OFF alg byte is the final byte.
  auto fde =
      test::BuildEventNoChecksum(static_cast<uint8_t>(BinlogEventType::kFormatDescriptionEvent),
                                 1000, 0, BuildFdeBody(kBinlogChecksumAlgOff));
  parser.Feed(fde.data(), fde.size());
  EXPECT_FALSE(parser.ChecksumEnabled());
}

TEST(StateMachineChecksumTest, ExplicitSetterControlsBodySize) {
  EventStreamParser parser;
  parser.SetChecksumEnabled(false);
  std::vector<uint8_t> body = {0x01, 0x02, 0x03, 0x04, 0x05};
  auto event = test::BuildEventNoChecksum(30, 1000, 0, body);
  ASSERT_EQ(parser.Feed(event.data(), event.size()), event.size());
  ASSERT_TRUE(parser.HasEvent());
  const uint8_t* body_ptr = nullptr;
  size_t body_len = 0;
  parser.CurrentBody(&body_ptr, &body_len);
  EXPECT_EQ(body_len, body.size());
}

TEST(StateMachineChecksumTest, ChecksumNoneStillStripsChecksummedArtificialRotate) {
  EventStreamParser parser;
  parser.SetChecksumEnabled(false);
  const auto body = test::BuildRotateBody(4, "mysql-bin.000010");
  auto event = test::BuildEvent(static_cast<uint8_t>(BinlogEventType::kRotateEvent), 0, 0, body);
  event[17] = static_cast<uint8_t>(kLogEventArtificialFlag);
  const uint32_t crc = ComputeCRC32(event.data(), event.size() - kChecksumSize);
  std::memcpy(event.data() + event.size() - kChecksumSize, &crc, sizeof(crc));

  ASSERT_EQ(parser.Feed(event.data(), event.size()), event.size());
  ASSERT_TRUE(parser.HasEvent());
  const uint8_t* body_ptr = nullptr;
  size_t body_len = 0;
  parser.CurrentBody(&body_ptr, &body_len);
  EXPECT_EQ(body_len, body.size());
  EXPECT_EQ(std::vector<uint8_t>(body_ptr, body_ptr + body_len), body);
  EXPECT_FALSE(parser.ChecksumEnabled());
}

TEST(StateMachineChecksumTest, ChecksumSetterDoesNotReframeTheEventBeingParsed) {
  EventStreamParser parser;  // defaults to checksummed framing
  const std::vector<uint8_t> body = {0x01, 0x02, 0x03, 0x04, 0x05};
  auto event = test::BuildEvent(30, 1000, 0, body);

  // Fix the framing by parsing the header, then flip the setter mid-event.
  const size_t prefix = kEventHeaderSize + 1;
  ASSERT_EQ(parser.Feed(event.data(), prefix), prefix);
  ASSERT_FALSE(parser.HasEvent());
  parser.SetChecksumEnabled(false);
  ASSERT_EQ(parser.Feed(event.data() + prefix, event.size() - prefix), event.size() - prefix);
  ASSERT_TRUE(parser.HasEvent());

  const uint8_t* body_ptr = nullptr;
  size_t body_len = 0;
  parser.CurrentBody(&body_ptr, &body_len);
  EXPECT_EQ(body_len, body.size());
  EXPECT_EQ(std::vector<uint8_t>(body_ptr, body_ptr + body_len), body);

  // The new setting applies from the next event on.
  parser.Advance();
  auto next = test::BuildEventNoChecksum(30, 1001, 0, body);
  ASSERT_EQ(parser.Feed(next.data(), next.size()), next.size());
  ASSERT_TRUE(parser.HasEvent());
  parser.CurrentBody(&body_ptr, &body_len);
  EXPECT_EQ(body_len, body.size());
}

TEST(StateMachineChecksumTest, ArtificialRotateDetectionDoesNotReframeTheEventBeingParsed) {
  EventStreamParser parser;  // defaults to checksummed framing
  const auto body = test::BuildRotateBody(4, "mysql-bin.000010");
  auto event = test::BuildEvent(static_cast<uint8_t>(BinlogEventType::kRotateEvent), 0, 0, body);
  event[17] = static_cast<uint8_t>(kLogEventArtificialFlag);
  const uint32_t crc = ComputeCRC32(event.data(), event.size() - kChecksumSize);
  std::memcpy(event.data() + event.size() - kChecksumSize, &crc, sizeof(crc));

  const size_t prefix = kEventHeaderSize + 1;
  ASSERT_EQ(parser.Feed(event.data(), prefix), prefix);
  parser.SetChecksumEnabled(false);
  ASSERT_EQ(parser.Feed(event.data() + prefix, event.size() - prefix), event.size() - prefix);
  ASSERT_TRUE(parser.HasEvent());

  const uint8_t* body_ptr = nullptr;
  size_t body_len = 0;
  parser.CurrentBody(&body_ptr, &body_len);
  EXPECT_EQ(body_len, body.size());
  EXPECT_EQ(std::vector<uint8_t>(body_ptr, body_ptr + body_len), body);
}

// --- Sharing a producer's trailer verification ---

// An event whose trailer bytes do not match its contents: the parser rejects
// it when it validates the trailer itself, and is expected to accept it when
// told the producer already did.
std::vector<uint8_t> BuildEventWithBrokenTrailer(uint8_t type_code,
                                                 const std::vector<uint8_t>& body) {
  auto event = test::BuildEvent(type_code, 1000, 0, body);
  event[event.size() - kChecksumSize] ^= 0xFF;
  return event;
}

TEST(StateMachineChecksumTest, PreVerifiedTrailerCostsNoChecksumPass) {
  const std::vector<uint8_t> body = {0x01, 0x02, 0x03, 0x04, 0x05};
  auto event = test::BuildEvent(30, 1000, 0, body);

  EventStreamParser verifying;
  ASSERT_EQ(verifying.Feed(event.data(), event.size()), event.size());
  ASSERT_TRUE(verifying.HasEvent());
  // The count has to move for its absence below to mean anything.
  ASSERT_EQ(EventStreamParserTestAccess::Crc32Passes(verifying), 1u);

  EventStreamParser sharing;
  sharing.SetTrailerPreVerified(true);
  ASSERT_EQ(sharing.Feed(event.data(), event.size()), event.size());
  ASSERT_TRUE(sharing.HasEvent());
  EXPECT_EQ(EventStreamParserTestAccess::Crc32Passes(sharing), 0u);

  // Framing is untouched: the trailer is still excluded from the body.
  const uint8_t* body_ptr = nullptr;
  size_t body_len = 0;
  sharing.CurrentBody(&body_ptr, &body_len);
  EXPECT_EQ(body_len, body.size());
  EXPECT_EQ(std::vector<uint8_t>(body_ptr, body_ptr + body_len), body);
}

TEST(StateMachineChecksumTest, PreVerifiedTrailerIsNotValidatedAgain) {
  auto event = BuildEventWithBrokenTrailer(30, {0x01, 0x02, 0x03, 0x04, 0x05});

  EventStreamParser verifying;
  ASSERT_EQ(verifying.Feed(event.data(), event.size()), event.size());
  ASSERT_EQ(verifying.GetState(), ParserState::kError);
  ASSERT_EQ(verifying.ErrorCode(), MES_ERR_CHECKSUM);

  EventStreamParser sharing;
  sharing.SetTrailerPreVerified(true);
  ASSERT_EQ(sharing.Feed(event.data(), event.size()), event.size());
  EXPECT_TRUE(sharing.HasEvent());
  EXPECT_EQ(sharing.ErrorCode(), MES_OK);
  EXPECT_EQ(EventStreamParserTestAccess::Crc32Passes(sharing), 0u);
}

TEST(StateMachineChecksumTest, FramingAndVerificationAreSeparateSwitches) {
  // Neither switch may reach into the other's behaviour. Across the four
  // combinations, the framed body length follows SetChecksumEnabled() alone
  // and whether a broken trailer is rejected follows SetTrailerPreVerified()
  // alone, so no single flag can silently trade one for the other.
  const std::vector<uint8_t> body = {0x01, 0x02, 0x03, 0x04, 0x05};
  auto event = BuildEventWithBrokenTrailer(30, body);

  struct Combination {
    bool checksum_framing;
    bool pre_verified;
    size_t expected_body_len;
    bool expect_rejected;
  };
  // With framing off the trailer bytes are body, so the body is four longer.
  const std::vector<Combination> combinations = {
      {true, false, body.size(), true},
      {true, true, body.size(), false},
      {false, false, body.size() + kChecksumSize, false},
      {false, true, body.size() + kChecksumSize, false},
  };

  for (const auto& c : combinations) {
    EventStreamParser parser;
    parser.SetChecksumEnabled(c.checksum_framing);
    parser.SetTrailerPreVerified(c.pre_verified);
    const std::string what = std::string("framing=") + (c.checksum_framing ? "on" : "off") +
                             " pre_verified=" + (c.pre_verified ? "on" : "off");

    ASSERT_EQ(parser.Feed(event.data(), event.size()), event.size()) << what;
    if (c.expect_rejected) {
      EXPECT_EQ(parser.GetState(), ParserState::kError) << what;
      EXPECT_EQ(parser.ErrorCode(), MES_ERR_CHECKSUM) << what;
      continue;
    }
    ASSERT_TRUE(parser.HasEvent()) << what;
    const uint8_t* body_ptr = nullptr;
    size_t body_len = 0;
    parser.CurrentBody(&body_ptr, &body_len);
    EXPECT_EQ(body_len, c.expected_body_len) << what;
  }
}

TEST(StateMachineChecksumTest, PreVerifiedTrailerStillFramesEveryEventInAStream) {
  // The flag is per-parser, not per-event: a whole stream keeps its framing.
  const std::vector<uint8_t> body = {0xAA, 0xBB, 0xCC};
  EventStreamParser parser;
  parser.SetTrailerPreVerified(true);

  for (uint32_t i = 0; i < 3; ++i) {
    auto event = BuildEventWithBrokenTrailer(30, body);
    ASSERT_EQ(parser.Feed(event.data(), event.size()), event.size()) << "event " << i;
    ASSERT_TRUE(parser.HasEvent()) << "event " << i;
    const uint8_t* body_ptr = nullptr;
    size_t body_len = 0;
    parser.CurrentBody(&body_ptr, &body_len);
    EXPECT_EQ(body_len, body.size()) << "event " << i;
    EXPECT_EQ(std::vector<uint8_t>(body_ptr, body_ptr + body_len), body) << "event " << i;
    parser.Advance();
  }
  EXPECT_EQ(EventStreamParserTestAccess::Crc32Passes(parser), 0u);
}

}  // namespace
}  // namespace mes
