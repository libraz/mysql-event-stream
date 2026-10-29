// Primitive framing pieces: byte readers, packed ints, bitmaps, CRC32, the
// common header, event type names and the ROTATE body.

#include <gtest/gtest.h>

#include <cstdint>
#include <cstring>
#include <limits>
#include <string>
#include <vector>

#include "mes/codec/bytes.h"
#include "mes/codec/crc32.h"
#include "mes/codec/event_header.h"
#include "mes/codec/event_types.h"
#include "mes/codec/rotate.h"

namespace mes::codec {
namespace {

// --- Little-/big-endian loads ---

TEST(CodecFraming, ReadU8) {
  uint8_t data[] = {0x42};
  EXPECT_EQ(ReadU8(data), 0x42);
}

TEST(CodecFraming, ReadU16Le) {
  uint8_t data[] = {0x34, 0x12};
  EXPECT_EQ(ReadU16Le(data), 0x1234);
  uint8_t zero[] = {0x00, 0x00};
  EXPECT_EQ(ReadU16Le(zero), 0x0000);
  uint8_t max[] = {0xFF, 0xFF};
  EXPECT_EQ(ReadU16Le(max), 0xFFFF);
}

TEST(CodecFraming, ReadU24Le) {
  uint8_t data[] = {0x56, 0x34, 0x12};
  EXPECT_EQ(ReadU24Le(data), 0x123456u);
}

TEST(CodecFraming, ReadU32Le) {
  uint8_t data[] = {0x78, 0x56, 0x34, 0x12};
  EXPECT_EQ(ReadU32Le(data), 0x12345678u);
}

TEST(CodecFraming, ReadU48Le) {
  uint8_t data[] = {0xBC, 0x9A, 0x78, 0x56, 0x34, 0x12};
  EXPECT_EQ(ReadU48Le(data), 0x123456789ABCuLL);
}

TEST(CodecFraming, ReadU64Le) {
  uint8_t data[] = {0xEF, 0xCD, 0xAB, 0x90, 0x78, 0x56, 0x34, 0x12};
  EXPECT_EQ(ReadU64Le(data), 0x1234567890ABCDEFuLL);
  uint8_t zero[] = {0, 0, 0, 0, 0, 0, 0, 0};
  EXPECT_EQ(ReadU64Le(zero), 0u);
}

TEST(CodecFraming, ReadBigEndian) {
  uint8_t d16[] = {0x12, 0x34};
  EXPECT_EQ(ReadU16Be(d16), 0x1234);
  uint8_t d24[] = {0x12, 0x34, 0x56};
  EXPECT_EQ(ReadU24Be(d24), 0x123456u);
  uint8_t d32[] = {0x12, 0x34, 0x56, 0x78};
  EXPECT_EQ(ReadU32Be(d32), 0x12345678u);
}

// --- Packed integers ---

struct Packed {
  Status status;
  uint64_t value;
  size_t consumed;
};

Packed DecodePacked(const uint8_t* data, size_t len) {
  Packed r{Status::kOk, 0xDEAD, 99};
  r.status = ReadPackedInt(data, len, &r.value, &r.consumed);
  return r;
}

TEST(CodecFraming, PackedIntOneByte) {
  uint8_t data[] = {42};
  auto r = DecodePacked(data, sizeof(data));
  EXPECT_EQ(r.status, Status::kOk);
  EXPECT_EQ(r.value, 42u);
  EXPECT_EQ(r.consumed, 1u);

  uint8_t zero[] = {0};
  uint8_t max[] = {250};
  EXPECT_EQ(DecodePacked(zero, 1).value, 0u);
  EXPECT_EQ(DecodePacked(max, 1).value, 250u);
}

TEST(CodecFraming, PackedIntWideForms) {
  uint8_t d2[] = {252, 0x00, 0x01};
  auto r2 = DecodePacked(d2, sizeof(d2));
  EXPECT_EQ(r2.status, Status::kOk);
  EXPECT_EQ(r2.value, 256u);
  EXPECT_EQ(r2.consumed, 3u);

  uint8_t d3[] = {253, 0x01, 0x00, 0x01};
  auto r3 = DecodePacked(d3, sizeof(d3));
  EXPECT_EQ(r3.status, Status::kOk);
  EXPECT_EQ(r3.value, 65537u);
  EXPECT_EQ(r3.consumed, 4u);

  uint8_t d8[] = {254, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F};
  auto r8 = DecodePacked(d8, sizeof(d8));
  EXPECT_EQ(r8.status, Status::kOk);
  EXPECT_EQ(r8.value, 0x7FFFFFFFFFFFFFFFuLL);
  EXPECT_EQ(r8.consumed, 9u);
}

TEST(CodecFraming, PackedIntRejectsNullMarkerAndUndefinedPrefix) {
  uint8_t null_marker[] = {0xFB};
  uint8_t undefined[] = {0xFF, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};
  for (auto r : {DecodePacked(null_marker, sizeof(null_marker)),
                 DecodePacked(undefined, sizeof(undefined))}) {
    EXPECT_EQ(r.status, Status::kInvalidMetadata);
    EXPECT_EQ(r.value, 0xDEADu);
    EXPECT_EQ(r.consumed, 99u);
  }
}

TEST(CodecFraming, PackedIntTruncated) {
  uint8_t empty[] = {0};
  EXPECT_EQ(DecodePacked(empty, 0).status, Status::kTruncated);
  uint8_t d252[] = {252, 0x00};
  uint8_t d253[] = {253, 0x01, 0x00};
  uint8_t d254[] = {254, 0x01, 0x00, 0x00, 0x00};
  for (auto r : {DecodePacked(d252, sizeof(d252)), DecodePacked(d253, sizeof(d253)),
                 DecodePacked(d254, sizeof(d254))}) {
    EXPECT_EQ(r.status, Status::kTruncated);
    EXPECT_EQ(r.consumed, 99u);
  }
}

// --- ByteReader ---

TEST(CodecFraming, ByteReaderReadsInOrderAndStopsAtEnd) {
  const uint8_t data[] = {0x01, 0x34, 0x12, 0xFC, 0x00, 0x01, 0xAA};
  ByteReader reader(data, sizeof(data));
  uint8_t u8 = 0;
  uint16_t u16 = 0;
  uint64_t packed = 0;
  ASSERT_EQ(reader.ReadU8(&u8), Status::kOk);
  ASSERT_EQ(reader.ReadU16Le(&u16), Status::kOk);
  ASSERT_EQ(reader.ReadPacked(&packed), Status::kOk);
  EXPECT_EQ(u8, 0x01);
  EXPECT_EQ(u16, 0x1234);
  EXPECT_EQ(packed, 256u);
  EXPECT_EQ(reader.remaining(), 1u);

  uint32_t u32 = 0x5555;
  EXPECT_EQ(reader.ReadU32Le(&u32), Status::kTruncated);
  EXPECT_EQ(u32, 0x5555u);
  EXPECT_EQ(reader.remaining(), 1u);
  EXPECT_EQ(reader.Skip(2), Status::kTruncated);
  const uint8_t* tail = nullptr;
  ASSERT_EQ(reader.ReadBytes(1, &tail), Status::kOk);
  EXPECT_EQ(*tail, 0xAA);
  EXPECT_EQ(reader.remaining(), 0u);
}

// A wire length near SIZE_MAX must fail the bound, not wrap a pointer sum.
TEST(CodecFraming, ByteReaderHugeLengthIsTruncated) {
  const uint8_t data[] = {0x00};
  ByteReader reader(data, sizeof(data));
  EXPECT_EQ(reader.Skip(std::numeric_limits<size_t>::max()), Status::kTruncated);
  EXPECT_EQ(reader.remaining(), 1u);
}

// --- Bitmaps ---

TEST(CodecFraming, BitmapBytes) {
  EXPECT_EQ(BitmapBytes(0), 0u);
  EXPECT_EQ(BitmapBytes(1), 1u);
  EXPECT_EQ(BitmapBytes(7), 1u);
  EXPECT_EQ(BitmapBytes(8), 1u);
  EXPECT_EQ(BitmapBytes(9), 2u);
  EXPECT_EQ(BitmapBytes(16), 2u);
  EXPECT_EQ(BitmapBytes(17), 3u);
}

TEST(CodecFraming, BitmapBytesSaturatesNearSizeMax) {
  constexpr size_t kMax = std::numeric_limits<size_t>::max();
  EXPECT_EQ(BitmapBytes(kMax - 7), kMax / 8);
  EXPECT_EQ(BitmapBytes(kMax - 6), kMax / 8 + 1);
  EXPECT_EQ(BitmapBytes(kMax - 1), kMax / 8 + 1);
  EXPECT_EQ(BitmapBytes(kMax), kMax / 8 + 1);
}

TEST(CodecFraming, BitmapIsSet) {
  uint8_t bitmap[] = {0b10100101, 0b00001111};
  const bool expected[] = {true, false, true, false, false, true,  false, true,
                           true, true,  true, true,  false, false, false, false};
  for (size_t i = 0; i < 16; ++i) {
    EXPECT_EQ(BitmapIsSet(bitmap, i), expected[i]) << "bit " << i;
  }
}

// --- CRC32 ---

TEST(CodecFraming, Crc32KnownValues) {
  EXPECT_EQ(ComputeCrc32("hello", 5), 0x3610A686u);
  EXPECT_EQ(ComputeCrc32(nullptr, 0), 0u);
}

TEST(CodecFraming, Crc32IncrementalMatchesOneShot) {
  const std::string text = "The quick brown fox jumps over the lazy dog";
  const uint32_t whole = ComputeCrc32(text.data(), text.size());
  EXPECT_EQ(whole, 0x414FA339u);
  uint32_t running = UpdateCrc32(0, text.data(), 10);
  running = UpdateCrc32(running, text.data() + 10, 0);
  running = UpdateCrc32(running, text.data() + 10, text.size() - 10);
  EXPECT_EQ(running, whole);
}

TEST(CodecFraming, Crc32DetectsCorruption) {
  std::vector<uint8_t> data = {0x01, 0x02, 0x03, 0x04, 0x05};
  const uint32_t clean = ComputeCrc32(data.data(), data.size());
  EXPECT_EQ(ComputeCrc32(data.data(), data.size()), clean);
  data[2] ^= 0xFF;
  EXPECT_NE(ComputeCrc32(data.data(), data.size()), clean);
}

// --- Common header ---

void BuildHeader(uint8_t* buf, uint32_t timestamp, uint8_t type_code, uint32_t server_id,
                 uint32_t event_length, uint32_t next_position, uint16_t flags) {
  std::memcpy(buf, &timestamp, 4);
  buf[4] = type_code;
  std::memcpy(buf + 5, &server_id, 4);
  std::memcpy(buf + 9, &event_length, 4);
  std::memcpy(buf + 13, &next_position, 4);
  std::memcpy(buf + 17, &flags, 2);
}

TEST(CodecFraming, ParseValidHeader) {
  uint8_t buf[19];
  BuildHeader(buf, 1700000000, 30, 1, 100, 200, 0x0001);
  EventHeader header;
  ASSERT_EQ(ParseEventHeader(buf, sizeof(buf), &header), Status::kOk);
  EXPECT_EQ(header.timestamp, 1700000000u);
  EXPECT_EQ(header.type_code, 30);
  EXPECT_EQ(header.server_id, 1u);
  EXPECT_EQ(header.event_length, 100u);
  EXPECT_EQ(header.next_position, 200u);
  EXPECT_EQ(header.flags, 0x0001);
}

TEST(CodecFraming, ParseHeaderFromLargerBuffer) {
  uint8_t buf[32] = {};
  BuildHeader(buf, 12345, 19, 42, 50, 73, 0);
  EventHeader header;
  ASSERT_EQ(ParseEventHeader(buf, sizeof(buf), &header), Status::kOk);
  EXPECT_EQ(header.timestamp, 12345u);
  EXPECT_EQ(header.type_code, 19);
  EXPECT_EQ(header.server_id, 42u);
  EXPECT_EQ(header.event_length, 50u);
  EXPECT_EQ(header.next_position, 73u);
  EXPECT_EQ(header.flags, 0);
}

TEST(CodecFraming, ParseHeaderTooShort) {
  uint8_t buf[18] = {};
  EventHeader header;
  EXPECT_EQ(ParseEventHeader(buf, sizeof(buf), &header), Status::kTruncated);
  EXPECT_EQ(ParseEventHeader(nullptr, 19, &header), Status::kTruncated);
}

// --- Event types ---

TEST(CodecFraming, IsRowsEvent) {
  for (EventType type :
       {EventType::kWriteRowsEventV1, EventType::kUpdateRowsEventV1, EventType::kDeleteRowsEventV1,
        EventType::kWriteRowsEvent, EventType::kUpdateRowsEvent, EventType::kDeleteRowsEvent}) {
    EXPECT_TRUE(IsRowsEvent(type)) << EventTypeName(type);
    EXPECT_TRUE(IsRowsEvent(static_cast<uint8_t>(type))) << EventTypeName(type);
  }
  for (EventType type :
       {EventType::kTableMapEvent, EventType::kRotateEvent, EventType::kFormatDescriptionEvent,
        EventType::kXidEvent, EventType::kGtidLogEvent, EventType::kUnknown,
        EventType::kPartialUpdateRowsEvent, EventType::kMariaDBWriteRowsCompressedEvent}) {
    EXPECT_FALSE(IsRowsEvent(type)) << EventTypeName(type);
  }
  EXPECT_FALSE(IsRowsEvent(static_cast<uint8_t>(99)));
}

TEST(CodecFraming, EventTypeNameKnown) {
  EXPECT_STREQ(EventTypeName(4), "ROTATE_EVENT");
  EXPECT_STREQ(EventTypeName(15), "FORMAT_DESCRIPTION_EVENT");
  EXPECT_STREQ(EventTypeName(16), "XID_EVENT");
  EXPECT_STREQ(EventTypeName(19), "TABLE_MAP_EVENT");
  EXPECT_STREQ(EventTypeName(23), "WRITE_ROWS_EVENT_V1");
  EXPECT_STREQ(EventTypeName(24), "UPDATE_ROWS_EVENT_V1");
  EXPECT_STREQ(EventTypeName(25), "DELETE_ROWS_EVENT_V1");
  EXPECT_STREQ(EventTypeName(30), "WRITE_ROWS_EVENT");
  EXPECT_STREQ(EventTypeName(31), "UPDATE_ROWS_EVENT");
  EXPECT_STREQ(EventTypeName(32), "DELETE_ROWS_EVENT");
  EXPECT_STREQ(EventTypeName(33), "GTID_LOG_EVENT");
  EXPECT_STREQ(EventTypeName(34), "ANONYMOUS_GTID_LOG_EVENT");
  EXPECT_STREQ(EventTypeName(42), "GTID_TAGGED_LOG_EVENT");
  EXPECT_STREQ(EventTypeName(160), "MARIADB_ANNOTATE_ROWS_EVENT");
  EXPECT_STREQ(EventTypeName(172), "MARIADB_PARTIAL_ROW_DATA_EVENT");
}

// Codes 0, 1 and 255 are enumerated now and carry their upstream names.
TEST(CodecFraming, EventTypeNameUnenumerated) {
  EXPECT_STREQ(EventTypeName(0), "UNKNOWN_EVENT");
  EXPECT_STREQ(EventTypeName(1), "START_EVENT_V3");
  EXPECT_STREQ(EventTypeName(255), "ENUM_END_EVENT");
  EXPECT_STREQ(EventTypeName(6), "UNKNOWN");
  EXPECT_STREQ(EventTypeName(43), "UNKNOWN");
  EXPECT_STREQ(EventTypeName(99), "UNKNOWN");
  EXPECT_STREQ(EventTypeName(173), "UNKNOWN");
}

TEST(CodecFraming, HeaderFlagValues) {
  EXPECT_EQ(kLogEventArtificialFlag, 0x20);
  EXPECT_EQ(kLogEventIgnorableFlag, 0x80);
}

TEST(CodecFraming, ColumnTypeValuesMatchWire) {
  EXPECT_EQ(static_cast<uint8_t>(ColumnType::kLong), 0x03);
  EXPECT_EQ(static_cast<uint8_t>(ColumnType::kTypedArray), 0x14);
  EXPECT_EQ(static_cast<uint8_t>(ColumnType::kBlobCompressed), 0x8C);
  EXPECT_EQ(static_cast<uint8_t>(ColumnType::kVector), 0xF2);
  EXPECT_EQ(static_cast<uint8_t>(ColumnType::kGeometry), 0xFF);
}

// --- ROTATE body ---

std::vector<uint8_t> RotateBody(uint64_t position, const std::string& name) {
  std::vector<uint8_t> body(8);
  std::memcpy(body.data(), &position, 8);
  body.insert(body.end(), name.begin(), name.end());
  return body;
}

TEST(CodecFraming, RotateValid) {
  auto body = RotateBody(4, "mysql-bin.000002");
  RotateEvent result;
  ASSERT_EQ(ParseRotateEvent(body.data(), body.size(), &result), Status::kOk);
  EXPECT_EQ(result.position, 4u);
  EXPECT_EQ(result.next_file, "mysql-bin.000002");
}

TEST(CodecFraming, RotateLargePosition) {
  auto body = RotateBody(123456789012345ULL, "binlog.000100");
  RotateEvent result;
  ASSERT_EQ(ParseRotateEvent(body.data(), body.size(), &result), Status::kOk);
  EXPECT_EQ(result.position, 123456789012345ULL);
  EXPECT_EQ(result.next_file, "binlog.000100");
}

TEST(CodecFraming, RotatePositionOnly) {
  auto body = RotateBody(4, "");
  RotateEvent result;
  result.next_file = "stale";
  ASSERT_EQ(ParseRotateEvent(body.data(), body.size(), &result), Status::kOk);
  EXPECT_EQ(result.position, 4u);
  EXPECT_TRUE(result.next_file.empty());
}

TEST(CodecFraming, RotateTrailingNulIsDropped) {
  auto body = RotateBody(4, std::string("binlog.000003\0", 14));
  RotateEvent result;
  ASSERT_EQ(ParseRotateEvent(body.data(), body.size(), &result), Status::kOk);
  EXPECT_EQ(result.next_file, "binlog.000003");
}

TEST(CodecFraming, RotateTooShort) {
  uint8_t buf[7] = {0};
  RotateEvent result;
  EXPECT_EQ(ParseRotateEvent(buf, sizeof(buf), &result), Status::kTruncated);
  EXPECT_EQ(ParseRotateEvent(nullptr, 100, &result), Status::kTruncated);
}

}  // namespace
}  // namespace mes::codec
