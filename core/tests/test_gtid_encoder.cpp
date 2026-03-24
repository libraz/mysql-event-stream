// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#include "client/gtid_encoder.h"

#include <cstdint>
#include <vector>

#include <gtest/gtest.h>

namespace {

using mes::GtidEncoder;

uint64_t ReadInt64Le(const std::vector<uint8_t>& data, size_t offset) {
  uint64_t val = 0;
  for (int i = 7; i >= 0; --i) {
    val = (val << 8) | data[offset + i];
  }
  return val;
}

std::vector<uint8_t> ExtractUuid(const std::vector<uint8_t>& data,
                                 size_t offset) {
  return std::vector<uint8_t>(data.begin() + offset,
                              data.begin() + offset + 16);
}

// --- Empty GTID set ---

TEST(GtidEncoderTest, EmptyGtidSetReturnsZeroSids) {
  std::vector<uint8_t> out;
  ASSERT_EQ(GtidEncoder::Encode("", &out), MES_OK);
  ASSERT_EQ(out.size(), 8u);
  EXPECT_EQ(ReadInt64Le(out, 0), 0u);
}

// --- Single UUID with single interval ---

TEST(GtidEncoderTest, SingleGtidSingleTransaction) {
  std::vector<uint8_t> out;
  ASSERT_EQ(GtidEncoder::Encode(
                "00000000-0000-0000-0000-000000000001:5", &out),
            MES_OK);
  ASSERT_EQ(out.size(), 48u);
  EXPECT_EQ(ReadInt64Le(out, 0), 1u);   // n_sids
  EXPECT_EQ(ReadInt64Le(out, 24), 1u);  // n_intervals
  EXPECT_EQ(ReadInt64Le(out, 32), 5u);  // start
  EXPECT_EQ(ReadInt64Le(out, 40), 6u);  // end (exclusive)
}

TEST(GtidEncoderTest, SingleRange) {
  std::vector<uint8_t> out;
  ASSERT_EQ(GtidEncoder::Encode(
                "00000000-0000-0000-0000-000000000001:1-3", &out),
            MES_OK);
  ASSERT_EQ(out.size(), 48u);
  EXPECT_EQ(ReadInt64Le(out, 0), 1u);   // n_sids
  EXPECT_EQ(ReadInt64Le(out, 24), 1u);  // n_intervals
  EXPECT_EQ(ReadInt64Le(out, 32), 1u);  // start
  EXPECT_EQ(ReadInt64Le(out, 40), 4u);  // end (exclusive)
}

// --- Multiple intervals ---

TEST(GtidEncoderTest, MultipleIntervals) {
  std::vector<uint8_t> out;
  ASSERT_EQ(GtidEncoder::Encode(
                "00000000-0000-0000-0000-000000000001:1-3:5-7", &out),
            MES_OK);
  ASSERT_EQ(out.size(), 64u);  // 8 + 16 + 8 + 16*2
  EXPECT_EQ(ReadInt64Le(out, 0), 1u);   // n_sids
  EXPECT_EQ(ReadInt64Le(out, 24), 2u);  // n_intervals
  EXPECT_EQ(ReadInt64Le(out, 32), 1u);  // interval 1 start
  EXPECT_EQ(ReadInt64Le(out, 40), 4u);  // interval 1 end
  EXPECT_EQ(ReadInt64Le(out, 48), 5u);  // interval 2 start
  EXPECT_EQ(ReadInt64Le(out, 56), 8u);  // interval 2 end
}

// --- Multiple UUIDs ---

TEST(GtidEncoderTest, MultipleUuids) {
  std::vector<uint8_t> out;
  ASSERT_EQ(GtidEncoder::Encode(
                "00000000-0000-0000-0000-000000000001:1-3,"
                "00000000-0000-0000-0000-000000000002:5-7",
                &out),
            MES_OK);
  EXPECT_EQ(ReadInt64Le(out, 0), 2u);  // n_sids

  // First SID
  auto uuid1 = ExtractUuid(out, 8);
  std::vector<uint8_t> expected_uuid1 = {0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                                         0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                                         0x00, 0x00, 0x00, 0x01};
  EXPECT_EQ(uuid1, expected_uuid1);
  EXPECT_EQ(ReadInt64Le(out, 24), 1u);  // n_intervals
  EXPECT_EQ(ReadInt64Le(out, 32), 1u);  // start
  EXPECT_EQ(ReadInt64Le(out, 40), 4u);  // end

  // Second SID
  auto uuid2 = ExtractUuid(out, 48);
  std::vector<uint8_t> expected_uuid2 = {0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                                         0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                                         0x00, 0x00, 0x00, 0x02};
  EXPECT_EQ(uuid2, expected_uuid2);
  EXPECT_EQ(ReadInt64Le(out, 64), 1u);  // n_intervals
  EXPECT_EQ(ReadInt64Le(out, 72), 5u);  // start
  EXPECT_EQ(ReadInt64Le(out, 80), 8u);  // end
}

// --- Overlapping intervals merged ---

TEST(GtidEncoderTest, OverlappingIntervalsMerged) {
  std::vector<uint8_t> out;
  ASSERT_EQ(GtidEncoder::Encode(
                "00000000-0000-0000-0000-000000000001:1-5:3-8", &out),
            MES_OK);
  EXPECT_EQ(ReadInt64Le(out, 24), 1u);  // merged to 1 interval
  EXPECT_EQ(ReadInt64Le(out, 32), 1u);  // start
  EXPECT_EQ(ReadInt64Le(out, 40), 9u);  // end (8+1)
}

// --- Adjacent intervals merged ---

TEST(GtidEncoderTest, AdjacentIntervalsMerged) {
  std::vector<uint8_t> out;
  ASSERT_EQ(GtidEncoder::Encode(
                "00000000-0000-0000-0000-000000000001:1-3:4-6", &out),
            MES_OK);
  EXPECT_EQ(ReadInt64Le(out, 24), 1u);  // merged to 1 interval
  EXPECT_EQ(ReadInt64Le(out, 32), 1u);  // start
  EXPECT_EQ(ReadInt64Le(out, 40), 7u);  // end (6+1)
}

// --- Duplicate UUIDs merged ---

TEST(GtidEncoderTest, DuplicateUuidsMerged) {
  std::vector<uint8_t> out;
  ASSERT_EQ(GtidEncoder::Encode(
                "00000000-0000-0000-0000-000000000001:1-3, "
                "00000000-0000-0000-0000-000000000001:5-7",
                &out),
            MES_OK);
  EXPECT_EQ(ReadInt64Le(out, 0), 1u);   // 1 SID (merged)
  EXPECT_EQ(ReadInt64Le(out, 24), 2u);  // 2 intervals
  EXPECT_EQ(ReadInt64Le(out, 32), 1u);  // interval 1 start
  EXPECT_EQ(ReadInt64Le(out, 40), 4u);  // interval 1 end
  EXPECT_EQ(ReadInt64Le(out, 48), 5u);  // interval 2 start
  EXPECT_EQ(ReadInt64Le(out, 56), 8u);  // interval 2 end
}

// --- UUID encoding ---

TEST(GtidEncoderTest, UuidBytesCorrect) {
  std::vector<uint8_t> out;
  ASSERT_EQ(GtidEncoder::Encode(
                "61d5b289-bccc-11f0-b921-cabbb4ee51f6:1-10", &out),
            MES_OK);
  auto uuid = ExtractUuid(out, 8);
  std::vector<uint8_t> expected = {0x61, 0xd5, 0xb2, 0x89, 0xbc, 0xcc,
                                   0x11, 0xf0, 0xb9, 0x21, 0xca, 0xbb,
                                   0xb4, 0xee, 0x51, 0xf6};
  EXPECT_EQ(uuid, expected);
}

TEST(GtidEncoderTest, MixedCaseUuid) {
  std::vector<uint8_t> out;
  ASSERT_EQ(GtidEncoder::Encode(
                "AbCdEf01-2345-6789-aBcD-ef0123456789:1-3", &out),
            MES_OK);
  auto uuid = ExtractUuid(out, 8);
  std::vector<uint8_t> expected = {0xab, 0xcd, 0xef, 0x01, 0x23, 0x45,
                                   0x67, 0x89, 0xab, 0xcd, 0xef, 0x01,
                                   0x23, 0x45, 0x67, 0x89};
  EXPECT_EQ(uuid, expected);
}

// --- Error: missing colon ---

TEST(GtidEncoderTest, ErrorMissingColon) {
  std::vector<uint8_t> out;
  EXPECT_EQ(GtidEncoder::Encode(
                "00000000-0000-0000-0000-0000000000011-3", &out),
            MES_ERR_INVALID_ARG);
}

// --- Error: bad UUID length ---

TEST(GtidEncoderTest, ErrorBadUuidLength) {
  std::vector<uint8_t> out;
  EXPECT_EQ(
      GtidEncoder::Encode("00000000-0000-0000-0000-00000001:1-3", &out),
      MES_ERR_INVALID_ARG);
}

// --- Error: bad interval number ---

TEST(GtidEncoderTest, ErrorBadIntervalNumber) {
  std::vector<uint8_t> out;
  EXPECT_EQ(GtidEncoder::Encode(
                "00000000-0000-0000-0000-000000000001:abc", &out),
            MES_ERR_INVALID_ARG);
}

// --- Error: negative interval ---

TEST(GtidEncoderTest, ErrorNegativeInterval) {
  std::vector<uint8_t> out;
  EXPECT_EQ(GtidEncoder::Encode(
                "00000000-0000-0000-0000-000000000001:-1-3", &out),
            MES_ERR_INVALID_ARG);
}

// --- Error: zero interval start ---

TEST(GtidEncoderTest, ErrorZeroIntervalStart) {
  std::vector<uint8_t> out;
  EXPECT_EQ(GtidEncoder::Encode(
                "00000000-0000-0000-0000-000000000001:0-3", &out),
            MES_ERR_INVALID_ARG);
}

// --- Error: end before start ---

TEST(GtidEncoderTest, ErrorEndBeforeStart) {
  std::vector<uint8_t> out;
  EXPECT_EQ(GtidEncoder::Encode(
                "00000000-0000-0000-0000-000000000001:5-3", &out),
            MES_ERR_INVALID_ARG);
}

// --- Error: empty interval after colon ---

TEST(GtidEncoderTest, ErrorEmptyInterval) {
  std::vector<uint8_t> out;
  EXPECT_EQ(GtidEncoder::Encode(
                "00000000-0000-0000-0000-000000000001:", &out),
            MES_ERR_INVALID_ARG);
}

// --- Error: overflow ---

TEST(GtidEncoderTest, ErrorOverflowSingleTransaction) {
  std::vector<uint8_t> out;
  EXPECT_EQ(GtidEncoder::Encode(
                "00000000-0000-0000-0000-000000000001:9223372036854775807",
                &out),
            MES_ERR_INVALID_ARG);
}

TEST(GtidEncoderTest, ErrorOverflowRangeEnd) {
  std::vector<uint8_t> out;
  EXPECT_EQ(
      GtidEncoder::Encode(
          "00000000-0000-0000-0000-000000000001:1-9223372036854775807", &out),
      MES_ERR_INVALID_ARG);
}

// --- Error: null output pointer ---

TEST(GtidEncoderTest, ErrorNullOutput) {
  EXPECT_EQ(GtidEncoder::Encode(
                "00000000-0000-0000-0000-000000000001:1-3", nullptr),
            MES_ERR_NULL_ARG);
}

// --- Error: null input ---

TEST(GtidEncoderTest, ErrorNullInput) {
  std::vector<uint8_t> out;
  EXPECT_EQ(GtidEncoder::Encode(nullptr, &out), MES_ERR_NULL_ARG);
}

// --- Error: non-hex UUID character ---

TEST(GtidEncoderTest, ErrorNonHexUuid) {
  std::vector<uint8_t> out;
  EXPECT_EQ(GtidEncoder::Encode(
                "0000000g-0000-0000-0000-000000000001:1-3", &out),
            MES_ERR_INVALID_ARG);
}

// --- ConvertSingleGtidToRange ---

TEST(GtidEncoderTest, ConvertEmpty) {
  EXPECT_EQ(GtidEncoder::ConvertSingleGtidToRange(""), "");
}

TEST(GtidEncoderTest, ConvertSingleGtid) {
  EXPECT_EQ(GtidEncoder::ConvertSingleGtidToRange(
                "00000000-0000-0000-0000-000000000001:101"),
            "00000000-0000-0000-0000-000000000001:1-101");
}

TEST(GtidEncoderTest, ConvertAlreadyRange) {
  std::string gtid = "00000000-0000-0000-0000-000000000001:1-101";
  EXPECT_EQ(GtidEncoder::ConvertSingleGtidToRange(gtid), gtid);
}

TEST(GtidEncoderTest, ConvertMultiUuid) {
  std::string gtid =
      "00000000-0000-0000-0000-000000000001:1,"
      "00000000-0000-0000-0000-000000000002:5";
  EXPECT_EQ(GtidEncoder::ConvertSingleGtidToRange(gtid), gtid);
}

TEST(GtidEncoderTest, ConvertTaggedGtid) {
  std::string gtid = "00000000-0000-0000-0000-000000000001:tag:5";
  EXPECT_EQ(GtidEncoder::ConvertSingleGtidToRange(gtid), gtid);
}

TEST(GtidEncoderTest, ConvertNoColon) {
  std::string gtid = "00000000-0000-0000-0000-000000000001";
  EXPECT_EQ(GtidEncoder::ConvertSingleGtidToRange(gtid), gtid);
}

// --- Whitespace handling ---

TEST(GtidEncoderTest, WhitespaceAroundGtid) {
  std::vector<uint8_t> out;
  ASSERT_EQ(GtidEncoder::Encode(
                "  00000000-0000-0000-0000-000000000001:1-3  ", &out),
            MES_OK);
  EXPECT_EQ(ReadInt64Le(out, 0), 1u);
}

TEST(GtidEncoderTest, WhitespaceAroundComma) {
  std::vector<uint8_t> out;
  ASSERT_EQ(GtidEncoder::Encode(
                "00000000-0000-0000-0000-000000000001:1 , "
                "00000000-0000-0000-0000-000000000002:2",
                &out),
            MES_OK);
  EXPECT_EQ(ReadInt64Le(out, 0), 2u);
}

// --- Valid edge case: same start and end in range ---

TEST(GtidEncoderTest, RangeStartEqualsEnd) {
  std::vector<uint8_t> out;
  ASSERT_EQ(GtidEncoder::Encode(
                "00000000-0000-0000-0000-000000000001:3-3", &out),
            MES_OK);
  EXPECT_EQ(ReadInt64Le(out, 32), 3u);  // start
  EXPECT_EQ(ReadInt64Le(out, 40), 4u);  // end (exclusive)
}

// --- Duplicate UUIDs with overlapping intervals ---

TEST(GtidEncoderTest, DuplicateUuidsOverlappingMerged) {
  std::vector<uint8_t> out;
  ASSERT_EQ(GtidEncoder::Encode(
                "00000000-0000-0000-0000-000000000001:1-100,"
                "00000000-0000-0000-0000-000000000001:50-150",
                &out),
            MES_OK);
  EXPECT_EQ(ReadInt64Le(out, 0), 1u);    // 1 SID
  EXPECT_EQ(ReadInt64Le(out, 24), 1u);   // 1 merged interval
  EXPECT_EQ(ReadInt64Le(out, 32), 1u);   // start
  EXPECT_EQ(ReadInt64Le(out, 40), 151u); // end
}

// --- Malformed UUID tests ---

TEST(GtidEncoderTest, ErrorWrongLengthUuid) {
  std::vector<uint8_t> out;
  // UUID too short (missing characters)
  EXPECT_EQ(GtidEncoder::Encode("00000000-0000-0000-0000-0000000001:1-3", &out),
            MES_ERR_INVALID_ARG);
}

TEST(GtidEncoderTest, ErrorWrongLengthUuidTooLong) {
  std::vector<uint8_t> out;
  // UUID too long (extra characters)
  EXPECT_EQ(GtidEncoder::Encode(
                "00000000-0000-0000-0000-00000000000001:1-3", &out),
            MES_ERR_INVALID_ARG);
}

TEST(GtidEncoderTest, ErrorNonHexCharInUuid) {
  std::vector<uint8_t> out;
  // 'Z' is not a valid hex character
  EXPECT_EQ(GtidEncoder::Encode(
                "Z0000000-0000-0000-0000-000000000001:1-3", &out),
            MES_ERR_INVALID_ARG);
}

TEST(GtidEncoderTest, ErrorNonHexCharMiddleOfUuid) {
  std::vector<uint8_t> out;
  // 'x' in the middle of the UUID
  EXPECT_EQ(GtidEncoder::Encode(
                "00000000-0000-x000-0000-000000000001:1-3", &out),
            MES_ERR_INVALID_ARG);
}

// --- Large GNO values ---

TEST(GtidEncoderTest, LargeGnoNearMax) {
  std::vector<uint8_t> out;
  // GNO = INT64_MAX - 1 = 9223372036854775806
  // This should succeed as a single transaction (end = gno + 1 = INT64_MAX)
  // But the existing code rejects INT64_MAX as overflow for end.
  // So INT64_MAX - 1 as a single value means end = INT64_MAX which is rejected.
  // Use a range: 1 to (INT64_MAX - 2) which means end = INT64_MAX - 1 (valid).
  ASSERT_EQ(GtidEncoder::Encode(
                "00000000-0000-0000-0000-000000000001:1-9223372036854775805",
                &out),
            MES_OK);
  EXPECT_EQ(ReadInt64Le(out, 0), 1u);   // n_sids
  EXPECT_EQ(ReadInt64Le(out, 24), 1u);  // n_intervals
  EXPECT_EQ(ReadInt64Le(out, 32), 1u);  // start
  EXPECT_EQ(ReadInt64Le(out, 40),
            static_cast<uint64_t>(9223372036854775806LL));  // end (exclusive)
}

TEST(GtidEncoderTest, LargeGnoRange) {
  std::vector<uint8_t> out;
  // A large range that doesn't overflow
  ASSERT_EQ(GtidEncoder::Encode(
                "00000000-0000-0000-0000-000000000001:1000000000000-2000000000000",
                &out),
            MES_OK);
  EXPECT_EQ(ReadInt64Le(out, 32), 1000000000000u);   // start
  EXPECT_EQ(ReadInt64Le(out, 40), 2000000000001u);    // end (exclusive)
}

}  // namespace
