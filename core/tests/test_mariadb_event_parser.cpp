// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * @file test_mariadb_event_parser.cpp
 * @brief Unit tests for MariaDB-specific binlog event parsing
 */

#include <gtest/gtest.h>

#include <cstring>
#include <string>
#include <vector>

#include "event_header.h"
#include "mariadb_event_parser.h"
#include "mariadb_gtid.h"
#include "mes.h"

namespace mes {
namespace {

// ===========================================================================
// Binary encoding helpers
// ===========================================================================

void PutU32Le(std::vector<uint8_t>& buf, uint32_t v) {
  uint8_t bytes[4];
  std::memcpy(bytes, &v, sizeof(v));
  buf.insert(buf.end(), bytes, bytes + sizeof(bytes));
}

void PutU64Le(std::vector<uint8_t>& buf, uint64_t v) {
  uint8_t bytes[8];
  std::memcpy(bytes, &v, sizeof(v));
  buf.insert(buf.end(), bytes, bytes + sizeof(bytes));
}

void AppendCRC32Placeholder(std::vector<uint8_t>& buf) {
  buf.push_back(0);
  buf.push_back(0);
  buf.push_back(0);
  buf.push_back(0);
}

/// Build a minimal 19-byte binlog event header.
std::vector<uint8_t> BuildHeader(uint8_t type_code, uint32_t server_id, uint32_t event_length) {
  std::vector<uint8_t> header(kEventHeaderSize, 0);
  // timestamp (4 bytes) = 0
  header[4] = type_code;
  std::memcpy(header.data() + 5, &server_id, sizeof(server_id));
  std::memcpy(header.data() + 9, &event_length, sizeof(event_length));
  // next_position (4 bytes) = 0
  // flags (2 bytes) = 0
  return header;
}

// ===========================================================================
// ExtractGtid tests
// ===========================================================================

class MariaDBEventParserGtidTest : public ::testing::Test {
 protected:
  /// Build a MariaDB GTID event (type 162).
  /// Layout: header(19) + seq_no(8) + domain_id(4) + flags(1) + CRC32(4)
  std::vector<uint8_t> BuildGtidEvent(uint32_t domain_id, uint32_t server_id, uint64_t seq_no,
                                      uint8_t flags = 0) {
    uint32_t total_len = kEventHeaderSize + 8 + 4 + 1 + kChecksumSize;
    auto buf = BuildHeader(162, server_id, total_len);
    PutU64Le(buf, seq_no);
    PutU32Le(buf, domain_id);
    buf.push_back(flags);
    AppendCRC32Placeholder(buf);
    return buf;
  }
};

TEST_F(MariaDBEventParserGtidTest, BasicGtid) {
  auto event = BuildGtidEvent(0, 1, 42);
  std::string out;
  ASSERT_EQ(MES_OK, MariaDBEventParser::ExtractGtid(event.data(), event.size(), &out));
  EXPECT_EQ(out, "0-1-42");
}

TEST_F(MariaDBEventParserGtidTest, LargeValues) {
  auto event = BuildGtidEvent(100, 200, 999999999);
  std::string out;
  ASSERT_EQ(MES_OK, MariaDBEventParser::ExtractGtid(event.data(), event.size(), &out));
  EXPECT_EQ(out, "100-200-999999999");
}

TEST_F(MariaDBEventParserGtidTest, MaxDomainAndServer) {
  auto event = BuildGtidEvent(UINT32_MAX, UINT32_MAX, UINT64_MAX);
  std::string gtid_str;
  ASSERT_EQ(MES_OK, MariaDBEventParser::ExtractGtid(event.data(), event.size(), &gtid_str));

  // Verify round-trip through MariaDBGtid::Parse
  MariaDBGtid parsed;
  ASSERT_EQ(MES_OK, MariaDBGtid::Parse(gtid_str, &parsed));
  EXPECT_EQ(parsed.domain_id, UINT32_MAX);
  EXPECT_EQ(parsed.server_id, UINT32_MAX);
  EXPECT_EQ(parsed.sequence_no, UINT64_MAX);
}

TEST_F(MariaDBEventParserGtidTest, ZeroValues) {
  auto event = BuildGtidEvent(0, 0, 0);
  std::string out;
  ASSERT_EQ(MES_OK, MariaDBEventParser::ExtractGtid(event.data(), event.size(), &out));
  EXPECT_EQ(out, "0-0-0");
}

TEST_F(MariaDBEventParserGtidTest, WithFlags) {
  auto event = BuildGtidEvent(1, 2, 100, 0x01);
  std::string out;
  ASSERT_EQ(MES_OK, MariaDBEventParser::ExtractGtid(event.data(), event.size(), &out));
  EXPECT_EQ(out, "1-2-100");
}

TEST_F(MariaDBEventParserGtidTest, NullBuffer) {
  std::string out;
  EXPECT_NE(MES_OK, MariaDBEventParser::ExtractGtid(nullptr, 100, &out));
}

TEST_F(MariaDBEventParserGtidTest, TooShort) {
  // Header only, no post-header data
  auto header = BuildHeader(162, 1, 19);
  std::string out;
  EXPECT_NE(MES_OK, MariaDBEventParser::ExtractGtid(header.data(), header.size(), &out));
}

TEST_F(MariaDBEventParserGtidTest, ExactMinimumSize) {
  // header(19) + seq_no(8) + domain_id(4) + flags(1) = 32 bytes (no CRC32)
  auto event = BuildGtidEvent(5, 10, 50);
  size_t min_size = kEventHeaderSize + 13;  // 32 bytes
  std::string out;
  ASSERT_EQ(MES_OK, MariaDBEventParser::ExtractGtid(event.data(), min_size, &out));
  EXPECT_EQ(out, "5-10-50");
}

TEST_F(MariaDBEventParserGtidTest, NullOutput) {
  auto event = BuildGtidEvent(0, 1, 42);
  EXPECT_NE(MES_OK, MariaDBEventParser::ExtractGtid(event.data(), event.size(), nullptr));
}

// ===========================================================================
// ParseGtidList tests
// ===========================================================================

class MariaDBEventParserGtidListTest : public ::testing::Test {
 protected:
  /// Build a GTID_LIST event (type 163).
  /// Layout: header(19) + count_and_flags(4) + entries(16 each) + CRC32(4)
  std::vector<uint8_t> BuildGtidListEvent(const std::vector<MariaDBGtid>& gtids,
                                          uint32_t flags = 0) {
    uint32_t count = static_cast<uint32_t>(gtids.size());
    uint32_t count_and_flags = (flags << 28) | (count & 0x0FFFFFFFu);
    uint32_t total_len = kEventHeaderSize + 4 + (count * 16) + kChecksumSize;

    auto buf = BuildHeader(163, 1, total_len);
    PutU32Le(buf, count_and_flags);

    for (const auto& gtid : gtids) {
      PutU32Le(buf, gtid.domain_id);
      PutU32Le(buf, gtid.server_id);
      PutU64Le(buf, gtid.sequence_no);
    }

    AppendCRC32Placeholder(buf);
    return buf;
  }
};

TEST_F(MariaDBEventParserGtidListTest, EmptyList) {
  auto event = BuildGtidListEvent({});
  std::vector<MariaDBGtid> out;
  ASSERT_EQ(MES_OK, MariaDBEventParser::ParseGtidList(event.data(), event.size(), &out));
  EXPECT_TRUE(out.empty());
}

TEST_F(MariaDBEventParserGtidListTest, SingleEntry) {
  std::vector<MariaDBGtid> gtids = {{0, 1, 42}};
  auto event = BuildGtidListEvent(gtids);
  std::vector<MariaDBGtid> out;
  ASSERT_EQ(MES_OK, MariaDBEventParser::ParseGtidList(event.data(), event.size(), &out));
  ASSERT_EQ(out.size(), 1u);
  EXPECT_EQ(out[0], (MariaDBGtid{0, 1, 42}));
}

TEST_F(MariaDBEventParserGtidListTest, MultipleEntries) {
  std::vector<MariaDBGtid> gtids = {{0, 1, 42}, {1, 2, 100}, {3, 5, 999}};
  auto event = BuildGtidListEvent(gtids);
  std::vector<MariaDBGtid> out;
  ASSERT_EQ(MES_OK, MariaDBEventParser::ParseGtidList(event.data(), event.size(), &out));
  ASSERT_EQ(out.size(), 3u);
  EXPECT_EQ(out[0], (MariaDBGtid{0, 1, 42}));
  EXPECT_EQ(out[1], (MariaDBGtid{1, 2, 100}));
  EXPECT_EQ(out[2], (MariaDBGtid{3, 5, 999}));
}

TEST_F(MariaDBEventParserGtidListTest, WithFlags) {
  // Upper 4 bits of count_and_flags are flags, should not affect count
  std::vector<MariaDBGtid> gtids = {{0, 1, 42}};
  auto event = BuildGtidListEvent(gtids, 0x5);
  std::vector<MariaDBGtid> out;
  ASSERT_EQ(MES_OK, MariaDBEventParser::ParseGtidList(event.data(), event.size(), &out));
  ASSERT_EQ(out.size(), 1u);
  EXPECT_EQ(out[0], (MariaDBGtid{0, 1, 42}));
}

TEST_F(MariaDBEventParserGtidListTest, NullBuffer) {
  std::vector<MariaDBGtid> out;
  EXPECT_NE(MES_OK, MariaDBEventParser::ParseGtidList(nullptr, 100, &out));
}

TEST_F(MariaDBEventParserGtidListTest, TooShortForHeader) {
  auto header = BuildHeader(163, 1, 20);
  std::vector<MariaDBGtid> out;
  EXPECT_NE(MES_OK, MariaDBEventParser::ParseGtidList(header.data(), header.size(), &out));
}

TEST_F(MariaDBEventParserGtidListTest, TruncatedEntries) {
  // Claim 2 entries but only provide data for 1
  std::vector<MariaDBGtid> gtids = {{0, 1, 42}};
  auto event = BuildGtidListEvent(gtids);
  // Overwrite count to 2
  uint32_t fake_count = 2;
  std::memcpy(event.data() + kEventHeaderSize, &fake_count, sizeof(fake_count));
  std::vector<MariaDBGtid> out;
  EXPECT_NE(MES_OK, MariaDBEventParser::ParseGtidList(event.data(), event.size(), &out));
}

TEST_F(MariaDBEventParserGtidListTest, LargeValues) {
  std::vector<MariaDBGtid> gtids = {{UINT32_MAX, UINT32_MAX, UINT64_MAX}};
  auto event = BuildGtidListEvent(gtids);
  std::vector<MariaDBGtid> out;
  ASSERT_EQ(MES_OK, MariaDBEventParser::ParseGtidList(event.data(), event.size(), &out));
  ASSERT_EQ(out.size(), 1u);
  EXPECT_EQ(out[0].domain_id, UINT32_MAX);
  EXPECT_EQ(out[0].server_id, UINT32_MAX);
  EXPECT_EQ(out[0].sequence_no, UINT64_MAX);
}

TEST_F(MariaDBEventParserGtidListTest, NullOutput) {
  auto event = BuildGtidListEvent({});
  EXPECT_NE(MES_OK, MariaDBEventParser::ParseGtidList(event.data(), event.size(), nullptr));
}

// ===========================================================================
// ExtractAnnotateRows tests
// ===========================================================================

class MariaDBEventParserAnnotateTest : public ::testing::Test {
 protected:
  /// Build an ANNOTATE_ROWS event (type 160).
  /// Layout: header(19) + sql_text + CRC32(4)
  std::vector<uint8_t> BuildAnnotateRowsEvent(const std::string& sql) {
    uint32_t total_len = kEventHeaderSize + static_cast<uint32_t>(sql.size()) + kChecksumSize;
    auto buf = BuildHeader(160, 1, total_len);
    buf.insert(buf.end(), sql.begin(), sql.end());
    AppendCRC32Placeholder(buf);
    return buf;
  }
};

TEST_F(MariaDBEventParserAnnotateTest, SimpleQuery) {
  auto event = BuildAnnotateRowsEvent("INSERT INTO t1 VALUES (1, 'hello')");
  std::string out;
  ASSERT_EQ(MES_OK,
            MariaDBEventParser::ExtractAnnotateRows(event.data(), event.size(), true, &out));
  EXPECT_EQ(out, "INSERT INTO t1 VALUES (1, 'hello')");
}

TEST_F(MariaDBEventParserAnnotateTest, ComplexQuery) {
  std::string sql =
      "UPDATE articles SET content = 'new text', updated_at = NOW() "
      "WHERE id = 42";
  auto event = BuildAnnotateRowsEvent(sql);
  std::string out;
  ASSERT_EQ(MES_OK,
            MariaDBEventParser::ExtractAnnotateRows(event.data(), event.size(), true, &out));
  EXPECT_EQ(out, sql);
}

TEST_F(MariaDBEventParserAnnotateTest, UnicodeQuery) {
  std::string sql =
      u8"INSERT INTO t1 VALUES (1, '\xe6\x97\xa5\xe6\x9c\xac"
      u8"\xe8\xaa\x9e\xe3\x83\x86\xe3\x82\xb9\xe3\x83\x88')";
  auto event = BuildAnnotateRowsEvent(sql);
  std::string out;
  ASSERT_EQ(MES_OK,
            MariaDBEventParser::ExtractAnnotateRows(event.data(), event.size(), true, &out));
  EXPECT_EQ(out, sql);
}

TEST_F(MariaDBEventParserAnnotateTest, NullBuffer) {
  std::string out;
  EXPECT_NE(MES_OK, MariaDBEventParser::ExtractAnnotateRows(nullptr, 100, true, &out));
}

TEST_F(MariaDBEventParserAnnotateTest, EmptyText) {
  // header(19) + CRC32(4) = 23 bytes, no text
  auto buf = BuildHeader(160, 1, 23);
  AppendCRC32Placeholder(buf);
  std::string out;
  EXPECT_NE(MES_OK, MariaDBEventParser::ExtractAnnotateRows(buf.data(), buf.size(), true, &out));
}

TEST_F(MariaDBEventParserAnnotateTest, TooShortForHeaderAndChecksum) {
  // Just the header, not even room for CRC32
  auto header = BuildHeader(160, 1, 19);
  std::string out;
  EXPECT_NE(MES_OK,
            MariaDBEventParser::ExtractAnnotateRows(header.data(), header.size(), true, &out));
}

TEST_F(MariaDBEventParserAnnotateTest, NullOutput) {
  auto event = BuildAnnotateRowsEvent("SELECT 1");
  EXPECT_NE(MES_OK,
            MariaDBEventParser::ExtractAnnotateRows(event.data(), event.size(), true, nullptr));
}

TEST_F(MariaDBEventParserAnnotateTest, NoChecksum) {
  // Build event without CRC32: header(19) + sql_text only
  std::string sql = "DELETE FROM t1 WHERE id = 99";
  uint32_t total_len = kEventHeaderSize + static_cast<uint32_t>(sql.size());
  auto buf = BuildHeader(160, 1, total_len);
  buf.insert(buf.end(), sql.begin(), sql.end());

  std::string out;
  ASSERT_EQ(MES_OK, MariaDBEventParser::ExtractAnnotateRows(buf.data(), buf.size(), false, &out));
  EXPECT_EQ(out, sql);
}

TEST_F(MariaDBEventParserAnnotateTest, NoChecksumEmptyText) {
  // header(19) only, no text and no checksum
  auto header = BuildHeader(160, 1, 19);
  std::string out;
  EXPECT_NE(MES_OK,
            MariaDBEventParser::ExtractAnnotateRows(header.data(), header.size(), false, &out));
}

// ===========================================================================
// Integration: GTID round-trip (event -> extract -> parse -> verify)
// ===========================================================================

TEST(MariaDBEventParserIntegrationTest, GtidRoundTrip) {
  // Build a GTID event with known values
  uint32_t total_len = kEventHeaderSize + 13 + kChecksumSize;
  auto buf = BuildHeader(162, 42, total_len);

  uint64_t seq_no = 12345;
  uint32_t domain_id = 7;
  uint8_t flags = 0;
  PutU64Le(buf, seq_no);
  PutU32Le(buf, domain_id);
  buf.push_back(flags);
  AppendCRC32Placeholder(buf);

  // Extract GTID string from event
  std::string gtid_str;
  ASSERT_EQ(MES_OK, MariaDBEventParser::ExtractGtid(buf.data(), buf.size(), &gtid_str));
  EXPECT_EQ(gtid_str, "7-42-12345");

  // Parse GTID string back to struct
  MariaDBGtid parsed;
  ASSERT_EQ(MES_OK, MariaDBGtid::Parse(gtid_str, &parsed));
  EXPECT_EQ(parsed.domain_id, 7u);
  EXPECT_EQ(parsed.server_id, 42u);
  EXPECT_EQ(parsed.sequence_no, 12345u);

  // ToString and verify
  EXPECT_EQ(parsed.ToString(), "7-42-12345");
}

TEST(MariaDBEventParserIntegrationTest, GtidListToSet) {
  std::vector<MariaDBGtid> gtids = {{0, 1, 42}, {1, 2, 100}};

  // Build GTID_LIST event
  uint32_t count = 2;
  uint32_t total_len = kEventHeaderSize + 4 + (count * 16) + kChecksumSize;
  auto buf = BuildHeader(163, 1, total_len);
  PutU32Le(buf, count);
  for (const auto& gtid : gtids) {
    PutU32Le(buf, gtid.domain_id);
    PutU32Le(buf, gtid.server_id);
    PutU64Le(buf, gtid.sequence_no);
  }
  AppendCRC32Placeholder(buf);

  // Parse
  std::vector<MariaDBGtid> out;
  ASSERT_EQ(MES_OK, MariaDBEventParser::ParseGtidList(buf.data(), buf.size(), &out));
  ASSERT_EQ(out.size(), 2u);

  // Convert to set string
  std::string set_str = MariaDBGtid::SetToString(out);
  EXPECT_EQ(set_str, "0-1-42,1-2-100");
}

}  // namespace
}  // namespace mes
