// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * @file test_mariadb_gtid.cpp
 * @brief Unit tests for MariaDB GTID parsing, representation, and related utilities
 */

#include "mariadb_gtid.h"

#include <gtest/gtest.h>

#include <climits>
#include <string>
#include <vector>

#include "event_header.h"
#include "mes.h"
#include "server_flavor.h"

using mes::MariaDBGtid;

// ===========================================================================
// Parse: valid single GTID
// ===========================================================================

TEST(MariaDBGtidTest, ParseBasicGtid) {
  MariaDBGtid gtid;
  ASSERT_EQ(MES_OK, MariaDBGtid::Parse("0-1-42", &gtid));
  EXPECT_EQ(gtid.domain_id, 0u);
  EXPECT_EQ(gtid.server_id, 1u);
  EXPECT_EQ(gtid.sequence_no, 42u);
}

TEST(MariaDBGtidTest, ParseLargeValues) {
  MariaDBGtid gtid;
  ASSERT_EQ(MES_OK,
            MariaDBGtid::Parse("100-4294967295-18446744073709551615", &gtid));
  EXPECT_EQ(gtid.domain_id, 100u);
  EXPECT_EQ(gtid.server_id, 4294967295u);               // uint32_t max
  EXPECT_EQ(gtid.sequence_no, 18446744073709551615ULL);  // uint64_t max
}

TEST(MariaDBGtidTest, ParseZeroValues) {
  MariaDBGtid gtid;
  ASSERT_EQ(MES_OK, MariaDBGtid::Parse("0-0-0", &gtid));
  EXPECT_EQ(gtid.domain_id, 0u);
  EXPECT_EQ(gtid.server_id, 0u);
  EXPECT_EQ(gtid.sequence_no, 0u);
}

TEST(MariaDBGtidTest, ParseWithWhitespace) {
  MariaDBGtid gtid;
  ASSERT_EQ(MES_OK, MariaDBGtid::Parse("  0-1-42  ", &gtid));
  EXPECT_EQ(gtid.domain_id, 0u);
  EXPECT_EQ(gtid.server_id, 1u);
  EXPECT_EQ(gtid.sequence_no, 42u);
}

TEST(MariaDBGtidTest, ParseWithNewlines) {
  MariaDBGtid gtid;
  ASSERT_EQ(MES_OK, MariaDBGtid::Parse("\n0-1-42\n", &gtid));
  EXPECT_EQ(gtid.domain_id, 0u);
  EXPECT_EQ(gtid.server_id, 1u);
  EXPECT_EQ(gtid.sequence_no, 42u);
}

// ===========================================================================
// Parse: error cases
// ===========================================================================

TEST(MariaDBGtidTest, ParseEmptyString) {
  MariaDBGtid gtid;
  EXPECT_EQ(MES_ERR_INVALID_ARG, MariaDBGtid::Parse("", &gtid));
}

TEST(MariaDBGtidTest, ParseMissingDash) {
  MariaDBGtid gtid;
  EXPECT_EQ(MES_ERR_INVALID_ARG, MariaDBGtid::Parse("0-1", &gtid));
}

TEST(MariaDBGtidTest, ParseExtraDash) {
  MariaDBGtid gtid;
  EXPECT_EQ(MES_ERR_INVALID_ARG, MariaDBGtid::Parse("0-1-2-3", &gtid));
}

TEST(MariaDBGtidTest, ParseNonNumericDomain) {
  MariaDBGtid gtid;
  EXPECT_EQ(MES_ERR_INVALID_ARG, MariaDBGtid::Parse("abc-1-42", &gtid));
}

TEST(MariaDBGtidTest, ParseNonNumericServer) {
  MariaDBGtid gtid;
  EXPECT_EQ(MES_ERR_INVALID_ARG, MariaDBGtid::Parse("0-abc-42", &gtid));
}

TEST(MariaDBGtidTest, ParseNonNumericSequence) {
  MariaDBGtid gtid;
  EXPECT_EQ(MES_ERR_INVALID_ARG, MariaDBGtid::Parse("0-1-abc", &gtid));
}

TEST(MariaDBGtidTest, ParseMySQLGtidFormat) {
  MariaDBGtid gtid;
  EXPECT_EQ(MES_ERR_INVALID_ARG,
            MariaDBGtid::Parse(
                "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-77", &gtid));
}

TEST(MariaDBGtidTest, ParseNegativeNumber) {
  MariaDBGtid gtid;
  EXPECT_NE(MES_OK, MariaDBGtid::Parse("0--1-42", &gtid));
}

TEST(MariaDBGtidTest, ParseDomainIdOverflow) {
  MariaDBGtid gtid;
  // uint32_t max + 1 = 4294967296
  EXPECT_EQ(MES_ERR_INVALID_ARG,
            MariaDBGtid::Parse("4294967296-1-42", &gtid));
}

TEST(MariaDBGtidTest, ParseNullOutput) {
  EXPECT_NE(MES_OK, MariaDBGtid::Parse("0-1-42", nullptr));
}

// ===========================================================================
// ParseSet: valid sets
// ===========================================================================

TEST(MariaDBGtidTest, ParseSetSingleGtid) {
  std::vector<MariaDBGtid> gtids;
  ASSERT_EQ(MES_OK, MariaDBGtid::ParseSet("0-1-42", &gtids));
  ASSERT_EQ(gtids.size(), 1u);
  EXPECT_EQ(gtids[0].domain_id, 0u);
  EXPECT_EQ(gtids[0].server_id, 1u);
  EXPECT_EQ(gtids[0].sequence_no, 42u);
}

TEST(MariaDBGtidTest, ParseSetMultipleGtids) {
  std::vector<MariaDBGtid> gtids;
  ASSERT_EQ(MES_OK, MariaDBGtid::ParseSet("0-1-42,1-2-100", &gtids));
  ASSERT_EQ(gtids.size(), 2u);
  EXPECT_EQ(gtids[0].domain_id, 0u);
  EXPECT_EQ(gtids[0].sequence_no, 42u);
  EXPECT_EQ(gtids[1].domain_id, 1u);
  EXPECT_EQ(gtids[1].sequence_no, 100u);
}

TEST(MariaDBGtidTest, ParseSetThreeGtids) {
  std::vector<MariaDBGtid> gtids;
  ASSERT_EQ(MES_OK,
            MariaDBGtid::ParseSet("0-1-42,1-2-100,2-3-200", &gtids));
  ASSERT_EQ(gtids.size(), 3u);
}

TEST(MariaDBGtidTest, ParseSetEmptyString) {
  std::vector<MariaDBGtid> gtids;
  ASSERT_EQ(MES_OK, MariaDBGtid::ParseSet("", &gtids));
  EXPECT_TRUE(gtids.empty());
}

TEST(MariaDBGtidTest, ParseSetWithWhitespace) {
  std::vector<MariaDBGtid> gtids;
  ASSERT_EQ(MES_OK,
            MariaDBGtid::ParseSet("  0-1-42 , 1-2-100  ", &gtids));
  ASSERT_EQ(gtids.size(), 2u);
  EXPECT_EQ(gtids[0].sequence_no, 42u);
  EXPECT_EQ(gtids[1].sequence_no, 100u);
}

TEST(MariaDBGtidTest, ParseSetWithNewlines) {
  std::vector<MariaDBGtid> gtids;
  ASSERT_EQ(MES_OK, MariaDBGtid::ParseSet("0-1-42,\n1-2-100", &gtids));
  ASSERT_EQ(gtids.size(), 2u);
}

TEST(MariaDBGtidTest, ParseSetTrailingComma) {
  std::vector<MariaDBGtid> gtids;
  ASSERT_EQ(MES_OK, MariaDBGtid::ParseSet("0-1-42,", &gtids));
  ASSERT_EQ(gtids.size(), 1u);
}

// ===========================================================================
// ParseSet: error cases
// ===========================================================================

TEST(MariaDBGtidTest, ParseSetInvalidEntry) {
  std::vector<MariaDBGtid> gtids;
  EXPECT_EQ(MES_ERR_INVALID_ARG,
            MariaDBGtid::ParseSet("0-1-42,invalid", &gtids));
}

TEST(MariaDBGtidTest, ParseSetNullOutput) {
  EXPECT_NE(MES_OK, MariaDBGtid::ParseSet("0-1-42", nullptr));
}

// ===========================================================================
// ToString
// ===========================================================================

TEST(MariaDBGtidTest, ToStringBasic) {
  MariaDBGtid gtid{0, 1, 42};
  EXPECT_EQ(gtid.ToString(), "0-1-42");
}

TEST(MariaDBGtidTest, ToStringLargeValues) {
  MariaDBGtid gtid{100, 4294967295u, 18446744073709551615ULL};
  EXPECT_EQ(gtid.ToString(), "100-4294967295-18446744073709551615");
}

TEST(MariaDBGtidTest, ToStringZeros) {
  MariaDBGtid gtid{0, 0, 0};
  EXPECT_EQ(gtid.ToString(), "0-0-0");
}

// ===========================================================================
// SetToString
// ===========================================================================

TEST(MariaDBGtidTest, SetToStringEmpty) {
  EXPECT_EQ(MariaDBGtid::SetToString({}), "");
}

TEST(MariaDBGtidTest, SetToStringSingle) {
  std::vector<MariaDBGtid> gtids = {{0, 1, 42}};
  EXPECT_EQ(MariaDBGtid::SetToString(gtids), "0-1-42");
}

TEST(MariaDBGtidTest, SetToStringMultiple) {
  std::vector<MariaDBGtid> gtids = {{0, 1, 42}, {1, 2, 100}};
  EXPECT_EQ(MariaDBGtid::SetToString(gtids), "0-1-42,1-2-100");
}

// ===========================================================================
// Roundtrip: Parse -> ToString
// ===========================================================================

TEST(MariaDBGtidTest, RoundtripSingle) {
  std::string original = "0-1-42";
  MariaDBGtid gtid;
  ASSERT_EQ(MES_OK, MariaDBGtid::Parse(original, &gtid));
  EXPECT_EQ(gtid.ToString(), original);
}

TEST(MariaDBGtidTest, RoundtripSet) {
  std::string original = "0-1-42,1-2-100";
  std::vector<MariaDBGtid> gtids;
  ASSERT_EQ(MES_OK, MariaDBGtid::ParseSet(original, &gtids));
  EXPECT_EQ(MariaDBGtid::SetToString(gtids), original);
}

// ===========================================================================
// IsMariaDBGtidFormat
// ===========================================================================

TEST(MariaDBGtidTest, IsMariaDBFormatValid) {
  EXPECT_TRUE(MariaDBGtid::IsMariaDBGtidFormat("0-1-42"));
  EXPECT_TRUE(MariaDBGtid::IsMariaDBGtidFormat("100-200-300"));
  EXPECT_TRUE(MariaDBGtid::IsMariaDBGtidFormat("0-0-0"));
}

TEST(MariaDBGtidTest, IsMariaDBFormatWithWhitespace) {
  EXPECT_TRUE(MariaDBGtid::IsMariaDBGtidFormat("  0-1-42  "));
}

TEST(MariaDBGtidTest, IsMariaDBFormatMySQLGtid) {
  EXPECT_FALSE(MariaDBGtid::IsMariaDBGtidFormat(
      "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-77"));
}

TEST(MariaDBGtidTest, IsMariaDBFormatEmpty) {
  EXPECT_FALSE(MariaDBGtid::IsMariaDBGtidFormat(""));
}

TEST(MariaDBGtidTest, IsMariaDBFormatOneDash) {
  EXPECT_FALSE(MariaDBGtid::IsMariaDBGtidFormat("0-1"));
}

TEST(MariaDBGtidTest, IsMariaDBFormatThreeDashes) {
  EXPECT_FALSE(MariaDBGtid::IsMariaDBGtidFormat("0-1-2-3"));
}

TEST(MariaDBGtidTest, IsMariaDBFormatNonNumeric) {
  EXPECT_FALSE(MariaDBGtid::IsMariaDBGtidFormat("abc-1-42"));
  EXPECT_FALSE(MariaDBGtid::IsMariaDBGtidFormat("0-abc-42"));
  EXPECT_FALSE(MariaDBGtid::IsMariaDBGtidFormat("0-1-abc"));
}

// ===========================================================================
// Equality operators
// ===========================================================================

TEST(MariaDBGtidTest, EqualityOperator) {
  MariaDBGtid a{0, 1, 42};
  MariaDBGtid b{0, 1, 42};
  MariaDBGtid c{0, 1, 43};

  EXPECT_EQ(a, b);
  EXPECT_NE(a, c);
}

TEST(MariaDBGtidTest, EqualityDifferentDomain) {
  MariaDBGtid a{0, 1, 42};
  MariaDBGtid b{1, 1, 42};
  EXPECT_NE(a, b);
}

TEST(MariaDBGtidTest, EqualityDifferentServer) {
  MariaDBGtid a{0, 1, 42};
  MariaDBGtid b{0, 2, 42};
  EXPECT_NE(a, b);
}

// ===========================================================================
// ServerFlavor detection
// ===========================================================================

TEST(ServerFlavorTest, DetectMySQL) {
  using mes::DetectServerFlavor;
  using mes::ServerFlavor;
  EXPECT_EQ(DetectServerFlavor("8.4.7"), ServerFlavor::kMySQL);
  EXPECT_EQ(DetectServerFlavor("9.0.1"), ServerFlavor::kMySQL);
  EXPECT_EQ(DetectServerFlavor("5.7.44"), ServerFlavor::kMySQL);
}

TEST(ServerFlavorTest, DetectMariaDB) {
  using mes::DetectServerFlavor;
  using mes::ServerFlavor;
  EXPECT_EQ(DetectServerFlavor("10.11.6-MariaDB"), ServerFlavor::kMariaDB);
  EXPECT_EQ(DetectServerFlavor("11.4.0-MariaDB-1:11.4.0+maria~ubu2404"),
            ServerFlavor::kMariaDB);
  EXPECT_EQ(DetectServerFlavor("10.6.12-MariaDB-log"),
            ServerFlavor::kMariaDB);
}

TEST(ServerFlavorTest, DetectMariaDBLowercase) {
  using mes::DetectServerFlavor;
  using mes::ServerFlavor;
  EXPECT_EQ(DetectServerFlavor("10.11.6-mariadb"), ServerFlavor::kMariaDB);
}

TEST(ServerFlavorTest, DetectEmpty) {
  using mes::DetectServerFlavor;
  using mes::ServerFlavor;
  EXPECT_EQ(DetectServerFlavor(""), ServerFlavor::kMySQL);
}

TEST(ServerFlavorTest, GetFlavorName) {
  using mes::GetServerFlavorName;
  using mes::ServerFlavor;
  EXPECT_STREQ(GetServerFlavorName(ServerFlavor::kMySQL), "MySQL");
  EXPECT_STREQ(GetServerFlavorName(ServerFlavor::kMariaDB), "MariaDB");
}

// ===========================================================================
// BinlogEventType: MariaDB event constants
// ===========================================================================

TEST(BinlogEventTypeTest, MariaDBEventValues) {
  using mes::BinlogEventType;
  EXPECT_EQ(static_cast<uint8_t>(BinlogEventType::kMariaDBAnnotateRowsEvent),
            160);
  EXPECT_EQ(
      static_cast<uint8_t>(BinlogEventType::kMariaDBBinlogCheckpointEvent),
      161);
  EXPECT_EQ(static_cast<uint8_t>(BinlogEventType::kMariaDBGtidEvent), 162);
  EXPECT_EQ(static_cast<uint8_t>(BinlogEventType::kMariaDBGtidListEvent),
            163);
  EXPECT_EQ(
      static_cast<uint8_t>(BinlogEventType::kMariaDBStartEncryptionEvent),
      164);
}

TEST(BinlogEventTypeTest, MariaDBEventNames) {
  using mes::BinlogEventTypeName;
  // Verify that MariaDB event types have recognizable names (not "UNKNOWN")
  const char* annotate_name = BinlogEventTypeName(160);
  const char* gtid_name = BinlogEventTypeName(162);
  const char* gtid_list_name = BinlogEventTypeName(163);

  EXPECT_STRNE(annotate_name, "UNKNOWN");
  EXPECT_STRNE(gtid_name, "UNKNOWN");
  EXPECT_STRNE(gtid_list_name, "UNKNOWN");
}
