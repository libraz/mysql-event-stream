// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "client/gtid_encoder.h"
#include "mybinlog/mysql_gtid.h"

namespace {

using mes::GtidEncoder;

constexpr char kSid[] = "00000000-0000-0000-0000-000000000001";
constexpr char kSid2[] = "00000000-0000-0000-0000-000000000002";

// --- Encode: mes_error_t adapter over mybinlog::GtidSet ---

TEST(GtidEncoderTest, EmptyGtidSetReturnsZeroSids) {
  std::vector<uint8_t> out;
  ASSERT_EQ(GtidEncoder::Encode("", &out), MES_OK);
  ASSERT_EQ(out.size(), 8u);
  EXPECT_EQ(out, std::vector<uint8_t>(8, 0));
}

TEST(GtidEncoderTest, EncodeMatchesTheCodecEncoding) {
  const std::string sid(kSid);
  for (const std::string& text :
       {sid + ":1-3:7", sid + ":1-2," + kSid2 + ":tag:7", sid + ":MyTag:1-5"}) {
    mybinlog::GtidSet set;
    ASSERT_EQ(mybinlog::GtidSet::Parse(text, &set), mybinlog::Status::kOk) << text;
    std::vector<uint8_t> expected;
    set.EncodeBinary(&expected);

    std::vector<uint8_t> out;
    ASSERT_EQ(GtidEncoder::Encode(text.c_str(), &out), MES_OK) << text;
    EXPECT_EQ(out, expected) << text;
  }
}

TEST(GtidEncoderTest, ErrorMissingColon) {
  std::vector<uint8_t> out;
  EXPECT_EQ(GtidEncoder::Encode("00000000-0000-0000-0000-0000000000011-3", &out),
            MES_ERR_INVALID_ARG);
}

TEST(GtidEncoderTest, ErrorOverflowSingleTransaction) {
  std::vector<uint8_t> out;
  EXPECT_EQ(GtidEncoder::Encode("00000000-0000-0000-0000-000000000001:9223372036854775807", &out),
            MES_ERR_INVALID_ARG);
}

TEST(GtidEncoderTest, TaggedGtidRejectsInvalidTag) {
  std::vector<uint8_t> out;
  EXPECT_EQ(GtidEncoder::Encode("00000000-0000-0000-0000-000000000001:bad-tag:1-9", &out),
            MES_ERR_INVALID_ARG);
}

TEST(GtidEncoderTest, ErrorNullOutput) {
  EXPECT_EQ(GtidEncoder::Encode("00000000-0000-0000-0000-000000000001:1-3", nullptr),
            MES_ERR_NULL_ARG);
}

TEST(GtidEncoderTest, ErrorNullInput) {
  std::vector<uint8_t> out;
  EXPECT_EQ(GtidEncoder::Encode(nullptr, &out), MES_ERR_NULL_ARG);
}

// --- ConvertSingleGtidToRange ---

TEST(GtidEncoderTest, ConvertEmpty) { EXPECT_EQ(GtidEncoder::ConvertSingleGtidToRange(""), ""); }

TEST(GtidEncoderTest, ConvertSingleGtid) {
  EXPECT_EQ(GtidEncoder::ConvertSingleGtidToRange("00000000-0000-0000-0000-000000000001:101"),
            "00000000-0000-0000-0000-000000000001:1-101");
}

TEST(GtidEncoderTest, ConvertAlreadyRange) {
  std::string gtid = "00000000-0000-0000-0000-000000000001:1-101";
  EXPECT_EQ(GtidEncoder::ConvertSingleGtidToRange(gtid), gtid);
}

TEST(GtidEncoderTest, ConvertMultiUuid) {
  // Each comma-separated single GTID must be normalized to a range so the
  // server does not resend already-processed transactions (duplicate replay).
  std::string gtid =
      "00000000-0000-0000-0000-000000000001:100,"
      "00000000-0000-0000-0000-000000000002:200";
  EXPECT_EQ(GtidEncoder::ConvertSingleGtidToRange(gtid),
            "00000000-0000-0000-0000-000000000001:1-100,"
            "00000000-0000-0000-0000-000000000002:1-200");
}

TEST(GtidEncoderTest, ConvertMultiUuidMixedRangeAndSingle) {
  // A part already expressed as a range is preserved; a bare single is widened.
  std::string gtid =
      "00000000-0000-0000-0000-000000000001:1-50,"
      "00000000-0000-0000-0000-000000000002:7";
  EXPECT_EQ(GtidEncoder::ConvertSingleGtidToRange(gtid),
            "00000000-0000-0000-0000-000000000001:1-50,"
            "00000000-0000-0000-0000-000000000002:1-7");
}

TEST(GtidEncoderTest, ConvertMultiUuidDropsZero) {
  // "uuid:0" (no transactions) is dropped from a multi-UUID set.
  std::string gtid =
      "00000000-0000-0000-0000-000000000001:0,"
      "00000000-0000-0000-0000-000000000002:5";
  EXPECT_EQ(GtidEncoder::ConvertSingleGtidToRange(gtid),
            "00000000-0000-0000-0000-000000000002:1-5");
}

TEST(GtidEncoderTest, ConvertTaggedSingleGtidWidensLikeTheUntaggedForm) {
  // A tag names a TSID, it does not change interval semantics: "uuid:tag:5"
  // denotes [5,6) exactly as "uuid:5" does. Leaving the tagged form untouched
  // would make the server resend tagged transactions 1 through 4.
  EXPECT_EQ(GtidEncoder::ConvertSingleGtidToRange("00000000-0000-0000-0000-000000000001:tag:5"),
            "00000000-0000-0000-0000-000000000001:tag:1-5");
  EXPECT_EQ(GtidEncoder::ConvertSingleGtidToRange("00000000-0000-0000-0000-000000000001:5"),
            "00000000-0000-0000-0000-000000000001:1-5");
}

TEST(GtidEncoderTest, ConvertTaggedRangeAndMultiIntervalUnchanged) {
  // Explicit intervals are used as given, tagged or not.
  for (const char* gtid : {"00000000-0000-0000-0000-000000000001:tag:1-5",
                           "00000000-0000-0000-0000-000000000001:tag:1-3:7",
                           "00000000-0000-0000-0000-000000000001:1-3:7"}) {
    EXPECT_EQ(GtidEncoder::ConvertSingleGtidToRange(gtid), gtid);
  }
}

TEST(GtidEncoderTest, ConvertLeavesUnparsableTagUntouched) {
  // "9tag" is not a valid tag, so the text after the UUID is not a tag plus a
  // transaction number and must not be rewritten into one.
  std::string gtid = "00000000-0000-0000-0000-000000000001:9tag:5";
  EXPECT_EQ(GtidEncoder::ConvertSingleGtidToRange(gtid), gtid);
}

TEST(GtidEncoderTest, ConvertTaggedZeroIsDropped) {
  // "tag:0" means no transactions for that TSID, same as the untagged form.
  EXPECT_EQ(GtidEncoder::ConvertSingleGtidToRange("00000000-0000-0000-0000-000000000001:tag:0,"
                                                  "00000000-0000-0000-0000-000000000002:5"),
            "00000000-0000-0000-0000-000000000002:1-5");
}

TEST(GtidEncoderTest, ConvertNoColon) {
  std::string gtid = "00000000-0000-0000-0000-000000000001";
  EXPECT_EQ(GtidEncoder::ConvertSingleGtidToRange(gtid), gtid);
}

TEST(GtidEncoderTest, ConvertAppliesTheSameRulesToPaddedAndUnpaddedTokens) {
  // Padding must not decide which rule applies. A single-entry request goes
  // through a different path than a comma-separated one, so both are checked.
  const std::string sid(kSid);
  const std::string sid2(kSid2);
  EXPECT_EQ(GtidEncoder::ConvertSingleGtidToRange(" " + sid + ":0 "), "");
  EXPECT_EQ(GtidEncoder::ConvertSingleGtidToRange("\t" + sid + ":tag:0\n"), "");
  EXPECT_EQ(GtidEncoder::ConvertSingleGtidToRange(" " + sid + ":0 ," + sid2 + ":0"), "");
  EXPECT_EQ(GtidEncoder::ConvertSingleGtidToRange(" " + sid + ":5 "), sid + ":1-5");
  EXPECT_EQ(GtidEncoder::ConvertSingleGtidToRange("\t" + sid + ":tag:5\n"), sid + ":tag:1-5");
  EXPECT_EQ(GtidEncoder::ConvertSingleGtidToRange(" " + sid + ":1-3 "), sid + ":1-3");
  EXPECT_EQ(GtidEncoder::ConvertSingleGtidToRange(" " + sid + ":0 , " + sid2 + ":5 "),
            sid2 + ":1-5");
}

TEST(GtidEncoderTest, SeparatorsOnlyStartPositionIsRejectedNotReadAsTheEmptySet) {
  // An empty set asks the server for every binlog it still retains, so a typo'd
  // start position must fail instead of being normalized into one. An omitted
  // option is the empty string and has to keep meaning "from the beginning".
  std::vector<uint8_t> from_the_beginning;
  ASSERT_EQ(GtidEncoder::Encode("", &from_the_beginning), MES_OK);
  EXPECT_EQ(from_the_beginning.size(), 8u);

  for (const char* text : {",", ",,", " ", "  ", "\t", "\n", " , ", ", ,", ",\n,"}) {
    // The text reaches the parser unchanged, and the parser rejects it. Both
    // passthrough routes are covered: a comma makes ConvertSingleGtidToRange
    // hand the text back when no entry held a token, and without one the
    // whitespace-only token comes back from NormalizeSingleSid instead.
    const std::string converted = GtidEncoder::ConvertSingleGtidToRange(text);
    EXPECT_EQ(converted, text) << "input: '" << text << "'";
    std::vector<uint8_t> encoded;
    EXPECT_EQ(GtidEncoder::Encode(converted.c_str(), &encoded), MES_ERR_INVALID_ARG)
        << "input: '" << text << "'";
  }

  // A request naming only TSIDs with no transactions is a different statement:
  // it denotes the empty set deliberately and still encodes as one.
  std::vector<uint8_t> encoded;
  const std::string all_zero = std::string(kSid) + ":0," + kSid2 + ":0";
  ASSERT_EQ(GtidEncoder::ConvertSingleGtidToRange(all_zero), "");
  ASSERT_EQ(GtidEncoder::Encode(GtidEncoder::ConvertSingleGtidToRange(all_zero).c_str(), &encoded),
            MES_OK);
  EXPECT_EQ(encoded.size(), 8u);
}

TEST(GtidEncoderTest, ConvertLeavesAnEntryWithSeveralRunsUntouched) {
  // An entry with several runs names no single transaction, so it is not widened.
  const std::string sid(kSid);
  for (const std::string& text : {sid + ":5:mytag:1:other:3", sid + ":a:1:b:2"}) {
    EXPECT_EQ(GtidEncoder::ConvertSingleGtidToRange(text), text);
  }
}

TEST(GtidEncoderTest, ConvertThenEncodeRejectsEveryEmptyOrDanglingColonPiece) {
  // The server never prints an empty piece, and skipping one would turn a
  // malformed position into a plausible one.
  const std::string sid(kSid);
  for (const std::string& text :
       {sid + ":1::2", sid + "::5", sid + ":1:", sid + ":tag:1:", sid + ":", sid + ":tag:",
        sid + ":tag", sid + ":a:b:1", sid + ":1:tag", sid + ":1: :2", sid + ":5:mytag:1:other:"}) {
    std::vector<uint8_t> encoded;
    EXPECT_EQ(GtidEncoder::Encode(GtidEncoder::ConvertSingleGtidToRange(text).c_str(), &encoded),
              MES_ERR_INVALID_ARG)
        << "input: '" << text << "'";
  }
}

}  // namespace
