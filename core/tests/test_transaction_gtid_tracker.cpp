// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "client/gtid_encoder.h"
#include "client/transaction_gtid_tracker.h"
#include "event_header.h"
#include "server_flavor.h"
#include "test_helpers.h"

namespace mes {
namespace {

constexpr char kGtid[] = "00000000-0000-0000-0000-000000000001:42";
constexpr char kNextGtid[] = "00000000-0000-0000-0000-000000000001:43";
constexpr char kSid1[] = "00000000-0000-0000-0000-000000000001";
constexpr char kSid2[] = "00000000-0000-0000-0000-000000000002";

std::vector<uint8_t> BuildMySQLGtid(uint64_t gno) {
  test::EventBuilder body;
  body.WriteU8(0);
  for (int i = 0; i < 15; ++i) body.WriteU8(0);
  body.WriteU8(1);
  body.WriteU64Le(gno);
  return test::BuildEvent(static_cast<uint8_t>(BinlogEventType::kGtidLogEvent), 0, 0, body.Data());
}

void WriteMySQLVarUInt(test::EventBuilder* body, uint64_t value) {
  size_t bytes = 1;
  while (bytes < 9 && value >= (uint64_t{1} << (7 * bytes))) ++bytes;
  body->WriteU8(static_cast<uint8_t>(((uint64_t{1} << (bytes - 1)) - 1) | (value << bytes)));
  if (bytes == 1) return;
  const uint64_t trailing = value >> (bytes == 9 ? 0 : 8 - bytes);
  for (size_t i = 0; i + 1 < bytes; ++i) {
    body->WriteU8(static_cast<uint8_t>(trailing >> (8 * i)));
  }
}

std::vector<uint8_t> BuildTaggedMySQLGtid(uint64_t gno, const std::string& tag) {
  test::EventBuilder fields;
  WriteMySQLVarUInt(&fields, 0);  // gtid_flags field ID
  WriteMySQLVarUInt(&fields, 0);  // gtid_flags
  WriteMySQLVarUInt(&fields, 1);  // TSID UUID field ID
  for (int i = 0; i < 15; ++i) WriteMySQLVarUInt(&fields, 0);
  WriteMySQLVarUInt(&fields, 1);
  WriteMySQLVarUInt(&fields, 2);         // GNO field ID
  WriteMySQLVarUInt(&fields, gno << 1);  // signed-integer zigzag encoding
  WriteMySQLVarUInt(&fields, 3);         // tag field ID
  WriteMySQLVarUInt(&fields, tag.size());
  for (char ch : tag) fields.WriteU8(static_cast<uint8_t>(ch));

  test::EventBuilder body;
  WriteMySQLVarUInt(&body, fields.Data().size() + 2);  // complete serialized message size
  WriteMySQLVarUInt(&body, 4);                         // final non-ignorable field ID + 1
  body.Buffer().insert(body.Buffer().end(), fields.Data().begin(), fields.Data().end());
  return test::BuildEventNoChecksum(static_cast<uint8_t>(BinlogEventType::kGtidTaggedLogEvent), 0,
                                    0, body.Data());
}

std::vector<uint8_t> BuildPreviousGtids(const std::string& gtid_set) {
  std::vector<uint8_t> encoded;
  EXPECT_EQ(GtidEncoder::Encode(gtid_set.c_str(), &encoded), MES_OK);
  return test::BuildEvent(static_cast<uint8_t>(BinlogEventType::kPreviousGtidsEvent), 0, 0,
                          encoded);
}

std::vector<uint8_t> BuildPreviousGtidsWithIntervalCount(uint64_t interval_count) {
  test::EventBuilder body;
  body.WriteU64Le(1);  // SID count
  for (int i = 0; i < 15; ++i) body.WriteU8(0);
  body.WriteU8(1);
  body.WriteU64Le(interval_count);
  for (uint64_t i = 0; i < interval_count; ++i) {
    body.WriteU64Le(i * 2 + 1);
    body.WriteU64Le(i * 2 + 2);
  }
  return test::BuildEvent(static_cast<uint8_t>(BinlogEventType::kPreviousGtidsEvent), 0, 0,
                          body.Data());
}

std::vector<uint8_t> BuildMariaDBGtidList(const std::vector<MariaDBGtid>& gtids) {
  test::EventBuilder body;
  body.WriteU32Le(static_cast<uint32_t>(gtids.size()));
  for (const auto& gtid : gtids) {
    body.WriteU32Le(gtid.domain_id);
    body.WriteU32Le(gtid.server_id);
    body.WriteU64Le(gtid.sequence_no);
  }
  return test::BuildEvent(static_cast<uint8_t>(BinlogEventType::kMariaDBGtidListEvent), 0, 0,
                          body.Data());
}

TEST(TransactionGtidTrackerTest, DoesNotCheckpointAtGtidOrRowReceipt) {
  TransactionGtidTracker tracker;
  auto gtid = BuildMySQLGtid(42);
  EXPECT_TRUE(tracker.Observe(gtid.data(), gtid.size(), true).empty());
  EXPECT_EQ(tracker.received_gtid(), kGtid);

  auto row = test::BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 0, 0, {});
  EXPECT_TRUE(tracker.Observe(row.data(), row.size(), true).empty());
  EXPECT_EQ(tracker.received_gtid(), kGtid);
}

TEST(TransactionGtidTrackerTest, CheckpointsOnlyAtXidBoundary) {
  TransactionGtidTracker tracker;
  auto gtid = BuildMySQLGtid(42);
  tracker.Observe(gtid.data(), gtid.size(), true);

  auto xid = test::BuildEvent(static_cast<uint8_t>(BinlogEventType::kXidEvent), 0, 0, {});
  EXPECT_EQ(tracker.Observe(xid.data(), xid.size(), true), std::string(kSid1) + ":42-42");
  EXPECT_TRUE(tracker.Observe(xid.data(), xid.size(), true).empty());
}

TEST(TransactionGtidTrackerTest, TaggedMySQLGtidCheckpointsAndResumesWithTaggedSetEncoding) {
  TransactionGtidTracker tracker;
  auto gtid = BuildTaggedMySQLGtid(42, "Analytics");
  EXPECT_TRUE(tracker.Observe(gtid.data(), gtid.size(), false).empty());
  EXPECT_EQ(tracker.received_gtid(), std::string(kSid1) + ":analytics:42");

  auto xid = test::BuildEvent(static_cast<uint8_t>(BinlogEventType::kXidEvent), 0, 0, {});
  const std::string checkpoint = tracker.Observe(xid.data(), xid.size(), false);
  ASSERT_EQ(checkpoint, std::string(kSid1) + ":analytics:42-42");

  std::vector<uint8_t> encoded;
  ASSERT_EQ(GtidEncoder::Encode(checkpoint.c_str(), &encoded), MES_OK);
  EXPECT_EQ(encoded[0], 1u);
  EXPECT_EQ(encoded[7], 1u);  // tagged set format is encoded in both positions.
}

TEST(TransactionGtidTrackerTest, TaggedPreviousGtidsMergesTaggedAndUntaggedTsids) {
  TransactionGtidTracker tracker;
  auto previous = BuildPreviousGtids(std::string(kSid1) + ":analytics:1-4," + kSid2 + ":2-3");
  EXPECT_EQ(tracker.Observe(previous.data(), previous.size(), true),
            std::string(kSid1) + ":analytics:1-4," + kSid2 + ":2-3");
}

TEST(TransactionGtidTrackerTest, NextGtidClosesUnknownStandaloneGroup) {
  TransactionGtidTracker tracker;
  auto first = BuildMySQLGtid(42);
  auto second = BuildMySQLGtid(43);
  tracker.Observe(first.data(), first.size(), true);
  EXPECT_EQ(tracker.Observe(second.data(), second.size(), true), std::string(kSid1) + ":42-42");
  EXPECT_EQ(tracker.received_gtid(), kNextGtid);
}

TEST(TransactionGtidTrackerTest, BeginQueryDoesNotCheckpoint) {
  TransactionGtidTracker tracker;
  auto gtid = BuildMySQLGtid(42);
  tracker.Observe(gtid.data(), gtid.size(), true);
  auto begin = test::BuildEvent(static_cast<uint8_t>(BinlogEventType::kQueryEvent), 0, 0,
                                test::BuildQueryEventBody("db", "BEGIN"));
  EXPECT_TRUE(tracker.Observe(begin.data(), begin.size(), true).empty());
}

TEST(TransactionGtidTrackerTest, DdlQueryWithLeadingCommentCheckpoints) {
  TransactionGtidTracker tracker;
  auto gtid = BuildMySQLGtid(42);
  tracker.Observe(gtid.data(), gtid.size(), true);
  auto ddl =
      test::BuildEvent(static_cast<uint8_t>(BinlogEventType::kQueryEvent), 0, 0,
                       test::BuildQueryEventBody("db", "/* audit */ ALTER TABLE t ADD c INT"));
  EXPECT_EQ(tracker.Observe(ddl.data(), ddl.size(), true), std::string(kSid1) + ":42-42");
}

TEST(TransactionGtidTrackerTest, RollbackToSavepointKeepsCheckpointPendingUntilCommit) {
  TransactionGtidTracker tracker;
  auto gtid = BuildMySQLGtid(42);
  EXPECT_TRUE(tracker.Observe(gtid.data(), gtid.size(), true).empty());

  auto observe_query = [&tracker](const char* statement) {
    auto query = test::BuildEvent(static_cast<uint8_t>(BinlogEventType::kQueryEvent), 0, 0,
                                  test::BuildQueryEventBody("db", statement));
    return tracker.Observe(query.data(), query.size(), true);
  };

  EXPECT_TRUE(observe_query("BEGIN").empty());
  EXPECT_TRUE(observe_query("SAVEPOINT before_update").empty());
  EXPECT_TRUE(observe_query("ROLLBACK TO SAVEPOINT before_update").empty());
  // A DDL-looking statement inside the explicit group must not advance the
  // checkpoint before the real transaction boundary either.
  EXPECT_TRUE(observe_query("ALTER TABLE t ADD COLUMN note INT").empty());
  EXPECT_EQ(observe_query("COMMIT"), std::string(kSid1) + ":42-42");
}

TEST(TransactionGtidTrackerTest, ResetDropsUncommittedState) {
  TransactionGtidTracker tracker;
  auto gtid = BuildMySQLGtid(42);
  tracker.Observe(gtid.data(), gtid.size(), true);
  tracker.Reset();
  auto xid = test::BuildEvent(static_cast<uint8_t>(BinlogEventType::kXidEvent), 0, 0, {});
  EXPECT_TRUE(tracker.Observe(xid.data(), xid.size(), true).empty());
  EXPECT_TRUE(tracker.received_gtid().empty());
}

TEST(TransactionGtidTrackerTest, MariaDBGtidAlsoWaitsForXid) {
  TransactionGtidTracker tracker;
  test::EventBuilder body;
  body.WriteU64Le(42);  // sequence number
  body.WriteU32Le(7);   // domain ID
  body.WriteU8(0);      // flags
  auto gtid =
      test::BuildEvent(static_cast<uint8_t>(BinlogEventType::kMariaDBGtidEvent), 0, 0, body.Data());
  EXPECT_TRUE(tracker.Observe(gtid.data(), gtid.size(), true).empty());
  EXPECT_EQ(tracker.received_gtid(), "7-1-42");

  auto xid = test::BuildEvent(static_cast<uint8_t>(BinlogEventType::kXidEvent), 0, 0, {});
  EXPECT_EQ(tracker.Observe(xid.data(), xid.size(), true), "7-1-42");
}

TEST(TransactionGtidTrackerTest, MariaDBStandaloneGtidCheckpointsAtTheGtidEvent) {
  TransactionGtidTracker tracker;
  test::EventBuilder body;
  body.WriteU64Le(42);
  body.WriteU32Le(7);
  body.WriteU8(1);  // MariaDB FL_STANDALONE: no terminating COMMIT/XID follows.
  auto gtid =
      test::BuildEvent(static_cast<uint8_t>(BinlogEventType::kMariaDBGtidEvent), 0, 0, body.Data());

  EXPECT_EQ(tracker.Observe(gtid.data(), gtid.size(), true), "7-1-42");
  EXPECT_EQ(tracker.received_gtid(), "7-1-42");
}

TEST(TransactionGtidTrackerTest, MySQLCommitAdvancesCompleteMultiSidSet) {
  TransactionGtidTracker tracker;
  const std::string initial = std::string(kSid1) + ":1-3:7," + kSid2 + ":1-8";
  ASSERT_TRUE(tracker.Reset(initial, ServerFlavor::kMySQL));

  auto gtid = BuildMySQLGtid(4);
  EXPECT_TRUE(tracker.Observe(gtid.data(), gtid.size(), true).empty());
  auto xid = test::BuildEvent(static_cast<uint8_t>(BinlogEventType::kXidEvent), 0, 0, {});
  EXPECT_EQ(tracker.Observe(xid.data(), xid.size(), true),
            std::string(kSid1) + ":1-4:7-7," + kSid2 + ":1-8");
}

TEST(TransactionGtidTrackerTest, PreviousGtidsMergesFailoverHistoryIntoBaseline) {
  TransactionGtidTracker tracker;
  ASSERT_TRUE(tracker.Reset(std::string(kSid2) + ":1-2", ServerFlavor::kMySQL));

  auto previous = BuildPreviousGtids(std::string(kSid1) + ":1-10," + kSid2 + ":1-3");
  EXPECT_EQ(tracker.Observe(previous.data(), previous.size(), true),
            std::string(kSid1) + ":1-10," + kSid2 + ":1-3");

  auto gtid = BuildMySQLGtid(11);
  tracker.Observe(gtid.data(), gtid.size(), true);
  auto xid = test::BuildEvent(static_cast<uint8_t>(BinlogEventType::kXidEvent), 0, 0, {});
  EXPECT_EQ(tracker.Observe(xid.data(), xid.size(), true),
            std::string(kSid1) + ":1-11," + kSid2 + ":1-3");
}

TEST(TransactionGtidTrackerTest, MalformedPreviousGtidsDoesNotPartiallyMutateSet) {
  TransactionGtidTracker tracker;
  ASSERT_TRUE(tracker.Reset(std::string(kSid1) + ":1-2", ServerFlavor::kMySQL));

  test::EventBuilder malformed_body;
  malformed_body.WriteU64Le(1);  // One SID, but no SID payload follows.
  auto malformed = test::BuildEvent(static_cast<uint8_t>(BinlogEventType::kPreviousGtidsEvent), 0,
                                    0, malformed_body.Data());
  EXPECT_TRUE(tracker.Observe(malformed.data(), malformed.size(), true).empty());

  auto gtid = BuildMySQLGtid(3);
  tracker.Observe(gtid.data(), gtid.size(), true);
  auto xid = test::BuildEvent(static_cast<uint8_t>(BinlogEventType::kXidEvent), 0, 0, {});
  EXPECT_EQ(tracker.Observe(xid.data(), xid.size(), true), std::string(kSid1) + ":1-3");
}

TEST(TransactionGtidTrackerTest, SingletonCheckpointResumesWithoutWideningTheInterval) {
  TransactionGtidTracker tracker;
  auto gtid = BuildMySQLGtid(42);
  ASSERT_TRUE(tracker.Observe(gtid.data(), gtid.size(), true).empty());
  auto xid = test::BuildEvent(static_cast<uint8_t>(BinlogEventType::kXidEvent), 0, 0, {});
  const std::string checkpoint = tracker.Observe(xid.data(), xid.size(), true);
  ASSERT_EQ(checkpoint, std::string(kSid1) + ":42-42");

  EXPECT_EQ(GtidEncoder::ConvertSingleGtidToRange(checkpoint), checkpoint);
}

TEST(TransactionGtidTrackerTest, MariaDBGtidListAndCommitKeepAllDomainHighWaters) {
  TransactionGtidTracker tracker;
  ASSERT_TRUE(tracker.Reset("7-1-10,9-2-20", ServerFlavor::kMariaDB));

  auto list = BuildMariaDBGtidList({{7, 3, 12}, {8, 4, 4}, {9, 6, 19}});
  EXPECT_EQ(tracker.Observe(list.data(), list.size(), true), "7-3-12,8-4-4,9-2-20");

  test::EventBuilder body;
  body.WriteU64Le(21);
  body.WriteU32Le(9);
  body.WriteU8(0);
  auto gtid =
      test::BuildEvent(static_cast<uint8_t>(BinlogEventType::kMariaDBGtidEvent), 0, 0, body.Data());
  EXPECT_TRUE(tracker.Observe(gtid.data(), gtid.size(), true).empty());
  auto xid = test::BuildEvent(static_cast<uint8_t>(BinlogEventType::kXidEvent), 0, 0, {});
  EXPECT_EQ(tracker.Observe(xid.data(), xid.size(), true), "7-3-12,8-4-4,9-1-21");
}

TEST(TransactionGtidTrackerTest, ResetRejectsInvalidInitialSets) {
  TransactionGtidTracker tracker;
  EXPECT_FALSE(tracker.Reset("not-a-gtid", ServerFlavor::kMySQL));
  EXPECT_FALSE(tracker.Reset("7-1-bad", ServerFlavor::kMariaDB));
}

TEST(TransactionGtidTrackerTest, PreviousGtidsRejectsExcessiveIntervalsWithoutMutatingCheckpoint) {
  TransactionGtidTracker tracker;
  ASSERT_TRUE(tracker.Reset(std::string(kSid1) + ":1-2", ServerFlavor::kMySQL));

  auto oversized = BuildPreviousGtidsWithIntervalCount(65537);
  EXPECT_TRUE(tracker.Observe(oversized.data(), oversized.size(), false).empty());

  auto gtid = BuildMySQLGtid(3);
  EXPECT_TRUE(tracker.Observe(gtid.data(), gtid.size(), false).empty());
  auto xid = test::BuildEvent(static_cast<uint8_t>(BinlogEventType::kXidEvent), 0, 0, {});
  EXPECT_EQ(tracker.Observe(xid.data(), xid.size(), false), std::string(kSid1) + ":1-3");
}

}  // namespace
}  // namespace mes
