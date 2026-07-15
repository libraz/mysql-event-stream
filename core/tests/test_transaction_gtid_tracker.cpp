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

std::vector<uint8_t> BuildPreviousGtids(const std::string& gtid_set) {
  std::vector<uint8_t> encoded;
  EXPECT_EQ(GtidEncoder::Encode(gtid_set.c_str(), &encoded), MES_OK);
  return test::BuildEvent(static_cast<uint8_t>(BinlogEventType::kPreviousGtidsEvent), 0, 0,
                          encoded);
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
  EXPECT_EQ(tracker.Observe(xid.data(), xid.size(), true), kGtid);
  EXPECT_TRUE(tracker.Observe(xid.data(), xid.size(), true).empty());
}

TEST(TransactionGtidTrackerTest, NextGtidClosesUnknownStandaloneGroup) {
  TransactionGtidTracker tracker;
  auto first = BuildMySQLGtid(42);
  auto second = BuildMySQLGtid(43);
  tracker.Observe(first.data(), first.size(), true);
  EXPECT_EQ(tracker.Observe(second.data(), second.size(), true), kGtid);
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
  EXPECT_EQ(tracker.Observe(ddl.data(), ddl.size(), true), kGtid);
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

TEST(TransactionGtidTrackerTest, MySQLCommitAdvancesCompleteMultiSidSet) {
  TransactionGtidTracker tracker;
  const std::string initial = std::string(kSid1) + ":1-3:7," + kSid2 + ":1-8";
  ASSERT_TRUE(tracker.Reset(initial, ServerFlavor::kMySQL));

  auto gtid = BuildMySQLGtid(4);
  EXPECT_TRUE(tracker.Observe(gtid.data(), gtid.size(), true).empty());
  auto xid = test::BuildEvent(static_cast<uint8_t>(BinlogEventType::kXidEvent), 0, 0, {});
  EXPECT_EQ(tracker.Observe(xid.data(), xid.size(), true),
            std::string(kSid1) + ":1-4:7," + kSid2 + ":1-8");
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

}  // namespace
}  // namespace mes
