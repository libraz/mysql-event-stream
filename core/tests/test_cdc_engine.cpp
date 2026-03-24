// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#include <gtest/gtest.h>

#include <cstring>
#include <vector>

#include "cdc_engine.h"
#include "event_header.h"
#include "test_helpers.h"

namespace mes {
namespace {

using test::BuildDeleteRowsBody;
using test::BuildEvent;
using test::BuildRotateBody;
using test::BuildTableMapBody;
using test::BuildUpdateRowsBody;
using test::BuildWriteRowsBody;

TEST(CdcEngineTest, InsertEvent) {
  CdcEngine engine;

  auto table_map_body = BuildTableMapBody(42, "testdb", "users");
  auto table_map_event =
      BuildEvent(static_cast<uint8_t>(BinlogEventType::kTableMapEvent), 1000, 100, table_map_body);

  auto write_body = BuildWriteRowsBody(42, 123);
  auto write_event =
      BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1001, 200, write_body);

  // Feed table map
  size_t consumed = engine.Feed(table_map_event.data(), table_map_event.size());
  EXPECT_EQ(consumed, table_map_event.size());
  EXPECT_FALSE(engine.HasEvents());

  // Feed write rows
  consumed = engine.Feed(write_event.data(), write_event.size());
  EXPECT_EQ(consumed, write_event.size());
  ASSERT_TRUE(engine.HasEvents());
  EXPECT_EQ(engine.PendingEventCount(), 1u);

  ChangeEvent event;
  ASSERT_TRUE(engine.NextEvent(&event));
  EXPECT_EQ(event.type, EventType::kInsert);
  EXPECT_EQ(event.database, "testdb");
  EXPECT_EQ(event.table, "users");
  EXPECT_EQ(event.timestamp, 1001u);
  ASSERT_EQ(event.after.columns.size(), 1u);
  EXPECT_EQ(event.after.columns[0].int_val, 123);
  EXPECT_TRUE(event.after.columns[0].name.empty());
  EXPECT_FALSE(engine.HasEvents());
}

TEST(CdcEngineTest, UpdateEvent) {
  CdcEngine engine;

  auto table_map_body = BuildTableMapBody(10, "db", "t");
  auto table_map_event =
      BuildEvent(static_cast<uint8_t>(BinlogEventType::kTableMapEvent), 1000, 100, table_map_body);

  auto update_body = BuildUpdateRowsBody(10, 100, 200);
  auto update_event =
      BuildEvent(static_cast<uint8_t>(BinlogEventType::kUpdateRowsEvent), 1002, 200, update_body);

  engine.Feed(table_map_event.data(), table_map_event.size());
  engine.Feed(update_event.data(), update_event.size());

  ASSERT_TRUE(engine.HasEvents());
  ChangeEvent event;
  ASSERT_TRUE(engine.NextEvent(&event));
  EXPECT_EQ(event.type, EventType::kUpdate);
  EXPECT_EQ(event.database, "db");
  EXPECT_EQ(event.table, "t");
  ASSERT_EQ(event.before.columns.size(), 1u);
  EXPECT_EQ(event.before.columns[0].int_val, 100);
  ASSERT_EQ(event.after.columns.size(), 1u);
  EXPECT_EQ(event.after.columns[0].int_val, 200);
  EXPECT_TRUE(event.before.columns[0].name.empty());
  EXPECT_TRUE(event.after.columns[0].name.empty());
}

TEST(CdcEngineTest, DeleteEvent) {
  CdcEngine engine;

  auto table_map_body = BuildTableMapBody(10, "db", "t");
  auto table_map_event =
      BuildEvent(static_cast<uint8_t>(BinlogEventType::kTableMapEvent), 1000, 100, table_map_body);

  auto delete_body = BuildDeleteRowsBody(10, 999);
  auto delete_event =
      BuildEvent(static_cast<uint8_t>(BinlogEventType::kDeleteRowsEvent), 1003, 200, delete_body);

  engine.Feed(table_map_event.data(), table_map_event.size());
  engine.Feed(delete_event.data(), delete_event.size());

  ASSERT_TRUE(engine.HasEvents());
  ChangeEvent event;
  ASSERT_TRUE(engine.NextEvent(&event));
  EXPECT_EQ(event.type, EventType::kDelete);
  ASSERT_EQ(event.before.columns.size(), 1u);
  EXPECT_EQ(event.before.columns[0].int_val, 999);
  EXPECT_TRUE(event.before.columns[0].name.empty());
}

TEST(CdcEngineTest, RotateEvent) {
  CdcEngine engine;

  auto rotate_body = BuildRotateBody(4, "binlog.000002");
  auto rotate_event =
      BuildEvent(static_cast<uint8_t>(BinlogEventType::kRotateEvent), 0, 0, rotate_body);

  engine.Feed(rotate_event.data(), rotate_event.size());
  EXPECT_FALSE(engine.HasEvents());
  EXPECT_EQ(engine.CurrentPosition().binlog_file, "binlog.000002");
  EXPECT_EQ(engine.CurrentPosition().offset, 4u);
}

TEST(CdcEngineTest, UnknownEventSkipped) {
  CdcEngine engine;

  // Event type 99 is unknown
  std::vector<uint8_t> body = {0x01, 0x02, 0x03, 0x04};
  auto event = BuildEvent(99, 1000, 100, body);

  size_t consumed = engine.Feed(event.data(), event.size());
  EXPECT_EQ(consumed, event.size());
  EXPECT_FALSE(engine.HasEvents());
}

TEST(CdcEngineTest, MultipleEvents) {
  CdcEngine engine;

  auto table_map_body = BuildTableMapBody(1, "db", "t");
  auto table_map_event =
      BuildEvent(static_cast<uint8_t>(BinlogEventType::kTableMapEvent), 1000, 100, table_map_body);

  auto write1 = BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1001, 200,
                           BuildWriteRowsBody(1, 10));
  auto write2 = BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1002, 300,
                           BuildWriteRowsBody(1, 20));

  // Feed all events in one buffer
  std::vector<uint8_t> combined;
  combined.insert(combined.end(), table_map_event.begin(), table_map_event.end());
  combined.insert(combined.end(), write1.begin(), write1.end());
  combined.insert(combined.end(), write2.begin(), write2.end());

  engine.Feed(combined.data(), combined.size());
  EXPECT_EQ(engine.PendingEventCount(), 2u);

  ChangeEvent event1, event2;
  ASSERT_TRUE(engine.NextEvent(&event1));
  EXPECT_EQ(event1.after.columns[0].int_val, 10);
  ASSERT_TRUE(engine.NextEvent(&event2));
  EXPECT_EQ(event2.after.columns[0].int_val, 20);
  EXPECT_FALSE(engine.HasEvents());
}

TEST(CdcEngineTest, ByteByByteFeeding) {
  CdcEngine engine;

  auto table_map_body = BuildTableMapBody(1, "db", "t");
  auto table_map_event =
      BuildEvent(static_cast<uint8_t>(BinlogEventType::kTableMapEvent), 1000, 100, table_map_body);

  auto write_body = BuildWriteRowsBody(1, 42);
  auto write_event =
      BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1001, 200, write_body);

  // Combine and feed byte by byte
  std::vector<uint8_t> combined;
  combined.insert(combined.end(), table_map_event.begin(), table_map_event.end());
  combined.insert(combined.end(), write_event.begin(), write_event.end());

  for (size_t i = 0; i < combined.size(); i++) {
    engine.Feed(&combined[i], 1);
  }

  ASSERT_TRUE(engine.HasEvents());
  ChangeEvent event;
  ASSERT_TRUE(engine.NextEvent(&event));
  EXPECT_EQ(event.type, EventType::kInsert);
  EXPECT_EQ(event.after.columns[0].int_val, 42);
}

TEST(CdcEngineTest, Reset) {
  CdcEngine engine;

  auto table_map_body = BuildTableMapBody(1, "db", "t");
  auto table_map_event =
      BuildEvent(static_cast<uint8_t>(BinlogEventType::kTableMapEvent), 1000, 100, table_map_body);

  auto write_body = BuildWriteRowsBody(1, 42);
  auto write_event =
      BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1001, 200, write_body);

  engine.Feed(table_map_event.data(), table_map_event.size());
  engine.Feed(write_event.data(), write_event.size());
  ASSERT_TRUE(engine.HasEvents());

  engine.Reset();
  EXPECT_FALSE(engine.HasEvents());
  EXPECT_EQ(engine.PendingEventCount(), 0u);
  EXPECT_EQ(engine.CurrentPosition().offset, 0u);
  EXPECT_TRUE(engine.CurrentPosition().binlog_file.empty());

  // After reset, row event without table map should be silently skipped
  engine.Feed(write_event.data(), write_event.size());
  EXPECT_FALSE(engine.HasEvents());
}

TEST(CdcEngineTest, NextEventNullOutput) {
  CdcEngine engine;
  EXPECT_FALSE(engine.NextEvent(nullptr));
}

TEST(CdcEngineTest, NextEventEmptyQueue) {
  CdcEngine engine;
  ChangeEvent event;
  EXPECT_FALSE(engine.NextEvent(&event));
}

TEST(CdcEngineTest, RowEventWithoutTableMap) {
  CdcEngine engine;

  // Feed a WRITE_ROWS_EVENT without a preceding TABLE_MAP_EVENT
  auto write_body = BuildWriteRowsBody(999, 42);
  auto write_event =
      BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1001, 200, write_body);

  engine.Feed(write_event.data(), write_event.size());
  EXPECT_FALSE(engine.HasEvents());
}

TEST(CdcEngineTest, BackpressureStopsFeedingWhenQueueFull) {
  CdcEngine engine;
  engine.SetMaxQueueSize(2);

  // Register table map
  auto table_map_body = BuildTableMapBody(1, "db", "t");
  auto table_map_event =
      BuildEvent(static_cast<uint8_t>(BinlogEventType::kTableMapEvent), 1000, 100, table_map_body);

  // Build 3 write events
  auto write1 = BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1001, 200,
                           BuildWriteRowsBody(1, 10));
  auto write2 = BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1002, 300,
                           BuildWriteRowsBody(1, 20));
  auto write3 = BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1003, 400,
                           BuildWriteRowsBody(1, 30));

  // Combine all events into one buffer
  std::vector<uint8_t> combined;
  combined.insert(combined.end(), table_map_event.begin(), table_map_event.end());
  combined.insert(combined.end(), write1.begin(), write1.end());
  combined.insert(combined.end(), write2.begin(), write2.end());
  combined.insert(combined.end(), write3.begin(), write3.end());

  // Feed should stop after queue reaches 2
  size_t consumed = engine.Feed(combined.data(), combined.size());
  EXPECT_LT(consumed, combined.size());
  EXPECT_EQ(engine.PendingEventCount(), 2u);

  // Drain one event
  ChangeEvent event;
  ASSERT_TRUE(engine.NextEvent(&event));
  EXPECT_EQ(event.after.columns[0].int_val, 10);
  EXPECT_EQ(engine.PendingEventCount(), 1u);

  // Re-feed the remaining bytes
  size_t consumed2 = engine.Feed(combined.data() + consumed, combined.size() - consumed);
  EXPECT_GT(consumed2, 0u);

  // Should now have the second event still queued plus at least one more
  EXPECT_GE(engine.PendingEventCount(), 2u);

  // Drain all remaining events
  ASSERT_TRUE(engine.NextEvent(&event));
  EXPECT_EQ(event.after.columns[0].int_val, 20);
  ASSERT_TRUE(engine.NextEvent(&event));
  EXPECT_EQ(event.after.columns[0].int_val, 30);
  EXPECT_FALSE(engine.HasEvents());
}

TEST(CdcEngineTest, BackpressureUnlimitedByDefault) {
  CdcEngine engine;

  auto table_map_body = BuildTableMapBody(1, "db", "t");
  auto table_map_event =
      BuildEvent(static_cast<uint8_t>(BinlogEventType::kTableMapEvent), 1000, 100, table_map_body);

  auto write1 = BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1001, 200,
                           BuildWriteRowsBody(1, 10));
  auto write2 = BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1002, 300,
                           BuildWriteRowsBody(1, 20));
  auto write3 = BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1003, 400,
                           BuildWriteRowsBody(1, 30));

  std::vector<uint8_t> combined;
  combined.insert(combined.end(), table_map_event.begin(), table_map_event.end());
  combined.insert(combined.end(), write1.begin(), write1.end());
  combined.insert(combined.end(), write2.begin(), write2.end());
  combined.insert(combined.end(), write3.begin(), write3.end());

  // With default max_queue_size_ = 0 (unlimited), all events should be consumed
  size_t consumed = engine.Feed(combined.data(), combined.size());
  EXPECT_EQ(consumed, combined.size());
  EXPECT_EQ(engine.PendingEventCount(), 3u);
}

TEST(CdcEngineTest, IncludeDatabasesFilter) {
  CdcEngine engine;
  engine.SetIncludeDatabases({"mydb"});

  // Register table in allowed database
  auto tm1 = BuildTableMapBody(1, "mydb", "users");
  auto ev1 = BuildEvent(static_cast<uint8_t>(BinlogEventType::kTableMapEvent), 1000, 100, tm1);
  engine.Feed(ev1.data(), ev1.size());

  auto w1 = BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1001, 200,
                        BuildWriteRowsBody(1, 10));
  engine.Feed(w1.data(), w1.size());
  EXPECT_EQ(engine.PendingEventCount(), 1u);

  // Register table in blocked database
  auto tm2 = BuildTableMapBody(2, "otherdb", "logs");
  auto ev2 = BuildEvent(static_cast<uint8_t>(BinlogEventType::kTableMapEvent), 1002, 300, tm2);
  engine.Feed(ev2.data(), ev2.size());

  auto w2 = BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1003, 400,
                        BuildWriteRowsBody(2, 20));
  engine.Feed(w2.data(), w2.size());

  // Should still have only 1 event (from mydb.users)
  EXPECT_EQ(engine.PendingEventCount(), 1u);

  ChangeEvent event;
  ASSERT_TRUE(engine.NextEvent(&event));
  EXPECT_EQ(event.database, "mydb");
  EXPECT_EQ(event.table, "users");
  EXPECT_EQ(event.after.columns[0].int_val, 10);
  EXPECT_FALSE(engine.HasEvents());
}

TEST(CdcEngineTest, ExcludeTablesFilter) {
  CdcEngine engine;
  engine.SetExcludeTables({"mydb.logs"});

  // Register allowed table
  auto tm1 = BuildTableMapBody(1, "mydb", "users");
  auto ev1 = BuildEvent(static_cast<uint8_t>(BinlogEventType::kTableMapEvent), 1000, 100, tm1);
  engine.Feed(ev1.data(), ev1.size());

  auto w1 = BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1001, 200,
                        BuildWriteRowsBody(1, 10));
  engine.Feed(w1.data(), w1.size());
  EXPECT_EQ(engine.PendingEventCount(), 1u);

  // Register excluded table
  auto tm2 = BuildTableMapBody(2, "mydb", "logs");
  auto ev2 = BuildEvent(static_cast<uint8_t>(BinlogEventType::kTableMapEvent), 1002, 300, tm2);
  engine.Feed(ev2.data(), ev2.size());

  auto w2 = BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1003, 400,
                        BuildWriteRowsBody(2, 20));
  engine.Feed(w2.data(), w2.size());

  // Should still have only 1 event
  EXPECT_EQ(engine.PendingEventCount(), 1u);

  ChangeEvent event;
  ASSERT_TRUE(engine.NextEvent(&event));
  EXPECT_EQ(event.database, "mydb");
  EXPECT_EQ(event.table, "users");
  EXPECT_FALSE(engine.HasEvents());
}

TEST(CdcEngineTest, IncludeTablesFilter) {
  CdcEngine engine;
  engine.SetIncludeTables({"mydb.users"});

  // Register included table
  auto tm1 = BuildTableMapBody(1, "mydb", "users");
  auto ev1 = BuildEvent(static_cast<uint8_t>(BinlogEventType::kTableMapEvent), 1000, 100, tm1);
  engine.Feed(ev1.data(), ev1.size());

  auto w1 = BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1001, 200,
                        BuildWriteRowsBody(1, 10));
  engine.Feed(w1.data(), w1.size());
  EXPECT_EQ(engine.PendingEventCount(), 1u);

  // Register non-included table
  auto tm2 = BuildTableMapBody(2, "mydb", "logs");
  auto ev2 = BuildEvent(static_cast<uint8_t>(BinlogEventType::kTableMapEvent), 1002, 300, tm2);
  engine.Feed(ev2.data(), ev2.size());

  auto w2 = BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1003, 400,
                        BuildWriteRowsBody(2, 20));
  engine.Feed(w2.data(), w2.size());

  EXPECT_EQ(engine.PendingEventCount(), 1u);

  ChangeEvent event;
  ASSERT_TRUE(engine.NextEvent(&event));
  EXPECT_EQ(event.table, "users");
  EXPECT_FALSE(engine.HasEvents());
}

TEST(CdcEngineTest, ExcludeTableUnqualifiedName) {
  CdcEngine engine;
  // Exclude by unqualified name - should match any database
  engine.SetExcludeTables({"logs"});

  auto tm1 = BuildTableMapBody(1, "db1", "logs");
  auto ev1 = BuildEvent(static_cast<uint8_t>(BinlogEventType::kTableMapEvent), 1000, 100, tm1);
  engine.Feed(ev1.data(), ev1.size());

  auto w1 = BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1001, 200,
                        BuildWriteRowsBody(1, 10));
  engine.Feed(w1.data(), w1.size());

  auto tm2 = BuildTableMapBody(2, "db2", "logs");
  auto ev2 = BuildEvent(static_cast<uint8_t>(BinlogEventType::kTableMapEvent), 1002, 300, tm2);
  engine.Feed(ev2.data(), ev2.size());

  auto w2 = BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1003, 400,
                        BuildWriteRowsBody(2, 20));
  engine.Feed(w2.data(), w2.size());

  // Both should be blocked
  EXPECT_FALSE(engine.HasEvents());
}

TEST(CdcEngineTest, FilterResetClearsBlockedIds) {
  CdcEngine engine;
  engine.SetIncludeDatabases({"mydb"});

  // Block a table
  auto tm1 = BuildTableMapBody(1, "otherdb", "t");
  auto ev1 = BuildEvent(static_cast<uint8_t>(BinlogEventType::kTableMapEvent), 1000, 100, tm1);
  engine.Feed(ev1.data(), ev1.size());

  auto w1 = BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1001, 200,
                        BuildWriteRowsBody(1, 10));
  engine.Feed(w1.data(), w1.size());
  EXPECT_FALSE(engine.HasEvents());

  // Reset clears blocked set and filters remain
  engine.Reset();

  // After reset, the table map is also cleared, so row event without table map is skipped
  engine.Feed(w1.data(), w1.size());
  EXPECT_FALSE(engine.HasEvents());
}

}  // namespace
}  // namespace mes
