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

}  // namespace
}  // namespace mes
