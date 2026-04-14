// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * @file test_e2e_binlog_ddl.cpp
 * @brief End-to-end tests for DDL handling during binlog streaming
 *
 * Verifies that the CDC engine correctly handles schema changes (CREATE, ALTER,
 * DROP, RENAME, TRUNCATE) and continues to parse row events with the updated
 * column layout.
 *
 * Requires a running MySQL 8.4 instance at localhost:13308 with:
 *   - root/test_root_password (mysql_native_password)
 *   - repl_user/test_password (replication grants)
 *   - Database: mes_test with table: ddl_test (id INT AUTO_INCREMENT PK, val VARCHAR(100))
 *
 * Start with: cd e2e/docker && docker compose up -d
 */

#include <gtest/gtest.h>

#include <cstring>
#include <string>
#include <vector>

#include "mes.h"
#include "test_e2e_helpers.h"

namespace {

using e2e::CaptureEvents;
using e2e::CaptureTableEvents;
using e2e::CapturedEvent;
using e2e::ExecuteDML;
using e2e::FilterByTable;
using e2e::GetCurrentGtid;

TEST(E2EBinlogDDL, CreateTableDuringStream) {
  // Ensure no leftover table from a previous failed run
  ExecuteDML("DROP TABLE IF EXISTS mes_test.dyn_create_test");

  std::string gtid = GetCurrentGtid();
  ASSERT_FALSE(gtid.empty()) << "Failed to fetch current GTID";

  ASSERT_EQ(ExecuteDML("CREATE TABLE mes_test.dyn_create_test ("
                        "id INT NOT NULL PRIMARY KEY, "
                        "name VARCHAR(50))"),
            MES_OK);
  ASSERT_EQ(
      ExecuteDML(
          "INSERT INTO mes_test.dyn_create_test (id, name) VALUES (1, 'hello')"),
      MES_OK);

  auto events = CaptureTableEvents(gtid, 400, "dyn_create_test", 1);
  auto filtered = FilterByTable(events, "dyn_create_test");

  ASSERT_GE(filtered.size(), 1u)
      << "Expected at least 1 INSERT for dyn_create_test";
  EXPECT_EQ(filtered[0].type, MES_EVENT_INSERT);
  EXPECT_EQ(filtered[0].after.size(), 2u);

  // Cleanup
  ExecuteDML("DROP TABLE IF EXISTS mes_test.dyn_create_test");
}

TEST(E2EBinlogDDL, AlterTableAddColumn) {
  std::string gtid = GetCurrentGtid();
  ASSERT_FALSE(gtid.empty()) << "Failed to fetch current GTID";

  ASSERT_EQ(
      ExecuteDML("ALTER TABLE mes_test.ddl_test ADD COLUMN extra INT DEFAULT 0"),
      MES_OK);
  ASSERT_EQ(
      ExecuteDML(
          "INSERT INTO mes_test.ddl_test (val, extra) VALUES ('added', 42)"),
      MES_OK);

  auto events = CaptureTableEvents(gtid, 401, "ddl_test", 1);
  auto filtered = FilterByTable(events, "ddl_test");

  ASSERT_GE(filtered.size(), 1u) << "Expected at least 1 INSERT for ddl_test";
  EXPECT_EQ(filtered[0].type, MES_EVENT_INSERT);
  EXPECT_EQ(filtered[0].after.size(), 3u) << "Expected 3 columns: id, val, extra";

  // Cleanup
  ExecuteDML("ALTER TABLE mes_test.ddl_test DROP COLUMN extra");
}

TEST(E2EBinlogDDL, AlterTableDropColumn) {
  // First add a temporary column
  ASSERT_EQ(
      ExecuteDML(
          "ALTER TABLE mes_test.ddl_test ADD COLUMN temp_col INT DEFAULT 0"),
      MES_OK);

  std::string gtid = GetCurrentGtid();
  ASSERT_FALSE(gtid.empty()) << "Failed to fetch current GTID";

  ASSERT_EQ(
      ExecuteDML("ALTER TABLE mes_test.ddl_test DROP COLUMN temp_col"),
      MES_OK);
  ASSERT_EQ(
      ExecuteDML("INSERT INTO mes_test.ddl_test (val) VALUES ('dropped')"),
      MES_OK);

  auto events = CaptureTableEvents(gtid, 402, "ddl_test", 1);
  auto filtered = FilterByTable(events, "ddl_test");

  ASSERT_GE(filtered.size(), 1u) << "Expected at least 1 INSERT for ddl_test";
  EXPECT_EQ(filtered[0].type, MES_EVENT_INSERT);
  EXPECT_EQ(filtered[0].after.size(), 2u) << "Expected 2 columns: id, val";
}

TEST(E2EBinlogDDL, AlterTableModifyType) {
  std::string gtid = GetCurrentGtid();
  ASSERT_FALSE(gtid.empty()) << "Failed to fetch current GTID";

  ASSERT_EQ(ExecuteDML("ALTER TABLE mes_test.ddl_test MODIFY val TEXT"),
            MES_OK);
  ASSERT_EQ(
      ExecuteDML(
          "INSERT INTO mes_test.ddl_test (val) VALUES ('modified_type_test')"),
      MES_OK);

  auto events = CaptureTableEvents(gtid, 403, "ddl_test", 1);
  auto filtered = FilterByTable(events, "ddl_test");

  ASSERT_GE(filtered.size(), 1u) << "Expected at least 1 INSERT for ddl_test";
  EXPECT_EQ(filtered[0].type, MES_EVENT_INSERT);

  // val column (index 1) — TEXT uses BLOB column type in binlog, decoded as BYTES
  ASSERT_GE(filtered[0].after.size(), 2u);
  EXPECT_TRUE(filtered[0].after[1].type == MES_COL_STRING ||
              filtered[0].after[1].type == MES_COL_BYTES);

  // Cleanup: revert to VARCHAR(100)
  ExecuteDML("ALTER TABLE mes_test.ddl_test MODIFY val VARCHAR(100)");
}

TEST(E2EBinlogDDL, DropTableDuringStream) {
  // Ensure no leftover table from a previous failed run
  ExecuteDML("DROP TABLE IF EXISTS mes_test.drop_me");

  ASSERT_EQ(ExecuteDML("CREATE TABLE mes_test.drop_me ("
                        "id INT NOT NULL PRIMARY KEY, v INT)"),
            MES_OK);

  std::string gtid = GetCurrentGtid();
  ASSERT_FALSE(gtid.empty()) << "Failed to fetch current GTID";

  ASSERT_EQ(ExecuteDML("INSERT INTO mes_test.drop_me VALUES (1, 10)"), MES_OK);
  ASSERT_EQ(ExecuteDML("DROP TABLE mes_test.drop_me"), MES_OK);
  ASSERT_EQ(
      ExecuteDML(
          "INSERT INTO mes_test.ddl_test (val) VALUES ('after_drop')"),
      MES_OK);

  // Capture events; we expect at least the ddl_test INSERT.
  // The drop_me INSERT may or may not arrive depending on timing,
  // but the stream must not error out.
  auto events = CaptureEvents(
      gtid, 404,
      [](const std::vector<CapturedEvent>& evts) {
        for (const auto& e : evts) {
          if (e.table == "ddl_test") return true;
        }
        return false;
      },
      200);

  auto ddl_events = FilterByTable(events, "ddl_test");
  ASSERT_GE(ddl_events.size(), 1u)
      << "Stream should continue after DROP TABLE";
  EXPECT_EQ(ddl_events[0].type, MES_EVENT_INSERT);
}

TEST(E2EBinlogDDL, RenameTableDuringStream) {
  std::string gtid = GetCurrentGtid();
  ASSERT_FALSE(gtid.empty()) << "Failed to fetch current GTID";

  ASSERT_EQ(
      ExecuteDML("RENAME TABLE mes_test.ddl_test TO mes_test.ddl_test_renamed"),
      MES_OK);
  ASSERT_EQ(
      ExecuteDML(
          "INSERT INTO mes_test.ddl_test_renamed (val) VALUES ('renamed')"),
      MES_OK);

  auto events = CaptureTableEvents(gtid, 405, "ddl_test_renamed", 1);
  auto filtered = FilterByTable(events, "ddl_test_renamed");

  ASSERT_GE(filtered.size(), 1u)
      << "Expected at least 1 INSERT for ddl_test_renamed";
  EXPECT_EQ(filtered[0].type, MES_EVENT_INSERT);
  EXPECT_EQ(filtered[0].table, "ddl_test_renamed");

  // Cleanup: rename back
  ExecuteDML("RENAME TABLE mes_test.ddl_test_renamed TO mes_test.ddl_test");
}

TEST(E2EBinlogDDL, TruncateTable) {
  // Insert an initial row so the table is not empty before truncate
  ASSERT_EQ(
      ExecuteDML(
          "INSERT INTO mes_test.ddl_test (val) VALUES ('before_truncate')"),
      MES_OK);

  std::string gtid = GetCurrentGtid();
  ASSERT_FALSE(gtid.empty()) << "Failed to fetch current GTID";

  ASSERT_EQ(ExecuteDML("TRUNCATE TABLE mes_test.ddl_test"), MES_OK);
  ASSERT_EQ(
      ExecuteDML(
          "INSERT INTO mes_test.ddl_test (val) VALUES ('after_truncate')"),
      MES_OK);

  auto events = CaptureTableEvents(gtid, 406, "ddl_test", 1);
  auto filtered = FilterByTable(events, "ddl_test");

  ASSERT_GE(filtered.size(), 1u)
      << "Expected at least 1 INSERT after TRUNCATE";
  EXPECT_EQ(filtered[0].type, MES_EVENT_INSERT);
}

TEST(E2EBinlogDDL, DmlAfterMultipleSchemaChanges) {
  std::string gtid = GetCurrentGtid();
  ASSERT_FALSE(gtid.empty()) << "Failed to fetch current GTID";

  // Schema change 1: add c1
  ASSERT_EQ(
      ExecuteDML(
          "ALTER TABLE mes_test.ddl_test ADD COLUMN c1 INT DEFAULT 0"),
      MES_OK);
  ASSERT_EQ(
      ExecuteDML(
          "INSERT INTO mes_test.ddl_test (val, c1) VALUES ('sc1', 10)"),
      MES_OK);

  // Schema change 2: add c2
  ASSERT_EQ(
      ExecuteDML(
          "ALTER TABLE mes_test.ddl_test ADD COLUMN c2 INT DEFAULT 0"),
      MES_OK);
  ASSERT_EQ(
      ExecuteDML(
          "INSERT INTO mes_test.ddl_test (val, c1, c2) VALUES ('sc2', 20, 30)"),
      MES_OK);

  // Schema change 3: drop c1
  ASSERT_EQ(
      ExecuteDML("ALTER TABLE mes_test.ddl_test DROP COLUMN c1"),
      MES_OK);
  ASSERT_EQ(
      ExecuteDML(
          "INSERT INTO mes_test.ddl_test (val, c2) VALUES ('sc3', 40)"),
      MES_OK);

  // Capture all 3 INSERTs
  auto events = CaptureTableEvents(gtid, 407, "ddl_test", 3);
  auto filtered = FilterByTable(events, "ddl_test");

  ASSERT_GE(filtered.size(), 3u)
      << "Expected 3 INSERTs for ddl_test after multiple schema changes";

  // 1st INSERT: id, val, c1 (3 columns)
  EXPECT_EQ(filtered[0].type, MES_EVENT_INSERT);
  EXPECT_EQ(filtered[0].after.size(), 3u)
      << "1st INSERT should have 3 columns (id, val, c1)";

  // 2nd INSERT: id, val, c1, c2 (4 columns)
  EXPECT_EQ(filtered[1].type, MES_EVENT_INSERT);
  EXPECT_EQ(filtered[1].after.size(), 4u)
      << "2nd INSERT should have 4 columns (id, val, c1, c2)";

  // 3rd INSERT: id, val, c2 (3 columns)
  EXPECT_EQ(filtered[2].type, MES_EVENT_INSERT);
  EXPECT_EQ(filtered[2].after.size(), 3u)
      << "3rd INSERT should have 3 columns (id, val, c2)";

  // Cleanup: drop c2
  ExecuteDML("ALTER TABLE mes_test.ddl_test DROP COLUMN c2");
}

}  // namespace
