// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * @file test_e2e_binlog_dml.cpp
 * @brief End-to-end tests for DML binlog event capture and decoding
 *
 * Requires a running MySQL 8.4+ instance at localhost:13307 with:
 *   - root/test_root_password (caching_sha2_password)
 *   - repl_user/test_password (replication grants)
 *   - Database: mes_test with tables: users, items, large_data
 *   - binlog_row_image=FULL
 *
 * Start with: cd e2e/docker && docker compose up -d
 *
 * Column indices used throughout (no metadata fetcher, so column names
 * are unavailable):
 *
 * users:  0=id, 1=name, 2=email, 3=age, 4=balance, 5=score,
 *         6=is_active, 7=bio, 8=avatar, 9=created_at, 10=updated_at
 * items:  0=id, 1=name, 2=value
 * large_data: 0=id, 1=big_text, 2=big_blob
 */

#include <gtest/gtest.h>

#include <algorithm>
#include <cstring>
#include <string>
#include <vector>

#include "mes.h"
#include "protocol/mysql_connection.h"
#include "protocol/mysql_query.h"
#include "test_e2e_helpers.h"

namespace {

// ---- InsertAllColumnTypes ----

TEST(E2EBinlogDML, InsertAllColumnTypes) {
  e2e::ExecuteDML("DELETE FROM mes_test.users WHERE id = 1000");
  e2e::ScopedCleanup cleanup("DELETE FROM mes_test.users WHERE id = 1000");

  auto gtid = e2e::GetCurrentGtid();
  ASSERT_FALSE(gtid.empty());

  auto rc = e2e::ExecuteDML(
      "INSERT INTO mes_test.users "
      "(id, name, email, age, balance, score, is_active, bio, "
      " created_at, updated_at) VALUES "
      "(1000, 'Alice', 'alice@test.com', 30, 1234.56, 3.14, 1, "
      " 'Hello world', '2024-01-15 10:30:00.123', "
      " '2024-01-15 10:30:00.123456')");
  ASSERT_EQ(rc, MES_OK);

  auto events = e2e::CaptureTableEvents(
      gtid, e2e::server_ids::kDmlInsertAllColumns, "users", 1);
  auto filtered = e2e::FilterByTable(events, "users");
  ASSERT_GE(filtered.size(), 1u);

  const auto& ev = filtered[0];
  EXPECT_EQ(ev.type, MES_EVENT_INSERT);
  EXPECT_TRUE(ev.before.empty());
  ASSERT_GE(ev.after.size(), 11u);

  // 0: id (BIGINT)
  EXPECT_EQ(ev.after[0].type, MES_COL_INT);
  // 1: name (VARCHAR)
  EXPECT_EQ(ev.after[1].type, MES_COL_STRING);
  // 2: email (VARCHAR)
  EXPECT_EQ(ev.after[2].type, MES_COL_STRING);
  // 3: age (INT)
  EXPECT_EQ(ev.after[3].type, MES_COL_INT);
  // 4: balance (DECIMAL)
  EXPECT_EQ(ev.after[4].type, MES_COL_STRING);
  // 5: score (DOUBLE)
  EXPECT_EQ(ev.after[5].type, MES_COL_DOUBLE);
  // 6: is_active (TINYINT)
  EXPECT_EQ(ev.after[6].type, MES_COL_INT);
  // 7: bio (TEXT — decoded as BYTES in binlog, TEXT uses BLOB column type)
  EXPECT_TRUE(ev.after[7].type == MES_COL_STRING ||
              ev.after[7].type == MES_COL_BYTES);
  // 9: created_at (DATETIME(3))
  EXPECT_EQ(ev.after[9].type, MES_COL_STRING);
  // 10: updated_at (TIMESTAMP(6))
  EXPECT_EQ(ev.after[10].type, MES_COL_STRING);
}

// ---- InsertWithNulls ----

TEST(E2EBinlogDML, InsertWithNulls) {
  e2e::ExecuteDML("DELETE FROM mes_test.users WHERE id = 1001");
  e2e::ScopedCleanup cleanup("DELETE FROM mes_test.users WHERE id = 1001");

  auto gtid = e2e::GetCurrentGtid();
  ASSERT_FALSE(gtid.empty());

  auto rc = e2e::ExecuteDML(
      "INSERT INTO mes_test.users (id, name, is_active, created_at, "
      "updated_at) "
      "VALUES (1001, 'NullUser', 1, NOW(3), NOW(6))");
  ASSERT_EQ(rc, MES_OK);

  auto events = e2e::CaptureTableEvents(
      gtid, e2e::server_ids::kDmlInsertWithNulls, "users", 1);
  auto filtered = e2e::FilterByTable(events, "users");
  ASSERT_GE(filtered.size(), 1u);

  const auto& ev = filtered[0];
  EXPECT_EQ(ev.type, MES_EVENT_INSERT);
  ASSERT_GE(ev.after.size(), 11u);

  // Nullable columns not set should be NULL
  // 2: email
  EXPECT_EQ(ev.after[2].type, MES_COL_NULL);
  // 3: age
  EXPECT_EQ(ev.after[3].type, MES_COL_NULL);
  // 4: balance
  EXPECT_EQ(ev.after[4].type, MES_COL_NULL);
  // 5: score
  EXPECT_EQ(ev.after[5].type, MES_COL_NULL);
  // 7: bio
  EXPECT_EQ(ev.after[7].type, MES_COL_NULL);
  // 8: avatar
  EXPECT_EQ(ev.after[8].type, MES_COL_NULL);
}

// ---- InsertMultiRow ----

TEST(E2EBinlogDML, InsertMultiRow) {
  e2e::ScopedCleanup cleanup(
      "DELETE FROM mes_test.items WHERE name IN ('multi_a','multi_b','multi_c')");

  auto gtid = e2e::GetCurrentGtid();
  ASSERT_FALSE(gtid.empty());

  auto rc = e2e::ExecuteDML(
      "INSERT INTO mes_test.items (name, value) VALUES "
      "('multi_a', 1), ('multi_b', 2), ('multi_c', 3)");
  ASSERT_EQ(rc, MES_OK);

  auto events = e2e::CaptureTableEvents(
      gtid, e2e::server_ids::kDmlInsertMultiRow, "items", 3);
  auto filtered = e2e::FilterByTable(events, "items");
  ASSERT_EQ(filtered.size(), 3u);

  for (const auto& ev : filtered) {
    EXPECT_EQ(ev.type, MES_EVENT_INSERT);
    EXPECT_FALSE(ev.after.empty());
  }

  // Verify values by index: items columns 0=id, 1=name, 2=value
  ASSERT_GT(filtered[0].after.size(), 2u);
  ASSERT_GT(filtered[1].after.size(), 2u);
  ASSERT_GT(filtered[2].after.size(), 2u);
  EXPECT_EQ(filtered[0].after[1].str_data, "multi_a");
  EXPECT_EQ(filtered[1].after[1].str_data, "multi_b");
  EXPECT_EQ(filtered[2].after[1].str_data, "multi_c");

  EXPECT_EQ(filtered[0].after[2].int_val, 1);
  EXPECT_EQ(filtered[1].after[2].int_val, 2);
  EXPECT_EQ(filtered[2].after[2].int_val, 3);
}

// ---- UpdateSubset ----

TEST(E2EBinlogDML, UpdateSubset) {
  // Insert a row first
  e2e::ExecuteDML(
      "INSERT INTO mes_test.items (name, value) VALUES ('upd_subset', 100)");

  auto gtid = e2e::GetCurrentGtid();
  ASSERT_FALSE(gtid.empty());

  auto rc = e2e::ExecuteDML(
      "UPDATE mes_test.items SET value = 999 WHERE name = 'upd_subset'");
  ASSERT_EQ(rc, MES_OK);

  auto events = e2e::CaptureTableEvents(
      gtid, e2e::server_ids::kDmlUpdateSubset, "items", 1);
  auto filtered = e2e::FilterByTable(events, "items");
  ASSERT_GE(filtered.size(), 1u);

  const auto& ev = filtered[0];
  EXPECT_EQ(ev.type, MES_EVENT_UPDATE);

  // FULL image: both before and after should have all 3 columns
  ASSERT_GE(ev.before.size(), 3u);
  ASSERT_GE(ev.after.size(), 3u);

  // Index 2: value
  EXPECT_NE(ev.before[2].int_val, ev.after[2].int_val);
  EXPECT_EQ(ev.after[2].int_val, 999);

  // Cleanup
  e2e::ExecuteDML("DELETE FROM mes_test.items WHERE name = 'upd_subset'");
}

// ---- UpdateAllColumns ----

TEST(E2EBinlogDML, UpdateAllColumns) {
  e2e::ExecuteDML("DELETE FROM mes_test.users WHERE id = 1004");
  e2e::ExecuteDML(
      "INSERT INTO mes_test.users (id, name, email, age, balance, score, "
      "is_active, bio, created_at, updated_at) VALUES "
      "(1004, 'BeforeUser', 'before@test.com', 20, 100.00, 1.0, 0, "
      "'old bio', '2024-01-01 00:00:00.000', '2024-01-01 00:00:00.000000')");

  auto gtid = e2e::GetCurrentGtid();
  ASSERT_FALSE(gtid.empty());

  auto rc = e2e::ExecuteDML(
      "UPDATE mes_test.users SET name='AfterUser', email='after@test.com', "
      "age=40, balance=200.00, score=2.0, is_active=1, bio='new bio', "
      "created_at='2024-06-15 12:00:00.456', "
      "updated_at='2024-06-15 12:00:00.456789' "
      "WHERE id = 1004");
  ASSERT_EQ(rc, MES_OK);

  auto events = e2e::CaptureTableEvents(
      gtid, e2e::server_ids::kDmlUpdateAllColumns, "users", 1);
  auto filtered = e2e::FilterByTable(events, "users");
  ASSERT_GE(filtered.size(), 1u);

  const auto& ev = filtered[0];
  EXPECT_EQ(ev.type, MES_EVENT_UPDATE);
  EXPECT_FALSE(ev.before.empty());
  EXPECT_FALSE(ev.after.empty());
  ASSERT_GE(ev.before.size(), 11u);
  ASSERT_GE(ev.after.size(), 11u);

  // Index 1: name
  EXPECT_EQ(ev.before[1].str_data, "BeforeUser");
  EXPECT_EQ(ev.after[1].str_data, "AfterUser");

  // Index 3: age
  EXPECT_EQ(ev.before[3].int_val, 20);
  EXPECT_EQ(ev.after[3].int_val, 40);

  // Cleanup
  e2e::ExecuteDML("DELETE FROM mes_test.users WHERE id = 1004");
}

// ---- DeleteSingleRow ----

TEST(E2EBinlogDML, DeleteSingleRow) {
  e2e::ExecuteDML(
      "INSERT INTO mes_test.items (name, value) VALUES ('del_single', 42)");

  auto gtid = e2e::GetCurrentGtid();
  ASSERT_FALSE(gtid.empty());

  auto rc =
      e2e::ExecuteDML("DELETE FROM mes_test.items WHERE name = 'del_single'");
  ASSERT_EQ(rc, MES_OK);

  auto events = e2e::CaptureTableEvents(gtid, 505, "items", 1);
  auto filtered = e2e::FilterByTable(events, "items");
  ASSERT_GE(filtered.size(), 1u);

  const auto& ev = filtered[0];
  EXPECT_EQ(ev.type, MES_EVENT_DELETE);
  EXPECT_FALSE(ev.before.empty());
  EXPECT_TRUE(ev.after.empty());

  // Index 1: name
  ASSERT_GT(ev.before.size(), 1u);
  EXPECT_EQ(ev.before[1].str_data, "del_single");
}

// ---- MultiRowUpdate ----

TEST(E2EBinlogDML, MultiRowUpdate) {
  e2e::ExecuteDML(
      "INSERT INTO mes_test.items (name, value) VALUES "
      "('mru_a', 10), ('mru_b', 20), ('mru_c', 30)");

  auto gtid = e2e::GetCurrentGtid();
  ASSERT_FALSE(gtid.empty());

  auto rc = e2e::ExecuteDML(
      "UPDATE mes_test.items SET value = value + 100 "
      "WHERE name IN ('mru_a', 'mru_b', 'mru_c')");
  ASSERT_EQ(rc, MES_OK);

  auto events = e2e::CaptureTableEvents(gtid, 506, "items", 3);
  auto filtered = e2e::FilterByTable(events, "items");
  ASSERT_EQ(filtered.size(), 3u);

  for (const auto& ev : filtered) {
    EXPECT_EQ(ev.type, MES_EVENT_UPDATE);
    EXPECT_FALSE(ev.before.empty());
    EXPECT_FALSE(ev.after.empty());
  }

  // Cleanup
  e2e::ExecuteDML(
      "DELETE FROM mes_test.items WHERE name IN ('mru_a','mru_b','mru_c')");
}

// ---- MultiRowDelete ----

TEST(E2EBinlogDML, MultiRowDelete) {
  e2e::ExecuteDML(
      "INSERT INTO mes_test.items (name, value) VALUES "
      "('mrd_a', 1), ('mrd_b', 2), ('mrd_c', 3)");

  auto gtid = e2e::GetCurrentGtid();
  ASSERT_FALSE(gtid.empty());

  auto rc = e2e::ExecuteDML(
      "DELETE FROM mes_test.items WHERE name IN ('mrd_a', 'mrd_b', 'mrd_c')");
  ASSERT_EQ(rc, MES_OK);

  auto events = e2e::CaptureTableEvents(gtid, 507, "items", 3);
  auto filtered = e2e::FilterByTable(events, "items");
  ASSERT_EQ(filtered.size(), 3u);

  for (const auto& ev : filtered) {
    EXPECT_EQ(ev.type, MES_EVENT_DELETE);
    EXPECT_FALSE(ev.before.empty());
    EXPECT_TRUE(ev.after.empty());
  }
}

// ---- TransactionMultipleDml ----

TEST(E2EBinlogDML, TransactionMultipleDml) {
  // Pre-insert a row for the UPDATE and DELETE within the transaction
  e2e::ExecuteDML(
      "INSERT INTO mes_test.items (name, value) VALUES ('txn_target', 50)");

  auto gtid = e2e::GetCurrentGtid();
  ASSERT_FALSE(gtid.empty());

  // Use a single connection for the entire transaction
  mes::protocol::MysqlConnection conn;
  ASSERT_EQ(conn.Connect(e2e::kHost, e2e::kPort, e2e::kRootUser,
                          e2e::kRootPass, e2e::kTimeout, e2e::kTimeout, 0, "",
                          "", ""),
            MES_OK);

  mes::protocol::QueryResult qr;
  std::string err;

  ASSERT_EQ(mes::protocol::ExecuteQuery(conn.Socket(), "BEGIN", &qr, &err),
            MES_OK);
  ASSERT_EQ(
      mes::protocol::ExecuteQuery(
          conn.Socket(),
          "INSERT INTO mes_test.items (name, value) VALUES ('txn_insert', 60)",
          &qr, &err),
      MES_OK);
  ASSERT_EQ(
      mes::protocol::ExecuteQuery(
          conn.Socket(),
          "UPDATE mes_test.items SET value = 55 WHERE name = 'txn_target'", &qr,
          &err),
      MES_OK);
  ASSERT_EQ(
      mes::protocol::ExecuteQuery(
          conn.Socket(),
          "DELETE FROM mes_test.items WHERE name = 'txn_insert'", &qr, &err),
      MES_OK);
  ASSERT_EQ(mes::protocol::ExecuteQuery(conn.Socket(), "COMMIT", &qr, &err),
            MES_OK);
  conn.Disconnect();

  // Capture 3 events for "items": INSERT, UPDATE, DELETE
  auto events = e2e::CaptureTableEvents(gtid, 508, "items", 3);
  auto filtered = e2e::FilterByTable(events, "items");
  ASSERT_GE(filtered.size(), 3u);

  EXPECT_EQ(filtered[0].type, MES_EVENT_INSERT);
  EXPECT_EQ(filtered[1].type, MES_EVENT_UPDATE);
  EXPECT_EQ(filtered[2].type, MES_EVENT_DELETE);

  // Cleanup
  e2e::ExecuteDML("DELETE FROM mes_test.items WHERE name = 'txn_target'");
}

// ---- LargeTextValue ----

TEST(E2EBinlogDML, LargeTextValue) {
  e2e::ExecuteDML("DELETE FROM mes_test.large_data WHERE id = 1");

  auto gtid = e2e::GetCurrentGtid();
  ASSERT_FALSE(gtid.empty());

  auto rc = e2e::ExecuteDML(
      "INSERT INTO mes_test.large_data (id, big_text) VALUES "
      "(1, REPEAT('x', 100000))");
  ASSERT_EQ(rc, MES_OK);

  auto events = e2e::CaptureTableEvents(gtid, 509, "large_data", 1);
  auto filtered = e2e::FilterByTable(events, "large_data");
  ASSERT_GE(filtered.size(), 1u);

  const auto& ev = filtered[0];
  EXPECT_EQ(ev.type, MES_EVENT_INSERT);

  // Index 1: big_text (LONGTEXT — decoded as BYTES in binlog, TEXT uses BLOB type)
  ASSERT_GT(ev.after.size(), 1u);
  EXPECT_TRUE(ev.after[1].type == MES_COL_STRING ||
              ev.after[1].type == MES_COL_BYTES);
  EXPECT_GE(ev.after[1].str_data.size(), 100000u);

  // Cleanup
  e2e::ExecuteDML("DELETE FROM mes_test.large_data WHERE id = 1");
}

// ---- LargeBlobValue ----

TEST(E2EBinlogDML, LargeBlobValue) {
  e2e::ExecuteDML("DELETE FROM mes_test.large_data WHERE id = 2");

  auto gtid = e2e::GetCurrentGtid();
  ASSERT_FALSE(gtid.empty());

  auto rc = e2e::ExecuteDML(
      "INSERT INTO mes_test.large_data (id, big_blob) VALUES "
      "(2, UNHEX(REPEAT('41', 100000)))");
  ASSERT_EQ(rc, MES_OK);

  auto events = e2e::CaptureTableEvents(gtid, 510, "large_data", 1);
  auto filtered = e2e::FilterByTable(events, "large_data");
  ASSERT_GE(filtered.size(), 1u);

  const auto& ev = filtered[0];
  EXPECT_EQ(ev.type, MES_EVENT_INSERT);

  // Index 2: big_blob
  ASSERT_GT(ev.after.size(), 2u);
  EXPECT_EQ(ev.after[2].type, MES_COL_BYTES);
  EXPECT_GE(ev.after[2].str_data.size(), 100000u);

  // Cleanup
  e2e::ExecuteDML("DELETE FROM mes_test.large_data WHERE id = 2");
}

// ---- UnicodeAndEmoji ----

TEST(E2EBinlogDML, UnicodeAndEmoji) {
  auto gtid = e2e::GetCurrentGtid();
  ASSERT_FALSE(gtid.empty());

  auto rc = e2e::ExecuteDML(
      u8"INSERT INTO mes_test.items (name, value) VALUES "
      u8"('\u65E5\u672C\u8A9E\u30C6\u30B9\u30C8\xF0\x9F\x8E\x89', 777)");
  ASSERT_EQ(rc, MES_OK);

  auto events = e2e::CaptureTableEvents(gtid, 511, "items", 1);
  auto filtered = e2e::FilterByTable(events, "items");
  ASSERT_GE(filtered.size(), 1u);

  const auto& ev = filtered[0];
  EXPECT_EQ(ev.type, MES_EVENT_INSERT);

  // Index 1: name
  ASSERT_GT(ev.after.size(), 1u);
  EXPECT_EQ(ev.after[1].type, MES_COL_STRING);

  // The string should contain the Japanese characters and emoji
  std::string expected =
      u8"\u65E5\u672C\u8A9E\u30C6\u30B9\u30C8\xF0\x9F\x8E\x89";
  EXPECT_EQ(ev.after[1].str_data, expected);

  // Cleanup
  e2e::ExecuteDML(
      u8"DELETE FROM mes_test.items WHERE name = "
      u8"'\u65E5\u672C\u8A9E\u30C6\u30B9\u30C8\xF0\x9F\x8E\x89'");
}

// ---- DecimalMaxPrecision ----

TEST(E2EBinlogDML, DecimalMaxPrecision) {
  e2e::ExecuteDML("DELETE FROM mes_test.users WHERE id = 1012");

  auto gtid = e2e::GetCurrentGtid();
  ASSERT_FALSE(gtid.empty());

  auto rc = e2e::ExecuteDML(
      "INSERT INTO mes_test.users (id, name, balance, is_active, "
      "created_at, updated_at) VALUES "
      "(1012, 'DecMax', 99999999.99, 1, NOW(3), NOW(6))");
  ASSERT_EQ(rc, MES_OK);

  auto events = e2e::CaptureTableEvents(gtid, 512, "users", 1);
  auto filtered = e2e::FilterByTable(events, "users");
  ASSERT_GE(filtered.size(), 1u);

  const auto& ev = filtered[0];
  EXPECT_EQ(ev.type, MES_EVENT_INSERT);

  // Index 4: balance (DECIMAL)
  ASSERT_GT(ev.after.size(), 4u);
  EXPECT_EQ(ev.after[4].type, MES_COL_STRING);
  EXPECT_NE(ev.after[4].str_data.find("99999999.99"), std::string::npos)
      << "balance = " << ev.after[4].str_data;

  // Cleanup
  e2e::ExecuteDML("DELETE FROM mes_test.users WHERE id = 1012");
}

// ---- DecimalNegative ----

TEST(E2EBinlogDML, DecimalNegative) {
  e2e::ExecuteDML("DELETE FROM mes_test.users WHERE id = 1013");

  auto gtid = e2e::GetCurrentGtid();
  ASSERT_FALSE(gtid.empty());

  auto rc = e2e::ExecuteDML(
      "INSERT INTO mes_test.users (id, name, balance, is_active, "
      "created_at, updated_at) VALUES "
      "(1013, 'DecNeg', -12345.67, 1, NOW(3), NOW(6))");
  ASSERT_EQ(rc, MES_OK);

  auto events = e2e::CaptureTableEvents(gtid, 513, "users", 1);
  auto filtered = e2e::FilterByTable(events, "users");
  ASSERT_GE(filtered.size(), 1u);

  const auto& ev = filtered[0];
  EXPECT_EQ(ev.type, MES_EVENT_INSERT);

  // Index 4: balance (DECIMAL)
  ASSERT_GT(ev.after.size(), 4u);
  EXPECT_EQ(ev.after[4].type, MES_COL_STRING);
  EXPECT_NE(ev.after[4].str_data.find("-12345.67"), std::string::npos)
      << "balance = " << ev.after[4].str_data;

  // Cleanup
  e2e::ExecuteDML("DELETE FROM mes_test.users WHERE id = 1013");
}

// ---- NullToNonNullUpdate ----

TEST(E2EBinlogDML, NullToNonNullUpdate) {
  e2e::ExecuteDML("DELETE FROM mes_test.users WHERE id = 1014");
  e2e::ExecuteDML(
      "INSERT INTO mes_test.users (id, name, email, is_active, "
      "created_at, updated_at) VALUES "
      "(1014, 'NullUpd', NULL, 1, NOW(3), NOW(6))");

  auto gtid = e2e::GetCurrentGtid();
  ASSERT_FALSE(gtid.empty());

  auto rc = e2e::ExecuteDML(
      "UPDATE mes_test.users SET email = 'new@test.com' WHERE id = 1014");
  ASSERT_EQ(rc, MES_OK);

  auto events = e2e::CaptureTableEvents(gtid, 514, "users", 1);
  auto filtered = e2e::FilterByTable(events, "users");
  ASSERT_GE(filtered.size(), 1u);

  const auto& ev = filtered[0];
  EXPECT_EQ(ev.type, MES_EVENT_UPDATE);

  // Index 2: email
  ASSERT_GT(ev.before.size(), 2u);
  ASSERT_GT(ev.after.size(), 2u);
  EXPECT_EQ(ev.before[2].type, MES_COL_NULL);
  EXPECT_EQ(ev.after[2].type, MES_COL_STRING);
  EXPECT_EQ(ev.after[2].str_data, "new@test.com");

  // Cleanup
  e2e::ExecuteDML("DELETE FROM mes_test.users WHERE id = 1014");
}

// ---- NonNullToNullUpdate ----

TEST(E2EBinlogDML, NonNullToNullUpdate) {
  e2e::ExecuteDML("DELETE FROM mes_test.users WHERE id = 1015");
  e2e::ExecuteDML(
      "INSERT INTO mes_test.users (id, name, email, is_active, "
      "created_at, updated_at) VALUES "
      "(1015, 'NonNullUpd', 'old@test.com', 1, NOW(3), NOW(6))");

  auto gtid = e2e::GetCurrentGtid();
  ASSERT_FALSE(gtid.empty());

  auto rc = e2e::ExecuteDML(
      "UPDATE mes_test.users SET email = NULL WHERE id = 1015");
  ASSERT_EQ(rc, MES_OK);

  auto events = e2e::CaptureTableEvents(gtid, 515, "users", 1);
  auto filtered = e2e::FilterByTable(events, "users");
  ASSERT_GE(filtered.size(), 1u);

  const auto& ev = filtered[0];
  EXPECT_EQ(ev.type, MES_EVENT_UPDATE);

  // Index 2: email
  ASSERT_GT(ev.before.size(), 2u);
  ASSERT_GT(ev.after.size(), 2u);
  EXPECT_EQ(ev.before[2].type, MES_COL_STRING);
  EXPECT_EQ(ev.before[2].str_data, "old@test.com");
  EXPECT_EQ(ev.after[2].type, MES_COL_NULL);

  // Cleanup
  e2e::ExecuteDML("DELETE FROM mes_test.users WHERE id = 1015");
}

// ---- EmptyStringVsNull ----

TEST(E2EBinlogDML, EmptyStringVsNull) {
  auto gtid = e2e::GetCurrentGtid();
  ASSERT_FALSE(gtid.empty());

  auto rc = e2e::ExecuteDML(
      "INSERT INTO mes_test.items (name, value) VALUES ('', 0)");
  ASSERT_EQ(rc, MES_OK);

  auto events = e2e::CaptureTableEvents(gtid, 516, "items", 1);
  auto filtered = e2e::FilterByTable(events, "items");
  ASSERT_GE(filtered.size(), 1u);

  const auto& ev = filtered[0];
  EXPECT_EQ(ev.type, MES_EVENT_INSERT);

  // Index 1: name
  ASSERT_GT(ev.after.size(), 1u);
  EXPECT_EQ(ev.after[1].type, MES_COL_STRING);
  EXPECT_TRUE(ev.after[1].str_data.empty());

  // Cleanup
  e2e::ExecuteDML("DELETE FROM mes_test.items WHERE name = '' AND value = 0");
}

// ---- BooleanValues ----

TEST(E2EBinlogDML, BooleanValues) {
  e2e::ExecuteDML("DELETE FROM mes_test.users WHERE id IN (1017, 1018)");

  auto gtid = e2e::GetCurrentGtid();
  ASSERT_FALSE(gtid.empty());

  auto rc = e2e::ExecuteDML(
      "INSERT INTO mes_test.users (id, name, is_active, created_at, "
      "updated_at) VALUES "
      "(1017, 'BoolFalse', 0, NOW(3), NOW(6))");
  ASSERT_EQ(rc, MES_OK);
  rc = e2e::ExecuteDML(
      "INSERT INTO mes_test.users (id, name, is_active, created_at, "
      "updated_at) VALUES "
      "(1018, 'BoolTrue', 1, NOW(3), NOW(6))");
  ASSERT_EQ(rc, MES_OK);

  auto events = e2e::CaptureTableEvents(gtid, 517, "users", 2);
  auto filtered = e2e::FilterByTable(events, "users");
  ASSERT_GE(filtered.size(), 2u);

  // Both should be INSERTs
  EXPECT_EQ(filtered[0].type, MES_EVENT_INSERT);
  EXPECT_EQ(filtered[1].type, MES_EVENT_INSERT);

  // Index 6: is_active
  ASSERT_GT(filtered[0].after.size(), 6u);
  ASSERT_GT(filtered[1].after.size(), 6u);
  EXPECT_EQ(filtered[0].after[6].type, MES_COL_INT);
  EXPECT_EQ(filtered[1].after[6].type, MES_COL_INT);
  EXPECT_EQ(filtered[0].after[6].int_val, 0);
  EXPECT_EQ(filtered[1].after[6].int_val, 1);

  // Cleanup
  e2e::ExecuteDML("DELETE FROM mes_test.users WHERE id IN (1017, 1018)");
}

// ---- VECTOR type tests (MySQL 9.0+) ----

TEST(E2EBinlogDML, VectorInsert) {
  if (!e2e::IsMysql9OrLater()) {
    GTEST_SKIP() << "VECTOR type requires MySQL 9.0+";
  }

  e2e::ExecuteDML(
      "CREATE TABLE IF NOT EXISTS mes_test.vec_test ("
      "  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,"
      "  embedding VECTOR(3)"
      ") ENGINE=InnoDB");
  e2e::ExecuteDML("DELETE FROM mes_test.vec_test WHERE id = 1");

  auto gtid = e2e::GetCurrentGtid();
  ASSERT_FALSE(gtid.empty());

  auto rc = e2e::ExecuteDML(
      "INSERT INTO mes_test.vec_test (id, embedding) VALUES "
      "(1, TO_VECTOR('[1.0, 2.0, 3.0]'))");
  ASSERT_EQ(rc, MES_OK);

  auto events = e2e::CaptureTableEvents(gtid, 520, "vec_test", 1);
  auto filtered = e2e::FilterByTable(events, "vec_test");
  ASSERT_GE(filtered.size(), 1u);

  const auto& ev = filtered[0];
  EXPECT_EQ(ev.type, MES_EVENT_INSERT);
  ASSERT_GE(ev.after.size(), 2u);

  // Index 0: id
  EXPECT_EQ(ev.after[0].type, MES_COL_INT);
  EXPECT_EQ(ev.after[0].int_val, 1);

  // Index 1: embedding (VECTOR -> BYTES, 3 float32 = 12 bytes)
  EXPECT_EQ(ev.after[1].type, MES_COL_BYTES);
  EXPECT_EQ(ev.after[1].str_data.size(), 12u);

  // Cleanup
  e2e::ExecuteDML("DROP TABLE IF EXISTS mes_test.vec_test");
}

TEST(E2EBinlogDML, VectorUpdate) {
  if (!e2e::IsMysql9OrLater()) {
    GTEST_SKIP() << "VECTOR type requires MySQL 9.0+";
  }

  e2e::ExecuteDML(
      "CREATE TABLE IF NOT EXISTS mes_test.vec_test ("
      "  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,"
      "  embedding VECTOR(3)"
      ") ENGINE=InnoDB");
  e2e::ExecuteDML(
      "INSERT INTO mes_test.vec_test (id, embedding) VALUES "
      "(1, TO_VECTOR('[1.0, 2.0, 3.0]'))");

  auto gtid = e2e::GetCurrentGtid();
  ASSERT_FALSE(gtid.empty());

  auto rc = e2e::ExecuteDML(
      "UPDATE mes_test.vec_test SET embedding = TO_VECTOR('[4.0, 5.0, 6.0]') "
      "WHERE id = 1");
  ASSERT_EQ(rc, MES_OK);

  auto events = e2e::CaptureTableEvents(gtid, 521, "vec_test", 1);
  auto filtered = e2e::FilterByTable(events, "vec_test");
  ASSERT_GE(filtered.size(), 1u);

  const auto& ev = filtered[0];
  EXPECT_EQ(ev.type, MES_EVENT_UPDATE);
  ASSERT_GE(ev.before.size(), 2u);
  ASSERT_GE(ev.after.size(), 2u);

  // Both before and after should have VECTOR as BYTES
  EXPECT_EQ(ev.before[1].type, MES_COL_BYTES);
  EXPECT_EQ(ev.after[1].type, MES_COL_BYTES);
  EXPECT_EQ(ev.before[1].str_data.size(), 12u);
  EXPECT_EQ(ev.after[1].str_data.size(), 12u);

  // Before and after should differ
  EXPECT_NE(ev.before[1].str_data, ev.after[1].str_data);

  // Cleanup
  e2e::ExecuteDML("DROP TABLE IF EXISTS mes_test.vec_test");
}

TEST(E2EBinlogDML, VectorDelete) {
  if (!e2e::IsMysql9OrLater()) {
    GTEST_SKIP() << "VECTOR type requires MySQL 9.0+";
  }

  e2e::ExecuteDML(
      "CREATE TABLE IF NOT EXISTS mes_test.vec_test ("
      "  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,"
      "  embedding VECTOR(3)"
      ") ENGINE=InnoDB");
  e2e::ExecuteDML(
      "INSERT INTO mes_test.vec_test (id, embedding) VALUES "
      "(1, TO_VECTOR('[1.0, 2.0, 3.0]'))");

  auto gtid = e2e::GetCurrentGtid();
  ASSERT_FALSE(gtid.empty());

  auto rc = e2e::ExecuteDML("DELETE FROM mes_test.vec_test WHERE id = 1");
  ASSERT_EQ(rc, MES_OK);

  auto events = e2e::CaptureTableEvents(gtid, 522, "vec_test", 1);
  auto filtered = e2e::FilterByTable(events, "vec_test");
  ASSERT_GE(filtered.size(), 1u);

  const auto& ev = filtered[0];
  EXPECT_EQ(ev.type, MES_EVENT_DELETE);
  EXPECT_FALSE(ev.before.empty());
  EXPECT_TRUE(ev.after.empty());

  // before should have VECTOR data
  ASSERT_GE(ev.before.size(), 2u);
  EXPECT_EQ(ev.before[1].type, MES_COL_BYTES);
  EXPECT_EQ(ev.before[1].str_data.size(), 12u);

  // Cleanup
  e2e::ExecuteDML("DROP TABLE IF EXISTS mes_test.vec_test");
}

}  // namespace
