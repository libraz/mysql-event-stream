// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * @file test_e2e_binlog_dml.cpp
 * @brief End-to-end tests for DML binlog event capture and decoding
 *
 * Requires a running MySQL 8.4+ instance at localhost:13308 with:
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
#include <chrono>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

#include "client/event_queue.h"
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

  auto events = e2e::CaptureTableEvents(gtid, e2e::server_ids::kDmlInsertAllColumns, "users", 1);
  auto filtered = e2e::FilterByTable(events, "users");
  ASSERT_GE(filtered.size(), 1u);

  const auto& ev = filtered[0];
  EXPECT_EQ(ev.type, MES_EVENT_INSERT);
  EXPECT_TRUE(ev.before.empty());
  ASSERT_GE(ev.after.size(), 11u);

  // 0: id (BIGINT)
  EXPECT_EQ(ev.after[0].type, MES_COL_INT);
  EXPECT_EQ(ev.after[0].int_val, 1000);
  // 1: name (VARCHAR)
  EXPECT_EQ(ev.after[1].type, MES_COL_STRING);
  EXPECT_EQ(ev.after[1].str_data, "Alice");
  // 2: email (VARCHAR)
  EXPECT_EQ(ev.after[2].type, MES_COL_STRING);
  EXPECT_EQ(ev.after[2].str_data, "alice@test.com");
  // 3: age (INT)
  EXPECT_EQ(ev.after[3].type, MES_COL_INT);
  EXPECT_EQ(ev.after[3].int_val, 30);
  // 4: balance (DECIMAL)
  EXPECT_EQ(ev.after[4].type, MES_COL_STRING);
  EXPECT_EQ(ev.after[4].str_data, "1234.56");
  // 5: score (DOUBLE)
  EXPECT_EQ(ev.after[5].type, MES_COL_DOUBLE);
  EXPECT_DOUBLE_EQ(ev.after[5].double_val, 3.14);
  // 6: is_active (TINYINT)
  EXPECT_EQ(ev.after[6].type, MES_COL_INT);
  EXPECT_EQ(ev.after[6].int_val, 1);
  // 7: bio (TEXT)
  EXPECT_EQ(ev.after[7].type, MES_COL_STRING);
  EXPECT_EQ(ev.after[7].str_data, "Hello world");
  // 9: created_at (DATETIME(3))
  EXPECT_EQ(ev.after[9].type, MES_COL_STRING);
  EXPECT_EQ(ev.after[9].str_data, "2024-01-15 10:30:00.123");
  // 10: updated_at (TIMESTAMP(6))
  EXPECT_EQ(ev.after[10].type, MES_COL_STRING);
  EXPECT_EQ(ev.after[10].str_data, "1705314600.123456");
}

TEST(E2EBinlogDML, ExtendedServerColumnTypes) {
  e2e::ExecuteDML("DELETE FROM mes_test.extended_type_values WHERE id = 1");
  e2e::ScopedCleanup cleanup("DELETE FROM mes_test.extended_type_values WHERE id = 1");

  const auto gtid = e2e::GetCurrentGtid();
  ASSERT_FALSE(gtid.empty());
  ASSERT_EQ(
      e2e::ExecuteDML(
          "INSERT INTO mes_test.extended_type_values "
          "(id, tiny_unsigned, small_unsigned, medium_unsigned, int_unsigned, year_value, "
          " date_value, time_value, float_value, wide_char, binary_value, geometry_value) VALUES "
          "(1, 255, 65535, 16777215, 4294967295, 2025, '2024-01-15', "
          " '12:34:56.123456', 1.25, 'wide char value', X'00FF10', "
          " ST_GeomFromText('POINT(1 2)'))"),
      MES_OK);

  const auto events =
      e2e::CaptureTableEvents(gtid, e2e::server_ids::kDmlExtendedTypes, "extended_type_values", 1);
  const auto filtered = e2e::FilterByTable(events, "extended_type_values");
  ASSERT_EQ(filtered.size(), 1u);
  const auto& after = filtered[0].after;
  ASSERT_EQ(after.size(), 12u);
  EXPECT_EQ(after[0].int_val, 1);
  EXPECT_EQ(after[1].int_val, 255);
  EXPECT_EQ(after[2].int_val, 65535);
  EXPECT_EQ(after[3].int_val, 16777215);
  EXPECT_EQ(after[4].int_val, 4294967295LL);
  EXPECT_EQ(after[5].int_val, 2025);
  EXPECT_EQ(after[6].str_data, "2024-01-15");
  EXPECT_EQ(after[7].str_data, "12:34:56.123456");
  EXPECT_EQ(after[8].type, MES_COL_DOUBLE);
  EXPECT_FLOAT_EQ(static_cast<float>(after[8].double_val), 1.25F);
  EXPECT_EQ(after[9].str_data, "wide char value");
  EXPECT_EQ(after[10].type, MES_COL_BYTES);
  EXPECT_EQ(after[10].str_data, std::string("\0\xFF\x10", 3));
  EXPECT_EQ(after[11].type, MES_COL_BYTES);
  EXPECT_FALSE(after[11].str_data.empty());
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

  auto events = e2e::CaptureTableEvents(gtid, e2e::server_ids::kDmlInsertWithNulls, "users", 1);
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

  auto events = e2e::CaptureTableEvents(gtid, e2e::server_ids::kDmlInsertMultiRow, "items", 3);
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
  e2e::ExecuteDML("INSERT INTO mes_test.items (name, value) VALUES ('upd_subset', 100)");

  auto gtid = e2e::GetCurrentGtid();
  ASSERT_FALSE(gtid.empty());

  auto rc = e2e::ExecuteDML("UPDATE mes_test.items SET value = 999 WHERE name = 'upd_subset'");
  ASSERT_EQ(rc, MES_OK);

  auto events = e2e::CaptureTableEvents(gtid, e2e::server_ids::kDmlUpdateSubset, "items", 1);
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

  auto events = e2e::CaptureTableEvents(gtid, e2e::server_ids::kDmlUpdateAllColumns, "users", 1);
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
  e2e::ExecuteDML("INSERT INTO mes_test.items (name, value) VALUES ('del_single', 42)");

  auto gtid = e2e::GetCurrentGtid();
  ASSERT_FALSE(gtid.empty());

  auto rc = e2e::ExecuteDML("DELETE FROM mes_test.items WHERE name = 'del_single'");
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
  e2e::ExecuteDML("DELETE FROM mes_test.items WHERE name IN ('mru_a','mru_b','mru_c')");
}

// ---- MultiRowDelete ----

TEST(E2EBinlogDML, MultiRowDelete) {
  e2e::ExecuteDML(
      "INSERT INTO mes_test.items (name, value) VALUES "
      "('mrd_a', 1), ('mrd_b', 2), ('mrd_c', 3)");

  auto gtid = e2e::GetCurrentGtid();
  ASSERT_FALSE(gtid.empty());

  auto rc = e2e::ExecuteDML("DELETE FROM mes_test.items WHERE name IN ('mrd_a', 'mrd_b', 'mrd_c')");
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
  e2e::ExecuteDML("INSERT INTO mes_test.items (name, value) VALUES ('txn_target', 50)");

  auto gtid = e2e::GetCurrentGtid();
  ASSERT_FALSE(gtid.empty());

  // Use a single connection for the entire transaction
  mes::protocol::MysqlConnection conn;
  ASSERT_EQ(conn.Connect(e2e::kHost, e2e::kPort, e2e::kRootUser, e2e::kRootPass, e2e::kTimeout,
                         e2e::kTimeout, 0, "", "", ""),
            MES_OK);

  mes::protocol::QueryResult qr;
  std::string err;

  ASSERT_EQ(mes::protocol::ExecuteQuery(conn.Socket(), "BEGIN", &qr, &err), MES_OK);
  ASSERT_EQ(mes::protocol::ExecuteQuery(
                conn.Socket(), "INSERT INTO mes_test.items (name, value) VALUES ('txn_insert', 60)",
                &qr, &err),
            MES_OK);
  ASSERT_EQ(mes::protocol::ExecuteQuery(
                conn.Socket(), "UPDATE mes_test.items SET value = 55 WHERE name = 'txn_target'",
                &qr, &err),
            MES_OK);
  ASSERT_EQ(mes::protocol::ExecuteQuery(
                conn.Socket(), "DELETE FROM mes_test.items WHERE name = 'txn_insert'", &qr, &err),
            MES_OK);
  ASSERT_EQ(mes::protocol::ExecuteQuery(conn.Socket(), "COMMIT", &qr, &err), MES_OK);
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
  EXPECT_TRUE(ev.after[1].type == MES_COL_STRING || ev.after[1].type == MES_COL_BYTES);
  EXPECT_GE(ev.after[1].str_data.size(), 100000u);

  // Cleanup
  e2e::ExecuteDML("DELETE FROM mes_test.large_data WHERE id = 1");
}

// ---- LargeBlobValue ----

TEST(E2EBinlogDML, LargeBlobValue) {
  e2e::ScopedCleanup cleanup("DELETE FROM mes_test.large_data WHERE id = 2");
  constexpr size_t kPayloadBytes = 16 * 1024 * 1024 + 1;

  auto gtid = e2e::GetCurrentGtid();
  ASSERT_FALSE(gtid.empty());

  auto rc = e2e::ExecuteDML(
      "INSERT INTO mes_test.large_data (id, big_blob) VALUES "
      "(2, UNHEX(REPEAT('41', " +
      std::to_string(kPayloadBytes) + ")))");
  ASSERT_EQ(rc, MES_OK);

  auto events = e2e::CaptureTableEvents(gtid, 510, "large_data", 1);
  auto filtered = e2e::FilterByTable(events, "large_data");
  ASSERT_GE(filtered.size(), 1u);

  const auto& ev = filtered[0];
  EXPECT_EQ(ev.type, MES_EVENT_INSERT);

  // Index 2: big_blob
  ASSERT_GT(ev.after.size(), 2u);
  EXPECT_EQ(ev.after[2].type, MES_COL_BYTES);
  ASSERT_EQ(ev.after[2].str_data.size(), kPayloadBytes);
  EXPECT_EQ(ev.after[2].str_data.front(), 'A');
  EXPECT_EQ(ev.after[2].str_data.back(), 'A');
}

TEST(E2EBinlogDML, ClientEventSizeLimitRejectsLargeBlobPacket) {
  e2e::ExecuteDML("DELETE FROM mes_test.large_data WHERE id = 3");
  e2e::ScopedCleanup cleanup("DELETE FROM mes_test.large_data WHERE id = 3");

  const std::string gtid = e2e::GetCurrentGtid();
  ASSERT_FALSE(gtid.empty());
  ASSERT_EQ(e2e::ExecuteDML("INSERT INTO mes_test.large_data (id, big_blob) VALUES "
                            "(3, UNHEX(REPEAT('42', 100000)))"),
            MES_OK);

  mes_client_t* client = mes_client_create();
  ASSERT_NE(client, nullptr);
  ASSERT_EQ(mes_client_set_max_event_size(client, 4096), MES_OK);

  mes_client_config_t config{};
  config.host = e2e::kHost;
  config.port = e2e::kPort;
  config.user = e2e::kReplUser;
  config.password = e2e::kReplPass;
  config.server_id = e2e::server_ids::kDmlClientEventSizeLimit;
  config.start_gtid = gtid.c_str();
  config.connect_timeout_s = e2e::kTimeout;
  config.read_timeout_s = 3;
  config.ssl_mode = static_cast<mes_ssl_mode_t>(e2e::DefaultSslMode());
  const std::string ca_path = e2e::DefaultCa();
  config.ssl_ca = ca_path.empty() ? nullptr : ca_path.c_str();

  ASSERT_EQ(mes_client_connect(client, &config), MES_OK);
  ASSERT_EQ(mes_client_start(client), MES_OK);

  for (int i = 0; i < 100 && mes_client_is_connected(client) != 0; ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  EXPECT_EQ(mes_client_is_connected(client), 0);
  EXPECT_EQ(mes_client_is_streaming(client), 1);

  mes_error_t terminal_error = MES_OK;
  for (int i = 0; i < 32 && terminal_error == MES_OK; ++i) {
    terminal_error = mes_client_poll(client).error;
  }
  EXPECT_EQ(terminal_error, MES_ERR_STREAM);
  EXPECT_EQ(mes_client_is_connected(client), 0);
  EXPECT_EQ(mes_client_is_streaming(client), 0);

  mes_client_stop(client);
  EXPECT_EQ(mes_client_is_connected(client), 0);
  EXPECT_EQ(mes_client_is_streaming(client), 0);
  mes_client_disconnect(client);
  mes_client_destroy(client);
}

TEST(E2EBinlogDML, ClientQueueByteBudgetBackpressuresLargeBlobBurst) {
  constexpr size_t kEventLimit = 200u * 1024u;
  // The tightest budget the client accepts: it must admit one event at the
  // configured ceiling together with the reserve for that event's checkpoint.
  // Taking it from MinQueueBytesForEvent() rather than writing a number keeps
  // this configuration legal by construction, and the tightest legal budget is
  // also the one that reaches backpressure soonest, which is what this drives.
  constexpr size_t kQueueBudget = mes::MinQueueBytesForEvent(kEventLimit);
  // Enough rows that the burst outweighs the budget several times over, so the
  // queue stops admitting events rather than merely holding all of them.
  constexpr int kFirstId = 30;
  constexpr int kLastId = 49;
  const std::string id_range = "DELETE FROM mes_test.large_data WHERE id BETWEEN " +
                               std::to_string(kFirstId) + " AND " + std::to_string(kLastId);
  e2e::ExecuteDML(id_range);
  e2e::ScopedCleanup cleanup(id_range);

  const std::string gtid = e2e::GetCurrentGtid();
  ASSERT_FALSE(gtid.empty());
  for (int id = kFirstId; id <= kLastId; ++id) {
    ASSERT_EQ(e2e::ExecuteDML("INSERT INTO mes_test.large_data (id, big_blob) VALUES (" +
                              std::to_string(id) + ", UNHEX(REPEAT('43', 100000)))"),
              MES_OK);
  }

  mes_client_t* client = mes_client_create();
  ASSERT_NE(client, nullptr);
  ASSERT_EQ(mes_client_set_max_event_size(client, kEventLimit), MES_OK);
  ASSERT_EQ(mes_client_set_max_queue_bytes(client, kQueueBudget), MES_OK);

  mes_client_config_t config{};
  config.host = e2e::kHost;
  config.port = e2e::kPort;
  config.user = e2e::kReplUser;
  config.password = e2e::kReplPass;
  config.server_id = e2e::server_ids::kDmlClientQueueByteBudget;
  config.start_gtid = gtid.c_str();
  config.connect_timeout_s = e2e::kTimeout;
  config.read_timeout_s = 3;
  config.ssl_mode = static_cast<mes_ssl_mode_t>(e2e::DefaultSslMode());
  const std::string ca_path = e2e::DefaultCa();
  config.ssl_ca = ca_path.empty() ? nullptr : ca_path.c_str();

  ASSERT_EQ(mes_client_connect(client, &config), MES_OK);
  ASSERT_EQ(mes_client_start(client), MES_OK);

  // Nothing polls, so the reader fills the queue until the budget refuses the
  // next event. Saturation is reached once an event at the ceiling would no
  // longer fit, which no single event can produce -- it takes the queue holding
  // several at once, which is the state backpressure has to bound.
  constexpr size_t kSaturated = kQueueBudget - kEventLimit;

  // Sample until the charge stops moving rather than until it first crosses the
  // threshold: the burst outweighs the budget, so an early sample can read a
  // legal value on its way past one. The peak is what the budget promises to
  // bound, so that is what is asserted -- a transient overshoot is a failure
  // even if the charge settles back under the budget afterwards.
  size_t peak_bytes = 0;
  size_t settled_bytes = 0;
  int stable_samples = 0;
  for (int i = 0; i < 400 && stable_samples < 20; ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
    const size_t sample = mes_client_queued_bytes(client);
    peak_bytes = std::max(peak_bytes, sample);
    stable_samples = sample == settled_bytes ? stable_samples + 1 : 0;
    settled_bytes = sample;
  }
  EXPECT_GT(settled_bytes, kSaturated) << "the queue never filled enough to apply backpressure";
  EXPECT_LE(peak_bytes, kQueueBudget) << "queued bytes exceeded the configured budget";

  mes_client_stop(client);
  mes_client_disconnect(client);
  mes_client_destroy(client);
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
  std::string expected = u8"\u65E5\u672C\u8A9E\u30C6\u30B9\u30C8\xF0\x9F\x8E\x89";
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

  auto rc = e2e::ExecuteDML("UPDATE mes_test.users SET email = 'new@test.com' WHERE id = 1014");
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

  auto rc = e2e::ExecuteDML("UPDATE mes_test.users SET email = NULL WHERE id = 1015");
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

  auto rc = e2e::ExecuteDML("INSERT INTO mes_test.items (name, value) VALUES ('', 0)");
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

TEST(E2EBinlogDML, YearConsumesSignednessBitBeforeBigInts) {
  e2e::ExecuteDML("DELETE FROM mes_test.signedness_values WHERE y = 2026");
  e2e::ScopedCleanup cleanup("DELETE FROM mes_test.signedness_values WHERE y = 2026");

  const auto gtid = e2e::GetCurrentGtid();
  ASSERT_FALSE(gtid.empty());
  ASSERT_EQ(e2e::ExecuteDML("INSERT INTO mes_test.signedness_values "
                            "(y, signed_value, unsigned_value) VALUES "
                            "(2026, -1, 18446744073709551615)"),
            MES_OK);

  const auto events =
      e2e::CaptureTableEvents(gtid, e2e::server_ids::kDmlYearSignedness, "signedness_values", 1);
  const auto filtered = e2e::FilterByTable(events, "signedness_values");
  ASSERT_EQ(filtered.size(), 1u);
  ASSERT_EQ(filtered[0].after.size(), 3u);

  EXPECT_EQ(filtered[0].after[0].type, MES_COL_INT);
  EXPECT_EQ(filtered[0].after[0].int_val, 2026);
  EXPECT_EQ(filtered[0].after[1].type, MES_COL_INT);
  EXPECT_EQ(filtered[0].after[1].int_val, -1);
  // Values beyond INT64_MAX are intentionally exposed as their exact decimal
  // string rather than an overflowing signed integer.
  EXPECT_EQ(filtered[0].after[2].type, MES_COL_STRING);
  EXPECT_EQ(filtered[0].after[2].str_data, "18446744073709551615");
}

TEST(E2EBinlogDML, CharsetMetadataPreservesBinaryAndTextTypes) {
  e2e::ExecuteDML("DELETE FROM mes_test.charset_values WHERE id = 1");
  e2e::ScopedCleanup cleanup("DELETE FROM mes_test.charset_values WHERE id = 1");

  const auto gtid = e2e::GetCurrentGtid();
  ASSERT_FALSE(gtid.empty());
  ASSERT_EQ(
      e2e::ExecuteDML("INSERT INTO mes_test.charset_values (id, binary_value, text_value) VALUES "
                      "(1, UNHEX('000102030405060708090A0B0C0D0E0F'), 'charset metadata text')"),
      MES_OK);

  const auto events =
      e2e::CaptureTableEvents(gtid, e2e::server_ids::kDmlCharsetMetadata, "charset_values", 1);
  const auto filtered = e2e::FilterByTable(events, "charset_values");
  ASSERT_EQ(filtered.size(), 1u);
  ASSERT_EQ(filtered[0].after.size(), 3u);
  EXPECT_FALSE(filtered[0].names_resolved);
  EXPECT_EQ(filtered[0].after[0].type, MES_COL_INT);
  EXPECT_EQ(filtered[0].after[1].type, MES_COL_BYTES);
  EXPECT_EQ(filtered[0].after[1].str_data, std::string("\x00\x01\x02\x03\x04\x05\x06\x07"
                                                       "\x08\x09\x0A\x0B\x0C\x0D\x0E\x0F",
                                                       16));
  EXPECT_EQ(filtered[0].after[2].type, MES_COL_STRING);
  EXPECT_EQ(filtered[0].after[2].str_data, "charset metadata text");
}

TEST(E2EBinlogDML, MariaCompressedColumns) {
  if (!e2e::IsMariaDB()) {
    GTEST_SKIP() << "MariaDB COMPRESSED columns are flavor-specific";
  }

  ASSERT_EQ(e2e::ExecuteDML("DROP TABLE IF EXISTS mes_test.compressed_values"), MES_OK);
  e2e::ScopedCleanup cleanup("DROP TABLE IF EXISTS mes_test.compressed_values");
  ASSERT_EQ(e2e::ExecuteDML("CREATE TABLE mes_test.compressed_values ("
                            "id INT PRIMARY KEY, short_value VARCHAR(100) COMPRESSED, "
                            "long_value VARCHAR(10000) COMPRESSED, blob_value BLOB COMPRESSED)"),
            MES_OK);

  const auto gtid = e2e::GetCurrentGtid();
  ASSERT_FALSE(gtid.empty());
  ASSERT_EQ(e2e::ExecuteDML("INSERT INTO mes_test.compressed_values VALUES "
                            "(1, 'short', REPEAT('x', 4096), REPEAT('b', 4096))"),
            MES_OK);

  mes_error_t feed_error = MES_OK;
  const auto events = e2e::CaptureTableEvents(gtid, e2e::server_ids::kDmlMariaCompressedColumns,
                                              "compressed_values", 1, nullptr, &feed_error);
  ASSERT_EQ(feed_error, MES_OK);
  const auto filtered = e2e::FilterByTable(events, "compressed_values");
  ASSERT_EQ(filtered.size(), 1u);
  ASSERT_EQ(filtered[0].after.size(), 4u);
  EXPECT_EQ(filtered[0].after[0].int_val, 1);
  EXPECT_EQ(filtered[0].after[1].type, MES_COL_STRING);
  EXPECT_EQ(filtered[0].after[1].str_data, "short");
  EXPECT_EQ(filtered[0].after[2].type, MES_COL_STRING);
  EXPECT_EQ(filtered[0].after[2].str_data, std::string(4096, 'x'));
  EXPECT_EQ(filtered[0].after[3].type, MES_COL_BYTES);
  EXPECT_EQ(filtered[0].after[3].str_data, std::string(4096, 'b'));
}

TEST(E2EBinlogDML, MysqlMultiValuedIndex) {
  if (e2e::IsMariaDB()) {
    GTEST_SKIP() << "MySQL multi-valued indexes are flavor-specific";
  }

  ASSERT_EQ(e2e::ExecuteDML("DROP TABLE IF EXISTS mes_test.mvi_values"), MES_OK);
  e2e::ScopedCleanup cleanup("DROP TABLE IF EXISTS mes_test.mvi_values");
  ASSERT_EQ(e2e::ExecuteDML("CREATE TABLE mes_test.mvi_values ("
                            "id BIGINT PRIMARY KEY, doc JSON NOT NULL, "
                            "INDEX tags_idx ((CAST(doc->'$.tags' AS UNSIGNED ARRAY))))"),
            MES_OK);

  const auto gtid = e2e::GetCurrentGtid();
  ASSERT_FALSE(gtid.empty());
  ASSERT_EQ(e2e::ExecuteDML("INSERT INTO mes_test.mvi_values VALUES "
                            "(1, JSON_OBJECT('tags', JSON_ARRAY(1, 2, 3)))"),
            MES_OK);

  const auto events =
      e2e::CaptureTableEvents(gtid, e2e::server_ids::kDmlMysqlMultiValuedIndex, "mvi_values", 1);
  const auto filtered = e2e::FilterByTable(events, "mvi_values");
  ASSERT_EQ(filtered.size(), 1u);
  ASSERT_GE(filtered[0].after.size(), 2u);
  EXPECT_EQ(filtered[0].after[0].int_val, 1);
  EXPECT_EQ(filtered[0].after[1].type, MES_COL_BYTES);
  EXPECT_FALSE(filtered[0].after[1].str_data.empty());
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

// A VECTOR column is a binary-charset character column as far as TABLE_MAP is
// concerned, so it occupies a DEFAULT_CHARSET slot ahead of the text column.
// The other VECTOR tests use tables without string columns and cannot observe
// that: a decoder that skips VECTOR shifts the utf8mb4 exception onto the wrong
// column and surfaces `label` as raw bytes.
TEST(E2EBinlogDML, VectorOccupiesACharsetSlotAheadOfStringColumns) {
  if (!e2e::IsMysql9OrLater()) {
    GTEST_SKIP() << "VECTOR type requires MySQL 9.0+";
  }

  ASSERT_EQ(e2e::ExecuteDML("DROP TABLE IF EXISTS mes_test.vec_charset_values"), MES_OK);
  e2e::ScopedCleanup cleanup("DROP TABLE IF EXISTS mes_test.vec_charset_values");
  // Two binary-charset character columns (embedding, payload) against one
  // utf8mb4 column make the server pack DEFAULT_CHARSET as "binary, with one
  // exception at character-column index 1".
  ASSERT_EQ(e2e::ExecuteDML("CREATE TABLE mes_test.vec_charset_values ("
                            "id INT NOT NULL PRIMARY KEY, "
                            "embedding VECTOR(3) NOT NULL, "
                            "label VARCHAR(64) CHARACTER SET utf8mb4 NOT NULL, "
                            "payload BLOB NOT NULL) ENGINE=InnoDB"),
            MES_OK);

  const auto gtid = e2e::GetCurrentGtid();
  ASSERT_FALSE(gtid.empty());
  ASSERT_EQ(e2e::ExecuteDML("INSERT INTO mes_test.vec_charset_values VALUES "
                            "(1, TO_VECTOR('[1.0, 2.0, 3.0]'), 'vector label', 'payload bytes')"),
            MES_OK);

  mes_error_t feed_error = MES_OK;
  const auto events = e2e::CaptureTableEvents(gtid, e2e::server_ids::kDmlVectorCharsetSlots,
                                              "vec_charset_values", 1, nullptr, &feed_error);
  ASSERT_EQ(feed_error, MES_OK);
  const auto filtered = e2e::FilterByTable(events, "vec_charset_values");
  ASSERT_EQ(filtered.size(), 1u);
  ASSERT_EQ(filtered[0].after.size(), 4u);

  EXPECT_EQ(filtered[0].after[0].type, MES_COL_INT);
  EXPECT_EQ(filtered[0].after[0].int_val, 1);
  // 3 float32 values, stored exactly like a BLOB payload.
  EXPECT_EQ(filtered[0].after[1].type, MES_COL_BYTES);
  EXPECT_EQ(filtered[0].after[1].str_data.size(), 12u);
  EXPECT_EQ(filtered[0].after[2].type, MES_COL_STRING);
  EXPECT_EQ(filtered[0].after[2].str_data, "vector label");
  EXPECT_EQ(filtered[0].after[3].type, MES_COL_BYTES);
  EXPECT_EQ(filtered[0].after[3].str_data, "payload bytes");
}

// binlog_row_metadata=FULL is the documented route to schema-derived column
// names without a metadata connection. The E2E containers run MINIMAL so that
// the MINIMAL path stays covered, so this test raises the server setting for
// its own DML and puts it back afterwards.
TEST(E2EBinlogDML, FullRowMetadataResolvesColumnNamesWithoutAMetadataConnection) {
  const std::string previous = e2e::QueryScalar("SELECT @@GLOBAL.binlog_row_metadata");
  if (previous.empty()) {
    GTEST_SKIP() << "server does not expose binlog_row_metadata";
  }
  e2e::ScopedCleanup restore_metadata("SET GLOBAL binlog_row_metadata = '" + previous + "'");
  ASSERT_EQ(e2e::ExecuteDML("SET GLOBAL binlog_row_metadata = 'FULL'"), MES_OK);

  ASSERT_EQ(e2e::ExecuteDML("DROP TABLE IF EXISTS mes_test.full_metadata_values"), MES_OK);
  e2e::ScopedCleanup drop_table("DROP TABLE IF EXISTS mes_test.full_metadata_values");
  // Two binary-charset columns against one utf8mb4 column force DEFAULT_CHARSET
  // to be packed as "binary, with one exception". That exception's index counts
  // character columns only, and ENUM and SET sit among them precisely so a
  // parser that lets them consume a slot mislabels text_value as binary.
  ASSERT_EQ(e2e::ExecuteDML("CREATE TABLE mes_test.full_metadata_values ("
                            "id INT NOT NULL PRIMARY KEY, "
                            "enum_value ENUM('first','second') NOT NULL, "
                            "text_value VARCHAR(64) CHARACTER SET utf8mb4 NOT NULL, "
                            "set_value SET('a','b','c') NOT NULL, "
                            "binary_value VARBINARY(16) NOT NULL, "
                            "blob_value BLOB NOT NULL) ENGINE=InnoDB"),
            MES_OK);

  const auto gtid = e2e::GetCurrentGtid();
  ASSERT_FALSE(gtid.empty());
  ASSERT_EQ(e2e::ExecuteDML("INSERT INTO mes_test.full_metadata_values VALUES "
                            "(1, 'second', 'full metadata text', 'a,c', "
                            "UNHEX('00112233445566778899AABBCCDDEEFF'), 'blob bytes')"),
            MES_OK);

  mes_error_t feed_error = MES_OK;
  const auto events = e2e::CaptureTableEvents(gtid, e2e::server_ids::kDmlFullRowMetadata,
                                              "full_metadata_values", 1, nullptr, &feed_error);
  ASSERT_EQ(feed_error, MES_OK);
  const auto filtered = e2e::FilterByTable(events, "full_metadata_values");
  ASSERT_EQ(filtered.size(), 1u);
  ASSERT_EQ(filtered[0].after.size(), 6u);

  // CaptureTableEvents configures no metadata connection, so resolved names can
  // only have come from the TABLE_MAP COLUMN_NAME field that FULL adds.
  EXPECT_TRUE(filtered[0].names_resolved);
  EXPECT_EQ(filtered[0].after[0].col_name, "id");
  EXPECT_EQ(filtered[0].after[1].col_name, "enum_value");
  EXPECT_EQ(filtered[0].after[2].col_name, "text_value");
  EXPECT_EQ(filtered[0].after[3].col_name, "set_value");
  EXPECT_EQ(filtered[0].after[4].col_name, "binary_value");
  EXPECT_EQ(filtered[0].after[5].col_name, "blob_value");

  EXPECT_EQ(filtered[0].after[0].type, MES_COL_INT);
  EXPECT_EQ(filtered[0].after[0].int_val, 1);
  // ENUM is exposed as its 1-based ordinal, SET as its member bitmask.
  EXPECT_EQ(filtered[0].after[1].type, MES_COL_INT);
  EXPECT_EQ(filtered[0].after[1].int_val, 2);
  EXPECT_EQ(filtered[0].after[3].type, MES_COL_INT);
  EXPECT_EQ(filtered[0].after[3].int_val, 5);  // 'a,c'

  EXPECT_EQ(filtered[0].after[2].type, MES_COL_STRING);
  EXPECT_EQ(filtered[0].after[2].str_data, "full metadata text");
  EXPECT_EQ(filtered[0].after[4].type, MES_COL_BYTES);
  EXPECT_EQ(filtered[0].after[4].str_data, std::string("\x00\x11\x22\x33\x44\x55\x66\x77"
                                                       "\x88\x99\xAA\xBB\xCC\xDD\xEE\xFF",
                                                       16));
  EXPECT_EQ(filtered[0].after[5].type, MES_COL_BYTES);
  EXPECT_EQ(filtered[0].after[5].str_data, "blob bytes");
}

// MariaDB withholds ANNOTATE_ROWS unless the dump request asks for it, so the
// documented source_sql field is only populated when that flag reaches the
// wire. Cover all three DML statement kinds: each row event must carry back the
// statement that produced it.
TEST(E2EBinlogDML, MariaAnnotateRowsRestoresSourceSql) {
  if (!e2e::IsMariaDB()) {
    GTEST_SKIP() << "ANNOTATE_ROWS is a MariaDB-only event";
  }

  e2e::ScopedCleanup cleanup("DELETE FROM mes_test.items WHERE name = 'annotate_sql'");

  auto gtid = e2e::GetCurrentGtid();
  ASSERT_FALSE(gtid.empty());

  const std::string insert_sql =
      "INSERT INTO mes_test.items (name, value) VALUES ('annotate_sql', 1)";
  const std::string update_sql = "UPDATE mes_test.items SET value = 2 WHERE name = 'annotate_sql'";
  const std::string delete_sql = "DELETE FROM mes_test.items WHERE name = 'annotate_sql'";
  ASSERT_EQ(e2e::ExecuteDML(insert_sql), MES_OK);
  ASSERT_EQ(e2e::ExecuteDML(update_sql), MES_OK);
  ASSERT_EQ(e2e::ExecuteDML(delete_sql), MES_OK);

  auto events =
      e2e::CaptureTableEvents(gtid, e2e::server_ids::kDmlMariaAnnotateSourceSql, "items", 3);
  auto filtered = e2e::FilterByTable(events, "items");
  ASSERT_GE(filtered.size(), 3u);

  EXPECT_EQ(filtered[0].type, MES_EVENT_INSERT);
  EXPECT_EQ(filtered[0].source_sql, insert_sql);
  EXPECT_EQ(filtered[1].type, MES_EVENT_UPDATE);
  EXPECT_EQ(filtered[1].source_sql, update_sql);
  EXPECT_EQ(filtered[2].type, MES_EVENT_DELETE);
  EXPECT_EQ(filtered[2].source_sql, delete_sql);
}

}  // namespace
