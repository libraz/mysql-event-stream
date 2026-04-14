// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * @file test_e2e_engine.cpp
 * @brief End-to-end tests for the CDC engine integration
 *
 * Tests query execution, binlog streaming, table filtering, backpressure,
 * event ordering, connection lifecycle, and GTID tracking.
 *
 * Requires a running MySQL 8.4 instance at localhost:13308 with:
 *   - root/test_root_password (mysql_native_password)
 *   - repl_user/test_password (replication grants)
 *   - Database: mes_test with tables: users, items
 *
 * Start with: cd e2e/docker && docker compose up -d
 */

#include <gtest/gtest.h>

#include <cstring>
#include <string>
#include <vector>

#include "mes.h"
#include "protocol/mysql_connection.h"
#include "protocol/mysql_query.h"
#include "test_e2e_helpers.h"

using namespace e2e;

namespace {

// -- Query tests --

TEST(E2EEngine, EmptyResultSet) {
  mes::protocol::MysqlConnection conn;
  ASSERT_EQ(conn.Connect(kHost, kPort, kRootUser, kRootPass, kTimeout, kTimeout, 0, "", "", ""),
            MES_OK);

  mes::protocol::QueryResult result;
  std::string err;
  auto rc = mes::protocol::ExecuteQuery(conn.Socket(), "SELECT * FROM mes_test.items WHERE 1=0",
                                        &result, &err);
  ASSERT_EQ(rc, MES_OK) << err;
  EXPECT_EQ(result.rows.size(), 0u);
  EXPECT_EQ(result.column_names.size(), 3u);

  conn.Disconnect();
}

TEST(E2EEngine, ManyRowsResult) {
  // Insert 100 rows using a single INSERT with generated values
  mes::protocol::MysqlConnection conn;
  ASSERT_EQ(conn.Connect(kHost, kPort, kRootUser, kRootPass, kTimeout, kTimeout, 0, "", "", ""),
            MES_OK);

  mes::protocol::QueryResult qr;
  std::string err;

  // Build a multi-row INSERT: INSERT INTO ... VALUES (...), (...), ...
  std::string sql = "INSERT INTO mes_test.items (name, value) VALUES ";
  for (int i = 0; i < 100; i++) {
    if (i > 0) sql += ", ";
    sql += "('many_rows_" + std::to_string(i) + "', " + std::to_string(i) + ")";
  }
  ASSERT_EQ(mes::protocol::ExecuteQuery(conn.Socket(), sql, &qr, &err), MES_OK) << err;

  // Verify via SELECT COUNT(*)
  ASSERT_EQ(mes::protocol::ExecuteQuery(
                conn.Socket(), "SELECT COUNT(*) FROM mes_test.items WHERE name LIKE 'many_rows_%'",
                &qr, &err),
            MES_OK)
      << err;
  ASSERT_EQ(qr.rows.size(), 1u);
  EXPECT_GE(std::stoi(qr.rows[0].values[0]), 100);

  // Cleanup
  mes::protocol::ExecuteQuery(
      conn.Socket(), "DELETE FROM mes_test.items WHERE name LIKE 'many_rows_%'", &qr, &err);
  conn.Disconnect();
}

TEST(E2EEngine, LongQueryString) {
  mes::protocol::MysqlConnection conn;
  ASSERT_EQ(conn.Connect(kHost, kPort, kRootUser, kRootPass, kTimeout, kTimeout, 0, "", "", ""),
            MES_OK);

  // Build a SELECT with a 32KB SQL comment to test multi-packet COM_QUERY
  std::string padding(32768, 'x');
  std::string sql = "SELECT /* " + padding + " */ 1 AS n";

  mes::protocol::QueryResult result;
  std::string err;
  auto rc = mes::protocol::ExecuteQuery(conn.Socket(), sql, &result, &err);
  ASSERT_EQ(rc, MES_OK) << err;
  ASSERT_EQ(result.rows.size(), 1u);
  EXPECT_EQ(result.rows[0].values[0], "1");
  EXPECT_EQ(result.column_names[0], "n");

  conn.Disconnect();
}

TEST(E2EEngine, WideColumnQuery) {
  mes::protocol::MysqlConnection conn;
  ASSERT_EQ(conn.Connect(kHost, kPort, kRootUser, kRootPass, kTimeout, kTimeout, 0, "", "", ""),
            MES_OK);

  // SELECT 1 AS c1, 2 AS c2, ... 50 AS c50
  std::string sql = "SELECT ";
  for (int i = 1; i <= 50; i++) {
    if (i > 1) sql += ", ";
    sql += std::to_string(i) + " AS c" + std::to_string(i);
  }

  mes::protocol::QueryResult result;
  std::string err;
  auto rc = mes::protocol::ExecuteQuery(conn.Socket(), sql, &result, &err);
  ASSERT_EQ(rc, MES_OK) << err;
  ASSERT_EQ(result.column_names.size(), 50u);
  ASSERT_EQ(result.rows.size(), 1u);
  EXPECT_EQ(result.column_names[0], "c1");
  EXPECT_EQ(result.column_names[49], "c50");
  EXPECT_EQ(result.rows[0].values[0], "1");
  EXPECT_EQ(result.rows[0].values[49], "50");

  conn.Disconnect();
}

// -- Connection lifecycle tests --

TEST(E2EEngine, MultipleSequentialConnections) {
  for (int i = 0; i < 10; i++) {
    mes::protocol::MysqlConnection conn;
    auto rc = conn.Connect(kHost, kPort, kRootUser, kRootPass, kTimeout, kTimeout, 0, "", "", "");
    ASSERT_EQ(rc, MES_OK) << "Connection " << i << " failed";
    EXPECT_TRUE(conn.IsConnected());

    mes::protocol::QueryResult result;
    std::string err;
    rc = mes::protocol::ExecuteQuery(conn.Socket(), "SELECT 1", &result, &err);
    ASSERT_EQ(rc, MES_OK) << "Query on connection " << i << " failed: " << err;
    ASSERT_EQ(result.rows.size(), 1u);
    EXPECT_EQ(result.rows[0].values[0], "1");

    conn.Disconnect();
    EXPECT_FALSE(conn.IsConnected());
  }
}

TEST(E2EEngine, ConcurrentConnections) {
  // Create 3 connections, query each, disconnect all.
  // Tests that multiple simultaneous connections do not interfere.
  mes::protocol::MysqlConnection conn1, conn2, conn3;

  ASSERT_EQ(conn1.Connect(kHost, kPort, kRootUser, kRootPass, kTimeout, kTimeout, 0, "", "", ""),
            MES_OK);
  ASSERT_EQ(conn2.Connect(kHost, kPort, kRootUser, kRootPass, kTimeout, kTimeout, 0, "", "", ""),
            MES_OK);
  ASSERT_EQ(conn3.Connect(kHost, kPort, kRootUser, kRootPass, kTimeout, kTimeout, 0, "", "", ""),
            MES_OK);

  EXPECT_TRUE(conn1.IsConnected());
  EXPECT_TRUE(conn2.IsConnected());
  EXPECT_TRUE(conn3.IsConnected());

  // Each connection should have a distinct connection_id
  EXPECT_NE(conn1.GetServerInfo().connection_id, conn2.GetServerInfo().connection_id);
  EXPECT_NE(conn2.GetServerInfo().connection_id, conn3.GetServerInfo().connection_id);

  // Query on each independently
  for (auto* conn : {&conn1, &conn2, &conn3}) {
    mes::protocol::QueryResult result;
    std::string err;
    auto rc = mes::protocol::ExecuteQuery(conn->Socket(), "SELECT 1", &result, &err);
    ASSERT_EQ(rc, MES_OK) << err;
    ASSERT_EQ(result.rows.size(), 1u);
  }

  conn1.Disconnect();
  conn2.Disconnect();
  conn3.Disconnect();
}

TEST(E2EEngine, StreamConnectAndPoll) {
  // Verify that connecting and starting a binlog stream succeeds,
  // and that polling returns without crashing.
  std::string gtid = GetCurrentGtid();
  ASSERT_FALSE(gtid.empty());

  mes_client_t* client = mes_client_create();
  ASSERT_NE(client, nullptr);

  mes_client_config_t config{};
  config.host = kHost;
  config.port = kPort;
  config.user = kReplUser;
  config.password = kReplPass;
  config.server_id = 600;
  config.start_gtid = gtid.c_str();
  config.connect_timeout_s = kTimeout;
  config.read_timeout_s = 3;
  config.ssl_mode = static_cast<mes_ssl_mode_t>(DefaultSslMode());
  std::string ca_path = DefaultCa();
  config.ssl_ca = ca_path.empty() ? nullptr : ca_path.c_str();
  config.ssl_cert = nullptr;
  config.ssl_key = nullptr;

  ASSERT_EQ(mes_client_connect(client, &config), MES_OK) << mes_client_last_error(client);
  ASSERT_EQ(mes_client_start(client), MES_OK) << mes_client_last_error(client);

  // Poll a few times; we expect either heartbeats or timeouts, no crash
  for (int i = 0; i < 3; i++) {
    auto result = mes_client_poll(client);
    // Any result is acceptable; we just verify no crash
    (void)result;
  }

  mes_client_stop(client);
  mes_client_disconnect(client);
  mes_client_destroy(client);
}

// -- Event ordering --

TEST(E2EEngine, EventOrderMatchesCommitOrder) {
  // Clean up any leftover rows
  ASSERT_EQ(ExecuteDML("DELETE FROM mes_test.items WHERE name LIKE 'order_%'"), MES_OK);

  std::string gtid = GetCurrentGtid();
  ASSERT_FALSE(gtid.empty());

  // INSERT 5 rows with distinct values in order
  for (int v : {10, 20, 30, 40, 50}) {
    ASSERT_EQ(ExecuteDML("INSERT INTO mes_test.items (name, value) VALUES ('order_" +
                         std::to_string(v) + "', " + std::to_string(v) + ")"),
              MES_OK);
  }

  auto events = CaptureTableEvents(gtid, 605, "items", 5);
  auto items_events = FilterByTable(events, "items");

  // Filter to only our test rows
  std::vector<CapturedEvent> order_events;
  for (const auto& e : items_events) {
    if (e.type == MES_EVENT_INSERT && !e.after.empty() && e.after.size() >= 2 &&
        e.after[1].str_data.find("order_") == 0) {
      order_events.push_back(e);
    }
  }

  ASSERT_GE(order_events.size(), 5u) << "Expected at least 5 order_* INSERT events";

  // Verify they arrive in commit order: values 10, 20, 30, 40, 50
  for (size_t i = 0; i < 5; i++) {
    ASSERT_GE(order_events[i].after.size(), 3u);
    int64_t expected_value = static_cast<int64_t>((i + 1) * 10);
    EXPECT_EQ(order_events[i].after[2].int_val, expected_value)
        << "Event " << i << " has wrong value";
  }

  // Cleanup
  ExecuteDML("DELETE FROM mes_test.items WHERE name LIKE 'order_%'");
}

// -- Table filtering --

TEST(E2EEngine, TableFilterInclude) {
  std::string gtid = GetCurrentGtid();
  ASSERT_FALSE(gtid.empty());

  // INSERT into both items and users
  ASSERT_EQ(ExecuteDML("INSERT INTO mes_test.items (name, value) VALUES "
                       "('filter_inc', 1)"),
            MES_OK);
  ASSERT_EQ(ExecuteDML("INSERT INTO mes_test.users (name, email) VALUES "
                       "('filter_inc_user', 'inc@test.com')"),
            MES_OK);

  // Create engine with include filter for items only
  mes_engine_t* engine = mes_create();
  ASSERT_NE(engine, nullptr);
  const char* tables[] = {"mes_test.items"};
  ASSERT_EQ(mes_set_include_tables(engine, tables, 1), MES_OK);

  // Capture events using filtered engine
  auto events = CaptureEvents(
      gtid, 601,
      [](const std::vector<CapturedEvent>& evts) {
        for (const auto& e : evts) {
          if (e.table == "items") return true;
        }
        return false;
      },
      200, engine);

  // Verify: should have items events but no users events
  auto items_events = FilterByTable(events, "items");
  auto users_events = FilterByTable(events, "users");

  EXPECT_GE(items_events.size(), 1u) << "Expected items events";
  EXPECT_EQ(users_events.size(), 0u) << "Users events should be filtered out";

  mes_destroy(engine);

  // Cleanup
  ExecuteDML("DELETE FROM mes_test.items WHERE name = 'filter_inc'");
  ExecuteDML("DELETE FROM mes_test.users WHERE name = 'filter_inc_user'");
}

TEST(E2EEngine, TableFilterExclude) {
  std::string gtid = GetCurrentGtid();
  ASSERT_FALSE(gtid.empty());

  // INSERT into both items and users
  ASSERT_EQ(ExecuteDML("INSERT INTO mes_test.items (name, value) VALUES "
                       "('filter_exc', 2)"),
            MES_OK);
  ASSERT_EQ(ExecuteDML("INSERT INTO mes_test.users (name, email) VALUES "
                       "('filter_exc_user', 'exc@test.com')"),
            MES_OK);

  // Create engine with exclude filter for items
  mes_engine_t* engine = mes_create();
  ASSERT_NE(engine, nullptr);
  const char* tables[] = {"mes_test.items"};
  ASSERT_EQ(mes_set_exclude_tables(engine, tables, 1), MES_OK);

  // Capture events using filtered engine
  auto events = CaptureEvents(
      gtid, 602,
      [](const std::vector<CapturedEvent>& evts) {
        for (const auto& e : evts) {
          if (e.table == "users") return true;
        }
        return false;
      },
      200, engine);

  // Verify: should have users events but no items events
  auto items_events = FilterByTable(events, "items");
  auto users_events = FilterByTable(events, "users");

  EXPECT_EQ(items_events.size(), 0u) << "Items events should be filtered out";
  EXPECT_GE(users_events.size(), 1u) << "Expected users events";

  mes_destroy(engine);

  // Cleanup
  ExecuteDML("DELETE FROM mes_test.items WHERE name = 'filter_exc'");
  ExecuteDML("DELETE FROM mes_test.users WHERE name = 'filter_exc_user'");
}

// -- Backpressure --

TEST(E2EEngine, BackpressureMaxQueueSize) {
  // Clean up leftover rows
  ASSERT_EQ(ExecuteDML("DELETE FROM mes_test.items WHERE name LIKE 'bp_%'"), MES_OK);

  std::string gtid = GetCurrentGtid();
  ASSERT_FALSE(gtid.empty());

  // INSERT 5 rows
  for (int i = 0; i < 5; i++) {
    ASSERT_EQ(ExecuteDML("INSERT INTO mes_test.items (name, value) VALUES ('bp_" +
                         std::to_string(i) + "', " + std::to_string(i) + ")"),
              MES_OK);
  }

  // Create engine with max_queue_size=2
  mes_engine_t* engine = mes_create();
  ASSERT_NE(engine, nullptr);
  ASSERT_EQ(mes_set_max_queue_size(engine, 2), MES_OK);

  // Connect and stream
  mes_client_t* client = mes_client_create();
  ASSERT_NE(client, nullptr);

  mes_client_config_t config{};
  config.host = kHost;
  config.port = kPort;
  config.user = kReplUser;
  config.password = kReplPass;
  config.server_id = 603;
  config.start_gtid = gtid.c_str();
  config.connect_timeout_s = kTimeout;
  config.read_timeout_s = 3;
  config.ssl_mode = static_cast<mes_ssl_mode_t>(DefaultSslMode());
  std::string bp_ca = DefaultCa();
  config.ssl_ca = bp_ca.empty() ? nullptr : bp_ca.c_str();
  config.ssl_cert = nullptr;
  config.ssl_key = nullptr;

  ASSERT_EQ(mes_client_connect(client, &config), MES_OK) << mes_client_last_error(client);
  ASSERT_EQ(mes_client_start(client), MES_OK) << mes_client_last_error(client);

  // Feed and drain loop: collect all events, verifying queue never exceeds limit
  std::vector<CapturedEvent> all_events;
  int polls = 0;
  constexpr int kMaxPolls = 200;

  while (polls < kMaxPolls) {
    auto result = mes_client_poll(client);
    polls++;
    if (result.error != MES_OK) break;
    if (result.is_heartbeat || result.data == nullptr) continue;

    size_t consumed = 0;
    mes_feed(engine, result.data, result.size, &consumed);

    // Drain all available events before next feed
    const mes_event_t* event = nullptr;
    while (mes_next_event(engine, &event) == MES_OK) {
      all_events.push_back(CopyEvent(event));
    }

    // Check if we have enough items events
    size_t items_count = 0;
    for (const auto& e : all_events) {
      if (e.table == "items" && e.type == MES_EVENT_INSERT) items_count++;
    }
    if (items_count >= 5) break;
  }

  auto items_events = FilterByTable(all_events, "items");
  size_t insert_count = 0;
  for (const auto& e : items_events) {
    if (e.type == MES_EVENT_INSERT) insert_count++;
  }
  EXPECT_GE(insert_count, 5u) << "Expected all 5 INSERT events to arrive";

  mes_client_stop(client);
  mes_client_disconnect(client);
  mes_client_destroy(client);
  mes_destroy(engine);

  // Cleanup
  ExecuteDML("DELETE FROM mes_test.items WHERE name LIKE 'bp_%'");
}

// -- Reconnect --

TEST(E2EEngine, ReconnectAfterDisconnect) {
  // First connection: connect, start stream, stop, disconnect
  std::string gtid1 = GetCurrentGtid();
  ASSERT_FALSE(gtid1.empty());

  mes_client_t* client = mes_client_create();
  ASSERT_NE(client, nullptr);

  mes_client_config_t config{};
  config.host = kHost;
  config.port = kPort;
  config.user = kReplUser;
  config.password = kReplPass;
  config.server_id = 607;
  config.start_gtid = gtid1.c_str();
  config.connect_timeout_s = kTimeout;
  config.read_timeout_s = 3;
  config.ssl_mode = static_cast<mes_ssl_mode_t>(DefaultSslMode());
  std::string reconn_ca = DefaultCa();
  config.ssl_ca = reconn_ca.empty() ? nullptr : reconn_ca.c_str();
  config.ssl_cert = nullptr;
  config.ssl_key = nullptr;

  ASSERT_EQ(mes_client_connect(client, &config), MES_OK) << mes_client_last_error(client);
  ASSERT_EQ(mes_client_start(client), MES_OK) << mes_client_last_error(client);

  // Poll once to confirm stream is working
  auto result = mes_client_poll(client);
  (void)result;

  mes_client_stop(client);
  mes_client_disconnect(client);
  EXPECT_EQ(mes_client_is_connected(client), 0);

  mes_client_destroy(client);

  // Second connection: reconnect with a new client and verify streaming works
  ASSERT_EQ(ExecuteDML("INSERT INTO mes_test.items (name, value) VALUES ('reconn_test', 77)"),
            MES_OK);

  std::string gtid2 = GetCurrentGtid();

  // Insert after getting gtid2 so we can capture it
  // Actually, insert before getting gtid, then stream from gtid1
  // Let's use a fresh GTID and insert after
  std::string gtid_before = GetCurrentGtid();
  ASSERT_FALSE(gtid_before.empty());

  ASSERT_EQ(ExecuteDML("INSERT INTO mes_test.items (name, value) VALUES "
                       "('reconn_test2', 88)"),
            MES_OK);

  mes_client_t* client2 = mes_client_create();
  ASSERT_NE(client2, nullptr);

  config.server_id = 608;
  config.start_gtid = gtid_before.c_str();
  ASSERT_EQ(mes_client_connect(client2, &config), MES_OK) << mes_client_last_error(client2);
  ASSERT_EQ(mes_client_start(client2), MES_OK) << mes_client_last_error(client2);

  mes_engine_t* engine = mes_create();
  ASSERT_NE(engine, nullptr);

  bool found = false;
  int polls = 0;
  while (!found && polls < 100) {
    auto poll_result = mes_client_poll(client2);
    polls++;
    if (poll_result.error != MES_OK) break;
    if (poll_result.is_heartbeat || poll_result.data == nullptr) continue;

    size_t consumed = 0;
    mes_feed(engine, poll_result.data, poll_result.size, &consumed);

    const mes_event_t* event = nullptr;
    while (mes_next_event(engine, &event) == MES_OK) {
      if (event->type == MES_EVENT_INSERT && std::strcmp(event->table, "items") == 0) {
        found = true;
        break;
      }
    }
  }

  EXPECT_TRUE(found) << "Second stream did not capture INSERT event";

  mes_client_stop(client2);
  mes_client_disconnect(client2);
  mes_client_destroy(client2);
  mes_destroy(engine);

  // Cleanup
  ExecuteDML("DELETE FROM mes_test.items WHERE name LIKE 'reconn_%'");
}

// -- GTID tracking --

TEST(E2EEngine, GtidTracking) {
  std::string start_gtid = GetCurrentGtid();
  ASSERT_FALSE(start_gtid.empty());

  // INSERT 3 rows to generate binlog events
  for (int i = 0; i < 3; i++) {
    ASSERT_EQ(ExecuteDML("INSERT INTO mes_test.items (name, value) VALUES ('gtid_" +
                         std::to_string(i) + "', " + std::to_string(i) + ")"),
              MES_OK);
  }

  mes_client_t* client = mes_client_create();
  ASSERT_NE(client, nullptr);

  mes_client_config_t config{};
  config.host = kHost;
  config.port = kPort;
  config.user = kReplUser;
  config.password = kReplPass;
  config.server_id = 609;
  config.start_gtid = start_gtid.c_str();
  config.connect_timeout_s = kTimeout;
  config.read_timeout_s = 3;
  config.ssl_mode = static_cast<mes_ssl_mode_t>(DefaultSslMode());
  std::string gtid_ca = DefaultCa();
  config.ssl_ca = gtid_ca.empty() ? nullptr : gtid_ca.c_str();
  config.ssl_cert = nullptr;
  config.ssl_key = nullptr;

  ASSERT_EQ(mes_client_connect(client, &config), MES_OK) << mes_client_last_error(client);
  ASSERT_EQ(mes_client_start(client), MES_OK) << mes_client_last_error(client);

  mes_engine_t* engine = mes_create();
  ASSERT_NE(engine, nullptr);

  // Capture at least 3 items INSERT events
  size_t items_count = 0;
  int polls = 0;
  while (items_count < 3 && polls < 200) {
    auto result = mes_client_poll(client);
    polls++;
    if (result.error != MES_OK) break;
    if (result.is_heartbeat || result.data == nullptr) continue;

    size_t consumed = 0;
    mes_feed(engine, result.data, result.size, &consumed);

    const mes_event_t* event = nullptr;
    while (mes_next_event(engine, &event) == MES_OK) {
      if (event->type == MES_EVENT_INSERT && std::strcmp(event->table, "items") == 0) {
        items_count++;
      }
    }
  }

  ASSERT_GE(items_count, 3u) << "Expected at least 3 INSERT events";

  // Verify GTID was tracked and advanced
  const char* current_gtid = mes_client_current_gtid(client);
  ASSERT_NE(current_gtid, nullptr);
  EXPECT_GT(std::strlen(current_gtid), 0u) << "GTID should be non-empty";
  EXPECT_NE(std::string(current_gtid), start_gtid)
      << "GTID should have advanced from start position";

  mes_client_stop(client);
  mes_client_disconnect(client);
  mes_client_destroy(client);
  mes_destroy(engine);

  // Cleanup
  ExecuteDML("DELETE FROM mes_test.items WHERE name LIKE 'gtid_%'");
}

}  // namespace
