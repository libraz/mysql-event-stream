// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * @file test_e2e_protocol.cpp
 * @brief End-to-end tests for the custom MySQL protocol implementation
 *
 * Requires a running MySQL 8.4 instance at localhost:13307 with:
 *   - root/test_root_password (mysql_native_password)
 *   - repl_user/test_password (replication grants)
 *   - Database: mes_test with tables: users, items
 *
 * Start with: cd e2e/docker && docker compose up -d
 */

#include <gtest/gtest.h>

#include <cstring>
#include <string>
#include <thread>
#include <vector>

#include "mes.h"
#include "protocol/mysql_binlog_stream.h"
#include "protocol/mysql_connection.h"
#include "protocol/mysql_query.h"
#include "test_e2e_helpers.h"

namespace {

constexpr const char* kHost = "127.0.0.1";
constexpr uint16_t kPort = 13308;
constexpr const char* kRootUser = "root";
constexpr const char* kRootPass = "test_root_password";
constexpr const char* kReplUser = "repl_user";
constexpr const char* kReplPass = "test_password";
constexpr uint32_t kTimeout = 5;

// -- MysqlConnection tests --

TEST(E2EProtocol, ConnectWithNativePassword) {
  mes::protocol::MysqlConnection conn;
  auto rc = conn.Connect(kHost, kPort, kRootUser, kRootPass, kTimeout, kTimeout,
                         0, "", "", "");
  ASSERT_EQ(rc, MES_OK) << conn.GetLastError();
  EXPECT_TRUE(conn.IsConnected());
  EXPECT_FALSE(conn.GetServerInfo().server_version.empty());
  EXPECT_GT(conn.GetServerInfo().connection_id, 0u);
  conn.Disconnect();
  EXPECT_FALSE(conn.IsConnected());
}

TEST(E2EProtocol, ConnectWithReplUser) {
  mes::protocol::MysqlConnection conn;
  auto rc = conn.Connect(kHost, kPort, kReplUser, kReplPass, kTimeout, kTimeout,
                         0, "", "", "");
  ASSERT_EQ(rc, MES_OK) << conn.GetLastError();
  EXPECT_TRUE(conn.IsConnected());
  conn.Disconnect();
}

TEST(E2EProtocol, ConnectBadPassword) {
  mes::protocol::MysqlConnection conn;
  auto rc = conn.Connect(kHost, kPort, kRootUser, "wrong_password", kTimeout,
                         kTimeout, 0, "", "", "");
  EXPECT_NE(rc, MES_OK);
  EXPECT_FALSE(conn.IsConnected());
  EXPECT_FALSE(conn.GetLastError().empty());
}

TEST(E2EProtocol, ConnectBadHost) {
  mes::protocol::MysqlConnection conn;
  auto rc = conn.Connect("192.0.2.1", kPort, kRootUser, kRootPass, 2, 2, 0, "",
                         "", "");
  EXPECT_NE(rc, MES_OK);
  EXPECT_FALSE(conn.IsConnected());
}

// -- ExecuteQuery tests --

TEST(E2EProtocol, SimpleSelect) {
  mes::protocol::MysqlConnection conn;
  ASSERT_EQ(conn.Connect(kHost, kPort, kRootUser, kRootPass, kTimeout, kTimeout,
                         0, "", "", ""),
            MES_OK);

  mes::protocol::QueryResult result;
  std::string err;
  auto rc = mes::protocol::ExecuteQuery(conn.Socket(), "SELECT 1 AS val, 'hello' AS msg",
                                        &result, &err);
  ASSERT_EQ(rc, MES_OK) << err;
  ASSERT_EQ(result.column_names.size(), 2u);
  EXPECT_EQ(result.column_names[0], "val");
  EXPECT_EQ(result.column_names[1], "msg");
  ASSERT_EQ(result.rows.size(), 1u);
  EXPECT_EQ(result.rows[0].values[0], "1");
  EXPECT_EQ(result.rows[0].values[1], "hello");

  conn.Disconnect();
}

TEST(E2EProtocol, ShowVariables) {
  if (e2e::IsMariaDB()) {
    GTEST_SKIP() << "gtid_mode variable is MySQL-specific";
  }
  mes::protocol::MysqlConnection conn;
  ASSERT_EQ(conn.Connect(kHost, kPort, kRootUser, kRootPass, kTimeout, kTimeout,
                         0, "", "", ""),
            MES_OK);

  mes::protocol::QueryResult result;
  std::string err;
  auto rc = mes::protocol::ExecuteQuery(
      conn.Socket(), "SHOW VARIABLES WHERE Variable_name = 'gtid_mode'",
      &result, &err);
  ASSERT_EQ(rc, MES_OK) << err;
  ASSERT_GE(result.rows.size(), 1u);
  // Column 0 = Variable_name, Column 1 = Value
  EXPECT_EQ(result.rows[0].values[1], "ON");

  conn.Disconnect();
}

TEST(E2EProtocol, SelectServerUuid) {
  if (e2e::IsMariaDB()) {
    GTEST_SKIP() << "@@server_uuid is MySQL-specific";
  }
  mes::protocol::MysqlConnection conn;
  ASSERT_EQ(conn.Connect(kHost, kPort, kRootUser, kRootPass, kTimeout, kTimeout,
                         0, "", "", ""),
            MES_OK);

  mes::protocol::QueryResult result;
  std::string err;
  auto rc = mes::protocol::ExecuteQuery(conn.Socket(), "SELECT @@server_uuid",
                                        &result, &err);
  ASSERT_EQ(rc, MES_OK) << err;
  ASSERT_EQ(result.rows.size(), 1u);
  // UUID format: 8-4-4-4-12 hex chars = 36 chars
  EXPECT_EQ(result.rows[0].values[0].size(), 36u);

  conn.Disconnect();
}

TEST(E2EProtocol, SetCommand) {
  if (e2e::IsMariaDB()) {
    GTEST_SKIP() << "@source_binlog_checksum is MySQL-specific";
  }
  mes::protocol::MysqlConnection conn;
  ASSERT_EQ(conn.Connect(kHost, kPort, kRootUser, kRootPass, kTimeout, kTimeout,
                         0, "", "", ""),
            MES_OK);

  mes::protocol::QueryResult result;
  std::string err;
  auto rc = mes::protocol::ExecuteQuery(
      conn.Socket(), "SET @source_binlog_checksum='NONE'", &result, &err);
  ASSERT_EQ(rc, MES_OK) << err;
  // SET returns no result set
  EXPECT_TRUE(result.rows.empty());

  conn.Disconnect();
}

TEST(E2EProtocol, ShowColumns) {
  mes::protocol::MysqlConnection conn;
  ASSERT_EQ(conn.Connect(kHost, kPort, kRootUser, kRootPass, kTimeout, kTimeout,
                         0, "", "", ""),
            MES_OK);

  mes::protocol::QueryResult result;
  std::string err;
  auto rc = mes::protocol::ExecuteQuery(
      conn.Socket(), "SHOW COLUMNS FROM `mes_test`.`items`", &result, &err);
  ASSERT_EQ(rc, MES_OK) << err;
  // items has 3 columns: id, name, value
  ASSERT_EQ(result.rows.size(), 3u);
  EXPECT_EQ(result.rows[0].values[0], "id");
  EXPECT_EQ(result.rows[1].values[0], "name");
  EXPECT_EQ(result.rows[2].values[0], "value");

  conn.Disconnect();
}

TEST(E2EProtocol, MultipleQueriesOnSameConnection) {
  mes::protocol::MysqlConnection conn;
  ASSERT_EQ(conn.Connect(kHost, kPort, kRootUser, kRootPass, kTimeout, kTimeout,
                         0, "", "", ""),
            MES_OK);

  for (int i = 0; i < 5; i++) {
    mes::protocol::QueryResult result;
    std::string err;
    auto rc = mes::protocol::ExecuteQuery(
        conn.Socket(), "SELECT " + std::to_string(i) + " AS n", &result, &err);
    ASSERT_EQ(rc, MES_OK) << "Query " << i << " failed: " << err;
    ASSERT_EQ(result.rows.size(), 1u);
    EXPECT_EQ(result.rows[0].values[0], std::to_string(i));
  }

  conn.Disconnect();
}

TEST(E2EProtocol, NullValues) {
  mes::protocol::MysqlConnection conn;
  ASSERT_EQ(conn.Connect(kHost, kPort, kRootUser, kRootPass, kTimeout, kTimeout,
                         0, "", "", ""),
            MES_OK);

  mes::protocol::QueryResult result;
  std::string err;
  auto rc = mes::protocol::ExecuteQuery(
      conn.Socket(), "SELECT NULL AS a, 'ok' AS b, NULL AS c", &result, &err);
  ASSERT_EQ(rc, MES_OK) << err;
  ASSERT_EQ(result.rows.size(), 1u);
  EXPECT_TRUE(result.rows[0].is_null[0]);
  EXPECT_FALSE(result.rows[0].is_null[1]);
  EXPECT_TRUE(result.rows[0].is_null[2]);
  EXPECT_EQ(result.rows[0].values[1], "ok");

  conn.Disconnect();
}

// -- ConnectionValidator via BinlogClient C ABI --

TEST(E2EProtocol, ConnectionValidatorViaCApi) {
  mes_client_t* client = mes_client_create();
  ASSERT_NE(client, nullptr);

  mes_client_config_t config{};
  config.host = kHost;
  config.port = kPort;
  config.user = kReplUser;
  config.password = kReplPass;
  config.server_id = 99;
  config.start_gtid = "";
  config.connect_timeout_s = kTimeout;
  config.read_timeout_s = kTimeout;
  config.ssl_mode =
      static_cast<mes_ssl_mode_t>(e2e::DefaultSslMode());
  std::string validator_ca = e2e::DefaultCa();
  config.ssl_ca = validator_ca.empty() ? nullptr : validator_ca.c_str();
  config.ssl_cert = nullptr;
  config.ssl_key = nullptr;

  auto rc = mes_client_connect(client, &config);
  ASSERT_EQ(rc, MES_OK) << mes_client_last_error(client);
  EXPECT_EQ(mes_client_is_connected(client), 1);

  mes_client_disconnect(client);
  EXPECT_EQ(mes_client_is_connected(client), 0);

  mes_client_destroy(client);
}

// -- BinlogStream via C ABI --

TEST(E2EProtocol, BinlogStreamInsertAndCapture) {
  if (e2e::IsMariaDB()) {
    GTEST_SKIP() << "Uses MySQL-specific GTID queries and SSL setup";
  }
  // 1. Insert a row via a separate connection to generate a binlog event
  mes::protocol::MysqlConnection data_conn;
  ASSERT_EQ(data_conn.Connect(kHost, kPort, kRootUser, kRootPass, kTimeout,
                              kTimeout, 0, "", "", ""),
            MES_OK);

  mes::protocol::QueryResult qr;
  std::string err;
  // Get current GTID position before INSERT
  ASSERT_EQ(mes::protocol::ExecuteQuery(data_conn.Socket(),
                                        "SELECT @@GLOBAL.gtid_executed", &qr,
                                        &err),
            MES_OK);
  std::string gtid_before = qr.rows[0].values[0];

  // Do the INSERT
  ASSERT_EQ(mes::protocol::ExecuteQuery(
                data_conn.Socket(),
                "INSERT INTO mes_test.items (name, value) VALUES ('e2e_test', 42)",
                &qr, &err),
            MES_OK)
      << err;

  data_conn.Disconnect();

  // 2. Start binlog client from the position before INSERT
  mes_client_t* client = mes_client_create();
  ASSERT_NE(client, nullptr);

  mes_client_config_t config{};
  config.host = kHost;
  config.port = kPort;
  config.user = kReplUser;
  config.password = kReplPass;
  config.server_id = 100;
  std::string start_gtid = gtid_before;
  config.start_gtid = start_gtid.c_str();
  config.connect_timeout_s = kTimeout;
  config.read_timeout_s = 3;
  config.ssl_mode = MES_SSL_DISABLED;
  config.ssl_ca = nullptr;
  config.ssl_cert = nullptr;
  config.ssl_key = nullptr;

  ASSERT_EQ(mes_client_connect(client, &config), MES_OK)
      << mes_client_last_error(client);
  ASSERT_EQ(mes_client_start(client), MES_OK)
      << mes_client_last_error(client);

  // 3. Create engine and feed events until we get the INSERT
  mes_engine_t* engine = mes_create();
  ASSERT_NE(engine, nullptr);

  bool found_insert = false;
  int poll_count = 0;
  constexpr int kMaxPolls = 50;

  while (!found_insert && poll_count < kMaxPolls) {
    auto result = mes_client_poll(client);
    poll_count++;

    if (result.error != MES_OK) {
      break;
    }
    if (result.is_heartbeat || result.data == nullptr) {
      continue;
    }

    size_t consumed = 0;
    mes_feed(engine, result.data, result.size, &consumed);

    const mes_event_t* event = nullptr;
    while (mes_next_event(engine, &event) == MES_OK) {
      if (event->type == MES_EVENT_INSERT &&
          std::strcmp(event->table, "items") == 0) {
        found_insert = true;
        // Verify we got the right data
        EXPECT_GT(event->after_count, 0u);
        break;
      }
    }
  }

  EXPECT_TRUE(found_insert) << "Did not capture INSERT event after "
                             << poll_count << " polls";

  // Check GTID was tracked
  const char* gtid = mes_client_current_gtid(client);
  EXPECT_NE(std::strlen(gtid), 0u);

  mes_client_stop(client);
  mes_client_disconnect(client);
  mes_client_destroy(client);
  mes_destroy(engine);
}

// -- MetadataFetcher via C ABI --

TEST(E2EProtocol, MetadataFetcherColumnNames) {
  if (e2e::IsMariaDB()) {
    GTEST_SKIP() << "Uses MySQL-specific GTID queries and SSL setup";
  }
  // Use the engine + metadata connection to verify column names are resolved
  mes_engine_t* engine = mes_create();
  ASSERT_NE(engine, nullptr);

  mes_client_config_t meta_config{};
  meta_config.host = kHost;
  meta_config.port = kPort;
  meta_config.user = kRootUser;
  meta_config.password = kRootPass;
  meta_config.server_id = 0;
  meta_config.start_gtid = nullptr;
  meta_config.connect_timeout_s = kTimeout;
  meta_config.read_timeout_s = kTimeout;
  meta_config.ssl_mode = MES_SSL_DISABLED;
  meta_config.ssl_ca = nullptr;
  meta_config.ssl_cert = nullptr;
  meta_config.ssl_key = nullptr;

  auto rc = mes_engine_set_metadata_conn(engine, &meta_config);
  ASSERT_EQ(rc, MES_OK);

  // Insert data via separate connection
  mes::protocol::MysqlConnection data_conn;
  ASSERT_EQ(data_conn.Connect(kHost, kPort, kRootUser, kRootPass, kTimeout,
                              kTimeout, 0, "", "", ""),
            MES_OK);

  mes::protocol::QueryResult qr;
  std::string err;
  ASSERT_EQ(mes::protocol::ExecuteQuery(data_conn.Socket(),
                                        "SELECT @@GLOBAL.gtid_executed", &qr,
                                        &err),
            MES_OK);
  std::string gtid_before = qr.rows[0].values[0];

  ASSERT_EQ(mes::protocol::ExecuteQuery(
                data_conn.Socket(),
                "INSERT INTO mes_test.items (name, value) VALUES ('meta_test', 99)",
                &qr, &err),
            MES_OK)
      << err;
  data_conn.Disconnect();

  // Start streaming
  mes_client_t* client = mes_client_create();
  ASSERT_NE(client, nullptr);

  mes_client_config_t config{};
  config.host = kHost;
  config.port = kPort;
  config.user = kReplUser;
  config.password = kReplPass;
  config.server_id = 101;
  std::string start_gtid = gtid_before;
  config.start_gtid = start_gtid.c_str();
  config.connect_timeout_s = kTimeout;
  config.read_timeout_s = 3;
  config.ssl_mode = MES_SSL_DISABLED;
  config.ssl_ca = nullptr;
  config.ssl_cert = nullptr;
  config.ssl_key = nullptr;

  ASSERT_EQ(mes_client_connect(client, &config), MES_OK)
      << mes_client_last_error(client);
  ASSERT_EQ(mes_client_start(client), MES_OK)
      << mes_client_last_error(client);

  bool found = false;
  int poll_count = 0;
  constexpr int kMaxPolls = 50;

  while (!found && poll_count < kMaxPolls) {
    auto result = mes_client_poll(client);
    poll_count++;
    if (result.error != MES_OK) break;
    if (result.is_heartbeat || result.data == nullptr) continue;

    size_t consumed = 0;
    mes_feed(engine, result.data, result.size, &consumed);

    const mes_event_t* event = nullptr;
    while (mes_next_event(engine, &event) == MES_OK) {
      if (event->type == MES_EVENT_INSERT &&
          std::strcmp(event->table, "items") == 0) {
        found = true;
        // Check column names are populated (via MetadataFetcher)
        ASSERT_GT(event->after_count, 0u);
        EXPECT_STREQ(event->after_columns[0].col_name, "id");
        EXPECT_STREQ(event->after_columns[1].col_name, "name");
        EXPECT_STREQ(event->after_columns[2].col_name, "value");
        break;
      }
    }
  }

  EXPECT_TRUE(found) << "Did not capture INSERT event with column names";

  mes_client_stop(client);
  mes_client_disconnect(client);
  mes_client_destroy(client);
  mes_destroy(engine);
}

// -- Stop() interrupts blocking Poll() --

TEST(E2EProtocol, StopInterruptsPoll) {
  if (e2e::IsMariaDB()) {
    GTEST_SKIP() << "Uses MySQL-specific GTID queries and SSL setup";
  }
  mes_client_t* client = mes_client_create();
  ASSERT_NE(client, nullptr);

  // Get current GTID to skip past existing events
  mes::protocol::MysqlConnection setup_conn;
  ASSERT_EQ(setup_conn.Connect(kHost, kPort, kRootUser, kRootPass, kTimeout,
                               kTimeout, 0, "", "", ""),
            MES_OK);
  mes::protocol::QueryResult qr;
  std::string err;
  ASSERT_EQ(mes::protocol::ExecuteQuery(setup_conn.Socket(),
                                        "SELECT @@GLOBAL.gtid_executed", &qr,
                                        &err),
            MES_OK);
  std::string current_gtid = qr.rows[0].values[0];
  setup_conn.Disconnect();

  mes_client_config_t config{};
  config.host = kHost;
  config.port = kPort;
  config.user = kReplUser;
  config.password = kReplPass;
  config.server_id = 102;
  std::string start_gtid = current_gtid;
  config.start_gtid = start_gtid.c_str();
  config.connect_timeout_s = kTimeout;
  config.read_timeout_s = 30;  // long timeout
  config.ssl_mode = MES_SSL_DISABLED;
  config.ssl_ca = nullptr;
  config.ssl_cert = nullptr;
  config.ssl_key = nullptr;

  ASSERT_EQ(mes_client_connect(client, &config), MES_OK)
      << mes_client_last_error(client);
  ASSERT_EQ(mes_client_start(client), MES_OK)
      << mes_client_last_error(client);

  // With the reader thread model, Poll() blocks on the event queue CV.
  // To test Stop() interrupting Poll(), just start polling immediately
  // in a background thread — the reader thread will drain initial events
  // from the socket, and Poll() will block on the queue once drained.
  std::atomic<bool> poll_returned{false};
  auto start = std::chrono::steady_clock::now();
  std::thread poll_thread([&]() {
    // Keep polling until blocked or error
    for (;;) {
      auto r = mes_client_poll(client);
      if (r.error != MES_OK) break;
      if (r.data == nullptr) break;
    }
    poll_returned.store(true, std::memory_order_release);
  });

  // Give the poll thread time to start blocking on empty queue
  std::this_thread::sleep_for(std::chrono::milliseconds(500));

  // Stop should interrupt the blocking poll
  mes_client_stop(client);

  // Wait for poll to return (should be fast after Stop)
  poll_thread.join();
  EXPECT_TRUE(poll_returned.load(std::memory_order_acquire));

  auto elapsed = std::chrono::steady_clock::now() - start;
  // Should complete well under the 30s read_timeout
  EXPECT_LT(std::chrono::duration_cast<std::chrono::seconds>(elapsed).count(),
             10);

  mes_client_disconnect(client);
  mes_client_destroy(client);
}

}  // namespace
