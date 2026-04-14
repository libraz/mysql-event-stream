// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * @file test_e2e_ssl.cpp
 * @brief End-to-end tests for SSL/TLS connection modes
 *
 * Requires a running MySQL 8.4 instance at localhost:13308 with:
 *   - SSL enabled with server certificates signed by the test CA
 *   - root/test_root_password
 *   - repl_user/test_password (replication grants)
 *   - sha2_user/sha2_test_pwd (caching_sha2_password, replication grants)
 *   - Database: mes_test with tables: users, items
 *
 * Start with: cd e2e/docker && docker compose up -d
 */

#include <gtest/gtest.h>

#include <cstring>
#include <string>
#include <vector>

#include "test_e2e_helpers.h"

using namespace e2e;

// -- SSL mode 1: preferred --

TEST(E2ESSL, SslPreferredConnects) {
  if (e2e::IsMariaDB()) {
    GTEST_SKIP() << "SSL tests require MySQL with certificate configuration";
  }
  mes::protocol::MysqlConnection conn;
  auto rc = conn.Connect(kHost, kPort, kRootUser, kRootPass, kTimeout, kTimeout,
                         1, CaCert(), "", "");
  ASSERT_EQ(rc, MES_OK) << conn.GetLastError();
  EXPECT_TRUE(conn.IsConnected());
  EXPECT_TRUE(conn.Socket()->IsTlsActive());
  conn.Disconnect();
}

// -- SSL mode 2: required --

TEST(E2ESSL, SslRequiredConnects) {
  if (e2e::IsMariaDB()) {
    GTEST_SKIP() << "SSL tests require MySQL with certificate configuration";
  }
  mes::protocol::MysqlConnection conn;
  auto rc = conn.Connect(kHost, kPort, kRootUser, kRootPass, kTimeout, kTimeout,
                         2, CaCert(), "", "");
  ASSERT_EQ(rc, MES_OK) << conn.GetLastError();
  EXPECT_TRUE(conn.IsConnected());
  EXPECT_TRUE(conn.Socket()->IsTlsActive());
  conn.Disconnect();
}

// -- SSL mode 3: verify_ca with valid CA --

TEST(E2ESSL, SslVerifyCaValid) {
  if (e2e::IsMariaDB()) {
    GTEST_SKIP() << "SSL tests require MySQL with certificate configuration";
  }
  mes::protocol::MysqlConnection conn;
  auto rc = conn.Connect(kHost, kPort, kRootUser, kRootPass, kTimeout, kTimeout,
                         3, CaCert(), "", "");
  ASSERT_EQ(rc, MES_OK) << conn.GetLastError();
  EXPECT_TRUE(conn.IsConnected());
  EXPECT_TRUE(conn.Socket()->IsTlsActive());
  conn.Disconnect();
}

// -- SSL mode 3: verify_ca with wrong CA --

TEST(E2ESSL, SslVerifyCaWrongCa) {
  if (e2e::IsMariaDB()) {
    GTEST_SKIP() << "SSL tests require MySQL with certificate configuration";
  }
  mes::protocol::MysqlConnection conn;
  auto rc = conn.Connect(kHost, kPort, kRootUser, kRootPass, kTimeout, kTimeout,
                         3, WrongCa(), "", "");
  EXPECT_NE(rc, MES_OK);
  EXPECT_TRUE(rc == MES_ERR_CONNECT || rc == MES_ERR_AUTH)
      << "Expected MES_ERR_CONNECT or MES_ERR_AUTH, got " << rc;
  EXPECT_FALSE(conn.IsConnected());
}

// -- SSL mode 3: verify_ca with nonexistent CA path --

TEST(E2ESSL, SslVerifyCaBadPath) {
  if (e2e::IsMariaDB()) {
    GTEST_SKIP() << "SSL tests require MySQL with certificate configuration";
  }
  mes::protocol::MysqlConnection conn;
  auto rc = conn.Connect(kHost, kPort, kRootUser, kRootPass, kTimeout, kTimeout,
                         3, "/nonexistent/ca.pem", "", "");
  EXPECT_NE(rc, MES_OK);
  EXPECT_FALSE(conn.IsConnected());
}

// -- SSL mode 4: verify_identity --

TEST(E2ESSL, SslVerifyIdentityValid) {
  if (e2e::IsMariaDB()) {
    GTEST_SKIP() << "SSL tests require MySQL with certificate configuration";
  }
  mes::protocol::MysqlConnection conn;
  auto rc = conn.Connect(kHost, kPort, kRootUser, kRootPass, kTimeout, kTimeout,
                         4, CaCert(), "", "");
  ASSERT_EQ(rc, MES_OK) << conn.GetLastError();
  EXPECT_TRUE(conn.IsConnected());
  EXPECT_TRUE(conn.Socket()->IsTlsActive());
  conn.Disconnect();
}

// -- SSL mode 2 with client certificate and key --

TEST(E2ESSL, SslClientCert) {
  if (e2e::IsMariaDB()) {
    GTEST_SKIP() << "SSL tests require MySQL with certificate configuration";
  }
  mes::protocol::MysqlConnection conn;
  auto rc = conn.Connect(kHost, kPort, kRootUser, kRootPass, kTimeout, kTimeout,
                         2, CaCert(), ClientCert(), ClientKey());
  ASSERT_EQ(rc, MES_OK) << conn.GetLastError();
  EXPECT_TRUE(conn.IsConnected());
  EXPECT_TRUE(conn.Socket()->IsTlsActive());
  conn.Disconnect();
}

// -- Query execution over TLS --

TEST(E2ESSL, SslQueryAfterUpgrade) {
  if (e2e::IsMariaDB()) {
    GTEST_SKIP() << "SSL tests require MySQL with certificate configuration";
  }
  mes::protocol::MysqlConnection conn;
  ASSERT_EQ(conn.Connect(kHost, kPort, kRootUser, kRootPass, kTimeout, kTimeout,
                         2, CaCert(), "", ""),
            MES_OK)
      << conn.GetLastError();
  ASSERT_TRUE(conn.Socket()->IsTlsActive());

  mes::protocol::QueryResult result;
  std::string err;
  auto rc = mes::protocol::ExecuteQuery(conn.Socket(), "SELECT 1 AS n",
                                        &result, &err);
  ASSERT_EQ(rc, MES_OK) << err;
  ASSERT_EQ(result.rows.size(), 1u);
  EXPECT_EQ(result.rows[0].values[0], "1");

  conn.Disconnect();
}

// -- Multiple queries over TLS --

TEST(E2ESSL, SslMultipleQueries) {
  if (e2e::IsMariaDB()) {
    GTEST_SKIP() << "SSL tests require MySQL with certificate configuration";
  }
  mes::protocol::MysqlConnection conn;
  ASSERT_EQ(conn.Connect(kHost, kPort, kRootUser, kRootPass, kTimeout, kTimeout,
                         2, CaCert(), "", ""),
            MES_OK)
      << conn.GetLastError();
  ASSERT_TRUE(conn.Socket()->IsTlsActive());

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

// -- Binlog streaming over TLS with caching_sha2_password --

TEST(E2ESSL, SslBinlogStreamCapture) {
  if (e2e::IsMariaDB()) {
    GTEST_SKIP() << "SSL tests require MySQL with certificate configuration";
  }
  // Get current GTID position before inserting data
  std::string gtid_before = GetCurrentGtid();
  ASSERT_FALSE(gtid_before.empty()) << "Failed to fetch current GTID";

  // Insert a row to generate a binlog event
  ASSERT_EQ(
      ExecuteDML(
          "INSERT INTO mes_test.items (name, value) VALUES ('ssl_e2e', 77)"),
      MES_OK);

  // Set up binlog client with SSL mode 2 and caching_sha2_password user
  mes_client_t* client = mes_client_create();
  ASSERT_NE(client, nullptr);

  std::string ca_path = CaCert();

  mes_client_config_t config{};
  config.host = kHost;
  config.port = kPort;
  config.user = "sha2_user";
  config.password = "sha2_test_pwd";
  config.server_id = 200;
  config.start_gtid = gtid_before.c_str();
  config.connect_timeout_s = kTimeout;
  config.read_timeout_s = 3;
  config.ssl_mode = MES_SSL_REQUIRED;
  config.ssl_ca = ca_path.c_str();
  config.ssl_cert = nullptr;
  config.ssl_key = nullptr;

  ASSERT_EQ(mes_client_connect(client, &config), MES_OK)
      << mes_client_last_error(client);
  ASSERT_EQ(mes_client_start(client), MES_OK)
      << mes_client_last_error(client);

  // Feed binlog data into engine and look for the INSERT event
  mes_engine_t* engine = mes_create();
  ASSERT_NE(engine, nullptr);

  bool found_insert = false;
  int poll_count = 0;
  constexpr int kMaxPolls = 100;

  while (!found_insert && poll_count < kMaxPolls) {
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
        found_insert = true;
        EXPECT_GT(event->after_count, 0u);
        break;
      }
    }
  }

  EXPECT_TRUE(found_insert) << "Did not capture INSERT event over TLS after "
                             << poll_count << " polls";

  // Verify GTID was tracked
  const char* gtid = mes_client_current_gtid(client);
  EXPECT_NE(std::strlen(gtid), 0u);

  mes_client_stop(client);
  mes_client_disconnect(client);
  mes_client_destroy(client);
  mes_destroy(engine);
}
