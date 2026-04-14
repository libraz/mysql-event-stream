// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * @file test_e2e_auth.cpp
 * @brief End-to-end tests for MySQL authentication edge cases
 *
 * Requires a running MySQL 8.4+ instance at localhost:13308 with:
 *   - root/test_root_password (caching_sha2_password)
 *   - repl_user/test_password (caching_sha2_password, REPLICATION SLAVE)
 *   - sha2_user/sha2_test_pwd (caching_sha2_password, REPLICATION SLAVE)
 *   - empty_pass_user/"" (caching_sha2_password, SELECT only)
 *   - no_repl_user/no_repl_pass (caching_sha2_password, SELECT only)
 *   - special_user/p@ss'w\\ord"! (caching_sha2_password, SELECT only)
 *
 * Start with: cd e2e/docker && docker compose up -d
 */

#include <gtest/gtest.h>

#include <cstring>
#include <string>

#include "mes.h"
#include "protocol/mysql_connection.h"
#include "protocol/mysql_query.h"
#include "test_e2e_helpers.h"

using namespace e2e;

namespace {

// Server IDs for binlog clients in this test suite: 300-399

// -- caching_sha2_password tests --

TEST(E2EAuth, CachingSha2OverTls) {
  if (e2e::IsMariaDB()) {
    GTEST_SKIP() << "caching_sha2_password is MySQL-specific";
  }
  mes::protocol::MysqlConnection conn;
  auto rc = conn.Connect(kHost, kPort, "sha2_user", "sha2_test_pwd", kTimeout, kTimeout, 2,
                         CaCert(), "", "");
  ASSERT_EQ(rc, MES_OK) << conn.GetLastError();
  EXPECT_TRUE(conn.IsConnected());
  conn.Disconnect();
}

TEST(E2EAuth, CachingSha2FastAuth) {
  if (e2e::IsMariaDB()) {
    GTEST_SKIP() << "caching_sha2_password is MySQL-specific";
  }
  // First connection: full auth over TLS to populate server's auth cache
  {
    mes::protocol::MysqlConnection conn;
    auto rc = conn.Connect(kHost, kPort, "sha2_user", "sha2_test_pwd", kTimeout, kTimeout, 2,
                           CaCert(), "", "");
    ASSERT_EQ(rc, MES_OK) << "First connect failed: " << conn.GetLastError();
    EXPECT_TRUE(conn.IsConnected());
    conn.Disconnect();
  }

  // Second connection: should use fast auth path (server cache is warm)
  {
    mes::protocol::MysqlConnection conn;
    auto rc = conn.Connect(kHost, kPort, "sha2_user", "sha2_test_pwd", kTimeout, kTimeout, 2,
                           CaCert(), "", "");
    ASSERT_EQ(rc, MES_OK) << "Second connect failed: " << conn.GetLastError();
    EXPECT_TRUE(conn.IsConnected());
    conn.Disconnect();
  }
}

TEST(E2EAuth, CachingSha2WithoutTlsColdCache) {
  if (e2e::IsMariaDB()) {
    GTEST_SKIP() << "caching_sha2_password is MySQL-specific";
  }
  // Create a temporary caching_sha2_password user to ensure cold cache
  const std::string temp_user = "sha2_cold_cache_test_user";
  const std::string temp_pass = "cold_cache_pwd_123";
  auto dml_rc = ExecuteDML("CREATE USER IF NOT EXISTS '" + temp_user +
                           "'@'%' IDENTIFIED WITH caching_sha2_password BY '" + temp_pass + "'");
  ASSERT_EQ(dml_rc, MES_OK) << "Failed to create temp user";

  // Grant minimal privileges so we can test auth
  ExecuteDML("GRANT SELECT ON *.* TO '" + temp_user + "'@'%'");
  ExecuteDML("FLUSH PRIVILEGES");

  // Attempt connection without TLS. With RSA public key auth, this should
  // succeed even with a cold cache.
  mes::protocol::MysqlConnection conn;
  auto rc = conn.Connect(kHost, kPort, temp_user.c_str(), temp_pass.c_str(), kTimeout, kTimeout, 0,
                         "", "", "");

  EXPECT_EQ(rc, MES_OK) << "RSA public key auth should succeed without TLS: "
                        << conn.GetLastError();
  if (conn.IsConnected()) {
    conn.Disconnect();
  }

  // Clean up temp user
  ExecuteDML("DROP USER IF EXISTS '" + temp_user + "'@'%'");
}

// -- Auth switch tests --

TEST(E2EAuth, CachingSha2DefaultPlugin) {
  if (e2e::IsMariaDB()) {
    GTEST_SKIP() << "caching_sha2_password is MySQL-specific";
  }
  // All users use caching_sha2_password (default in 8.4+, only option in 9.x).
  // repl_user uses caching_sha2_password, so no auth switch is needed.
  mes::protocol::MysqlConnection conn;
  auto rc =
      conn.Connect(kHost, kPort, kReplUser, kReplPass, kTimeout, kTimeout, 2, CaCert(), "", "");
  ASSERT_EQ(rc, MES_OK) << conn.GetLastError();
  EXPECT_TRUE(conn.IsConnected());

  // Server default auth plugin should be caching_sha2_password
  EXPECT_EQ(conn.GetServerInfo().auth_plugin_name, "caching_sha2_password");

  conn.Disconnect();
}

// -- Password edge cases --

TEST(E2EAuth, EmptyPassword) {
  if (e2e::IsMariaDB()) {
    GTEST_SKIP() << "empty_pass_user is only provisioned in the MySQL container";
  }
  mes::protocol::MysqlConnection conn;
  auto rc = conn.Connect(kHost, kPort, "empty_pass_user", "", kTimeout, kTimeout, 0, "", "", "");
  ASSERT_EQ(rc, MES_OK) << conn.GetLastError();
  EXPECT_TRUE(conn.IsConnected());
  conn.Disconnect();
}

TEST(E2EAuth, LongPassword) {
  // Generate a 200-character password (caching_sha2_password limit ~255)
  std::string long_pass(200, 'A');
  for (size_t i = 0; i < long_pass.size(); i++) {
    long_pass[i] = 'a' + static_cast<char>(i % 26);
  }

  const std::string user = "long_pwd_user";

  // Drop first to avoid stale state from previous runs
  ExecuteDML("DROP USER IF EXISTS '" + user + "'@'%'");

  // Create user with long password (default auth plugin)
  auto dml_rc = ExecuteDML("CREATE USER '" + user + "'@'%' IDENTIFIED BY '" + long_pass + "'");
  ASSERT_EQ(dml_rc, MES_OK) << "Failed to create long password user";
  ExecuteDML("GRANT SELECT ON *.* TO '" + user + "'@'%'");

  // Connect with the long password
  mes::protocol::MysqlConnection conn;
  auto rc = conn.Connect(kHost, kPort, user.c_str(), long_pass.c_str(), kTimeout, kTimeout,
                         DefaultSslMode(), DefaultCa(), "", "");
  ASSERT_EQ(rc, MES_OK) << conn.GetLastError();
  EXPECT_TRUE(conn.IsConnected());
  conn.Disconnect();

  // Clean up
  ExecuteDML("DROP USER IF EXISTS '" + user + "'@'%'");
}

TEST(E2EAuth, SpecialCharsInPassword) {
  if (e2e::IsMariaDB()) {
    GTEST_SKIP() << "special_user is only provisioned in the MySQL container";
  }
  // Connect over TLS since caching_sha2_password may need full auth
  mes::protocol::MysqlConnection conn;
  auto rc = conn.Connect(kHost, kPort, "special_user", "p@ss'w\\ord\"!", kTimeout, kTimeout, 2,
                         CaCert(), "", "");
  ASSERT_EQ(rc, MES_OK) << conn.GetLastError();
  EXPECT_TRUE(conn.IsConnected());
  conn.Disconnect();
}

// -- Authentication failure tests --

TEST(E2EAuth, NonExistentUser) {
  mes::protocol::MysqlConnection conn;
  auto rc = conn.Connect(kHost, kPort, "nobody_xyz_404", "any_password", kTimeout, kTimeout, 0, "",
                         "", "");
  EXPECT_EQ(rc, MES_ERR_AUTH);
  EXPECT_FALSE(conn.IsConnected());
}

TEST(E2EAuth, WrongPasswordSha2) {
  if (e2e::IsMariaDB()) {
    GTEST_SKIP() << "sha2_user is only provisioned in the MySQL container";
  }
  mes::protocol::MysqlConnection conn;
  auto rc = conn.Connect(kHost, kPort, "sha2_user", "wrong_password", kTimeout, kTimeout, 2,
                         CaCert(), "", "");
  EXPECT_EQ(rc, MES_ERR_AUTH);
  EXPECT_FALSE(conn.IsConnected());
  EXPECT_FALSE(conn.GetLastError().empty());
}

// -- Privilege tests --

TEST(E2EAuth, NoReplPrivilegesConnect) {
  if (e2e::IsMariaDB()) {
    GTEST_SKIP() << "no_repl_user is only provisioned in the MySQL container";
  }
  // Connection itself should succeed (SELECT privilege is enough for auth)
  mes::protocol::MysqlConnection conn;
  auto rc = conn.Connect(kHost, kPort, "no_repl_user", "no_repl_pass", kTimeout, kTimeout, 2,
                         CaCert(), "", "");
  ASSERT_EQ(rc, MES_OK) << conn.GetLastError();
  EXPECT_TRUE(conn.IsConnected());
  conn.Disconnect();
}

TEST(E2EAuth, NoReplPrivilegesStreamFails) {
  if (e2e::IsMariaDB()) {
    GTEST_SKIP() << "no_repl_user is only provisioned in the MySQL container";
  }
  // Connect as no_repl_user via the mes_client C API.
  // The connection should succeed, but mes_client_start() (which sends
  // COM_BINLOG_DUMP_GTID) should fail because the user lacks REPLICATION SLAVE.
  mes_client_t* client = mes_client_create();
  ASSERT_NE(client, nullptr);

  std::string gtid = GetCurrentGtid();
  ASSERT_FALSE(gtid.empty()) << "Failed to get current GTID";

  mes_client_config_t config{};
  config.host = kHost;
  config.port = kPort;
  config.user = "no_repl_user";
  config.password = "no_repl_pass";
  config.server_id = 300;
  config.start_gtid = gtid.c_str();
  config.connect_timeout_s = kTimeout;
  config.read_timeout_s = kTimeout;
  std::string ca_cert = CaCert();
  config.ssl_mode = MES_SSL_REQUIRED;
  config.ssl_ca = ca_cert.c_str();
  config.ssl_cert = nullptr;
  config.ssl_key = nullptr;

  auto rc = mes_client_connect(client, &config);
  ASSERT_EQ(rc, MES_OK) << mes_client_last_error(client);

  // Start binlog dump. This should fail due to missing REPLICATION SLAVE.
  auto start_rc = mes_client_start(client);
  if (start_rc == MES_OK) {
    // Some implementations may not fail on start but on the first poll
    auto result = mes_client_poll(client);
    EXPECT_NE(result.error, MES_OK) << "Expected poll to fail for user without REPLICATION SLAVE";
    mes_client_stop(client);
  } else {
    EXPECT_NE(start_rc, MES_OK) << "Expected start to fail for user without REPLICATION SLAVE";
  }

  mes_client_disconnect(client);
  mes_client_destroy(client);
}

// -- Recovery test --

TEST(E2EAuth, ReconnectAfterFailure) {
  mes::protocol::MysqlConnection conn;

  // First attempt: bad password, should fail
  auto rc = conn.Connect(kHost, kPort, kReplUser, "wrong_password", kTimeout, kTimeout,
                         DefaultSslMode(), DefaultCa(), "", "");
  EXPECT_EQ(rc, MES_ERR_AUTH);
  EXPECT_FALSE(conn.IsConnected());

  // Second attempt: correct password on the same connection object
  rc = conn.Connect(kHost, kPort, kReplUser, kReplPass, kTimeout, kTimeout, DefaultSslMode(),
                    DefaultCa(), "", "");
  ASSERT_EQ(rc, MES_OK) << conn.GetLastError();
  EXPECT_TRUE(conn.IsConnected());

  // Verify the connection is functional by running a query
  mes::protocol::QueryResult result;
  std::string err;
  auto qrc = mes::protocol::ExecuteQuery(conn.Socket(), "SELECT 1", &result, &err);
  EXPECT_EQ(qrc, MES_OK) << err;
  EXPECT_EQ(result.rows.size(), 1u);

  conn.Disconnect();
}

}  // namespace
