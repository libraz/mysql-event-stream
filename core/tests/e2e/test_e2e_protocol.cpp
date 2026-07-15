// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * @file test_e2e_protocol.cpp
 * @brief End-to-end tests for the custom MySQL protocol implementation
 *
 * Requires a running MySQL 8.4 instance at localhost:13308 with:
 *   - root/test_root_password (mysql_native_password)
 *   - repl_user/test_password (replication grants)
 *   - Database: mes_test with tables: users, items
 *
 * Start with: cd e2e/docker && docker compose up -d
 */

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

#ifndef _WIN32
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

#include "client/gtid_encoder.h"
#include "client/metadata_fetcher.h"
#include "event_header.h"
#include "mes.h"
#include "protocol/mysql_binlog_stream.h"
#include "protocol/mysql_connection.h"
#include "protocol/mysql_query.h"
#include "test_e2e_helpers.h"

namespace {

using namespace e2e;

std::vector<uint8_t> EncodeGtidSet(const std::string& gtid_set) {
  std::vector<uint8_t> encoded;
  EXPECT_EQ(mes::GtidEncoder::Encode(gtid_set.c_str(), &encoded), MES_OK);
  return encoded;
}

#ifndef _WIN32

class StallingMetadataProxy {
 public:
  StallingMetadataProxy() {
    listener_ = socket(AF_INET, SOCK_STREAM, 0);
    if (listener_ < 0) return;
    int reuse = 1;
    setsockopt(listener_, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = 0;
    if (bind(listener_, reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0 ||
        listen(listener_, 4) != 0) {
      close(listener_);
      listener_ = -1;
      return;
    }

    socklen_t address_len = sizeof(address);
    if (getsockname(listener_, reinterpret_cast<sockaddr*>(&address), &address_len) != 0) {
      close(listener_);
      listener_ = -1;
      return;
    }
    port_ = ntohs(address.sin_port);
    thread_ = std::thread([this]() { Run(); });
  }

  ~StallingMetadataProxy() {
    stop_.store(true, std::memory_order_release);
    if (listener_ >= 0) {
      shutdown(listener_, SHUT_RDWR);
      close(listener_);
      listener_ = -1;
    }
    if (thread_.joinable()) thread_.join();
  }

  bool IsReady() const { return listener_ >= 0 && port_ != 0; }
  uint16_t Port() const { return port_; }
  int StalledQueryCount() const { return stalled_queries_.load(std::memory_order_acquire); }

 private:
  static void CloseSocket(int fd) {
    if (fd >= 0) {
      shutdown(fd, SHUT_RDWR);
      close(fd);
    }
  }

  static bool SendAll(int fd, const uint8_t* data, size_t len) {
    size_t sent = 0;
    while (sent < len) {
#ifdef MSG_NOSIGNAL
      const ssize_t rc = send(fd, data + sent, len - sent, MSG_NOSIGNAL);
#else
      const ssize_t rc = send(fd, data + sent, len - sent, 0);
#endif
      if (rc <= 0) return false;
      sent += static_cast<size_t>(rc);
    }
    return true;
  }

  static void ConfigureNoSigPipe(int fd) {
#ifdef SO_NOSIGPIPE
    int enabled = 1;
    setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &enabled, sizeof(enabled));
#else
    (void)fd;
#endif
  }

  static int ConnectUpstream() {
    const int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    ConfigureNoSigPipe(fd);
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = htons(kPort);
    if (connect(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0) {
      close(fd);
      return -1;
    }
    return fd;
  }

  void HandleConnection(int client) {
    ConfigureNoSigPipe(client);
    const int upstream = ConnectUpstream();
    if (upstream < 0) {
      CloseSocket(client);
      return;
    }

    std::atomic<bool> relay_done{false};
    std::thread server_to_client([&]() {
      uint8_t buffer[4096];
      while (!relay_done.load(std::memory_order_acquire)) {
        const ssize_t count = recv(upstream, buffer, sizeof(buffer), 0);
        if (count <= 0 || !SendAll(client, buffer, static_cast<size_t>(count))) break;
      }
    });

    bool stall = false;
    std::string recent;
    uint8_t buffer[4096];
    while (!stop_.load(std::memory_order_acquire)) {
      const ssize_t count = recv(client, buffer, sizeof(buffer), 0);
      if (count <= 0) break;
      if (!stall) {
        recent.append(reinterpret_cast<const char*>(buffer), static_cast<size_t>(count));
        if (recent.find("SHOW COLUMNS") != std::string::npos) {
          stall = true;
          stalled_queries_.fetch_add(1, std::memory_order_acq_rel);
          continue;
        }
        if (recent.size() > 128) recent.erase(0, recent.size() - 128);
        if (!SendAll(upstream, buffer, static_cast<size_t>(count))) break;
      }
      // Once SHOW COLUMNS is observed, continue reading/discarding. The
      // client-side SO_RCVTIMEO must end the query and reconnect.
    }

    relay_done.store(true, std::memory_order_release);
    CloseSocket(client);
    CloseSocket(upstream);
    server_to_client.join();
  }

  void Run() {
    while (!stop_.load(std::memory_order_acquire)) {
      const int client = accept(listener_, nullptr, nullptr);
      if (client < 0) break;
      HandleConnection(client);
    }
  }

  int listener_ = -1;
  uint16_t port_ = 0;
  std::thread thread_;
  std::atomic<bool> stop_{false};
  std::atomic<int> stalled_queries_{0};
};

#endif

// -- MysqlConnection tests --

TEST(E2EProtocol, ConnectWithNativePassword) {
  mes::protocol::MysqlConnection conn;
  auto rc = conn.Connect(kHost, kPort, kRootUser, kRootPass, kTimeout, kTimeout, 0, "", "", "");
  ASSERT_EQ(rc, MES_OK) << conn.GetLastError();
  EXPECT_TRUE(conn.IsConnected());
  EXPECT_FALSE(conn.GetServerInfo().server_version.empty());
  EXPECT_GT(conn.GetServerInfo().connection_id, 0u);
  conn.Disconnect();
  EXPECT_FALSE(conn.IsConnected());
}

TEST(E2EProtocol, ConnectWithReplUser) {
  mes::protocol::MysqlConnection conn;
  // This test exercises a plaintext replication-user connection. MySQL 8.4
  // provisions it with caching_sha2_password, whose full-auth RSA key lookup
  // now requires an explicit opt-in (the default rejection is covered by the
  // dedicated cold-cache auth test).
  auto rc =
      conn.Connect(kHost, kPort, kReplUser, kReplPass, kTimeout, kTimeout, 0, "", "", "", true);
  ASSERT_EQ(rc, MES_OK) << conn.GetLastError();
  EXPECT_TRUE(conn.IsConnected());
  conn.Disconnect();
}

TEST(E2EProtocol, ConnectBadPassword) {
  mes::protocol::MysqlConnection conn;
  auto rc =
      conn.Connect(kHost, kPort, kRootUser, "wrong_password", kTimeout, kTimeout, 0, "", "", "");
  EXPECT_NE(rc, MES_OK);
  EXPECT_FALSE(conn.IsConnected());
  EXPECT_FALSE(conn.GetLastError().empty());
}

TEST(E2EProtocol, ConnectBadHost) {
  mes::protocol::MysqlConnection conn;
  auto rc = conn.Connect("192.0.2.1", kPort, kRootUser, kRootPass, 2, 2, 0, "", "", "");
  EXPECT_NE(rc, MES_OK);
  EXPECT_FALSE(conn.IsConnected());
}

// -- ExecuteQuery tests --

TEST(E2EProtocol, SimpleSelect) {
  mes::protocol::MysqlConnection conn;
  ASSERT_EQ(conn.Connect(kHost, kPort, kRootUser, kRootPass, kTimeout, kTimeout, 0, "", "", ""),
            MES_OK);

  mes::protocol::QueryResult result;
  std::string err;
  auto rc =
      mes::protocol::ExecuteQuery(conn.Socket(), "SELECT 1 AS val, 'hello' AS msg", &result, &err);
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
  ASSERT_EQ(conn.Connect(kHost, kPort, kRootUser, kRootPass, kTimeout, kTimeout, 0, "", "", ""),
            MES_OK);

  mes::protocol::QueryResult result;
  std::string err;
  auto rc = mes::protocol::ExecuteQuery(
      conn.Socket(), "SHOW VARIABLES WHERE Variable_name = 'gtid_mode'", &result, &err);
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
  ASSERT_EQ(conn.Connect(kHost, kPort, kRootUser, kRootPass, kTimeout, kTimeout, 0, "", "", ""),
            MES_OK);

  mes::protocol::QueryResult result;
  std::string err;
  auto rc = mes::protocol::ExecuteQuery(conn.Socket(), "SELECT @@server_uuid", &result, &err);
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
  ASSERT_EQ(conn.Connect(kHost, kPort, kRootUser, kRootPass, kTimeout, kTimeout, 0, "", "", ""),
            MES_OK);

  mes::protocol::QueryResult result;
  std::string err;
  auto rc = mes::protocol::ExecuteQuery(conn.Socket(), "SET @source_binlog_checksum='NONE'",
                                        &result, &err);
  ASSERT_EQ(rc, MES_OK) << err;
  // SET returns no result set
  EXPECT_TRUE(result.rows.empty());

  conn.Disconnect();
}

TEST(E2EProtocol, ShowColumns) {
  mes::protocol::MysqlConnection conn;
  ASSERT_EQ(conn.Connect(kHost, kPort, kRootUser, kRootPass, kTimeout, kTimeout, 0, "", "", ""),
            MES_OK);

  mes::protocol::QueryResult result;
  std::string err;
  auto rc = mes::protocol::ExecuteQuery(conn.Socket(), "SHOW COLUMNS FROM `mes_test`.`items`",
                                        &result, &err);
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
  ASSERT_EQ(conn.Connect(kHost, kPort, kRootUser, kRootPass, kTimeout, kTimeout, 0, "", "", ""),
            MES_OK);

  for (int i = 0; i < 5; i++) {
    mes::protocol::QueryResult result;
    std::string err;
    auto rc = mes::protocol::ExecuteQuery(conn.Socket(), "SELECT " + std::to_string(i) + " AS n",
                                          &result, &err);
    ASSERT_EQ(rc, MES_OK) << "Query " << i << " failed: " << err;
    ASSERT_EQ(result.rows.size(), 1u);
    EXPECT_EQ(result.rows[0].values[0], std::to_string(i));
  }

  conn.Disconnect();
}

TEST(E2EProtocol, NullValues) {
  mes::protocol::MysqlConnection conn;
  ASSERT_EQ(conn.Connect(kHost, kPort, kRootUser, kRootPass, kTimeout, kTimeout, 0, "", "", ""),
            MES_OK);

  mes::protocol::QueryResult result;
  std::string err;
  auto rc = mes::protocol::ExecuteQuery(conn.Socket(), "SELECT NULL AS a, 'ok' AS b, NULL AS c",
                                        &result, &err);
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
  config.ssl_mode = static_cast<mes_ssl_mode_t>(e2e::DefaultSslMode());
  std::string validator_ca = e2e::DefaultCa();
  config.ssl_ca = validator_ca.empty() ? nullptr : validator_ca.c_str();
  config.ssl_cert = nullptr;
  config.ssl_key = nullptr;

  auto rc = mes_client_connect(client, &config);
  ASSERT_EQ(rc, MES_OK) << mes_client_last_error(client);
  EXPECT_EQ(mes_client_is_connected(client), 1);
  EXPECT_EQ(mes_client_is_streaming(client), 0);

  ASSERT_EQ(mes_client_start(client), MES_OK) << mes_client_last_error(client);
  EXPECT_EQ(mes_client_is_connected(client), 1);
  EXPECT_EQ(mes_client_is_streaming(client), 1);

  mes_client_stop(client);
  EXPECT_EQ(mes_client_is_connected(client), 0);
  EXPECT_EQ(mes_client_is_streaming(client), 0);

  mes_client_disconnect(client);
  EXPECT_EQ(mes_client_is_connected(client), 0);
  EXPECT_EQ(mes_client_is_streaming(client), 0);

  mes_client_destroy(client);
}

TEST(E2EProtocol, ShortReadTimeoutStaysAliveAcrossHeartbeatPeriods) {
  mes_client_t* client = mes_client_create();
  ASSERT_NE(client, nullptr);

  mes_client_config_t config{};
  config.host = kHost;
  config.port = kPort;
  config.user = kReplUser;
  config.password = kReplPass;
  config.server_id = 113;
  config.start_gtid = "";
  config.connect_timeout_s = kTimeout;
  config.read_timeout_s = 1;
  config.ssl_mode = static_cast<mes_ssl_mode_t>(e2e::DefaultSslMode());
  const std::string ca_path = e2e::DefaultCa();
  config.ssl_ca = ca_path.empty() ? nullptr : ca_path.c_str();

  ASSERT_EQ(mes_client_connect(client, &config), MES_OK) << mes_client_last_error(client);
  ASSERT_EQ(mes_client_start(client), MES_OK) << mes_client_last_error(client);

  const auto started = std::chrono::steady_clock::now();
  int heartbeat_count = 0;
  for (int i = 0; i < 32 && heartbeat_count < 3; ++i) {
    const mes_poll_result_t result = mes_client_poll(client);
    ASSERT_EQ(result.error, MES_OK) << mes_client_last_error(client);
    if (result.is_heartbeat) ++heartbeat_count;
  }
  const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::steady_clock::now() - started);

  EXPECT_EQ(heartbeat_count, 3);
  EXPECT_EQ(mes_client_is_connected(client), 1);
  EXPECT_EQ(mes_client_is_streaming(client), 1);
  EXPECT_GE(elapsed.count(), 1000);
  EXPECT_LT(elapsed.count(), 5000);

  mes_client_stop(client);
  mes_client_disconnect(client);
  mes_client_destroy(client);
}

// -- BinlogStream via C ABI --

TEST(E2EProtocol, BinlogStreamInsertAndCapture) {
  if (e2e::IsMariaDB()) {
    GTEST_SKIP() << "Uses MySQL-specific GTID queries and SSL setup";
  }
  // 1. Insert a row via a separate connection to generate a binlog event
  mes::protocol::MysqlConnection data_conn;
  ASSERT_EQ(
      data_conn.Connect(kHost, kPort, kRootUser, kRootPass, kTimeout, kTimeout, 0, "", "", ""),
      MES_OK);

  mes::protocol::QueryResult qr;
  std::string err;
  // Get current GTID position before INSERT
  ASSERT_EQ(
      mes::protocol::ExecuteQuery(data_conn.Socket(), "SELECT @@GLOBAL.gtid_executed", &qr, &err),
      MES_OK);
  std::string gtid_before = qr.rows[0].values[0];

  // Do the INSERT
  ASSERT_EQ(mes::protocol::ExecuteQuery(
                data_conn.Socket(),
                "INSERT INTO mes_test.items (name, value) VALUES ('e2e_test', 42)", &qr, &err),
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

  ASSERT_EQ(mes_client_connect(client, &config), MES_OK) << mes_client_last_error(client);
  ASSERT_EQ(mes_client_start(client), MES_OK) << mes_client_last_error(client);

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
      if (event->type == MES_EVENT_INSERT && std::strcmp(event->table, "items") == 0) {
        found_insert = true;
        // Verify we got the right data
        EXPECT_GT(event->after_count, 0u);
        break;
      }
    }
  }

  EXPECT_TRUE(found_insert) << "Did not capture INSERT event after " << poll_count << " polls";

  // Check GTID was tracked
  const char* gtid = mes_client_current_gtid(client);
  EXPECT_NE(std::strlen(gtid), 0u);

  mes_client_stop(client);
  mes_client_disconnect(client);
  mes_client_destroy(client);
  mes_destroy(engine);
}

TEST(E2EProtocol, EmptyStartGtidSnapshotsCurrentPosition) {
  if (e2e::IsMariaDB()) {
    GTEST_SKIP() << "MySQL COM_BINLOG_DUMP_GTID-specific contract";
  }

  mes::protocol::MysqlConnection data_conn;
  ASSERT_EQ(
      data_conn.Connect(kHost, kPort, kRootUser, kRootPass, kTimeout, kTimeout, 0, "", "", ""),
      MES_OK);

  const std::string suffix =
      std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
  const std::string before_name = "empty_gtid_before_" + suffix;
  const std::string after_name = "empty_gtid_after_" + suffix;
  mes::protocol::QueryResult qr;
  std::string err;
  ASSERT_EQ(
      mes::protocol::ExecuteQuery(
          data_conn.Socket(),
          "INSERT INTO mes_test.items (name, value) VALUES ('" + before_name + "', 1)", &qr, &err),
      MES_OK)
      << err;

  mes_client_t* client = mes_client_create();
  ASSERT_NE(client, nullptr);
  mes_client_config_t config{};
  config.host = kHost;
  config.port = kPort;
  config.user = kReplUser;
  config.password = kReplPass;
  config.server_id = 109;
  config.start_gtid = "";  // Contract: snapshot current executed set in StartStream().
  config.connect_timeout_s = kTimeout;
  config.read_timeout_s = 5;
  config.ssl_mode = MES_SSL_DISABLED;
  config.allow_public_key_retrieval = true;

  ASSERT_EQ(mes_client_connect(client, &config), MES_OK) << mes_client_last_error(client);
  ASSERT_EQ(mes_client_start(client), MES_OK) << mes_client_last_error(client);

  ASSERT_EQ(
      mes::protocol::ExecuteQuery(
          data_conn.Socket(),
          "INSERT INTO mes_test.items (name, value) VALUES ('" + after_name + "', 2)", &qr, &err),
      MES_OK)
      << err;

  mes_engine_t* engine = mes_create();
  ASSERT_NE(engine, nullptr);
  ASSERT_EQ(mes_set_checksum_enabled(engine, mes_client_checksum_enabled(client)), MES_OK);
  bool saw_before = false;
  bool saw_after = false;
  for (int poll_count = 0; poll_count < 100 && !saw_after; ++poll_count) {
    mes_poll_result_t result = mes_client_poll(client);
    ASSERT_EQ(result.error, MES_OK) << mes_client_last_error(client);
    if (result.is_heartbeat || result.data == nullptr) continue;

    size_t consumed = 0;
    ASSERT_EQ(mes_feed(engine, result.data, result.size, &consumed), MES_OK);
    const mes_event_t* event = nullptr;
    while (mes_next_event(engine, &event) == MES_OK) {
      if (event->type != MES_EVENT_INSERT || std::strcmp(event->table, "items") != 0 ||
          event->after_count < 2 || event->after_columns[1].type != MES_COL_STRING) {
        continue;
      }
      std::string name(event->after_columns[1].str_data, event->after_columns[1].str_len);
      saw_before = saw_before || name == before_name;
      saw_after = saw_after || name == after_name;
    }
  }

  EXPECT_FALSE(saw_before) << "empty start_gtid replayed a pre-start transaction";
  EXPECT_TRUE(saw_after) << "empty start_gtid did not deliver a post-start transaction";

  mes_client_stop(client);
  mes_client_disconnect(client);
  mes_client_destroy(client);
  mes_destroy(engine);
  data_conn.Disconnect();
}

TEST(E2EProtocol, CurrentGtidDoesNotAdvanceWhileTransactionIsOnlyQueued) {
  if (e2e::IsMariaDB()) {
    GTEST_SKIP() << "MySQL GTID event layout-specific assertion";
  }

  mes::protocol::MysqlConnection data_conn;
  ASSERT_EQ(
      data_conn.Connect(kHost, kPort, kRootUser, kRootPass, kTimeout, kTimeout, 0, "", "", ""),
      MES_OK);
  mes::protocol::QueryResult qr;
  std::string err;
  ASSERT_EQ(
      mes::protocol::ExecuteQuery(data_conn.Socket(), "SELECT @@GLOBAL.gtid_executed", &qr, &err),
      MES_OK);
  const std::string initial_gtid = qr.rows[0].values[0];

  mes_client_t* client = mes_client_create();
  ASSERT_NE(client, nullptr);
  mes_client_config_t config{};
  config.host = kHost;
  config.port = kPort;
  config.user = kReplUser;
  config.password = kReplPass;
  config.server_id = 110;
  config.start_gtid = initial_gtid.c_str();
  config.connect_timeout_s = kTimeout;
  config.read_timeout_s = 5;
  config.ssl_mode = MES_SSL_DISABLED;
  config.max_queue_size = 10000;
  config.allow_public_key_retrieval = true;
  ASSERT_EQ(mes_client_connect(client, &config), MES_OK) << mes_client_last_error(client);
  ASSERT_EQ(mes_client_start(client), MES_OK) << mes_client_last_error(client);

  ASSERT_EQ(
      mes::protocol::ExecuteQuery(
          data_conn.Socket(),
          "INSERT INTO mes_test.items (name, value) VALUES ('queued_checkpoint', 3)", &qr, &err),
      MES_OK)
      << err;
  // Let the reader thread receive and queue the complete transaction without
  // allowing the consumer to poll any of it.
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  EXPECT_EQ(EncodeGtidSet(mes_client_current_gtid(client)), EncodeGtidSet(initial_gtid));

  bool saw_xid = false;
  for (int poll_count = 0; poll_count < 100 && !saw_xid; ++poll_count) {
    mes_poll_result_t result = mes_client_poll(client);
    ASSERT_EQ(result.error, MES_OK) << mes_client_last_error(client);
    if (result.data != nullptr && result.size >= mes::kEventHeaderSize) {
      saw_xid = result.data[4] == static_cast<uint8_t>(mes::BinlogEventType::kXidEvent);
    }
  }
  ASSERT_TRUE(saw_xid);
  // Poll returned the commit marker, but it is not acknowledged until the
  // caller asks for the following event.
  EXPECT_EQ(EncodeGtidSet(mes_client_current_gtid(client)), EncodeGtidSet(initial_gtid));

  mes_poll_result_t following = mes_client_poll(client);
  ASSERT_EQ(following.error, MES_OK) << mes_client_last_error(client);
  EXPECT_NE(EncodeGtidSet(mes_client_current_gtid(client)), EncodeGtidSet(initial_gtid));

  mes_client_stop(client);
  mes_client_disconnect(client);
  mes_client_destroy(client);
  data_conn.Disconnect();
}

TEST(E2EProtocol, DeliveredCheckpointPreservesCompleteMultiSidSet) {
  if (e2e::IsMariaDB()) {
    GTEST_SKIP() << "MySQL multi-SID GTID set test";
  }

  mes::protocol::MysqlConnection data_conn;
  ASSERT_EQ(
      data_conn.Connect(kHost, kPort, kRootUser, kRootPass, kTimeout, kTimeout, 0, "", "", ""),
      MES_OK)
      << data_conn.GetLastError();

  mes::protocol::QueryResult qr;
  std::string err;
  ASSERT_EQ(
      mes::protocol::ExecuteQuery(data_conn.Socket(), "SELECT UUID(), @@server_uuid", &qr, &err),
      MES_OK)
      << err;
  ASSERT_EQ(qr.rows.size(), 1u);
  ASSERT_EQ(qr.rows[0].values.size(), 2u);
  const std::string failover_sid = qr.rows[0].values[0];
  const std::string source_sid = qr.rows[0].values[1];
  const std::string failover_gtid = failover_sid + ":1";

  ASSERT_EQ(mes::protocol::ExecuteQuery(
                data_conn.Socket(), "SET @@SESSION.GTID_NEXT = '" + failover_gtid + "'", &qr, &err),
            MES_OK)
      << err;
  ASSERT_EQ(mes::protocol::ExecuteQuery(
                data_conn.Socket(),
                "INSERT INTO mes_test.items (name, value) VALUES ('multi_sid_seed_" +
                    failover_sid.substr(0, 8) + "', 1)",
                &qr, &err),
            MES_OK)
      << err;
  ASSERT_EQ(mes::protocol::ExecuteQuery(data_conn.Socket(), "SET @@SESSION.GTID_NEXT = 'AUTOMATIC'",
                                        &qr, &err),
            MES_OK)
      << err;

  ASSERT_EQ(
      mes::protocol::ExecuteQuery(data_conn.Socket(), "SELECT @@GLOBAL.gtid_executed", &qr, &err),
      MES_OK)
      << err;
  ASSERT_EQ(qr.rows.size(), 1u);
  const std::string initial_gtid = qr.rows[0].values[0];
  ASSERT_NE(initial_gtid.find(failover_gtid), std::string::npos);

  mes_client_t* client = mes_client_create();
  ASSERT_NE(client, nullptr);
  mes_client_config_t config{};
  config.host = kHost;
  config.port = kPort;
  config.user = kReplUser;
  config.password = kReplPass;
  config.server_id = 111;
  config.start_gtid = initial_gtid.c_str();
  config.connect_timeout_s = kTimeout;
  config.read_timeout_s = 5;
  config.ssl_mode = MES_SSL_DISABLED;
  config.max_queue_size = 10000;
  config.allow_public_key_retrieval = true;
  ASSERT_EQ(mes_client_connect(client, &config), MES_OK) << mes_client_last_error(client);
  ASSERT_EQ(mes_client_start(client), MES_OK) << mes_client_last_error(client);

  ASSERT_EQ(
      mes::protocol::ExecuteQuery(
          data_conn.Socket(),
          "INSERT INTO mes_test.items (name, value) VALUES ('multi_sid_after', 2)", &qr, &err),
      MES_OK)
      << err;

  bool saw_xid = false;
  for (int poll_count = 0; poll_count < 100 && !saw_xid; ++poll_count) {
    mes_poll_result_t result = mes_client_poll(client);
    ASSERT_EQ(result.error, MES_OK) << mes_client_last_error(client);
    if (result.data != nullptr && result.size >= mes::kEventHeaderSize) {
      saw_xid = result.data[4] == static_cast<uint8_t>(mes::BinlogEventType::kXidEvent);
    }
  }
  ASSERT_TRUE(saw_xid);
  mes_poll_result_t following = mes_client_poll(client);
  ASSERT_EQ(following.error, MES_OK) << mes_client_last_error(client);

  const std::string checkpoint = mes_client_current_gtid(client);
  EXPECT_NE(checkpoint, initial_gtid);
  EXPECT_NE(checkpoint.find(failover_gtid), std::string::npos);
  EXPECT_NE(checkpoint.find(source_sid + ":"), std::string::npos);

  mes_client_stop(client);
  mes_client_disconnect(client);
  mes_client_destroy(client);
  data_conn.Disconnect();
}

TEST(E2EProtocol, MariaDBDeliveredCheckpointPreservesAllDomains) {
  if (!e2e::IsMariaDB()) {
    GTEST_SKIP() << "MariaDB multi-domain GTID set test";
  }

  mes::protocol::MysqlConnection data_conn;
  ASSERT_EQ(data_conn.Connect(kHost, kPort, kRootUser, kRootPass, kTimeout, kTimeout,
                              MES_SSL_DISABLED, "", "", ""),
            MES_OK)
      << data_conn.GetLastError();

  mes::protocol::QueryResult qr;
  std::string err;
  ASSERT_EQ(mes::protocol::ExecuteQuery(data_conn.Socket(), "SET @@SESSION.gtid_domain_id = 7001",
                                        &qr, &err),
            MES_OK)
      << err;
  ASSERT_EQ(
      mes::protocol::ExecuteQuery(
          data_conn.Socket(),
          "INSERT INTO mes_test.items (name, value) VALUES ('domain_7001_seed', 1)", &qr, &err),
      MES_OK)
      << err;
  ASSERT_EQ(mes::protocol::ExecuteQuery(data_conn.Socket(), "SET @@SESSION.gtid_domain_id = 7002",
                                        &qr, &err),
            MES_OK)
      << err;
  ASSERT_EQ(
      mes::protocol::ExecuteQuery(
          data_conn.Socket(),
          "INSERT INTO mes_test.items (name, value) VALUES ('domain_7002_seed', 1)", &qr, &err),
      MES_OK)
      << err;
  ASSERT_EQ(mes::protocol::ExecuteQuery(data_conn.Socket(), "SELECT @@GLOBAL.gtid_current_pos", &qr,
                                        &err),
            MES_OK)
      << err;
  ASSERT_EQ(qr.rows.size(), 1u);
  const std::string initial_gtid = qr.rows[0].values[0];
  ASSERT_NE(initial_gtid.find("7001-"), std::string::npos);
  ASSERT_NE(initial_gtid.find("7002-"), std::string::npos);

  mes_client_t* client = mes_client_create();
  ASSERT_NE(client, nullptr);
  mes_client_config_t config{};
  config.host = kHost;
  config.port = kPort;
  config.user = kReplUser;
  config.password = kReplPass;
  config.server_id = 112;
  config.start_gtid = initial_gtid.c_str();
  config.connect_timeout_s = kTimeout;
  config.read_timeout_s = 5;
  config.ssl_mode = MES_SSL_DISABLED;
  config.max_queue_size = 10000;
  ASSERT_EQ(mes_client_connect(client, &config), MES_OK) << mes_client_last_error(client);
  ASSERT_EQ(mes_client_start(client), MES_OK) << mes_client_last_error(client);

  ASSERT_EQ(mes::protocol::ExecuteQuery(data_conn.Socket(), "SET @@SESSION.gtid_domain_id = 7001",
                                        &qr, &err),
            MES_OK)
      << err;
  ASSERT_EQ(
      mes::protocol::ExecuteQuery(
          data_conn.Socket(),
          "INSERT INTO mes_test.items (name, value) VALUES ('domain_7001_after', 2)", &qr, &err),
      MES_OK)
      << err;

  bool saw_xid = false;
  for (int poll_count = 0; poll_count < 100 && !saw_xid; ++poll_count) {
    mes_poll_result_t result = mes_client_poll(client);
    ASSERT_EQ(result.error, MES_OK) << mes_client_last_error(client);
    if (result.data != nullptr && result.size >= mes::kEventHeaderSize) {
      saw_xid = result.data[4] == static_cast<uint8_t>(mes::BinlogEventType::kXidEvent);
    }
  }
  ASSERT_TRUE(saw_xid);
  mes_poll_result_t following = mes_client_poll(client);
  ASSERT_EQ(following.error, MES_OK) << mes_client_last_error(client);

  const std::string checkpoint = mes_client_current_gtid(client);
  EXPECT_NE(checkpoint, initial_gtid);
  EXPECT_NE(checkpoint.find("7001-"), std::string::npos);
  EXPECT_NE(checkpoint.find("7002-"), std::string::npos);

  mes_client_stop(client);
  mes_client_disconnect(client);
  mes_client_destroy(client);
  data_conn.Disconnect();
}

// -- MetadataFetcher via C ABI --

TEST(E2EProtocol, MetadataFetcherReadTimeoutAppliesAfterReconnect) {
#ifdef _WIN32
  GTEST_SKIP() << "The test proxy currently uses POSIX sockets";
#else
  if (!e2e::IsMariaDB()) {
    GTEST_SKIP() << "The plaintext proxy test uses MariaDB authentication";
  }

  StallingMetadataProxy proxy;
  ASSERT_TRUE(proxy.IsReady());

  mes::MetadataFetcher fetcher;
  ASSERT_EQ(fetcher.Connect(kHost, proxy.Port(), kRootUser, kRootPass, kTimeout,
                            1 /* read_timeout_s */, MES_SSL_DISABLED),
            MES_OK);

  const auto started = std::chrono::steady_clock::now();
  const auto columns = fetcher.FetchColumnInfo("mes_test", "items", 3);
  const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::steady_clock::now() - started);

  EXPECT_TRUE(columns.empty());
  EXPECT_EQ(proxy.StalledQueryCount(), 2);  // initial query + one reconnect retry
  EXPECT_GE(elapsed.count(), 1500);
  EXPECT_LT(elapsed.count(), 5000);
  fetcher.Disconnect();
#endif
}

TEST(E2EProtocol, MetadataCacheSeparatesDottedIdentifiers) {
  mes::protocol::MysqlConnection conn;
  const std::string ca_path = e2e::DefaultCa();
  ASSERT_EQ(conn.Connect(kHost, kPort, kRootUser, kRootPass, kTimeout, kTimeout,
                         e2e::DefaultSslMode(), ca_path, "", ""),
            MES_OK);
  mes::protocol::QueryResult qr;
  std::string err;
  auto execute = [&](const std::string& sql) {
    return mes::protocol::ExecuteQuery(conn.Socket(), sql, &qr, &err,
                                       conn.DeprecateEofNegotiated());
  };

  execute("DROP DATABASE IF EXISTS `mes_cache_a`");
  execute("DROP DATABASE IF EXISTS `mes_cache_a.b`");
  ASSERT_EQ(execute("CREATE DATABASE `mes_cache_a`"), MES_OK) << err;
  ASSERT_EQ(execute("CREATE DATABASE `mes_cache_a.b`"), MES_OK) << err;
  ASSERT_EQ(execute("CREATE TABLE `mes_cache_a`.`b.c` (`left_name` INT)"), MES_OK) << err;
  ASSERT_EQ(execute("CREATE TABLE `mes_cache_a.b`.`c` (`right_name` BIGINT UNSIGNED)"), MES_OK)
      << err;

  mes::MetadataFetcher fetcher;
  ASSERT_EQ(fetcher.Connect(kHost, kPort, kRootUser, kRootPass, kTimeout, kTimeout,
                            e2e::DefaultSslMode(), ca_path),
            MES_OK);
  const auto left = fetcher.FetchColumnInfo("mes_cache_a", "b.c", 1);
  const auto right = fetcher.FetchColumnInfo("mes_cache_a.b", "c", 1);

  ASSERT_EQ(left.size(), 1u);
  EXPECT_EQ(left[0].name, "left_name");
  EXPECT_FALSE(left[0].is_unsigned);
  ASSERT_EQ(right.size(), 1u);
  EXPECT_EQ(right[0].name, "right_name");
  EXPECT_TRUE(right[0].is_unsigned);

  fetcher.Disconnect();
  ASSERT_EQ(execute("DROP DATABASE `mes_cache_a`"), MES_OK) << err;
  ASSERT_EQ(execute("DROP DATABASE `mes_cache_a.b`"), MES_OK) << err;
  conn.Disconnect();
}

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
  ASSERT_EQ(
      data_conn.Connect(kHost, kPort, kRootUser, kRootPass, kTimeout, kTimeout, 0, "", "", ""),
      MES_OK);

  mes::protocol::QueryResult qr;
  std::string err;
  ASSERT_EQ(
      mes::protocol::ExecuteQuery(data_conn.Socket(), "SELECT @@GLOBAL.gtid_executed", &qr, &err),
      MES_OK);
  std::string gtid_before = qr.rows[0].values[0];

  ASSERT_EQ(mes::protocol::ExecuteQuery(
                data_conn.Socket(),
                "INSERT INTO mes_test.items (name, value) VALUES ('meta_test', 99)", &qr, &err),
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

  ASSERT_EQ(mes_client_connect(client, &config), MES_OK) << mes_client_last_error(client);
  ASSERT_EQ(mes_client_start(client), MES_OK) << mes_client_last_error(client);

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
      if (event->type == MES_EVENT_INSERT && std::strcmp(event->table, "items") == 0) {
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
  ASSERT_EQ(
      setup_conn.Connect(kHost, kPort, kRootUser, kRootPass, kTimeout, kTimeout, 0, "", "", ""),
      MES_OK);
  mes::protocol::QueryResult qr;
  std::string err;
  ASSERT_EQ(
      mes::protocol::ExecuteQuery(setup_conn.Socket(), "SELECT @@GLOBAL.gtid_executed", &qr, &err),
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

  ASSERT_EQ(mes_client_connect(client, &config), MES_OK) << mes_client_last_error(client);
  ASSERT_EQ(mes_client_start(client), MES_OK) << mes_client_last_error(client);

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
  EXPECT_LT(std::chrono::duration_cast<std::chrono::seconds>(elapsed).count(), 10);

  mes_client_disconnect(client);
  mes_client_destroy(client);
}

}  // namespace
