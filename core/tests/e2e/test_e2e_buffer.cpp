// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * @file test_e2e_buffer.cpp
 * @brief E2E tests for reader thread buffering behavior
 *
 * Tests slow consumer resilience, stop unblocking, high-throughput bursts,
 * configurable queue size, and graceful shutdown.
 *
 * Requires a running MySQL 8.4 instance at localhost:13308 with:
 *   - root/test_root_password (mysql_native_password)
 *   - repl_user/test_password (replication grants)
 *   - Database: mes_test with tables: users, items
 *
 * Start with: cd e2e/docker && docker compose up -d
 */

#include <gtest/gtest.h>

#include <chrono>
#include <string>
#include <thread>
#include <vector>

#include "mes.h"
#include "protocol/mysql_connection.h"
#include "protocol/mysql_query.h"
#include "test_e2e_helpers.h"

using namespace e2e;

namespace {

TEST(E2EBuffer, SlowConsumerDoesNotKillStream) {
  auto gtid = GetCurrentGtid();
  ASSERT_FALSE(gtid.empty()) << "Could not get current GTID";

  // Insert 3 rows
  for (int i = 0; i < 3; i++) {
    auto rc = ExecuteDML(
        "INSERT INTO mes_test.items (name, value) VALUES ('slow_" +
        std::to_string(i) + "', " + std::to_string(i) + ")");
    ASSERT_EQ(rc, MES_OK);
  }

  mes_client_t* client = mes_client_create();
  ASSERT_NE(client, nullptr);

  mes_client_config_t config{};
  config.host = kHost;
  config.port = kPort;
  config.user = kReplUser;
  config.password = kReplPass;
  config.server_id = 700;
  config.start_gtid = gtid.c_str();
  config.connect_timeout_s = kTimeout;
  config.read_timeout_s = 10;
  config.ssl_mode = static_cast<mes_ssl_mode_t>(DefaultSslMode());
  std::string slow_ca = DefaultCa();
  config.ssl_ca = slow_ca.empty() ? nullptr : slow_ca.c_str();

  ASSERT_EQ(mes_client_connect(client, &config), MES_OK);
  ASSERT_EQ(mes_client_start(client), MES_OK);

  // Poll one event
  auto result = mes_client_poll(client);
  EXPECT_EQ(result.error, MES_OK);

  // Simulate slow consumer
  std::this_thread::sleep_for(std::chrono::seconds(5));

  // Stream should still be alive; poll more events
  int events_received = 0;
  for (int i = 0; i < 50; i++) {
    result = mes_client_poll(client);
    if (result.error != MES_OK) break;
    if (result.data != nullptr && !result.is_heartbeat) {
      events_received++;
    }
    if (events_received >= 2) break;
  }
  EXPECT_GE(events_received, 2);

  mes_client_stop(client);
  mes_client_disconnect(client);
  mes_client_destroy(client);
}

// Consumer sleeps longer than read_timeout.
// Without the reader thread, this would cause MES_ERR_STREAM (socket timeout).
// With the reader thread, the reader keeps draining the socket while the
// consumer sleeps, so no timeout occurs.
TEST(E2EBuffer, SlowConsumerExceedsReadTimeout) {
  auto gtid = GetCurrentGtid();
  ASSERT_FALSE(gtid.empty());

  // Insert rows before starting stream
  for (int i = 0; i < 3; i++) {
    ASSERT_EQ(ExecuteDML("INSERT INTO mes_test.items (name, value) VALUES "
                         "('exceed_" + std::to_string(i) + "', " +
                         std::to_string(i) + ")"),
              MES_OK);
  }

  mes_client_t* client = mes_client_create();
  ASSERT_NE(client, nullptr);

  mes_client_config_t config{};
  config.host = kHost;
  config.port = kPort;
  config.user = kReplUser;
  config.password = kReplPass;
  config.server_id = 710;
  config.start_gtid = gtid.c_str();
  config.connect_timeout_s = kTimeout;
  config.read_timeout_s = 3;  // Very short — 3 seconds
  config.ssl_mode = static_cast<mes_ssl_mode_t>(DefaultSslMode());
  std::string exceed_ca = DefaultCa();
  config.ssl_ca = exceed_ca.empty() ? nullptr : exceed_ca.c_str();

  ASSERT_EQ(mes_client_connect(client, &config), MES_OK);
  ASSERT_EQ(mes_client_start(client), MES_OK);

  // Poll one event to confirm stream is working
  auto r = mes_client_poll(client);
  ASSERT_EQ(r.error, MES_OK) << "Initial poll failed";

  // Sleep LONGER than read_timeout (3s) — this would kill the old sync model
  std::this_thread::sleep_for(std::chrono::seconds(5));

  // Stream should still be alive because reader thread kept reading
  int events_after = 0;
  for (int i = 0; i < 50; i++) {
    r = mes_client_poll(client);
    if (r.error != MES_OK) break;
    if (r.data != nullptr) events_after++;
    if (events_after >= 2) break;
  }
  EXPECT_GE(events_after, 2)
      << "Stream died despite reader thread (consumer slept > read_timeout)";

  mes_client_stop(client);
  mes_client_disconnect(client);
  mes_client_destroy(client);
}

// Insert events WHILE the consumer is sleeping.
// Reader thread should buffer them, and consumer should get them after waking.
TEST(E2EBuffer, EventsBufferedDuringConsumerSleep) {
  auto gtid = GetCurrentGtid();
  ASSERT_FALSE(gtid.empty());

  mes_client_t* client = mes_client_create();
  ASSERT_NE(client, nullptr);

  mes_client_config_t config{};
  config.host = kHost;
  config.port = kPort;
  config.user = kReplUser;
  config.password = kReplPass;
  config.server_id = 711;
  config.start_gtid = gtid.c_str();
  config.connect_timeout_s = kTimeout;
  config.read_timeout_s = 3;
  config.ssl_mode = static_cast<mes_ssl_mode_t>(DefaultSslMode());
  std::string sleep_ca = DefaultCa();
  config.ssl_ca = sleep_ca.empty() ? nullptr : sleep_ca.c_str();

  ASSERT_EQ(mes_client_connect(client, &config), MES_OK);
  ASSERT_EQ(mes_client_start(client), MES_OK);

  // Insert events in a background thread while consumer "sleeps"
  std::thread inserter([&]() {
    // Stagger inserts over 3 seconds
    for (int i = 0; i < 5; i++) {
      ExecuteDML("INSERT INTO mes_test.items (name, value) VALUES "
                 "('during_sleep_" + std::to_string(i) + "', " +
                 std::to_string(i) + ")");
      std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
  });

  // Consumer sleeps for 4 seconds (> read_timeout of 3s)
  std::this_thread::sleep_for(std::chrono::seconds(4));

  inserter.join();

  // Now poll — reader thread should have buffered all events during sleep
  mes_engine_t* engine = mes_create();
  ASSERT_NE(engine, nullptr);

  int item_count = 0;
  for (int i = 0; i < 200; i++) {
    auto r = mes_client_poll(client);
    if (r.error != MES_OK) break;
    if (r.data == nullptr) continue;

    size_t consumed = 0;
    mes_feed(engine, r.data, r.size, &consumed);

    const mes_event_t* event = nullptr;
    while (mes_next_event(engine, &event) == MES_OK) {
      if (std::strcmp(event->table, "items") == 0) {
        item_count++;
      }
    }
    if (item_count >= 5) break;
  }

  EXPECT_GE(item_count, 5)
      << "Events inserted during consumer sleep were not buffered";

  mes_client_stop(client);
  mes_client_disconnect(client);
  mes_client_destroy(client);
  mes_destroy(engine);
}

TEST(E2EBuffer, StopUnblocksPoll) {
  auto gtid = GetCurrentGtid();
  ASSERT_FALSE(gtid.empty()) << "Could not get current GTID";

  mes_client_t* client = mes_client_create();
  ASSERT_NE(client, nullptr);

  mes_client_config_t config{};
  config.host = kHost;
  config.port = kPort;
  config.user = kReplUser;
  config.password = kReplPass;
  config.server_id = 702;
  config.start_gtid = gtid.c_str();
  config.connect_timeout_s = kTimeout;
  config.read_timeout_s = 30;
  config.ssl_mode = static_cast<mes_ssl_mode_t>(DefaultSslMode());
  std::string stop_ca = DefaultCa();
  config.ssl_ca = stop_ca.empty() ? nullptr : stop_ca.c_str();

  ASSERT_EQ(mes_client_connect(client, &config), MES_OK);
  ASSERT_EQ(mes_client_start(client), MES_OK);

  auto start = std::chrono::steady_clock::now();

  std::thread poller([&]() {
    // This will block since no new events are being inserted
    mes_client_poll(client);
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(500));

  mes_client_stop(client);

  poller.join();

  auto elapsed = std::chrono::steady_clock::now() - start;
  auto elapsed_s =
      std::chrono::duration_cast<std::chrono::seconds>(elapsed).count();
  EXPECT_LT(elapsed_s, 5) << "Stop did not unblock poll quickly enough";

  mes_client_disconnect(client);
  mes_client_destroy(client);
}

TEST(E2EBuffer, HighThroughputBurst) {
  auto gtid = GetCurrentGtid();
  ASSERT_FALSE(gtid.empty()) << "Could not get current GTID";

  // Insert 50 rows via a single connection for speed
  {
    mes::protocol::MysqlConnection conn;
    ASSERT_EQ(conn.Connect(kHost, kPort, kRootUser, kRootPass, kTimeout,
                           kTimeout, 0, "", "", ""),
              MES_OK);
    for (int i = 0; i < 50; i++) {
      mes::protocol::QueryResult qr;
      std::string err;
      ASSERT_EQ(
          mes::protocol::ExecuteQuery(
              conn.Socket(),
              "INSERT INTO mes_test.items (name, value) VALUES ('burst_" +
                  std::to_string(i) + "', " + std::to_string(i) + ")",
              &qr, &err),
          MES_OK)
          << err;
    }
  }

  // 50 INSERTs × ~5 binlog events each = ~250 packets needed
  auto events = CaptureEvents(
      gtid, 704,
      [](const std::vector<CapturedEvent>& evts) {
        size_t n = 0;
        for (const auto& e : evts) {
          if (e.table == "items") n++;
        }
        return n >= 50;
      },
      500);
  auto item_events = FilterByTable(events, "items");
  EXPECT_GE(item_events.size(), 50u);
}

TEST(E2EBuffer, QueueSizeConfigurable) {
  auto gtid = GetCurrentGtid();
  ASSERT_FALSE(gtid.empty()) << "Could not get current GTID";

  // Insert 10 rows
  for (int i = 0; i < 10; i++) {
    auto rc = ExecuteDML(
        "INSERT INTO mes_test.items (name, value) VALUES ('qsize_" +
        std::to_string(i) + "', " + std::to_string(i) + ")");
    ASSERT_EQ(rc, MES_OK);
  }

  mes_client_t* client = mes_client_create();
  ASSERT_NE(client, nullptr);

  mes_client_config_t config{};
  config.host = kHost;
  config.port = kPort;
  config.user = kReplUser;
  config.password = kReplPass;
  config.server_id = 705;
  config.start_gtid = gtid.c_str();
  config.connect_timeout_s = kTimeout;
  config.read_timeout_s = 5;
  config.ssl_mode = static_cast<mes_ssl_mode_t>(DefaultSslMode());
  std::string qsize_ca = DefaultCa();
  config.ssl_ca = qsize_ca.empty() ? nullptr : qsize_ca.c_str();
  config.max_queue_size = 5;

  ASSERT_EQ(mes_client_connect(client, &config), MES_OK);
  ASSERT_EQ(mes_client_start(client), MES_OK);

  mes_engine_t* engine = mes_create();
  ASSERT_NE(engine, nullptr);

  int events_received = 0;
  for (int i = 0; i < 200; i++) {
    auto result = mes_client_poll(client);
    if (result.error != MES_OK) break;
    if (result.is_heartbeat || result.data == nullptr) continue;

    size_t consumed = 0;
    mes_feed(engine, result.data, result.size, &consumed);

    const mes_event_t* event = nullptr;
    while (mes_next_event(engine, &event) == MES_OK) {
      events_received++;
    }
    if (events_received >= 10) break;
  }

  EXPECT_GE(events_received, 10)
      << "Small queue size should not lose events";

  mes_client_stop(client);
  mes_client_disconnect(client);
  mes_client_destroy(client);
  mes_destroy(engine);
}

TEST(E2EBuffer, GracefulShutdownNoLeak) {
  // Connect, start, poll a few events, stop, disconnect — repeat 3 times.
  // Verifies no crash or hang on repeated lifecycle.
  for (int round = 0; round < 3; round++) {
    auto gtid = GetCurrentGtid();
    ASSERT_FALSE(gtid.empty()) << "Round " << round << ": no GTID";

    ExecuteDML(
        "INSERT INTO mes_test.items (name, value) VALUES ('shutdown_" +
        std::to_string(round) + "', " + std::to_string(round) + ")");

    mes_client_t* client = mes_client_create();
    ASSERT_NE(client, nullptr);

    mes_client_config_t config{};
    config.host = kHost;
    config.port = kPort;
    config.user = kReplUser;
    config.password = kReplPass;
    config.server_id = 703;
    config.start_gtid = gtid.c_str();
    config.connect_timeout_s = kTimeout;
    config.read_timeout_s = 3;
    config.ssl_mode = static_cast<mes_ssl_mode_t>(DefaultSslMode());
    std::string shutdown_ca = DefaultCa();
    config.ssl_ca = shutdown_ca.empty() ? nullptr : shutdown_ca.c_str();

    ASSERT_EQ(mes_client_connect(client, &config), MES_OK)
        << "Round " << round;
    ASSERT_EQ(mes_client_start(client), MES_OK) << "Round " << round;

    // Poll a few events
    for (int i = 0; i < 5; i++) {
      auto result = mes_client_poll(client);
      if (result.error != MES_OK) break;
    }

    mes_client_stop(client);
    mes_client_disconnect(client);
    mes_client_destroy(client);
  }
}

}  // namespace
