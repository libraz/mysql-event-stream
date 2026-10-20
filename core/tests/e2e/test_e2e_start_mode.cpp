// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * @file test_e2e_start_mode.cpp
 * @brief E2E tests for the state every start mode must establish
 *
 * mes_client_start() succeeds through three start modes (current position,
 * explicit GTID, file offset) on two flavours. The set of state established
 * before the first event must not vary between them: the source's checksum
 * mode is detected, and the checkpoint is either a real GTID set or reported
 * as absent. Run once per flavour; DB_FLAVOR selects which container is up.
 *
 * Requires a running MySQL 8.4+ or MariaDB 10.11+ instance at localhost:13308.
 *
 * Start with: cd e2e/docker && docker compose up -d
 */

#include <gtest/gtest.h>

#include <algorithm>
#include <cctype>
#include <chrono>
#include <string>
#include <thread>

#include "client/gtid_set.h"
#include "mes.h"
#include "test_e2e_helpers.h"

using namespace e2e;

namespace {

struct StartModeCase {
  const char* name;
  mes_start_position_mode_t mode;
  uint32_t server_id;
};

/// Owns the strings referenced by mes_client_config_t for one connection.
struct ClientFixture {
  mes_client_t* client = nullptr;
  std::string ca;
  std::string gtid;
  std::string binlog_file;

  ~ClientFixture() {
    if (client == nullptr) return;
    mes_client_stop(client);
    mes_client_disconnect(client);
    mes_client_destroy(client);
  }
};

TEST(E2EStartMode, EveryStartModeEstablishesChecksumAndCheckpointState) {
  const std::string checksum_setting = GetBinlogChecksumSetting();
  ASSERT_TRUE(checksum_setting == "CRC32" || checksum_setting == "NONE")
      << "unexpected binlog_checksum: " << checksum_setting;
  const int expected_checksum = checksum_setting == "CRC32" ? 1 : 0;

  // Guarantee the source has written at least one transaction, so the GTID
  // start modes have a non-empty set to report as their checkpoint.
  ASSERT_EQ(ExecuteDML("INSERT INTO mes_test.items (name, value) VALUES ('start_mode', 1)"),
            MES_OK);
  ScopedCleanup cleanup("DELETE FROM mes_test.items WHERE name = 'start_mode'");

  const std::string current_gtid = GetCurrentGtid();
  ASSERT_FALSE(current_gtid.empty());
  const BinlogCoordinates coords = GetCurrentBinlogCoordinates();
  ASSERT_FALSE(coords.file.empty());
  ASSERT_GE(coords.position, 4u);

  const StartModeCase cases[] = {
      {"current position", MES_START_AT_CURRENT, server_ids::kStartModeCurrent},
      {"explicit gtid", MES_START_AT_GTID, server_ids::kStartModeExplicitGtid},
      {"file position", MES_START_AT_POSITION, server_ids::kStartModeFilePosition},
  };

  for (const StartModeCase& start_mode : cases) {
    SCOPED_TRACE(std::string(GetServerFlavorName(GetDbFlavor())) + " / " + start_mode.name);

    ClientFixture fixture;
    fixture.client = mes_client_create();
    ASSERT_NE(fixture.client, nullptr);
    fixture.ca = DefaultCa();
    fixture.gtid = current_gtid;
    fixture.binlog_file = coords.file;

    mes_client_config_t config{};
    config.host = kHost;
    config.port = kPort;
    config.user = kReplUser;
    config.password = kReplPass;
    config.server_id = start_mode.server_id;
    config.connect_timeout_s = kTimeout;
    config.read_timeout_s = kTimeout;
    config.ssl_mode = static_cast<mes_ssl_mode_t>(DefaultSslMode());
    config.ssl_ca = fixture.ca.empty() ? nullptr : fixture.ca.c_str();
    config.start_position_mode = start_mode.mode;
    if (start_mode.mode == MES_START_AT_GTID) {
      config.start_gtid = fixture.gtid.c_str();
    } else if (start_mode.mode == MES_START_AT_POSITION) {
      config.binlog_file = fixture.binlog_file.c_str();
      config.binlog_position = coords.position;
    }

    ASSERT_EQ(mes_client_connect(fixture.client, &config), MES_OK)
        << mes_client_last_error(fixture.client);
    ASSERT_EQ(mes_client_start(fixture.client), MES_OK) << mes_client_last_error(fixture.client);

    // Checksum detection runs for every mode, so the reported mode always
    // matches the source's actual setting.
    EXPECT_EQ(mes_client_checksum_enabled(fixture.client), expected_checksum);

    const char* checkpoint = mes_client_current_gtid(fixture.client);
    ASSERT_NE(checkpoint, nullptr);
    if (start_mode.mode == MES_START_AT_POSITION) {
      // A file offset carries no GTID. The checkpoint must read as absent, not
      // as the empty GTID set (which would mean "replay everything retained").
      EXPECT_STREQ(checkpoint, "");
    } else {
      EXPECT_STRNE(checkpoint, "");
    }
  }
}

TEST(E2EStartMode, FilePositionStartWithholdsTheCheckpointUntilThePreviousGtidsOfALaterFile) {
  const BinlogCoordinates coords = GetCurrentBinlogCoordinates();
  ASSERT_FALSE(coords.file.empty());
  ASSERT_GE(coords.position, 4u);

  ClientFixture fixture;
  fixture.client = mes_client_create();
  ASSERT_NE(fixture.client, nullptr);
  fixture.ca = DefaultCa();
  fixture.binlog_file = coords.file;

  mes_client_config_t config{};
  config.host = kHost;
  config.port = kPort;
  config.user = kReplUser;
  config.password = kReplPass;
  config.server_id = server_ids::kStartModeFilePositionCheckpoint;
  config.connect_timeout_s = kTimeout;
  config.read_timeout_s = kTimeout;
  config.ssl_mode = static_cast<mes_ssl_mode_t>(DefaultSslMode());
  config.ssl_ca = fixture.ca.empty() ? nullptr : fixture.ca.c_str();
  config.start_position_mode = MES_START_AT_POSITION;
  config.binlog_file = fixture.binlog_file.c_str();
  config.binlog_position = coords.position;

  ASSERT_EQ(mes_client_connect(fixture.client, &config), MES_OK)
      << mes_client_last_error(fixture.client);
  ASSERT_EQ(mes_client_start(fixture.client), MES_OK) << mes_client_last_error(fixture.client);
  ASSERT_STREQ(mes_client_current_gtid(fixture.client), "");

  ScopedCleanup cleanup("DELETE FROM mes_test.items WHERE name = 'position_checkpoint'");
  ASSERT_EQ(
      ExecuteDML("INSERT INTO mes_test.items (name, value) VALUES ('position_checkpoint', 7)"),
      MES_OK);

  // A mid-file offset start has seen no PREVIOUS_GTIDS / GTID_LIST of its own
  // file, so a commit alone cannot make a complete checkpoint.
  constexpr uint8_t kXidEvent = 16;
  constexpr uint8_t kPreviousGtidsEvent = 35;
  constexpr uint8_t kMariaDbGtidListEvent = 163;
  const auto event_type = [](const mes_poll_result_t& result) {
    return result.data != nullptr && result.size > 4 ? result.data[4] : uint8_t{0};
  };

  bool saw_commit = false;
  for (int i = 0; i < 40 && !saw_commit; i++) {
    const mes_poll_result_t result = mes_client_poll(fixture.client);
    ASSERT_EQ(result.error, MES_OK) << mes_client_last_error(fixture.client);
    ASSERT_STREQ(mes_client_current_gtid(fixture.client), "");
    saw_commit = event_type(result) == kXidEvent;
  }
  ASSERT_TRUE(saw_commit) << "the first commit never arrived";

  // A new file opens with the full executed set, which is what completes the checkpoint.
  ASSERT_EQ(ExecuteDML("FLUSH BINARY LOGS"), MES_OK);
  ASSERT_EQ(
      ExecuteDML("INSERT INTO mes_test.items (name, value) VALUES ('position_checkpoint', 8)"),
      MES_OK);
  const std::string executed = GetCurrentGtid();
  ASSERT_FALSE(executed.empty());

  // The server prints a single-transaction interval as N where the library
  // prints N-N, so MySQL sets are compared parsed; MariaDB positions are not
  // sets and compare as text without whitespace.
  const auto canonical = [](std::string text) {
    mes::GtidSet set;
    if (mes::GtidSet::Parse(text, &set) == MES_OK) return set.ToString();
    text.erase(std::remove_if(text.begin(), text.end(),
                              [](unsigned char c) { return std::isspace(c) != 0; }),
               text.end());
    return text;
  };
  bool passed_gtid_set_event = false;
  std::string checkpoint;
  for (int i = 0; i < 80 && canonical(checkpoint) != canonical(executed); i++) {
    const mes_poll_result_t result = mes_client_poll(fixture.client);
    ASSERT_EQ(result.error, MES_OK) << mes_client_last_error(fixture.client);
    checkpoint = mes_client_current_gtid(fixture.client);
    if (!passed_gtid_set_event) {
      EXPECT_EQ(checkpoint, "") << "checkpoint published before the file's executed set";
    }
    const uint8_t type = event_type(result);
    passed_gtid_set_event =
        passed_gtid_set_event || type == kPreviousGtidsEvent || type == kMariaDbGtidListEvent;
  }
  EXPECT_TRUE(passed_gtid_set_event);
  EXPECT_EQ(canonical(checkpoint), canonical(executed));
}

}  // namespace
