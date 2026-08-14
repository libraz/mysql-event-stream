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

#include <chrono>
#include <string>
#include <thread>

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

TEST(E2EStartMode, FilePositionStartPublishesACheckpointAfterTheFirstCommit) {
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

  // The GTID tracker is seeded for this flavour even without a start GTID, so
  // the first committed transaction turns into a resumable checkpoint.
  std::string checkpoint;
  for (int i = 0; i < 20 && checkpoint.empty(); i++) {
    const mes_poll_result_t result = mes_client_poll(fixture.client);
    if (result.error != MES_OK) break;
    checkpoint = mes_client_current_gtid(fixture.client);
  }
  EXPECT_FALSE(checkpoint.empty())
      << "no checkpoint published after a commit on a file/position start";
}

}  // namespace
