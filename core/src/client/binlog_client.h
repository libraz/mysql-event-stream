// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * @file binlog_client.h
 * @brief MySQL binlog streaming client using COM_BINLOG_DUMP_GTID
 */

#ifndef MES_CLIENT_BINLOG_CLIENT_H_
#define MES_CLIENT_BINLOG_CLIENT_H_

#ifdef MES_HAS_MYSQL

#include <mysql.h>

#include <cstdint>
#include <string>
#include <vector>

#include "mes.h"

namespace mes {

/**
 * @brief Configuration for BinlogClient
 */
struct BinlogClientConfig {
  std::string host = "127.0.0.1";
  uint16_t port = 3306;
  std::string user;
  std::string password;
  uint32_t server_id = 1;
  std::string start_gtid;  // Empty = start from current position
  uint32_t connect_timeout_s = 10;
  uint32_t read_timeout_s = 30;
};

/**
 * @brief Result of a single Poll() call
 */
struct PollResult {
  mes_error_t error = MES_OK;
  const uint8_t* data = nullptr;  // Points to RPL buffer (valid until next
                                  // Poll())
  size_t size = 0;
  bool is_heartbeat = false;
};

/**
 * @brief MySQL binlog streaming client
 *
 * Connects to MySQL and receives binlog events via COM_BINLOG_DUMP_GTID.
 * Synchronous polling model - caller controls the loop.
 *
 * Usage:
 *   BinlogClient client;
 *   client.Connect(config);
 *   client.StartStream();
 *   while (client.IsConnected()) {
 *     auto result = client.Poll();
 *     if (result.data) engine.Feed(result.data, result.size);
 *   }
 */
class BinlogClient {
 public:
  BinlogClient();
  ~BinlogClient();

  // Non-copyable, non-movable
  BinlogClient(const BinlogClient&) = delete;
  BinlogClient& operator=(const BinlogClient&) = delete;
  BinlogClient(BinlogClient&&) = delete;
  BinlogClient& operator=(BinlogClient&&) = delete;

  /**
   * @brief Connect to MySQL server and validate configuration
   * @param config Connection parameters
   * @return MES_OK on success
   */
  mes_error_t Connect(const BinlogClientConfig& config);

  /**
   * @brief Start binlog streaming
   *
   * Sends SET @source_binlog_checksum, SET @master_heartbeat_period,
   * encodes GTID set, and calls mysql_binlog_open().
   *
   * @return MES_OK on success
   */
  mes_error_t StartStream();

  /**
   * @brief Poll for next binlog event (blocking)
   * @return PollResult with event data or error
   */
  PollResult Poll();

  /** @brief Disconnect from MySQL server */
  void Disconnect();

  /** @brief Check if connected */
  bool IsConnected() const;

  /** @brief Get last error message */
  const char* GetLastError() const;

  /** @brief Get current GTID position */
  const char* GetCurrentGtid() const;

 private:
  MYSQL* conn_ = nullptr;
  MYSQL_RPL rpl_{};
  BinlogClientConfig config_;
  std::vector<uint8_t> gtid_encoded_;
  std::string current_gtid_;
  std::string last_error_;
  bool streaming_ = false;

  /** @brief MYSQL_RPL callback to copy GTID data */
  static void FixGtidSetCallback(MYSQL_RPL* rpl,
                                 unsigned char* packet_gtid_set);

  /** @brief Update current_gtid_ from GTID_LOG_EVENT */
  void UpdateGtidFromEvent(const uint8_t* event_data, size_t event_size);
};

}  // namespace mes

#endif  // MES_HAS_MYSQL
#endif  // MES_CLIENT_BINLOG_CLIENT_H_
