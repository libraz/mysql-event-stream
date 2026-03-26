// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * @file binlog_client.h
 * @brief MySQL binlog streaming client using COM_BINLOG_DUMP_GTID
 *
 * Internally uses a dedicated reader thread and bounded event queue to
 * decouple network I/O from consumer processing, preventing stream
 * disconnection when the consumer is temporarily slow.
 */

#ifndef MES_CLIENT_BINLOG_CLIENT_H_
#define MES_CLIENT_BINLOG_CLIENT_H_

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "client/event_queue.h"
#include "mes.h"
#include "protocol/mysql_binlog_stream.h"
#include "protocol/mysql_connection.h"

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
  mes_ssl_mode_t ssl_mode = MES_SSL_DISABLED;
  std::string ssl_ca;      // Path to CA certificate file
  std::string ssl_cert;    // Path to client certificate file
  std::string ssl_key;     // Path to client private key file
  size_t max_queue_size = 10000;  // 0 = use default (10000)
};

/**
 * @brief Result of a single Poll() call
 */
struct PollResult {
  mes_error_t error = MES_OK;
  const uint8_t* data = nullptr;  // Valid until next Poll() call
  size_t size = 0;
  bool is_heartbeat = false;
};

/**
 * @brief MySQL binlog streaming client with internal buffering
 *
 * Connects to MySQL and receives binlog events via COM_BINLOG_DUMP_GTID.
 * A dedicated reader thread continuously reads from the socket and pushes
 * events into a bounded queue. Poll() dequeues from this buffer.
 *
 * Thread safety:
 *   - Stop() may be called from any thread to interrupt a blocking Poll().
 *   - GetCurrentGtid() may be called from any thread.
 *   - All other methods must be called from a single thread.
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
   * @brief Start binlog streaming with a dedicated reader thread
   * @return MES_OK on success
   */
  mes_error_t StartStream();

  /**
   * @brief Poll for next binlog event (blocking)
   *
   * Blocks until an event is available in the internal queue or the
   * stream is stopped. Data pointer is valid until the next Poll() call.
   *
   * @return PollResult with event data or error
   */
  PollResult Poll();

  /** @brief Request stream stop from any thread. Unblocks a pending Poll(). */
  void Stop();

  /** @brief Disconnect from MySQL server */
  void Disconnect();

  /** @brief Check if connected */
  bool IsConnected() const;

  /** @brief Get last error message */
  const char* GetLastError() const;

  /** @brief Get current GTID position (thread-safe) */
  const char* GetCurrentGtid() const;

 private:
  protocol::MysqlConnection conn_;
  protocol::BinlogStream binlog_stream_;
  BinlogClientConfig config_;
  std::vector<uint8_t> gtid_encoded_;
  std::string last_error_;
  bool streaming_ = false;
  std::atomic<bool> stop_requested_{false};

  // Reader thread infrastructure
  std::unique_ptr<EventQueue> event_queue_;
  std::thread reader_thread_;
  QueuedEvent current_event_;  // Holds data for current Poll() result
  std::mutex stop_mutex_;      // Serializes Stop() calls

  // GTID tracking (reader thread writes, GetCurrentGtid reads)
  std::string current_gtid_;
  mutable std::mutex gtid_mutex_;
  mutable std::string gtid_snapshot_;  // For safe c_str() return

  /** @brief Reader thread main loop */
  void ReaderLoop();

  /** @brief Stop reader thread, join, clear queue */
  void StopReaderThread();

  /** @brief Update current_gtid_ from GTID_LOG_EVENT (thread-safe) */
  void UpdateGtidFromEvent(const uint8_t* event_data, size_t event_size);
};

}  // namespace mes

#endif  // MES_CLIENT_BINLOG_CLIENT_H_
