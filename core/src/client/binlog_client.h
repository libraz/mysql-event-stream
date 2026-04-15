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
#include "server_flavor.h"

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
  std::string ssl_ca;             // Path to CA certificate file
  std::string ssl_cert;           // Path to client certificate file
  std::string ssl_key;            // Path to client private key file
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

  /** @brief Get the current GTID position as a string.
   *
   * Thread-safe: protected by gtid_mutex_. The returned pointer is valid
   * until the next call to GetCurrentGtid() from the same thread.
   * At the C ABI boundary (mes_client_current_gtid), this is documented
   * as single-owner-thread for pointer lifetime safety.
   */
  const char* GetCurrentGtid() const;

  /** @brief Get total CRC32 checksum errors detected (thread-safe) */
  uint64_t GetCRCErrors() const;

 private:
  protocol::MysqlConnection conn_;
  protocol::BinlogStream binlog_stream_;
  ServerFlavor server_flavor_ = ServerFlavor::kMySQL;
  BinlogClientConfig config_;
  std::vector<uint8_t> gtid_encoded_;
  // last_error_ is written from both the owner thread (Connect/StartStream/
  // Poll) and any thread calling Stop()/Disconnect(). Protect with its own
  // mutex so GetLastError() can snapshot safely.
  mutable std::mutex last_error_mutex_;
  std::string last_error_;
  mutable std::string last_error_snapshot_;  // stable buffer for c_str()
  // streaming_ is "reader thread is alive and Poll() may dequeue events".
  // Written by Poll() on error/drain and by StopReaderThread() under
  // stop_mutex_. It is also read from Poll() before taking any lock, so
  // make it atomic to avoid torn reads / data races.
  //
  // NOTE(review): Poll() reads event_queue_ without a lock. The thread
  // contract (see class-level Doxygen) requires that Poll(), Connect(),
  // and StartStream() be serialised on the single owner thread, so the
  // event_queue_ unique_ptr cannot be reassigned by StartStream() while
  // a Poll() on the same thread is in progress. Stop() may run from any
  // thread but does not reassign event_queue_; it only Close()s it.
  std::atomic<bool> streaming_{false};
  bool checksum_enabled_ = true;  // Whether server uses CRC32 binlog checksum
  std::atomic<bool> stop_requested_{false};

  // Reader thread infrastructure
  std::unique_ptr<EventQueue> event_queue_;
  std::thread reader_thread_;
  QueuedEvent current_event_;  // Holds data for current Poll() result
  std::mutex stop_mutex_;      // Serializes Stop() calls

  // GTID tracking (reader thread writes, GetCurrentGtid reads)
  std::string current_gtid_;
  // NOTE(review): gtid_snapshot_ is written under gtid_mutex_ every call.
  // Per mes.h contract, the returned pointer is valid only until the next
  // GetCurrentGtid() call on the same BinlogClient. Concurrent callers from
  // different threads may see the buffer re-assigned; single-owner-thread
  // usage is required for stable pointer reads.
  mutable std::string gtid_snapshot_;  // protected by gtid_mutex_
  mutable std::mutex gtid_mutex_;

  /** @brief Set last_error_ under its mutex (safe from any thread). */
  void SetLastError(const std::string& msg);

  // CRC error tracking (reader thread writes, GetCRCErrors reads)
  std::atomic<uint64_t> crc_errors_{0};

  /** @brief Reader thread main loop */
  void ReaderLoop();

  /** @brief Stop reader thread, join, clear queue */
  void StopReaderThread();

  /** @brief MySQL-specific stream setup (COM_BINLOG_DUMP_GTID) */
  mes_error_t StartStreamMySQL();

  /** @brief MariaDB-specific stream setup (COM_BINLOG_DUMP) */
  mes_error_t StartStreamMariaDB();

  /** @brief Update current_gtid_ from GTID_LOG_EVENT or MariaDB GTID (thread-safe) */
  void UpdateGtidFromEvent(const uint8_t* event_data, size_t event_size);
};

}  // namespace mes

#endif  // MES_CLIENT_BINLOG_CLIENT_H_
