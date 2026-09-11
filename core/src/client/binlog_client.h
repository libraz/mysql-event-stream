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
#include "client/transaction_gtid_tracker.h"
#include "mes.h"
#include "protocol/mysql_binlog_stream.h"
#include "protocol/mysql_connection.h"
#include "server_flavor.h"

namespace mes {

class BinlogClientTestAccess;

/**
 * @brief Configuration for BinlogClient
 */
struct BinlogClientConfig {
  std::string host = "127.0.0.1";
  uint16_t port = 3306;
  std::string user;
  std::string password;
  uint32_t server_id = 1;
  std::string start_gtid;
  bool start_at_current = true;
  bool start_at_file_position = false;
  std::string binlog_file;
  uint64_t binlog_position = kBinlogMagicOffset;
  uint32_t connect_timeout_s = 10;
  uint32_t read_timeout_s = 30;
  mes_ssl_mode_t ssl_mode = MES_SSL_DISABLED;
  std::string ssl_ca;             // Path to CA certificate file
  std::string ssl_cert;           // Path to client certificate file
  std::string ssl_key;            // Path to client private key file
  size_t max_queue_size = 10000;  // 0 = use default (10000)
  bool allow_public_key_retrieval = false;
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
 *   - Stop() may be called from any thread to interrupt a blocking Poll() or a
 *     StartStream() that is waiting on the server.
 *   - IsConnected(), IsStreaming(), ChecksumEnabled(), GetCRCErrors() and
 *     QueuedBytes() may be called from a thread other than the owner thread,
 *     including while the owner thread is inside StartStream() or Poll().
 *   - GetLastError() and GetCurrentGtid() may be called on those same terms,
 *     but each returns a pointer into one buffer shared by all of its callers:
 *     the next call to the same accessor, from any thread, overwrites what the
 *     previous one returned. Making the call needs no external lock; holding
 *     the result does, unless each caller copies it before releasing control.
 *   - All other methods must be called from a single thread.
 *
 * Usage:
 *   BinlogClient client;
 *   client.Connect(config);
 *   client.StartStream();
 *   // The reader thread already verified every queued event's CRC32, so the
 *   // engine frames the trailer without computing it a second time.
 *   engine.SetChecksumEnabled(client.ChecksumEnabled());
 *   engine.SetTrailerPreVerified(true);
 *   while (client.IsStreaming()) {
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

  /**
   * @brief Block for one event, then drain up to @p max_events already queued events.
   *
   * Result data pointers remain valid until the next Poll() or PollBatch()
   * call. A terminal error is returned as the final batch element.
   */
  size_t PollBatch(size_t max_events, std::vector<PollResult>* results);

  /**
   * @brief Synchronously stop from any thread and unblock a pending Poll().
   * @note Acquires locks and joins the reader; not async-signal-safe.
   */
  void Stop();

  /** @brief Disconnect from MySQL server */
  void Disconnect();

  /**
   * @brief Check whether the transport is still usable.
   *
   * False once anything has made the socket unusable, including a stream setup
   * query that failed and poisoned it, not only an explicit Stop()/Disconnect()
   * or a terminal reader error.
   */
  bool IsConnected() const;

  /** @brief Check whether Poll() can still drain events or a terminal error. */
  bool IsStreaming() const;

  /** @brief Get the server flavor detected during Connect(). */
  ServerFlavor GetServerFlavor() const;

  /** @brief Get last error message, or an empty string if there is none.
   *
   * Safe to call from any thread: the read is serialised on last_error_mutex_,
   * which is also what a Stop() from another thread takes to write the message.
   * The returned pointer refers to a buffer shared by every caller, so it stays
   * valid only until the next GetLastError() call on this client, from any
   * thread. Copy the string to keep it past that point.
   */
  const char* GetLastError() const;

  /** @brief Get the current GTID position as a string.
   *
   * Safe to call from any thread: the read is serialised on gtid_mutex_. As
   * with GetLastError(), the returned pointer refers to a buffer shared by
   * every caller and stays valid only until the next GetCurrentGtid() call on
   * this client, from any thread.
   */
  const char* GetCurrentGtid() const;

  /** @brief Get total CRC32 checksum errors detected (thread-safe) */
  uint64_t GetCRCErrors() const;

  /** @brief Whether wire events currently carry CRC32 trailers. */
  bool ChecksumEnabled() const;

  /**
   * @brief Set the maximum wire event size accepted by the reader.
   *
   * Uses the same normalization contract as EventStreamParser: 0 resolves to
   * the 1 GiB hard cap and other out-of-range values are clamped. Call before
   * StartStream(); changing it while the reader is running is not supported.
   */
  void SetMaxEventSize(uint32_t max_event_size);

  /** @brief Get the normalized reader event-size ceiling. */
  uint32_t MaxEventSize() const;

  /**
   * @brief Set/get the total byte budget for queued events.
   *
   * The budget covers every byte a queued event keeps resident: the wire buffer
   * plus the per-event checkpoint and sentinel bookkeeping (see
   * QueuedEventCharge()). StartStream() rejects a budget below
   * MinQueueBytesForEvent(MaxEventSize()), so a configuration that survives
   * start always admits an event at the ceiling together with its checkpoint.
   */
  void SetMaxQueueBytes(size_t max_queue_bytes);
  size_t MaxQueueBytes() const;

  /**
   * @brief Bytes currently charged to the event queue.
   *
   * Callable from a monitoring thread while the owner thread polls, starts or
   * restarts the stream: the queue pointer is read under queue_ptr_mutex_, the
   * same lock StartStream() holds while it installs a replacement queue.
   */
  size_t QueuedBytes() const;

 private:
  friend class BinlogClientTestAccess;

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
  // Written by StartStream() once the reader exists, by Poll() on error/drain
  // and by StopReaderThread() under stop_mutex_. It is also read from Poll()
  // before taking any lock, so make it atomic to avoid torn reads / data races.
  //
  // Note: Poll() reads event_queue_ without a lock. The thread
  // contract (see class-level Doxygen) requires that Poll(), Connect(),
  // and StartStream() be serialised on the single owner thread, so the
  // event_queue_ unique_ptr cannot be reassigned by StartStream() while
  // a Poll() on the same thread is in progress. Stop() may run from any
  // thread but does not reassign event_queue_; it only Close()s it, and it
  // shares stop_mutex_ with the reassignment. QueuedBytes() is the one
  // cross-thread reader of the pointer itself, so it takes queue_ptr_mutex_.
  std::atomic<bool> streaming_{false};
  std::atomic<bool> connected_{false};
  // Whether the server emits a CRC32 trailer on every binlog event. This is
  // a protocol-layer flag distinct from EventStreamParser::has_checksum_:
  // BinlogClient detects it via SQL (SELECT @@global.binlog_checksum) because
  // it must (a) send "SET @source_binlog_checksum='CRC32'" before streaming
  // and (b) verify the CRC on the wire for the integrity counter, both of
  // which happen before any FORMAT_DESCRIPTION_EVENT is seen. The parser
  // independently auto-detects checksums from the FDE byte when it strips the
  // trailer during decode. The two layers are intentionally decoupled so each
  // component is usable on its own; only the kChecksumSize constant is shared.
  std::atomic<bool> checksum_enabled_{true};
  std::atomic<bool> stop_requested_{false};
  // Set for the duration of StartStream(). Setup performs blocking socket round
  // trips, so it must not run under stop_mutex_; this flag excludes a second
  // concurrent setup in its place, leaving Stop() free to shut the socket down
  // mid-setup. Stop() deliberately does not wait on it: waiting would reinstate
  // the very dependency on setup I/O that the split removes.
  std::atomic<bool> setup_in_progress_{false};
  // Keep the default far enough below the 48 MiB queue budget that a valid
  // maximum-sized event plus its checkpoint reserve can always enter the queue.
  // Larger events remain an explicit opt-in together with a larger
  // max_queue_bytes setting.
  uint32_t max_event_size_ = 32u * 1024u * 1024u;
  size_t max_queue_bytes_ = kDefaultEventQueueBytes;

  // Reader thread infrastructure
  std::unique_ptr<EventQueue> event_queue_;
  std::thread reader_thread_;
  QueuedEvent current_event_;              // Holds data for current Poll() result
  std::vector<QueuedEvent> batch_events_;  // Holds data for current PollBatch() results
  TransactionGtidTracker gtid_tracker_;    // Reader-thread received/commit state
  // Lifecycle lock. Guards the queue swap, the reader join and the teardown
  // flags, and nothing else. It is never held across a blocking socket
  // operation, because Stop() and Disconnect() must acquire it before reaching
  // SocketHandle::Shutdown() -- the call that unblocks such an operation.
  std::mutex stop_mutex_;
  // Guards the event_queue_ pointer itself against the reassignment in
  // StartStream(), which destroys the queue the previous stream used. It is
  // deliberately not stop_mutex_: that lock is held across the reader join, so
  // a monitoring thread sampling QueuedBytes() would wait for reader shutdown.
  // Only the pointer is guarded -- the queue's own state stays behind its
  // internal mutex -- and it is never held across a blocking queue operation,
  // so Poll()'s blocking Pop() must not take it.
  mutable std::mutex queue_ptr_mutex_;

  // Reusable scratch buffer for FetchEvent() packet reads. Lives on the
  // reader thread: after a successful non-heartbeat read, the buffer is
  // moved into the QueuedEvent and a fresh (moved-from, empty) vector
  // takes its place on the next iteration. This avoids the per-event
  // copy that a thread-local buffer would otherwise require.
  std::vector<uint8_t> reader_scratch_;

  // GTID tracking (reader thread writes, GetCurrentGtid reads)
  std::string current_gtid_;
  // Note: gtid_snapshot_ is written under gtid_mutex_ every call.
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

  /**
   * @brief The start position resolved once, shared by every start mode.
   *
   * Produced by EstablishStartState() and consumed by the flavor's dump
   * request, so a new start mode cannot reach the wire without first passing
   * through the common establishment step.
   */
  struct StartState {
    std::string gtid_set;             ///< Resolved GTID set; empty for a file/position start.
    bool from_file_position = false;  ///< True when the caller pinned an exact file offset.
  };

  /** @brief Reader thread main loop */
  void ReaderLoop();

  /** @brief Stop reader thread, join, clear queue */
  void StopReaderThread();

  /** @brief Negotiate MySQL session variables before any state is read. */
  mes_error_t BeginSessionMySQL();

  /** @brief Negotiate MariaDB session variables before any state is read. */
  mes_error_t BeginSessionMariaDB();

  /**
   * @brief Establish everything StartStream() promises on success.
   *
   * Detects the source's checksum mode, configures the heartbeat, resolves the
   * start position, and seeds both the GTID tracker and the published
   * checkpoint. Every flavor and every start mode goes through this function,
   * so the set of established state cannot vary by start mode.
   *
   * A checkpoint this client already published outranks the configured start
   * position, so a restart resumes where the previous stream stopped instead of
   * re-resolving where it began.
   */
  mes_error_t EstablishStartState(StartState* state);

  /** @brief Read @\@global.binlog_checksum into checksum_enabled_. */
  mes_error_t DetectBinlogChecksum();

  /**
   * @brief Resolve and pre-validate the MySQL start GTID set.
   * @param resume_checkpoint Checkpoint to resume from, or nullptr to resolve
   *        the position from the configuration.
   */
  mes_error_t ResolveStartGtidMySQL(const std::string* resume_checkpoint, std::string* gtid_set);

  /**
   * @brief Resolve and validate the MariaDB start GTID set.
   * @param resume_checkpoint Checkpoint to resume from, or nullptr to resolve
   *        the position from the configuration.
   */
  mes_error_t ResolveStartGtidMariaDB(const std::string* resume_checkpoint, std::string* gtid_set);

  /** @brief Issue COM_BINLOG_DUMP_GTID (or COM_BINLOG_DUMP for a file offset). */
  mes_error_t SendBinlogDumpMySQL(const StartState& state);

  /** @brief Issue COM_BINLOG_DUMP with the MariaDB dump flags. */
  mes_error_t SendBinlogDumpMariaDB(const StartState& state);

  /** Configure a heartbeat period safely below the socket read timeout. */
  mes_error_t ConfigureHeartbeat();

  /** Promote the prior Poll() event's commit checkpoint to delivered state. */
  void PromoteDeliveredCheckpoint();

  /**
   * @brief Release every event buffer retained from the previous poll.
   *
   * current_event_ and batch_events_ are two halves of one piece of state --
   * the events already handed to the consumer -- so they are only ever reset
   * together. Clearing one alone would leave a checkpoint from the released
   * half reachable by PromoteDeliveredCheckpoint(), which is how a restarted
   * stream can publish a position belonging to the stream before it.
   *
   * Owner thread only: Poll() writes both members without a lock, so Stop(),
   * which may run on another thread, must not reset them.
   */
  void ResetDeliveredEvents();
};

}  // namespace mes

#endif  // MES_CLIENT_BINLOG_CLIENT_H_
