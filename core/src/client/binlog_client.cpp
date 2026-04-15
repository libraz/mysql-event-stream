// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#include "client/binlog_client.h"

#include <cstdio>

#include "binary_util.h"
#include "client/connection_validator.h"
#include "client/gtid_encoder.h"
#include "crc32.h"
#include "event_header.h"
#include "logger.h"
#include "mariadb_event_parser.h"
#include "protocol/mysql_query.h"

namespace mes {

namespace {

/** @brief Validate MariaDB GTID set format to prevent SQL injection.
 *
 * A valid MariaDB GTID set contains only digits, hyphens, commas, and spaces
 * (e.g., "0-1-42" or "0-1-42,1-2-100").
 */
bool IsValidMariaDBGtidSet(const std::string& gtid) {
  if (gtid.empty()) return true;
  for (char c : gtid) {
    if (c != '-' && c != ',' && c != ' ' && (c < '0' || c > '9')) {
      return false;
    }
  }
  return true;
}

/// MySQL GTID_LOG_EVENT post-header body size: commit_flag(1) + UUID(16) + GNO(8)
constexpr size_t kMySQLGtidBodySize = 25;

/// Binlog files begin with a 4-byte magic number (0xFE 0x62 0x69 0x6E).
/// Streaming starts at offset 4 to skip the magic header.
constexpr uint32_t kBinlogMagicOffset = 4;

/// Heartbeat period in nanoseconds (3 seconds)
constexpr uint64_t kHeartbeatPeriodNs = 3'000'000'000ULL;

}  // namespace

BinlogClient::BinlogClient() = default;

BinlogClient::~BinlogClient() { Disconnect(); }

void BinlogClient::SetLastError(const std::string& msg) {
  std::lock_guard<std::mutex> lock(last_error_mutex_);
  last_error_ = msg;
}

mes_error_t BinlogClient::Connect(const BinlogClientConfig& config) {
  // Tear down any previous stream first
  if (conn_.IsConnected()) {
    Disconnect();
  }
  // Reset stop flag after previous stream is fully torn down
  stop_requested_.store(false, std::memory_order_release);

  if (config.ssl_mode > MES_SSL_VERIFY_IDENTITY) {
    SetLastError("Invalid ssl_mode value");
    return MES_ERR_INVALID_ARG;
  }

  mes_error_t rc = conn_.Connect(config.host, config.port, config.user, config.password,
                                 config.connect_timeout_s, config.read_timeout_s, config.ssl_mode,
                                 config.ssl_ca, config.ssl_cert, config.ssl_key);
  if (rc != MES_OK) {
    std::string err = conn_.GetLastError();
    SetLastError(err);
    LogMySQLConnectionError(config.host, config.port, err);
    return rc;
  }

  // Detect server flavor (MySQL vs MariaDB)
  server_flavor_ = conn_.GetServerFlavor();

  // Validate server configuration
  ValidationResult validation = ConnectionValidator::Validate(&conn_, server_flavor_);
  if (validation.error != MES_OK) {
    SetLastError(validation.message);
    conn_.Disconnect();
    return MES_ERR_VALIDATION;
  }

  StructuredLog()
      .Event("mysql_connected")
      .Field("host", config.host)
      .Field("port", static_cast<int>(config.port))
      .Field("flavor", GetServerFlavorName(server_flavor_))
      .Info();
  config_ = config;
  return MES_OK;
}

mes_error_t BinlogClient::StartStream() {
  if (!conn_.IsConnected()) {
    SetLastError("Not connected");
    return MES_ERR_DISCONNECTED;
  }

  if (streaming_.load(std::memory_order_acquire)) {
    return MES_OK;
  }

  mes_error_t rc = MES_OK;

  if (server_flavor_ == ServerFlavor::kMariaDB) {
    rc = StartStreamMariaDB();
  } else {
    rc = StartStreamMySQL();
  }

  if (rc != MES_OK) {
    return rc;
  }

  streaming_.store(true, std::memory_order_release);

  // Create bounded event queue and launch reader thread
  size_t queue_size = config_.max_queue_size > 0 ? config_.max_queue_size : MES_DEFAULT_QUEUE_SIZE;
  event_queue_ = std::make_unique<EventQueue>(queue_size);
  reader_thread_ = std::thread(&BinlogClient::ReaderLoop, this);

  return MES_OK;
}

mes_error_t BinlogClient::StartStreamMySQL() {
  // Enable CRC32 checksums for data integrity verification.
  // MySQL always uses CRC32 after this SET succeeds.
  checksum_enabled_ = true;
  {
    protocol::QueryResult qr;
    std::string err;
    if (protocol::ExecuteQuery(conn_.Socket(), "SET @source_binlog_checksum='CRC32'", &qr, &err) !=
        MES_OK) {
      SetLastError(err);
      return MES_ERR_STREAM;
    }
  }

  // Set heartbeat period
  {
    protocol::QueryResult qr;
    std::string err;
    std::string hb_query = "SET @master_heartbeat_period = " + std::to_string(kHeartbeatPeriodNs);
    mes_error_t hb_rc = protocol::ExecuteQuery(conn_.Socket(), hb_query, &qr, &err);
    if (hb_rc != MES_OK) {
      StructuredLog().Event("heartbeat_setup_failed").Field("error", err).Warn();
    }
  }

  // Encode GTID set
  std::string start_gtid = GtidEncoder::ConvertSingleGtidToRange(config_.start_gtid);

  if (!start_gtid.empty()) {
    mes_error_t rc = GtidEncoder::Encode(start_gtid.c_str(), &gtid_encoded_);
    if (rc != MES_OK) {
      SetLastError("Failed to encode GTID set");
      return rc;
    }
  }

  // Start binlog stream via COM_BINLOG_DUMP_GTID
  protocol::BinlogStreamConfig stream_config;
  stream_config.server_id = config_.server_id;
  stream_config.binlog_position = kBinlogMagicOffset;
  stream_config.gtid_encoded = gtid_encoded_;

  auto rc = binlog_stream_.Start(conn_.Socket(), stream_config);
  if (rc != MES_OK) {
    SetLastError("Failed to start binlog stream");
    return MES_ERR_STREAM;
  }

  return MES_OK;
}

mes_error_t BinlogClient::StartStreamMariaDB() {
  protocol::QueryResult qr;
  std::string err;

  // Advertise MariaDB slave capability so the server sends GTID events (type 162)
  // and ANNOTATE_ROWS events. Without this, MariaDB falls back to the legacy
  // replication format that omits per-transaction GTID events.
  // Capability 4 = MARIA_SLAVE_CAPABILITY_GTID (MariaDB 10.0.2+)
  {
    mes_error_t rc =
        protocol::ExecuteQuery(conn_.Socket(), "SET @mariadb_slave_capability = 4", &qr, &err);
    if (rc != MES_OK) {
      StructuredLog().Event("mariadb_slave_capability_failed").Field("error", err).Warn();
    }
  }

  // MariaDB uses @master_binlog_checksum (not @source_binlog_checksum)
  if (protocol::ExecuteQuery(conn_.Socket(),
                             "SET @master_binlog_checksum = @@global.binlog_checksum", &qr,
                             &err) != MES_OK) {
    SetLastError("Failed to set MariaDB binlog checksum: " + err);
    return MES_ERR_STREAM;
  }

  // Strict GTID mode: fail on GTID gap rather than silently skipping
  {
    mes_error_t rc =
        protocol::ExecuteQuery(conn_.Socket(), "SET @slave_gtid_strict_mode = 1", &qr, &err);
    if (rc != MES_OK) {
      StructuredLog().Event("mariadb_strict_mode_failed").Field("error", err).Warn();
    }
  }

  // Don't skip duplicate GTIDs
  {
    mes_error_t rc =
        protocol::ExecuteQuery(conn_.Socket(), "SET @slave_gtid_ignore_duplicates = 0", &qr, &err);
    if (rc != MES_OK) {
      StructuredLog().Event("mariadb_ignore_duplicates_failed").Field("error", err).Warn();
    }
  }

  // Heartbeat period
  {
    std::string hb_query = "SET @master_heartbeat_period = " + std::to_string(kHeartbeatPeriodNs);
    mes_error_t rc = protocol::ExecuteQuery(conn_.Socket(), hb_query, &qr, &err);
    if (rc != MES_OK) {
      StructuredLog().Event("heartbeat_setup_failed").Field("error", err).Warn();
    }
  }

  // Detect whether checksum is actually enabled
  {
    protocol::QueryResult checksum_qr;
    std::string checksum_err;
    if (protocol::ExecuteQuery(conn_.Socket(), "SELECT @@global.binlog_checksum", &checksum_qr,
                               &checksum_err) == MES_OK &&
        !checksum_qr.rows.empty() && !checksum_qr.rows[0].values.empty()) {
      std::string val = checksum_qr.rows[0].values[0];
      checksum_enabled_ = (val != "NONE" && val != "none");
    }
  }

  // Set MariaDB GTID position via session variable.
  // MariaDB reads @slave_connect_state to know which GTIDs the replica has.
  // Empty string means "start from current binlog position".
  std::string gtid = config_.start_gtid;
  if (!IsValidMariaDBGtidSet(gtid)) {
    SetLastError("Invalid MariaDB GTID format: contains disallowed characters");
    return MES_ERR_INVALID_ARG;
  }
  std::string gtid_query = "SET @slave_connect_state = '" + gtid + "'";
  if (protocol::ExecuteQuery(conn_.Socket(), gtid_query, &qr, &err) != MES_OK) {
    SetLastError("Failed to set slave_connect_state: " + err);
    return MES_ERR_STREAM;
  }

  StructuredLog().Event("mariadb_gtid_state_set").Field("gtid", gtid).Debug();

  // Start binlog stream via COM_BINLOG_DUMP (not COM_BINLOG_DUMP_GTID)
  protocol::BinlogStreamConfig stream_config;
  stream_config.server_id = config_.server_id;
  stream_config.binlog_position = kBinlogMagicOffset;

  auto rc = binlog_stream_.StartComBinlogDump(conn_.Socket(), stream_config);
  if (rc != MES_OK) {
    SetLastError("Failed to start MariaDB binlog stream");
    return MES_ERR_STREAM;
  }

  return MES_OK;
}

void BinlogClient::ReaderLoop() {
  while (!stop_requested_.load(std::memory_order_acquire)) {
    protocol::BinlogEventPacket event_pkt;
    mes_error_t rc = binlog_stream_.FetchEvent(conn_.Socket(), &reader_scratch_, &event_pkt);

    // Check stop flag after blocking call returns
    if (stop_requested_.load(std::memory_order_acquire)) {
      break;
    }

    if (rc != MES_OK) {
      // Push error sentinel so Poll() can surface the error
      QueuedEvent err_event;
      err_event.error = rc;
      event_queue_->Push(std::move(err_event));
      return;
    }

    // Heartbeat: surface to the consumer as an empty event with
    // is_heartbeat=true. The public mes_poll_result_t contract (see mes.h)
    // documents is_heartbeat as a first-class signal; dropping heartbeats
    // here would make that field unreachable. Consumers that do not care
    // about heartbeats can filter on `data == nullptr` (matches the Node
    // and Python high-level streams, which already skip null-data results).
    if (event_pkt.is_heartbeat) {
      QueuedEvent hb;
      hb.is_heartbeat = true;
      hb.error = MES_OK;
      if (!event_queue_->Push(std::move(hb))) {
        return;  // queue closed
      }
      continue;
    }

    // Verify CRC32 checksum for data integrity.
    // MySQL appends a 4-byte CRC32 to every event when @source_binlog_checksum='CRC32'.
    // MariaDB may have binlog_checksum=NONE, in which case we skip verification.
    if (checksum_enabled_ && event_pkt.size >= kEventHeaderSize + kChecksumSize) {
      const size_t data_length = event_pkt.size - kChecksumSize;
      uint32_t computed_crc = ComputeCRC32(event_pkt.data, data_length);
      uint32_t stored_crc = binary::ReadU32Le(event_pkt.data + data_length);
      if (computed_crc != stored_crc) {
        crc_errors_.fetch_add(1, std::memory_order_relaxed);
        StructuredLog()
            .Event("binlog_error")
            .Field("type", "crc32_checksum_mismatch")
            .Field("computed_crc", static_cast<uint64_t>(computed_crc))
            .Field("stored_crc", static_cast<uint64_t>(stored_crc))
            .Field("event_length", static_cast<uint64_t>(event_pkt.size))
            .Error();
        // Push error event so the consumer can detect the corrupted event
        QueuedEvent crc_err;
        crc_err.error = MES_ERR_CHECKSUM;
        event_queue_->Push(std::move(crc_err));
        return;  // Stop reader; consistent with stream error handling above
      }
    }

    // Update GTID tracking (protected by gtid_mutex_)
    UpdateGtidFromEvent(event_pkt.data, event_pkt.size);

    // Move the packet buffer into the queue. event_pkt.data_offset (typically
    // 1 to skip the OK byte) lets the consumer locate the real event bytes
    // inside the moved buffer without an intermediate copy. After the move,
    // reader_scratch_ is left in a valid-but-unspecified (empty) state; the
    // next FetchEvent() will resize it as needed.
    QueuedEvent qe;
    qe.data = std::move(reader_scratch_);
    qe.data_offset = event_pkt.data_offset;
    qe.error = MES_OK;

    if (!event_queue_->Push(std::move(qe))) {
      // Queue was closed (shutdown in progress)
      return;
    }
  }
}

PollResult BinlogClient::Poll() {
  if (!streaming_.load(std::memory_order_acquire) || !event_queue_) {
    return {MES_ERR_DISCONNECTED, nullptr, 0, false};
  }

  QueuedEvent event;
  if (!event_queue_->Pop(&event)) {
    // Queue closed (shutdown)
    streaming_.store(false, std::memory_order_release);
    return {MES_ERR_DISCONNECTED, nullptr, 0, false};
  }

  // Heartbeat: empty data, is_heartbeat=true. No current_event_ buffer
  // update because there is no data to retain.
  if (event.is_heartbeat) {
    return {MES_OK, nullptr, 0, true};
  }

  if (event.error != MES_OK) {
    // Error from reader thread. Snapshot the current GTID under the mutex
    // directly (instead of routing through GetCurrentGtid() which returns
    // a pointer into a shared buffer) so the log line's GTID remains valid
    // for the duration of the structured log build-up, and so we avoid the
    // "valid-until-next-call" contract that GetCurrentGtid() carries.
    std::string gtid_snap;
    {
      std::lock_guard<std::mutex> lock(gtid_mutex_);
      gtid_snap = current_gtid_;
    }
    const std::string err_msg = "Binlog stream read error";
    SetLastError(err_msg);
    StructuredLog()
        .Event("binlog_error")
        .Field("type", "poll_error")
        .Field("gtid", gtid_snap)
        .Field("error", err_msg)
        .Field("error_code", static_cast<int64_t>(event.error))
        .Error();
    streaming_.store(false, std::memory_order_release);
    return {event.error, nullptr, 0, false};
  }

  // Store event data so pointer remains valid until next Poll().
  // Apply data_offset to skip the OK byte prefix kept in the buffer so
  // ownership could be moved in from the reader thread without a copy.
  current_event_ = std::move(event);
  const size_t offset = current_event_.data_offset;
  const uint8_t* payload = current_event_.data.data() + offset;
  const size_t payload_size = current_event_.data.size() - offset;
  return {MES_OK, payload, payload_size, false};
}

void BinlogClient::Stop() {
  std::lock_guard<std::mutex> lock(stop_mutex_);
  StopReaderThread();
}

void BinlogClient::StopReaderThread() {
  // Shutdown order (must match FetchEvent unblock contract):
  //   1. stop_requested_  -> reader loop condition flips to break on next iter
  //   2. streaming_       -> Poll() fast-path short-circuits for any concurrent
  //                          caller before event_queue_ is destroyed below
  //   3. event_queue_.Close() -> unblocks Pop() in Poll() and Push() in reader
  //   4. socket.Shutdown() -> unblocks a FetchEvent() currently in recv(). The
  //      protocol layer relies on the socket's half-close causing the pending
  //      syscall to return with an error; reader thread then sees
  //      stop_requested_ and exits cleanly.
  //   5. join() the reader.
  stop_requested_.store(true, std::memory_order_release);
  streaming_.store(false, std::memory_order_release);

  if (event_queue_) {
    event_queue_->Close();
  }

  if (conn_.IsConnected()) {
    conn_.Socket()->Shutdown();
  }

  if (reader_thread_.joinable()) {
    reader_thread_.join();
  }

  // NOTE(thread-safety): event_queue_ is intentionally NOT reset here.
  // Poll() reads event_queue_ without a lock (only streaming_ is atomic),
  // and Stop() can be called from any thread. Resetting event_queue_ here
  // would race with Poll()'s non-atomic read. The closed queue stays alive
  // until StartStream() replaces it or the destructor runs.
  if (event_queue_) {
    event_queue_->Clear();
  }
}

void BinlogClient::Disconnect() {
  {
    std::lock_guard<std::mutex> lock(stop_mutex_);
    StopReaderThread();
  }
  conn_.Disconnect();
  StructuredLog().Event("mysql_disconnected").Info();
}

bool BinlogClient::IsConnected() const { return conn_.IsConnected(); }

const char* BinlogClient::GetLastError() const {
  // Snapshot last_error_ into a separate buffer so the returned c_str() is
  // not invalidated by a concurrent writer resizing last_error_. Per mes.h
  // contract, the pointer is valid until the next GetLastError() call on
  // this BinlogClient.
  std::lock_guard<std::mutex> lock(last_error_mutex_);
  last_error_snapshot_ = last_error_;
  return last_error_snapshot_.c_str();
}

const char* BinlogClient::GetCurrentGtid() const {
  // NOTE(review): gtid_snapshot_ is a shared buffer protected by
  // gtid_mutex_. The returned pointer is valid only until the next
  // GetCurrentGtid() call on this BinlogClient. If multiple threads call
  // this method concurrently, they must synchronize externally to avoid
  // one caller's pointer being invalidated by another's assignment.
  std::lock_guard<std::mutex> lock(gtid_mutex_);
  gtid_snapshot_ = current_gtid_;
  return gtid_snapshot_.c_str();
}

void BinlogClient::UpdateGtidFromEvent(const uint8_t* data, size_t size) {
  // Need at least the event header
  if (size < kEventHeaderSize) {
    return;
  }

  // Event type is at offset 4 in the header
  uint8_t event_type = data[4];

  // MariaDB GTID event (type 162)
  if (event_type == static_cast<uint8_t>(BinlogEventType::kMariaDBGtidEvent)) {
    std::string gtid_str;
    if (MariaDBEventParser::ExtractGtid(data, size, &gtid_str) == MES_OK) {
      std::lock_guard<std::mutex> lock(gtid_mutex_);
      current_gtid_ = gtid_str;
    }
    return;
  }

  if (event_type != static_cast<uint8_t>(BinlogEventType::kGtidLogEvent)) {
    return;
  }

  // Body starts after header
  // Need: 1 (commit_flag) + 16 (UUID) + 8 (GNO) = 25 bytes after header
  if (size < kEventHeaderSize + kMySQLGtidBodySize) {
    return;
  }

  const uint8_t* body = data + kEventHeaderSize;
  const uint8_t* uuid = body + 1;  // Skip commit_flag

  // Read GNO (little-endian int64 at body+17)
  int64_t gno = static_cast<int64_t>(binary::ReadU64Le(body + 17));

  // Format UUID
  char uuid_str[37];
  std::snprintf(uuid_str, sizeof(uuid_str),
                "%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x", uuid[0],
                uuid[1], uuid[2], uuid[3], uuid[4], uuid[5], uuid[6], uuid[7], uuid[8], uuid[9],
                uuid[10], uuid[11], uuid[12], uuid[13], uuid[14], uuid[15]);

  // Format: "uuid:gno"
  char gtid_buf[80];
  std::snprintf(gtid_buf, sizeof(gtid_buf), "%s:%lld", uuid_str, static_cast<long long>(gno));

  std::lock_guard<std::mutex> lock(gtid_mutex_);
  current_gtid_ = gtid_buf;
}

uint64_t BinlogClient::GetCRCErrors() const { return crc_errors_.load(std::memory_order_relaxed); }

}  // namespace mes
