// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#include "client/binlog_client.h"

#include <cstdio>
#include <cstring>

#include "binary_util.h"
#include "client/connection_validator.h"
#include "client/gtid_encoder.h"
#include "event_header.h"
#include "logger.h"
#include "protocol/mysql_query.h"

namespace mes {

BinlogClient::BinlogClient() = default;

BinlogClient::~BinlogClient() { Disconnect(); }

mes_error_t BinlogClient::Connect(const BinlogClientConfig& config) {
  stop_requested_.store(false, std::memory_order_release);

  if (conn_.IsConnected()) {
    Disconnect();
  }

  if (config.ssl_mode > MES_SSL_VERIFY_IDENTITY) {
    last_error_ = "Invalid ssl_mode value";
    return MES_ERR_INVALID_ARG;
  }

  mes_error_t rc = conn_.Connect(config.host, config.port, config.user,
                                 config.password, config.connect_timeout_s,
                                 config.read_timeout_s, config.ssl_mode,
                                 config.ssl_ca, config.ssl_cert, config.ssl_key);
  if (rc != MES_OK) {
    last_error_ = conn_.GetLastError();
    LogMySQLConnectionError(config.host, config.port, last_error_);
    return rc;
  }

  // Validate server configuration
  ValidationResult validation = ConnectionValidator::Validate(&conn_);
  if (validation.error != MES_OK) {
    last_error_ = validation.message;
    conn_.Disconnect();
    return MES_ERR_VALIDATION;
  }

  StructuredLog()
      .Event("mysql_connected")
      .Field("host", config.host)
      .Field("port", static_cast<int>(config.port))
      .Info();
  config_ = config;
  return MES_OK;
}

mes_error_t BinlogClient::StartStream() {
  if (!conn_.IsConnected()) {
    last_error_ = "Not connected";
    return MES_ERR_DISCONNECTED;
  }

  if (streaming_) {
    return MES_OK;
  }

  // Disable checksums
  {
    protocol::QueryResult qr;
    std::string err;
    if (protocol::ExecuteQuery(conn_.Socket(),
                               "SET @source_binlog_checksum='NONE'", &qr,
                               &err) != MES_OK) {
      last_error_ = err;
      return MES_ERR_STREAM;
    }
  }

  // Set heartbeat period (3 seconds = 3000000000 nanoseconds)
  // Shorter than read_timeout to keep connection alive in the reader thread
  {
    protocol::QueryResult qr;
    std::string err;
    mes_error_t hb_rc = protocol::ExecuteQuery(
        conn_.Socket(), "SET @master_heartbeat_period = 3000000000", &qr, &err);
    if (hb_rc != MES_OK) {
      StructuredLog()
          .Event("heartbeat_setup_failed")
          .Field("error", err)
          .Warn();
    }
  }

  // Encode GTID set
  std::string start_gtid =
      GtidEncoder::ConvertSingleGtidToRange(config_.start_gtid);

  if (!start_gtid.empty()) {
    mes_error_t rc = GtidEncoder::Encode(start_gtid.c_str(), &gtid_encoded_);
    if (rc != MES_OK) {
      last_error_ = "Failed to encode GTID set";
      return rc;
    }
  }

  // Start binlog stream
  protocol::BinlogStreamConfig stream_config;
  stream_config.server_id = config_.server_id;
  stream_config.binlog_position = 4;
  stream_config.gtid_encoded = gtid_encoded_;

  auto rc = binlog_stream_.Start(conn_.Socket(), stream_config);
  if (rc != MES_OK) {
    last_error_ = "Failed to start binlog stream";
    return MES_ERR_STREAM;
  }

  streaming_ = true;

  // Create bounded event queue and launch reader thread
  size_t queue_size =
      config_.max_queue_size > 0 ? config_.max_queue_size : 10000;
  event_queue_ = std::make_unique<EventQueue>(queue_size);
  reader_thread_ = std::thread(&BinlogClient::ReaderLoop, this);

  return MES_OK;
}

void BinlogClient::ReaderLoop() {
  while (!stop_requested_.load(std::memory_order_acquire)) {
    protocol::BinlogEventPacket event_pkt;
    mes_error_t rc = binlog_stream_.FetchEvent(conn_.Socket(), &event_pkt);

    // Check stop flag after blocking call returns
    if (stop_requested_.load(std::memory_order_acquire)) {
      break;
    }

    if (rc != MES_OK) {
      // Push error sentinel so Poll() can surface the error
      QueuedEvent err_event;
      err_event.error = MES_ERR_STREAM;
      event_queue_->Push(std::move(err_event));
      return;
    }

    // Heartbeat: consume silently
    if (event_pkt.is_heartbeat) {
      continue;
    }

    // Update GTID tracking (protected by gtid_mutex_)
    UpdateGtidFromEvent(event_pkt.data, event_pkt.size);

    // Copy event data and push to queue
    // Push blocks if queue is full (backpressure → TCP flow control)
    QueuedEvent qe;
    qe.data.assign(event_pkt.data, event_pkt.data + event_pkt.size);
    qe.error = MES_OK;

    if (!event_queue_->Push(std::move(qe))) {
      // Queue was closed (shutdown in progress)
      return;
    }
  }
}

PollResult BinlogClient::Poll() {
  if (!streaming_ || !event_queue_) {
    return {MES_ERR_DISCONNECTED, nullptr, 0, false};
  }

  QueuedEvent event;
  if (!event_queue_->Pop(&event)) {
    // Queue closed (shutdown)
    streaming_ = false;
    return {MES_ERR_DISCONNECTED, nullptr, 0, false};
  }

  if (event.error != MES_OK) {
    // Error from reader thread
    last_error_ = "Binlog stream read error";
    LogBinlogError("poll_error", current_gtid_, last_error_);
    streaming_ = false;
    return {event.error, nullptr, 0, false};
  }

  // Store event data so pointer remains valid until next Poll()
  current_event_ = std::move(event);
  return {MES_OK, current_event_.data.data(), current_event_.data.size(),
          false};
}

void BinlogClient::Stop() {
  std::lock_guard<std::mutex> lock(stop_mutex_);
  StopReaderThread();
}

void BinlogClient::StopReaderThread() {
  stop_requested_.store(true, std::memory_order_release);

  if (event_queue_) {
    event_queue_->Close();
  }

  if (conn_.IsConnected()) {
    conn_.Socket()->Shutdown();
  }

  if (reader_thread_.joinable()) {
    reader_thread_.join();
  }

  if (event_queue_) {
    event_queue_->Clear();
    event_queue_.reset();
  }
}

void BinlogClient::Disconnect() {
  StopReaderThread();
  streaming_ = false;
  conn_.Disconnect();
  StructuredLog().Event("mysql_disconnected").Info();
}

bool BinlogClient::IsConnected() const { return conn_.IsConnected(); }

const char* BinlogClient::GetLastError() const { return last_error_.c_str(); }

const char* BinlogClient::GetCurrentGtid() const {
  std::lock_guard<std::mutex> lock(gtid_mutex_);
  thread_local std::string tl_snapshot;
  tl_snapshot = current_gtid_;
  return tl_snapshot.c_str();
}

void BinlogClient::UpdateGtidFromEvent(const uint8_t* data, size_t size) {
  // Need at least the event header (19 bytes)
  if (size < 19) {
    return;
  }

  // Event type is at offset 4 in the header
  uint8_t event_type = data[4];

  if (event_type != static_cast<uint8_t>(BinlogEventType::kGtidLogEvent)) {
    return;
  }

  // Body starts at offset 19 (after header)
  // Need: 1 (commit_flag) + 16 (UUID) + 8 (GNO) = 25 bytes after header
  if (size < 19 + 25) {
    return;
  }

  const uint8_t* body = data + 19;
  const uint8_t* uuid = body + 1;  // Skip commit_flag

  // Read GNO (little-endian int64 at body+17)
  int64_t gno = static_cast<int64_t>(binary::ReadU64Le(body + 17));

  // Format UUID
  char uuid_str[37];
  std::snprintf(
      uuid_str, sizeof(uuid_str),
      "%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
      uuid[0], uuid[1], uuid[2], uuid[3], uuid[4], uuid[5], uuid[6], uuid[7],
      uuid[8], uuid[9], uuid[10], uuid[11], uuid[12], uuid[13], uuid[14],
      uuid[15]);

  // Format: "uuid:gno"
  char gtid_buf[80];
  std::snprintf(gtid_buf, sizeof(gtid_buf), "%s:%lld", uuid_str,
                static_cast<long long>(gno));

  std::lock_guard<std::mutex> lock(gtid_mutex_);
  current_gtid_ = gtid_buf;
}

}  // namespace mes
