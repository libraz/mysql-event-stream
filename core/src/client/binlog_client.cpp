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

  mes_error_t rc = conn_.Connect(config.host, config.port, config.user,
                                 config.password, config.connect_timeout_s,
                                 config.read_timeout_s, config.ssl_mode,
                                 config.ssl_ca, config.ssl_cert, config.ssl_key);
  if (rc != MES_OK) {
    last_error_ = conn_.GetLastError();
    LogMySQLConnectionError(config.host, config.port, last_error_);
    return MES_ERR_CONNECT;
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

  // Set heartbeat period (30 seconds = 30000000000 nanoseconds)
  // Non-fatal if this fails
  {
    protocol::QueryResult qr;
    std::string err;
    protocol::ExecuteQuery(conn_.Socket(),
                           "SET @master_heartbeat_period = 30000000000", &qr,
                           &err);
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
  return MES_OK;
}

PollResult BinlogClient::Poll() {
  if (stop_requested_.load(std::memory_order_acquire)) {
    streaming_ = false;
    return {MES_ERR_DISCONNECTED, nullptr, 0, false};
  }

  if (!streaming_) {
    return {MES_ERR_DISCONNECTED, nullptr, 0, false};
  }

  protocol::BinlogEventPacket event_pkt;
  mes_error_t rc = binlog_stream_.FetchEvent(conn_.Socket(), &event_pkt);

  // Check stop flag after blocking call returns
  if (stop_requested_.load(std::memory_order_acquire)) {
    streaming_ = false;
    return {MES_ERR_DISCONNECTED, nullptr, 0, false};
  }

  if (rc != MES_OK) {
    last_error_ = "Binlog stream read error";
    LogBinlogError("poll_error", current_gtid_, last_error_);
    streaming_ = false;
    return {MES_ERR_STREAM, nullptr, 0, false};
  }

  // Heartbeat
  if (event_pkt.is_heartbeat) {
    return {MES_OK, nullptr, 0, true};
  }

  const uint8_t* event_data = event_pkt.data;
  size_t event_size = event_pkt.size;

  // Update GTID tracking from GTID_LOG_EVENT
  UpdateGtidFromEvent(event_data, event_size);

  return {MES_OK, event_data, event_size, false};
}

void BinlogClient::Stop() {
  stop_requested_.store(true, std::memory_order_release);
  // Interrupt a blocking FetchEvent by shutting down the socket
  conn_.Socket()->Shutdown();
}

void BinlogClient::Disconnect() {
  streaming_ = false;
  conn_.Disconnect();
  StructuredLog().Event("mysql_disconnected").Info();
}

bool BinlogClient::IsConnected() const { return conn_.IsConnected(); }

const char* BinlogClient::GetLastError() const { return last_error_.c_str(); }

const char* BinlogClient::GetCurrentGtid() const {
  return current_gtid_.c_str();
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
  current_gtid_ = gtid_buf;
}

}  // namespace mes
