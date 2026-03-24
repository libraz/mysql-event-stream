// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#include "client/binlog_client.h"

#ifdef MES_HAS_MYSQL

#include <cstdio>
#include <cstring>

#include <mysql.h>

#include "binary_util.h"
#include "client/connection_validator.h"
#include "client/gtid_encoder.h"
#include "event_header.h"
#include "logger.h"

namespace mes {

BinlogClient::BinlogClient() { std::memset(&rpl_, 0, sizeof(rpl_)); }

BinlogClient::~BinlogClient() { Disconnect(); }

mes_error_t BinlogClient::Connect(const BinlogClientConfig& config) {
  stop_requested_.store(false, std::memory_order_release);

  if (conn_ != nullptr) {
    Disconnect();
  }

  conn_ = mysql_init(nullptr);
  if (conn_ == nullptr) {
    last_error_ = "mysql_init failed";
    StructuredLog().Event("mysql_connection_error").Field("error", "mysql_init failed").Error();
    return MES_ERR_CONNECT;
  }

  mysql_options(conn_, MYSQL_OPT_CONNECT_TIMEOUT, &config.connect_timeout_s);
  mysql_options(conn_, MYSQL_OPT_READ_TIMEOUT, &config.read_timeout_s);

  // Apply SSL/TLS options
  if (config.ssl_mode > 0) {
    unsigned int ssl_mode_val;
    switch (config.ssl_mode) {
      case 1:
        ssl_mode_val = SSL_MODE_PREFERRED;
        break;
      case 2:
        ssl_mode_val = SSL_MODE_REQUIRED;
        break;
      case 3:
        ssl_mode_val = SSL_MODE_VERIFY_CA;
        break;
      case 4:
        ssl_mode_val = SSL_MODE_VERIFY_IDENTITY;
        break;
      default:
        ssl_mode_val = SSL_MODE_PREFERRED;
        break;
    }
    mysql_options(conn_, MYSQL_OPT_SSL_MODE, &ssl_mode_val);
  } else {
    // Explicitly disable SSL to avoid MySQL's default SSL_MODE_PREFERRED
    unsigned int ssl_mode_val = SSL_MODE_DISABLED;
    mysql_options(conn_, MYSQL_OPT_SSL_MODE, &ssl_mode_val);
  }
  if (!config.ssl_ca.empty()) {
    mysql_options(conn_, MYSQL_OPT_SSL_CA, config.ssl_ca.c_str());
  }
  if (!config.ssl_cert.empty()) {
    mysql_options(conn_, MYSQL_OPT_SSL_CERT, config.ssl_cert.c_str());
  }
  if (!config.ssl_key.empty()) {
    mysql_options(conn_, MYSQL_OPT_SSL_KEY, config.ssl_key.c_str());
  }

  if (mysql_real_connect(conn_, config.host.c_str(), config.user.c_str(),
                         config.password.c_str(), nullptr, config.port,
                         nullptr, 0) == nullptr) {
    last_error_ = mysql_error(conn_);
    LogMySQLConnectionError(config.host, config.port, last_error_);
    mysql_close(conn_);
    conn_ = nullptr;
    return MES_ERR_CONNECT;
  }

  // Validate server configuration
  ValidationResult validation = ConnectionValidator::Validate(conn_);
  if (validation.error != MES_OK) {
    last_error_ = validation.message;
    mysql_close(conn_);
    conn_ = nullptr;
    return MES_ERR_VALIDATION;
  }

  StructuredLog().Event("mysql_connected").Field("host", config.host).Field("port", static_cast<int>(config.port)).Info();
  config_ = config;
  return MES_OK;
}

mes_error_t BinlogClient::StartStream() {
  if (conn_ == nullptr) {
    last_error_ = "Not connected";
    return MES_ERR_DISCONNECTED;
  }

  if (streaming_) {
    return MES_OK;
  }

  // Disable checksums
  if (mysql_query(conn_, "SET @source_binlog_checksum='NONE'") != 0) {
    last_error_ = mysql_error(conn_);
    return MES_ERR_STREAM;
  }

  // Set heartbeat period (30 seconds = 30000000000 nanoseconds)
  // Non-fatal if this fails
  mysql_query(conn_, "SET @master_heartbeat_period = 30000000000");

  // Setup MYSQL_RPL
  std::memset(&rpl_, 0, sizeof(rpl_));
  rpl_.file_name_length = 0;
  rpl_.file_name = nullptr;
  rpl_.start_position = 4;
  rpl_.server_id = config_.server_id;
  rpl_.flags = MYSQL_RPL_GTID;

  // Encode GTID set
  std::string start_gtid =
      GtidEncoder::ConvertSingleGtidToRange(config_.start_gtid);

  if (!start_gtid.empty()) {
    mes_error_t rc = GtidEncoder::Encode(start_gtid.c_str(), &gtid_encoded_);
    if (rc != MES_OK) {
      last_error_ = "Failed to encode GTID set";
      return rc;
    }
    rpl_.gtid_set_encoded_size = gtid_encoded_.size();
    rpl_.gtid_set_arg = &gtid_encoded_;
    rpl_.fix_gtid_set = &BinlogClient::FixGtidSetCallback;
  } else {
    rpl_.gtid_set_encoded_size = 0;
    rpl_.gtid_set_arg = nullptr;
    rpl_.fix_gtid_set = nullptr;
  }

  if (mysql_binlog_open(conn_, &rpl_) != 0) {
    last_error_ = mysql_error(conn_);
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

  int result = mysql_binlog_fetch(conn_, &rpl_);

  // Check stop flag after blocking call returns
  if (stop_requested_.load(std::memory_order_acquire)) {
    streaming_ = false;
    return {MES_ERR_DISCONNECTED, nullptr, 0, false};
  }

  if (result != 0) {
    last_error_ = mysql_error(conn_);
    LogBinlogError("poll_error", current_gtid_, last_error_);
    streaming_ = false;
    return {MES_ERR_STREAM, nullptr, 0, false};
  }

  // Heartbeat: size=0 or buffer=nullptr
  if (rpl_.size == 0 || rpl_.buffer == nullptr) {
    return {MES_OK, nullptr, 0, true};
  }

  // OK byte check: first byte should be 0x00
  if (rpl_.buffer[0] != 0x00) {
    last_error_ = "Unexpected status byte in binlog stream";
    streaming_ = false;
    return {MES_ERR_STREAM, nullptr, 0, false};
  }

  // Skip OK byte, return event data
  const uint8_t* event_data = rpl_.buffer + 1;
  size_t event_size = rpl_.size - 1;

  // Update GTID tracking from GTID_LOG_EVENT
  UpdateGtidFromEvent(event_data, event_size);

  return {MES_OK, event_data, event_size, false};
}

void BinlogClient::Stop() {
  stop_requested_.store(true, std::memory_order_release);
}

void BinlogClient::Disconnect() {
  if (streaming_ && conn_ != nullptr) {
    mysql_binlog_close(conn_, &rpl_);
    streaming_ = false;
  }
  if (conn_ != nullptr) {
    mysql_close(conn_);
    conn_ = nullptr;
    StructuredLog().Event("mysql_disconnected").Info();
  }
}

bool BinlogClient::IsConnected() const { return conn_ != nullptr; }

const char* BinlogClient::GetLastError() const { return last_error_.c_str(); }

const char* BinlogClient::GetCurrentGtid() const {
  return current_gtid_.c_str();
}

void BinlogClient::FixGtidSetCallback(MYSQL_RPL* rpl,
                                      unsigned char* packet_gtid_set) {
  auto* encoded = static_cast<std::vector<uint8_t>*>(rpl->gtid_set_arg);
  std::memcpy(packet_gtid_set, encoded->data(), encoded->size());
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

#endif  // MES_HAS_MYSQL
