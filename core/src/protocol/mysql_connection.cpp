// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#include "protocol/mysql_connection.h"

#include <algorithm>
#include <cstring>

#include "logger.h"
#include "protocol/mysql_auth.h"
#include "protocol/mysql_packet.h"
#include "protocol/mysql_socket.h"

namespace mes::protocol {

// MySQL capability flags
constexpr uint32_t kClientProtocol41 = 0x00000200;
constexpr uint32_t kClientSSL = 0x00000800;
constexpr uint32_t kClientTransactions = 0x00002000;
constexpr uint32_t kClientSecureConnection = 0x00008000;
constexpr uint32_t kClientPluginAuth = 0x00080000;
constexpr uint32_t kClientPluginAuthLenencData = 0x00200000;
constexpr uint32_t kClientDeprecateEOF = 0x01000000;

// Max packet size advertised to server (16 MB)
constexpr uint32_t kMaxPacketSize = 0x01000000;

// Default charset: utf8mb4_general_ci
constexpr uint8_t kCharsetUtf8mb4 = 45;

// MySQL protocol markers
constexpr uint8_t kPacketOk = 0x00;
constexpr uint8_t kPacketAuthMoreData = 0x01;
constexpr uint8_t kPacketErr = 0xFF;
constexpr uint8_t kPacketAuthSwitchRequest = 0xFE;

// caching_sha2_password status bytes
constexpr uint8_t kCachingSha2FastAuthSuccess = 0x03;
constexpr uint8_t kCachingSha2FullAuthRequired = 0x04;

// COM_QUIT command byte
constexpr uint8_t kComQuit = 0x01;

// Plugin name constants
static const std::string kPluginNativePassword = "mysql_native_password";
static const std::string kPluginCachingSha2Password = "caching_sha2_password";

MysqlConnection::MysqlConnection() = default;

MysqlConnection::~MysqlConnection() { Disconnect(); }

mes_error_t MysqlConnection::Connect(const std::string& host, uint16_t port,
                                     const std::string& user,
                                     const std::string& password,
                                     uint32_t connect_timeout_s,
                                     uint32_t read_timeout_s,
                                     uint32_t ssl_mode,
                                     const std::string& ssl_ca,
                                     const std::string& ssl_cert,
                                     const std::string& ssl_key) {
  // Ensure clean state
  if (connected_) {
    Disconnect();
  }

  // Step 1: TCP connect
  mes_error_t rc = socket_.Connect(host.c_str(), port, connect_timeout_s);
  if (rc != MES_OK) {
    last_error_ = "Failed to connect to " + host + ":" + std::to_string(port);
    return rc;
  }

  // Step 2: Read server handshake
  std::vector<uint8_t> handshake_packet;
  sequence_id_ = 0;
  rc = ReadPacket(&socket_, &handshake_packet, &sequence_id_);
  if (rc != MES_OK) {
    last_error_ = "Failed to read server handshake";
    socket_ = SocketHandle();
    return MES_ERR_AUTH;
  }
  // Advance sequence_id so the client response uses the next number
  ++sequence_id_;

  // Step 3: Parse server handshake
  rc = ParseServerHandshake(handshake_packet);
  if (rc != MES_OK) {
    socket_ = SocketHandle();
    return rc;
  }

  // Step 4: Send handshake response (with optional TLS upgrade)
  rc = SendHandshakeResponse(user, password, server_info_.auth_plugin_name,
                             server_info_.auth_data, ssl_mode, ssl_ca,
                             ssl_cert, ssl_key, host);
  if (rc != MES_OK) {
    socket_ = SocketHandle();
    return rc;
  }

  // Step 5: Handle auth response
  rc = HandleAuthResponse(password);
  if (rc != MES_OK) {
    socket_ = SocketHandle();
    return rc;
  }

  // Step 6: Set read timeout
  if (read_timeout_s > 0) {
    rc = socket_.SetReadTimeout(read_timeout_s);
    if (rc != MES_OK) {
      last_error_ = "Failed to set read timeout";
      socket_ = SocketHandle();
      return rc;
    }
  }

  // Step 7: Mark as connected
  connected_ = true;
  return MES_OK;
}

void MysqlConnection::Disconnect() {
  if (connected_) {
    // Send COM_QUIT, ignore errors
    uint8_t quit_seq = 0;
    PacketBuffer pkt;
    const uint8_t quit_cmd = kComQuit;
    pkt.WritePacket(&quit_cmd, 1, &quit_seq);
    socket_.WriteAll(pkt.Data(), pkt.Size());
    connected_ = false;
  }

  // Close socket by replacing with a default-constructed one
  socket_ = SocketHandle();
}

bool MysqlConnection::IsConnected() const { return connected_; }

SocketHandle* MysqlConnection::Socket() { return &socket_; }

const std::string& MysqlConnection::GetLastError() const {
  return last_error_;
}

const ServerHandshake& MysqlConnection::GetServerInfo() const {
  return server_info_;
}

mes_error_t MysqlConnection::ParseServerHandshake(
    const std::vector<uint8_t>& packet) {
  const uint8_t* data = packet.data();
  const size_t len = packet.size();

  if (len < 4) {
    last_error_ = "Server handshake packet too short";
    return MES_ERR_AUTH;
  }

  // Check for ERR packet (server immediately rejected the connection)
  if (data[0] == kPacketErr) {
    return ProcessOkOrError(packet);
  }

  size_t pos = 0;

  // Protocol version (must be 10)
  server_info_.protocol_version = data[pos++];
  if (server_info_.protocol_version != 10) {
    last_error_ = "Unsupported protocol version: " +
                  std::to_string(server_info_.protocol_version);
    return MES_ERR_AUTH;
  }

  // Server version: NUL-terminated string
  const uint8_t* nul = static_cast<const uint8_t*>(
      std::memchr(data + pos, 0, len - pos));
  if (nul == nullptr) {
    last_error_ = "Invalid handshake: missing server version terminator";
    return MES_ERR_AUTH;
  }
  server_info_.server_version.assign(
      reinterpret_cast<const char*>(data + pos),
      reinterpret_cast<const char*>(nul));
  pos = static_cast<size_t>(nul - data) + 1;

  // Connection ID (4 bytes LE)
  if (pos + 4 > len) {
    last_error_ = "Invalid handshake: truncated connection ID";
    return MES_ERR_AUTH;
  }
  server_info_.connection_id =
      static_cast<uint32_t>(ReadFixedInt(data + pos, 4));
  pos += 4;

  // auth_plugin_data_part_1 (8 bytes)
  if (pos + 8 > len) {
    last_error_ = "Invalid handshake: truncated auth data part 1";
    return MES_ERR_AUTH;
  }
  server_info_.auth_data.assign(data + pos, data + pos + 8);
  pos += 8;

  // Filler (1 byte, 0x00)
  if (pos + 1 > len) {
    last_error_ = "Invalid handshake: truncated filler";
    return MES_ERR_AUTH;
  }
  pos += 1;

  // Capability flags lower 2 bytes
  if (pos + 2 > len) {
    last_error_ = "Invalid handshake: truncated capabilities lower";
    return MES_ERR_AUTH;
  }
  uint32_t cap_lower = static_cast<uint32_t>(ReadFixedInt(data + pos, 2));
  pos += 2;

  // Charset (1 byte)
  if (pos + 1 > len) {
    last_error_ = "Invalid handshake: truncated charset";
    return MES_ERR_AUTH;
  }
  server_info_.charset = data[pos++];

  // Status flags (2 bytes LE)
  if (pos + 2 > len) {
    last_error_ = "Invalid handshake: truncated status flags";
    return MES_ERR_AUTH;
  }
  server_info_.status_flags =
      static_cast<uint16_t>(ReadFixedInt(data + pos, 2));
  pos += 2;

  // Capability flags upper 2 bytes
  if (pos + 2 > len) {
    last_error_ = "Invalid handshake: truncated capabilities upper";
    return MES_ERR_AUTH;
  }
  uint32_t cap_upper = static_cast<uint32_t>(ReadFixedInt(data + pos, 2));
  pos += 2;

  server_info_.server_capabilities = cap_lower | (cap_upper << 16);

  // auth_plugin_data_length (1 byte)
  if (pos + 1 > len) {
    last_error_ = "Invalid handshake: truncated auth data length";
    return MES_ERR_AUTH;
  }
  uint8_t auth_plugin_data_len = data[pos++];

  // Reserved (10 bytes, zeros)
  if (pos + 10 > len) {
    last_error_ = "Invalid handshake: truncated reserved bytes";
    return MES_ERR_AUTH;
  }
  pos += 10;

  // auth_plugin_data_part_2
  if (server_info_.server_capabilities & kClientSecureConnection) {
    // Length of part2 to read: max(13, auth_plugin_data_len - 8)
    size_t part2_read_len = 13;
    if (auth_plugin_data_len > 8) {
      part2_read_len = std::max(static_cast<size_t>(13),
                                static_cast<size_t>(auth_plugin_data_len - 8));
    }

    if (pos + part2_read_len > len) {
      last_error_ = "Invalid handshake: truncated auth data part 2";
      return MES_ERR_AUTH;
    }

    // Only use the meaningful bytes (exclude trailing NUL at position 13)
    size_t part2_use_len = 12;
    if (auth_plugin_data_len > 8) {
      part2_use_len =
          std::min(static_cast<size_t>(auth_plugin_data_len - 8),
                   static_cast<size_t>(12));
    }

    server_info_.auth_data.insert(server_info_.auth_data.end(),
                                  data + pos, data + pos + part2_use_len);
    pos += part2_read_len;
  }

  // auth_plugin_name (NUL-terminated, if CLIENT_PLUGIN_AUTH)
  if (server_info_.server_capabilities & kClientPluginAuth) {
    if (pos < len) {
      const uint8_t* plugin_nul = static_cast<const uint8_t*>(
          std::memchr(data + pos, 0, len - pos));
      if (plugin_nul != nullptr) {
        server_info_.auth_plugin_name.assign(
            reinterpret_cast<const char*>(data + pos),
            reinterpret_cast<const char*>(plugin_nul));
        pos = static_cast<size_t>(plugin_nul - data) + 1;
      } else {
        // No NUL terminator: use remaining bytes
        server_info_.auth_plugin_name.assign(
            reinterpret_cast<const char*>(data + pos),
            reinterpret_cast<const char*>(data + len));
      }
    }
  }

  // Default plugin if none specified
  if (server_info_.auth_plugin_name.empty()) {
    server_info_.auth_plugin_name = kPluginCachingSha2Password;
  }

  return MES_OK;
}

mes_error_t MysqlConnection::SendHandshakeResponse(
    const std::string& user, const std::string& password,
    const std::string& auth_plugin, const std::vector<uint8_t>& auth_data,
    uint32_t ssl_mode, const std::string& ssl_ca,
    const std::string& ssl_cert, const std::string& ssl_key,
    const std::string& host) {
  // Build client capabilities
  uint32_t client_caps = kClientProtocol41 | kClientSecureConnection |
                         kClientPluginAuth | kClientPluginAuthLenencData |
                         kClientTransactions | kClientDeprecateEOF;

  // Intersect with server capabilities
  client_caps &= server_info_.server_capabilities;

  // Always keep Protocol41 (required)
  client_caps |= kClientProtocol41;

  bool do_tls = (ssl_mode > 0) &&
                (server_info_.server_capabilities & kClientSSL);

  // ssl_mode >= 2 (required, verify_ca, verify_identity) demands TLS.
  // Fail early if the server does not advertise SSL support.
  if (ssl_mode >= 2 && !do_tls) {
    StructuredLog().Event("ssl_required_not_available").Error();
    last_error_ = "SSL is required (ssl_mode=" + std::to_string(ssl_mode) +
                  ") but the server does not support SSL";
    return MES_ERR_CONNECT;
  }

  if (do_tls) {
    client_caps |= kClientSSL;
  }

  // If TLS is requested, send SSL Request packet first
  if (do_tls) {
    std::vector<uint8_t> ssl_request;
    ssl_request.reserve(32);

    // capabilities (4 bytes)
    WriteFixedInt(&ssl_request, client_caps, 4);
    // max_packet_size (4 bytes)
    WriteFixedInt(&ssl_request, kMaxPacketSize, 4);
    // charset (1 byte)
    ssl_request.push_back(kCharsetUtf8mb4);
    // reserved (23 zeros)
    ssl_request.resize(ssl_request.size() + 23, 0);

    mes_error_t rc = SendPacket(ssl_request);
    if (rc != MES_OK) {
      last_error_ = "Failed to send SSL request packet";
      return rc;
    }

    // Upgrade to TLS
    const char* ca = ssl_ca.empty() ? nullptr : ssl_ca.c_str();
    const char* cert = ssl_cert.empty() ? nullptr : ssl_cert.c_str();
    const char* key = ssl_key.empty() ? nullptr : ssl_key.c_str();
    const char* hn = host.empty() ? nullptr : host.c_str();
    rc = socket_.UpgradeToTLS(ssl_mode, ca, cert, key, hn);
    if (rc != MES_OK) {
      last_error_ = "TLS handshake failed";
      return rc;
    }
  }

  // Compute auth response
  std::vector<uint8_t> auth_response;
  mes_error_t rc = ComputeAuthResponse(auth_plugin, password, auth_data,
                                       &auth_response);
  if (rc != MES_OK) {
    return rc;
  }

  // Build Handshake Response 41 packet
  std::vector<uint8_t> payload;
  payload.reserve(128);

  // capabilities (4 bytes)
  WriteFixedInt(&payload, client_caps, 4);
  // max_packet_size (4 bytes)
  WriteFixedInt(&payload, kMaxPacketSize, 4);
  // charset (1 byte)
  payload.push_back(kCharsetUtf8mb4);
  // reserved (23 zeros)
  payload.resize(payload.size() + 23, 0);

  // username (NUL-terminated)
  payload.insert(payload.end(), user.begin(), user.end());
  payload.push_back(0);

  // auth response as length-encoded string
  if (client_caps & kClientPluginAuthLenencData) {
    WriteLenEncString(&payload,
                      std::string(auth_response.begin(), auth_response.end()));
  } else {
    // Fallback: length-encoded with single byte length
    payload.push_back(static_cast<uint8_t>(auth_response.size()));
    payload.insert(payload.end(), auth_response.begin(), auth_response.end());
  }

  // auth_plugin_name (NUL-terminated, if CLIENT_PLUGIN_AUTH)
  if (client_caps & kClientPluginAuth) {
    payload.insert(payload.end(), auth_plugin.begin(), auth_plugin.end());
    payload.push_back(0);
  }

  return SendPacket(payload);
}

mes_error_t MysqlConnection::HandleAuthResponse(const std::string& password) {
  std::vector<uint8_t> packet;
  mes_error_t rc = ReadPacket(&socket_, &packet, &sequence_id_);
  if (rc != MES_OK) {
    last_error_ = "Failed to read auth response";
    return MES_ERR_AUTH;
  }
  ++sequence_id_;

  if (packet.empty()) {
    last_error_ = "Empty auth response packet";
    return MES_ERR_AUTH;
  }

  switch (packet[0]) {
    case kPacketOk:
      return MES_OK;

    case kPacketErr:
      return ProcessOkOrError(packet);

    case kPacketAuthSwitchRequest:
      return HandleAuthSwitchRequest(packet, password);

    case kPacketAuthMoreData: {
      // caching_sha2_password additional exchange
      if (packet.size() < 2) {
        last_error_ = "Truncated AuthMoreData packet";
        return MES_ERR_AUTH;
      }

      uint8_t status = packet[1];
      if (status == kCachingSha2FastAuthSuccess) {
        // Fast auth succeeded; read final OK packet
        std::vector<uint8_t> ok_packet;
        rc = ReadPacket(&socket_, &ok_packet, &sequence_id_);
        if (rc != MES_OK) {
          last_error_ = "Failed to read OK after fast auth success";
          return MES_ERR_AUTH;
        }
        ++sequence_id_;
        return ProcessOkOrError(ok_packet);
      }

      if (status == kCachingSha2FullAuthRequired) {
        // Full authentication required
        if (!socket_.IsTlsActive()) {
          last_error_ =
              "caching_sha2_password full auth requires TLS connection";
          return MES_ERR_AUTH;
        }

        // Send cleartext password + NUL terminator over TLS
        std::vector<uint8_t> cleartext_payload(password.begin(),
                                               password.end());
        cleartext_payload.push_back(0);

        rc = SendPacket(cleartext_payload);
        if (rc != MES_OK) {
          last_error_ = "Failed to send cleartext password";
          return MES_ERR_AUTH;
        }

        // Read final OK/ERR
        std::vector<uint8_t> final_packet;
        rc = ReadPacket(&socket_, &final_packet, &sequence_id_);
        if (rc != MES_OK) {
          last_error_ = "Failed to read final auth response";
          return MES_ERR_AUTH;
        }
        ++sequence_id_;
        return ProcessOkOrError(final_packet);
      }

      last_error_ = "Unknown AuthMoreData status: " + std::to_string(status);
      return MES_ERR_AUTH;
    }

    default:
      last_error_ = "Unexpected auth response marker: " +
                    std::to_string(packet[0]);
      return MES_ERR_AUTH;
  }
}

mes_error_t MysqlConnection::HandleAuthSwitchRequest(
    const std::vector<uint8_t>& packet, const std::string& password) {
  if (packet.size() < 2) {
    last_error_ = "Truncated auth switch request";
    return MES_ERR_AUTH;
  }

  // Parse plugin name (NUL-terminated, starting at data[1])
  const uint8_t* data = packet.data();
  const size_t len = packet.size();
  size_t pos = 1;

  const uint8_t* nul = static_cast<const uint8_t*>(
      std::memchr(data + pos, 0, len - pos));
  if (nul == nullptr) {
    last_error_ = "Invalid auth switch: missing plugin name terminator";
    return MES_ERR_AUTH;
  }

  std::string plugin_name(reinterpret_cast<const char*>(data + pos),
                           reinterpret_cast<const char*>(nul));
  pos = static_cast<size_t>(nul - data) + 1;

  // Remaining bytes are the new auth data.
  // Strip trailing NUL byte if present (mysql_native_password sends 20-byte
  // scramble + NUL terminator, but the salt must be exactly 20 bytes).
  size_t auth_data_len = len - pos;
  if (auth_data_len > 0 && data[len - 1] == 0x00) {
    --auth_data_len;
  }
  std::vector<uint8_t> new_auth_data(data + pos, data + pos + auth_data_len);

  // Compute auth response with new plugin
  std::vector<uint8_t> auth_response;
  mes_error_t rc =
      ComputeAuthResponse(plugin_name, password, new_auth_data, &auth_response);
  if (rc != MES_OK) {
    return rc;
  }

  // Send auth response
  rc = SendPacket(auth_response);
  if (rc != MES_OK) {
    last_error_ = "Failed to send auth switch response";
    return rc;
  }

  // Read and process the server's reply (may recurse into AuthMoreData)
  return HandleAuthResponse(password);
}

mes_error_t MysqlConnection::ProcessOkOrError(
    const std::vector<uint8_t>& packet) {
  if (packet.empty()) {
    last_error_ = "Empty packet in ProcessOkOrError";
    return MES_ERR_AUTH;
  }

  if (packet[0] == kPacketOk) {
    return MES_OK;
  }

  if (packet[0] == kPacketErr) {
    if (packet.size() < 3) {
      last_error_ = "Unknown error (truncated ERR packet)";
      return MES_ERR_AUTH;
    }

    uint16_t error_code =
        static_cast<uint16_t>(ReadFixedInt(packet.data() + 1, 2));
    size_t msg_start = 3;

    // CLIENT_PROTOCOL_41: skip '#' marker + 5-byte SQL state
    if (packet.size() > 3 && packet[3] == '#') {
      msg_start = 9;  // 1(marker) + 2(error_code) + 1('#') + 5(sql_state)
    }

    if (msg_start < packet.size()) {
      last_error_ =
          "MySQL error " + std::to_string(error_code) + ": " +
          std::string(reinterpret_cast<const char*>(packet.data() + msg_start),
                      packet.size() - msg_start);
    } else {
      last_error_ = "MySQL error " + std::to_string(error_code);
    }

    return MES_ERR_AUTH;
  }

  last_error_ =
      "Unexpected packet marker: " + std::to_string(packet[0]);
  return MES_ERR_AUTH;
}

mes_error_t MysqlConnection::ComputeAuthResponse(
    const std::string& plugin, const std::string& password,
    const std::vector<uint8_t>& salt, std::vector<uint8_t>* response) {
  if (password.empty()) {
    response->clear();
    return MES_OK;
  }

  if (plugin == kPluginNativePassword) {
    return AuthNativePassword(password, salt.data(), salt.size(), response);
  }

  if (plugin == kPluginCachingSha2Password) {
    return AuthCachingSha2Password(password, salt.data(), salt.size(), response);
  }

  last_error_ = "Unsupported auth plugin: " + plugin;
  return MES_ERR_AUTH;
}

mes_error_t MysqlConnection::SendPacket(const std::vector<uint8_t>& payload) {
  PacketBuffer pkt;
  pkt.WritePacket(payload.data(), payload.size(), &sequence_id_);

  mes_error_t rc = socket_.WriteAll(pkt.Data(), pkt.Size());
  if (rc != MES_OK) {
    last_error_ = "Failed to send packet";
    return MES_ERR_STREAM;
  }

  return MES_OK;
}

}  // namespace mes::protocol
