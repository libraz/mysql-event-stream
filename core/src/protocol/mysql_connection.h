// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * @file mysql_connection.h
 * @brief MySQL connection and handshake protocol handler
 *
 * Implements the MySQL client/server handshake including capability
 * negotiation, optional TLS upgrade, and pluggable authentication
 * (mysql_native_password and caching_sha2_password).
 */

#ifndef MES_PROTOCOL_MYSQL_CONNECTION_H_
#define MES_PROTOCOL_MYSQL_CONNECTION_H_

#include <cstdint>
#include <string>
#include <vector>

#include "mes.h"
#include "protocol/mysql_socket.h"

namespace mes::protocol {

/**
 * @brief Parsed server handshake (Initial Handshake Packet v10)
 */
struct ServerHandshake {
  uint8_t protocol_version = 0;
  std::string server_version;
  uint32_t connection_id = 0;
  std::vector<uint8_t> auth_data;  ///< Salt (part1 + part2, typically 20 bytes)
  uint32_t server_capabilities = 0;
  uint8_t charset = 0;
  uint16_t status_flags = 0;
  std::string auth_plugin_name;
};

/**
 * @brief MySQL connection with handshake and authentication support
 *
 * Manages the full lifecycle of a MySQL client connection: TCP connect,
 * server handshake parsing, optional TLS upgrade, authentication (including
 * auth switch and caching_sha2_password fast/full auth), and graceful
 * disconnect via COM_QUIT.
 *
 * Thread safety: Not thread-safe. All methods must be called from a single
 * thread, except that the underlying socket's Shutdown() may be called from
 * any thread to interrupt blocking I/O.
 */
class MysqlConnection {
 public:
  MysqlConnection();
  ~MysqlConnection();

  // Non-copyable, non-movable (owns socket state)
  MysqlConnection(const MysqlConnection&) = delete;
  MysqlConnection& operator=(const MysqlConnection&) = delete;

  /**
   * @brief Connect to a MySQL server and complete authentication
   *
   * Performs the full connection sequence: TCP connect, read server handshake,
   * optional TLS upgrade, send client handshake response, and handle the
   * authentication exchange.
   *
   * @param host            Hostname or IP address
   * @param port            TCP port number
   * @param user            MySQL username
   * @param password        MySQL password (plaintext)
   * @param connect_timeout_s  TCP connection timeout in seconds (0 = OS default)
   * @param read_timeout_s    Socket read timeout in seconds (0 = no timeout)
   * @param ssl_mode        TLS mode: 0=disabled, 1=preferred, 2=required,
   *                        3=verify_ca, 4=verify_identity
   * @param ssl_ca          Path to CA certificate file (empty to skip)
   * @param ssl_cert        Path to client certificate file (empty to skip)
   * @param ssl_key         Path to client private key file (empty to skip)
   * @return MES_OK on success, MES_ERR_CONNECT on TCP failure,
   *         MES_ERR_AUTH on authentication failure
   */
  mes_error_t Connect(const std::string& host, uint16_t port,
                      const std::string& user, const std::string& password,
                      uint32_t connect_timeout_s, uint32_t read_timeout_s,
                      uint32_t ssl_mode, const std::string& ssl_ca,
                      const std::string& ssl_cert, const std::string& ssl_key);

  /** @brief Send COM_QUIT and close the connection */
  void Disconnect();

  /** @brief Check if the connection is established and authenticated */
  bool IsConnected() const;

  /** @brief Access the underlying socket for direct I/O */
  SocketHandle* Socket();

  /** @brief Get the last error message (empty if no error) */
  const std::string& GetLastError() const;

  /** @brief Get parsed server handshake information */
  const ServerHandshake& GetServerInfo() const;

 private:
  SocketHandle socket_;
  ServerHandshake server_info_;
  std::string last_error_;
  bool connected_ = false;
  uint8_t sequence_id_ = 0;

  /**
   * @brief Parse the server's Initial Handshake Packet (protocol v10)
   * @param packet  Raw packet payload
   * @return MES_OK on success, MES_ERR_AUTH on parse failure
   */
  mes_error_t ParseServerHandshake(const std::vector<uint8_t>& packet);

  /**
   * @brief Build and send the client Handshake Response packet
   *
   * Optionally sends an SSL Request packet first if TLS is requested.
   *
   * @param user        MySQL username
   * @param password    MySQL password
   * @param auth_plugin Authentication plugin name from server
   * @param auth_data   Server-provided auth salt
   * @param ssl_mode    TLS mode (0=disabled)
   * @param ssl_ca      Path to CA certificate file
   * @param ssl_cert    Path to client certificate file
   * @param ssl_key     Path to client private key file
   * @param host        Server hostname for TLS SNI and hostname verification
   * @return MES_OK on success
   */
  mes_error_t SendHandshakeResponse(const std::string& user,
                                    const std::string& password,
                                    const std::string& auth_plugin,
                                    const std::vector<uint8_t>& auth_data,
                                    uint32_t ssl_mode,
                                    const std::string& ssl_ca,
                                    const std::string& ssl_cert,
                                    const std::string& ssl_key,
                                    const std::string& host);

  /**
   * @brief Handle server's auth response (OK, ERR, AuthSwitch, AuthMoreData)
   * @param password  User password for re-authentication if needed
   * @return MES_OK on success, MES_ERR_AUTH on failure
   */
  mes_error_t HandleAuthResponse(const std::string& password);

  /**
   * @brief Handle an Authentication Method Switch Request (0xFE)
   * @param packet    Raw packet payload
   * @param password  User password
   * @return MES_OK on success, MES_ERR_AUTH on failure
   */
  mes_error_t HandleAuthSwitchRequest(const std::vector<uint8_t>& packet,
                                      const std::string& password);

  /**
   * @brief Check if a packet is OK or ERR and process accordingly
   * @param packet  Raw packet payload
   * @return MES_OK for OK packet, MES_ERR_AUTH for ERR packet
   */
  mes_error_t ProcessOkOrError(const std::vector<uint8_t>& packet);

  /**
   * @brief Compute auth response bytes for a given plugin
   * @param plugin    Plugin name
   * @param password  User password
   * @param salt      Auth salt data
   * @param response  Output: computed auth response
   * @return MES_OK on success
   */
  mes_error_t ComputeAuthResponse(const std::string& plugin,
                                  const std::string& password,
                                  const std::vector<uint8_t>& salt,
                                  std::vector<uint8_t>* response);

  /**
   * @brief Send a packet and advance the sequence ID
   * @param payload  Payload data
   * @return MES_OK on success
   */
  mes_error_t SendPacket(const std::vector<uint8_t>& payload);
};

}  // namespace mes::protocol

#endif  // MES_PROTOCOL_MYSQL_CONNECTION_H_
