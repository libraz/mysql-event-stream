// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * @file mysql_socket.h
 * @brief Platform-portable TCP socket with optional OpenSSL TLS support
 */

#ifndef MES_PROTOCOL_MYSQL_SOCKET_H_
#define MES_PROTOCOL_MYSQL_SOCKET_H_

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <mutex>

#include "mes.h"

// Forward declarations to avoid exposing OpenSSL headers to consumers.
struct ssl_st;
struct ssl_ctx_st;
typedef struct ssl_st SSL;
typedef struct ssl_ctx_st SSL_CTX;

namespace mes::protocol {

/**
 * @brief Platform-portable TCP socket with optional OpenSSL TLS upgrade.
 *
 * Provides blocking read/write over a plain TCP or TLS-encrypted connection.
 * The typical lifecycle is:
 *   1. Connect()       - establish TCP connection
 *   2. UpgradeToTLS()  - optional, before MySQL authentication
 *   3. ReadExact() / WriteAll() - data transfer
 *   4. Shutdown() or destructor
 *
 * Thread safety: Shutdown() may be called from any thread to interrupt a
 * blocking ReadExact(). All other methods must be called from a single thread.
 */
class SocketHandle {
 public:
  SocketHandle();
  ~SocketHandle();

  // Movable but not copyable.
  SocketHandle(SocketHandle&& other) noexcept;
  SocketHandle& operator=(SocketHandle&& other) noexcept;
  SocketHandle(const SocketHandle&) = delete;
  SocketHandle& operator=(const SocketHandle&) = delete;

  /**
   * @brief Establish a TCP connection to the given host and port.
   *
   * Uses non-blocking connect with poll() to enforce the timeout, then
   * switches the socket back to blocking mode.
   *
   * @param host      Hostname or IP address (resolved via getaddrinfo).
   * @param port      TCP port number.
   * @param timeout_s Connection timeout in seconds (0 = OS default).
   * @return MES_OK on success, MES_ERR_CONNECT on failure.
   */
  mes_error_t Connect(const char* host, uint16_t port, uint32_t timeout_s);

  /**
   * @brief Upgrade the existing TCP connection to TLS.
   *
   * Must be called after Connect() and before any application-layer I/O
   * that requires encryption (e.g., before MySQL authentication when
   * the server supports SSL).
   *
   * @param ssl_mode  0=disabled (no-op), 1=preferred, 2=required,
   *                  3=verify_ca, 4=verify_identity.
   * @param ssl_ca    Path to CA certificate file. In verify modes, NULL/empty
   *                  uses the operating system trust store.
   * @param ssl_cert  Path to client certificate file (NULL to skip).
   * @param ssl_key   Path to client private key file (NULL to skip).
   * @param hostname  Server hostname for SNI and verify_identity checks
   *                  (NULL to skip).
   * @return MES_OK on success, MES_ERR_CONNECT on TLS handshake failure.
   */
  mes_error_t UpgradeToTLS(uint32_t ssl_mode, const char* ssl_ca, const char* ssl_cert,
                           const char* ssl_key, const char* hostname);

  /**
   * @brief Set the I/O timeout on the underlying socket.
   *
   * Applies SO_RCVTIMEO and SO_SNDTIMEO so blocking receive/send calls time
   * out instead of blocking indefinitely.
   *
   * @param timeout_s Timeout in seconds (0 = no timeout).
   * @return MES_OK on success, MES_ERR_CONNECT on setsockopt failure.
   */
  mes_error_t SetReadTimeout(uint32_t timeout_s);

  /**
   * @brief Read exactly @p len bytes into @p buf.
   *
   * Loops internally until all bytes are received. TLS-aware: uses
   * SSL_read when a TLS session is active, otherwise plain recv.
   *
   * @param buf  Destination buffer (must hold at least @p len bytes).
   * @param len  Number of bytes to read.
   * @return MES_OK on success, MES_ERR_STREAM on error or unexpected EOF.
   */
  mes_error_t ReadExact(uint8_t* buf, size_t len);

  /**
   * @brief Write all @p len bytes from @p buf.
   *
   * Loops internally until all bytes are sent. TLS-aware: uses
   * SSL_write when a TLS session is active, otherwise plain send.
   *
   * @param buf  Source buffer.
   * @param len  Number of bytes to write.
   * @return MES_OK on success, MES_ERR_STREAM on error.
   */
  mes_error_t WriteAll(const uint8_t* buf, size_t len);

  /**
   * @brief Shut down the socket to interrupt blocking I/O.
   *
   * Safe to call from any thread. Issues shutdown(SHUT_RDWR) which
   * causes any blocking ReadExact() in another thread to return an error.
   */
  void Shutdown();

  /** @brief Returns true if the socket file descriptor is valid. */
  bool IsValid() const;

  /** @brief Returns true if a TLS session is active on this socket. */
  bool IsTlsActive() const;

  /**
   * @brief Permanently close a connection whose protocol state is no longer usable.
   *
   * Unlike a normal MySQL disconnect this does not send COM_QUIT. Use it after
   * a malformed or partially consumed server response so a later command
   * cannot interpret leftover packets as its own response. The handle is
   * invalid after this call.
   */
  void Poison();

 private:
  /** @brief Platform socket descriptor (-1 when invalid). */
  std::atomic<int> fd_{-1};

  // Serializes shutdown() with Close(). Without it, a Stop() thread can
  // observe an old descriptor, while Disconnect() closes it and a subsequent
  // Connect() reuses the same number for an unrelated socket.
  mutable std::mutex lifecycle_mutex_;

  /** @brief OpenSSL context (owned, may be null). */
  SSL_CTX* ssl_ctx_ = nullptr;

  /** @brief OpenSSL session (owned, may be null). */
  SSL* ssl_ = nullptr;

  /** @brief True after a successful TLS handshake. */
  bool tls_active_ = false;

  /** Monotonic deadline budget used for TLS read/write retry loops. */
  uint32_t read_timeout_s_ = 0;

  // Plain-TCP read-ahead prevents packet framing from issuing one recv() for
  // the 4-byte header and another for its payload. TLS has its own encrypted
  // record buffering inside OpenSSL, so this buffer is intentionally bypassed
  // for tls_active_ connections.
  static constexpr size_t kReadAheadCapacity = 64u * 1024u;
  std::array<uint8_t, kReadAheadCapacity> read_ahead_{};
  size_t read_ahead_begin_ = 0;
  size_t read_ahead_end_ = 0;

  /** @brief Release all resources (SSL objects and socket). */
  void Close();
};

}  // namespace mes::protocol

#endif  // MES_PROTOCOL_MYSQL_SOCKET_H_
