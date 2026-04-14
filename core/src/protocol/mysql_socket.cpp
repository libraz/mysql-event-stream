// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#include "protocol/mysql_socket.h"

#include <openssl/err.h>
#include <openssl/ssl.h>
#include <openssl/x509v3.h>

#include <cerrno>
#include <climits>
#include <cstring>

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#pragma comment(lib, "ws2_32.lib")
#else
#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

#include "logger.h"

namespace mes::protocol {

namespace {

#ifdef _WIN32

/** @brief One-time Winsock initializer. */
struct WinsockInit {
  WinsockInit() {
    WSADATA wsa;
    WSAStartup(MAKEWORD(2, 2), &wsa);
  }
  ~WinsockInit() { WSACleanup(); }
};

static WinsockInit& EnsureWinsockInit() {
  static WinsockInit instance;
  return instance;
}

inline int CloseSocket(int fd) { return closesocket(fd); }

inline int SetNonBlocking(int fd, bool enable) {
  u_long mode = enable ? 1 : 0;
  return ioctlsocket(fd, FIONBIO, &mode);
}

inline int GetSocketError(int fd) {
  int err = 0;
  int len = sizeof(err);
  getsockopt(fd, SOL_SOCKET, SO_ERROR, reinterpret_cast<char*>(&err), &len);
  return err;
}

#else  // POSIX

inline int CloseSocket(int fd) { return close(fd); }

inline int SetNonBlocking(int fd, bool enable) {
  int flags = fcntl(fd, F_GETFL, 0);
  if (flags < 0) return -1;
  if (enable) {
    flags |= O_NONBLOCK;
  } else {
    flags &= ~O_NONBLOCK;
  }
  return fcntl(fd, F_SETFL, flags);
}

inline int GetSocketError(int fd) {
  int err = 0;
  socklen_t len = sizeof(err);
  getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &len);
  return err;
}

#endif

/** @brief Collect all OpenSSL error strings from the error queue. */
std::string GetOpenSSLError() {
  unsigned long err = ERR_get_error();
  if (err == 0) return "unknown SSL error";
  char buf[256];
  ERR_error_string_n(err, buf, sizeof(buf));
  std::string result(buf);
  // Drain remaining errors from the queue
  while ((err = ERR_get_error()) != 0) {
    ERR_error_string_n(err, buf, sizeof(buf));
    result += "; ";
    result += buf;
  }
  return result;
}

}  // namespace

// --- Construction / Destruction ---

SocketHandle::SocketHandle() = default;

SocketHandle::~SocketHandle() { Close(); }

SocketHandle::SocketHandle(SocketHandle&& other) noexcept
    : fd_(other.fd_),
      ssl_ctx_(other.ssl_ctx_),
      ssl_(other.ssl_),
      tls_active_(other.tls_active_) {
  other.fd_ = -1;
  other.ssl_ctx_ = nullptr;
  other.ssl_ = nullptr;
  other.tls_active_ = false;
}

SocketHandle& SocketHandle::operator=(SocketHandle&& other) noexcept {
  if (this != &other) {
    Close();
    fd_ = other.fd_;
    ssl_ctx_ = other.ssl_ctx_;
    ssl_ = other.ssl_;
    tls_active_ = other.tls_active_;
    other.fd_ = -1;
    other.ssl_ctx_ = nullptr;
    other.ssl_ = nullptr;
    other.tls_active_ = false;
  }
  return *this;
}

// --- Connect ---

mes_error_t SocketHandle::Connect(const char* host, uint16_t port,
                                  uint32_t timeout_s) {
  if (host == nullptr) return MES_ERR_NULL_ARG;

#ifdef _WIN32
  EnsureWinsockInit();
#endif

  // Close any previously open socket.
  Close();

  // Resolve hostname.
  struct addrinfo hints {};
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_protocol = IPPROTO_TCP;

  char port_str[8];
  snprintf(port_str, sizeof(port_str), "%u", static_cast<unsigned>(port));

  struct addrinfo* result = nullptr;
  int rc = getaddrinfo(host, port_str, &hints, &result);
  if (rc != 0 || result == nullptr) {
    StructuredLog()
        .Event("socket_resolve_failed")
        .Field("host", host)
        .Field("port", static_cast<int>(port))
        .Field("error", gai_strerror(rc))
        .Error();
    return MES_ERR_CONNECT;
  }

  // Try each resolved address until one succeeds.
  mes_error_t connect_err = MES_ERR_CONNECT;
  for (struct addrinfo* rp = result; rp != nullptr; rp = rp->ai_next) {
    fd_ = static_cast<int>(
        socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol));
    if (fd_ < 0) continue;

    if (timeout_s > 0) {
      // Non-blocking connect with timeout via poll().
      if (SetNonBlocking(fd_, true) < 0) {
        CloseSocket(fd_);
        fd_ = -1;
        continue;
      }

      rc = ::connect(fd_, rp->ai_addr, static_cast<int>(rp->ai_addrlen));
      if (rc < 0) {
#ifdef _WIN32
        int err = WSAGetLastError();
        if (err != WSAEWOULDBLOCK) {
#else
        int err = errno;
        if (err != EINPROGRESS) {
#endif
          CloseSocket(fd_);
          fd_ = -1;
          continue;
        }

        // Wait for connect to complete.
        struct pollfd pfd {};
        pfd.fd = fd_;
        pfd.events = POLLOUT;

#ifdef _WIN32
        int timeout_ms = (timeout_s > static_cast<uint32_t>(INT_MAX / 1000))
                             ? INT_MAX
                             : static_cast<int>(timeout_s) * 1000;
        rc = WSAPoll(&pfd, 1, timeout_ms);
#else
        int timeout_ms = (timeout_s > static_cast<uint32_t>(INT_MAX / 1000))
                             ? INT_MAX
                             : static_cast<int>(timeout_s) * 1000;
        rc = poll(&pfd, 1, timeout_ms);
#endif
        if (rc <= 0) {
          // Timeout or poll error.
          StructuredLog()
              .Event("socket_connect_timeout")
              .Field("host", host)
              .Field("port", static_cast<int>(port))
              .Field("timeout_s", static_cast<int>(timeout_s))
              .Error();
          CloseSocket(fd_);
          fd_ = -1;
          continue;
        }

        // Check for connect error.
        int sock_err = GetSocketError(fd_);
        if (sock_err != 0) {
          CloseSocket(fd_);
          fd_ = -1;
          continue;
        }
      }

      // Restore blocking mode.
      if (SetNonBlocking(fd_, false) < 0) {
        CloseSocket(fd_);
        fd_ = -1;
        continue;
      }
    } else {
      // Blocking connect (no timeout).
      rc = ::connect(fd_, rp->ai_addr, static_cast<int>(rp->ai_addrlen));
      if (rc < 0) {
        CloseSocket(fd_);
        fd_ = -1;
        continue;
      }
    }

    // Successfully connected.
#if defined(__APPLE__)
    // Prevent SIGPIPE on write to a closed peer socket (macOS).
    int optval = 1;
    setsockopt(fd_, SOL_SOCKET, SO_NOSIGPIPE, &optval, sizeof(optval));
#endif
    connect_err = MES_OK;
    break;
  }

  freeaddrinfo(result);

  if (connect_err != MES_OK) {
    StructuredLog()
        .Event("socket_connect_failed")
        .Field("host", host)
        .Field("port", static_cast<int>(port))
        .Error();
    fd_ = -1;
  } else {
    StructuredLog()
        .Event("socket_connected")
        .Field("host", host)
        .Field("port", static_cast<int>(port))
        .Debug();
  }

  return connect_err;
}

// --- TLS ---

mes_error_t SocketHandle::UpgradeToTLS(uint32_t ssl_mode, const char* ssl_ca,
                                       const char* ssl_cert,
                                       const char* ssl_key,
                                       const char* hostname) {
  // Mode 0 = disabled: nothing to do.
  if (ssl_mode == 0) return MES_OK;

  if (fd_ < 0) return MES_ERR_CONNECT;

  // Create SSL context.
  ssl_ctx_ = SSL_CTX_new(TLS_client_method());
  if (ssl_ctx_ == nullptr) {
    StructuredLog()
        .Event("ssl_ctx_create_failed")
        .Field("error", GetOpenSSLError())
        .Error();
    return MES_ERR_CONNECT;
  }

  // Require TLS 1.2 as minimum.
  SSL_CTX_set_min_proto_version(ssl_ctx_, TLS1_2_VERSION);

  // Load CA certificate for server verification.
  if (ssl_ca != nullptr && ssl_ca[0] != '\0') {
    if (SSL_CTX_load_verify_locations(ssl_ctx_, ssl_ca, nullptr) != 1) {
      StructuredLog()
          .Event("ssl_ca_load_failed")
          .Field("path", ssl_ca)
          .Field("error", GetOpenSSLError())
          .Error();
      SSL_CTX_free(ssl_ctx_);
      ssl_ctx_ = nullptr;
      return MES_ERR_CONNECT;
    }
  }

  // Load client certificate if provided.
  if (ssl_cert != nullptr && ssl_cert[0] != '\0') {
    if (SSL_CTX_use_certificate_file(ssl_ctx_, ssl_cert, SSL_FILETYPE_PEM) !=
        1) {
      StructuredLog()
          .Event("ssl_cert_load_failed")
          .Field("path", ssl_cert)
          .Field("error", GetOpenSSLError())
          .Error();
      SSL_CTX_free(ssl_ctx_);
      ssl_ctx_ = nullptr;
      return MES_ERR_CONNECT;
    }
  }

  // Load client private key if provided.
  if (ssl_key != nullptr && ssl_key[0] != '\0') {
    if (SSL_CTX_use_PrivateKey_file(ssl_ctx_, ssl_key, SSL_FILETYPE_PEM) !=
        1) {
      StructuredLog()
          .Event("ssl_key_load_failed")
          .Field("path", ssl_key)
          .Field("error", GetOpenSSLError())
          .Error();
      SSL_CTX_free(ssl_ctx_);
      ssl_ctx_ = nullptr;
      return MES_ERR_CONNECT;
    }
  }

  // Set verification mode based on ssl_mode.
  // 1=preferred, 2=required: encrypt but do not verify server cert.
  // 3=verify_ca: verify the server certificate against the CA.
  // 4=verify_identity: verify CA + hostname.
  if (ssl_mode >= 3) {
    SSL_CTX_set_verify(ssl_ctx_, SSL_VERIFY_PEER, nullptr);
  } else {
    SSL_CTX_set_verify(ssl_ctx_, SSL_VERIFY_NONE, nullptr);
  }

  // Create SSL session.
  ssl_ = SSL_new(ssl_ctx_);
  if (ssl_ == nullptr) {
    StructuredLog()
        .Event("ssl_new_failed")
        .Field("error", GetOpenSSLError())
        .Error();
    SSL_CTX_free(ssl_ctx_);
    ssl_ctx_ = nullptr;
    return MES_ERR_CONNECT;
  }

  SSL_set_fd(ssl_, fd_);

  // For verify_identity mode, enable hostname checking.
  if (ssl_mode >= 4 && hostname != nullptr && hostname[0] != '\0') {
    SSL_set_hostflags(ssl_, X509_CHECK_FLAG_NO_PARTIAL_WILDCARDS);
    if (SSL_set1_host(ssl_, hostname) != 1) {
      StructuredLog()
          .Event("ssl_set1_host_failed")
          .Field("hostname", hostname)
          .Field("error", GetOpenSSLError())
          .Error();
      SSL_free(ssl_);
      ssl_ = nullptr;
      SSL_CTX_free(ssl_ctx_);
      ssl_ctx_ = nullptr;
      return MES_ERR_CONNECT;
    }
  }

  // Set SNI (Server Name Indication) extension so the server can select
  // the correct certificate when hosting multiple virtual hosts.
  if (hostname != nullptr && hostname[0] != '\0') {
    SSL_set_tlsext_host_name(ssl_, hostname);
  }

  // Perform TLS handshake.
  int ret = SSL_connect(ssl_);
  if (ret != 1) {
    int ssl_err = SSL_get_error(ssl_, ret);
    StructuredLog()
        .Event("ssl_handshake_failed")
        .Field("ssl_error", ssl_err)
        .Field("error", GetOpenSSLError())
        .Error();
    SSL_free(ssl_);
    ssl_ = nullptr;
    SSL_CTX_free(ssl_ctx_);
    ssl_ctx_ = nullptr;
    return MES_ERR_CONNECT;
  }

  tls_active_ = true;
  StructuredLog()
      .Event("ssl_handshake_complete")
      .Field("protocol", SSL_get_version(ssl_))
      .Debug();

  return MES_OK;
}

// --- Timeout ---

mes_error_t SocketHandle::SetReadTimeout(uint32_t timeout_s) {
  if (fd_ < 0) return MES_ERR_CONNECT;

#ifdef _WIN32
  DWORD tv = static_cast<DWORD>(timeout_s) * 1000;
  if (setsockopt(fd_, SOL_SOCKET, SO_RCVTIMEO, reinterpret_cast<const char*>(&tv),
                 sizeof(tv)) != 0) {
    return MES_ERR_CONNECT;
  }
#else
  struct timeval tv {};
  tv.tv_sec = static_cast<time_t>(timeout_s);
  tv.tv_usec = 0;
  if (setsockopt(fd_, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) != 0) {
    return MES_ERR_CONNECT;
  }
#endif

  return MES_OK;
}

// --- Read / Write ---

mes_error_t SocketHandle::ReadExact(uint8_t* buf, size_t len) {
  if (buf == nullptr) return MES_ERR_NULL_ARG;
  if (fd_ < 0) return MES_ERR_STREAM;

  size_t total = 0;
  while (total < len) {
    int n;
    if (tls_active_) {
      n = SSL_read(ssl_, buf + total, static_cast<int>(len - total));
      if (n <= 0) {
        int ssl_err = SSL_get_error(ssl_, n);
        // SSL_ERROR_ZERO_RETURN means clean shutdown (EOF).
        if (ssl_err == SSL_ERROR_ZERO_RETURN) {
          StructuredLog().Event("socket_read_eof").Debug();
        } else {
          StructuredLog()
              .Event("ssl_read_error")
              .Field("ssl_error", ssl_err)
              .Field("error", GetOpenSSLError())
              .Error();
        }
        return MES_ERR_STREAM;
      }
    } else {
#ifdef _WIN32
      n = recv(fd_, reinterpret_cast<char*>(buf + total),
               static_cast<int>(len - total), 0);
#else
      n = static_cast<int>(
          recv(fd_, buf + total, len - total, 0));
#endif
      if (n <= 0) {
        if (n == 0) {
          StructuredLog().Event("socket_read_eof").Debug();
        } else {
          StructuredLog()
              .Event("socket_read_error")
              .Field("errno", errno)
              .Field("error", strerror(errno))
              .Error();
        }
        return MES_ERR_STREAM;
      }
    }
    total += static_cast<size_t>(n);
  }

  return MES_OK;
}

mes_error_t SocketHandle::WriteAll(const uint8_t* buf, size_t len) {
  if (buf == nullptr) return MES_ERR_NULL_ARG;
  if (fd_ < 0) return MES_ERR_STREAM;

  size_t total = 0;
  while (total < len) {
    int n;
    if (tls_active_) {
      n = SSL_write(ssl_, buf + total, static_cast<int>(len - total));
      if (n <= 0) {
        int ssl_err = SSL_get_error(ssl_, n);
        StructuredLog()
            .Event("ssl_write_error")
            .Field("ssl_error", ssl_err)
            .Field("error", GetOpenSSLError())
            .Error();
        return MES_ERR_STREAM;
      }
    } else {
#ifdef _WIN32
      n = send(fd_, reinterpret_cast<const char*>(buf + total),
               static_cast<int>(len - total), 0);
#elif defined(__linux__)
      n = static_cast<int>(
          send(fd_, buf + total, len - total, MSG_NOSIGNAL));
#else
      n = static_cast<int>(
          send(fd_, buf + total, len - total, 0));
#endif
      if (n <= 0) {
        StructuredLog()
            .Event("socket_write_error")
            .Field("errno", errno)
            .Field("error", strerror(errno))
            .Error();
        return MES_ERR_STREAM;
      }
    }
    total += static_cast<size_t>(n);
  }

  return MES_OK;
}

// --- Shutdown / Close ---

void SocketHandle::Shutdown() {
  if (fd_ >= 0) {
#ifdef _WIN32
    shutdown(fd_, SD_BOTH);
#else
    shutdown(fd_, SHUT_RDWR);
#endif
  }
}

bool SocketHandle::IsValid() const { return fd_ >= 0; }

bool SocketHandle::IsTlsActive() const { return tls_active_; }

void SocketHandle::Close() {
  if (ssl_ != nullptr) {
    // Attempt a clean TLS shutdown; ignore errors (we are tearing down).
    SSL_shutdown(ssl_);
    SSL_free(ssl_);
    ssl_ = nullptr;
  }

  if (ssl_ctx_ != nullptr) {
    SSL_CTX_free(ssl_ctx_);
    ssl_ctx_ = nullptr;
  }

  tls_active_ = false;

  if (fd_ >= 0) {
    CloseSocket(fd_);
    fd_ = -1;
  }
}

}  // namespace mes::protocol
