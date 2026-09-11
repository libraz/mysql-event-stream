// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#include "protocol/mysql_socket.h"

#include <openssl/err.h>
#include <openssl/ssl.h>
#include <openssl/x509v3.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <climits>
#include <cstring>
#include <ctime>
#include <optional>

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
#include <pthread.h>
#include <signal.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

#include "logger.h"

namespace mes::protocol {

namespace {

#ifdef __linux__
// Suppresses SIGPIPE for the calling thread across a TLS operation.
//
// OpenSSL writes to the underlying socket without MSG_NOSIGNAL, and SO_NOSIGPIPE
// does not exist on Linux, so an operation on a peer that has closed the
// connection would deliver SIGPIPE and terminate the process. This is not a
// write-only concern: SSL_read() sends on the socket as well, for renegotiation
// and close_notify. Block SIGPIPE for this thread for the scope of the guard and
// drain any SIGPIPE raised inside it, without touching process-wide signal
// disposition or consuming a SIGPIPE that was already pending on entry.
//
// Other platforms do not need this: Windows has no SIGPIPE, and macOS/BSD apply
// SO_NOSIGPIPE to the socket, which covers the OpenSSL entry points too.
class ScopedSigPipeSuppressor {
 public:
  ScopedSigPipeSuppressor() {
    sigset_t pipe_set;
    sigemptyset(&pipe_set);
    sigaddset(&pipe_set, SIGPIPE);

    sigset_t pending;
    sigemptyset(&pending);
    sigpending(&pending);
    already_pending_ = sigismember(&pending, SIGPIPE) == 1;

    sigset_t old_set;
    sigemptyset(&old_set);
    if (pthread_sigmask(SIG_BLOCK, &pipe_set, &old_set) == 0) {
      was_blocked_ = sigismember(&old_set, SIGPIPE) == 1;
      active_ = true;
    }
  }

  ~ScopedSigPipeSuppressor() {
    if (!active_) return;
    if (!already_pending_) {
      // Consume a SIGPIPE raised inside this scope while it was blocked.
      sigset_t pipe_set;
      sigemptyset(&pipe_set);
      sigaddset(&pipe_set, SIGPIPE);
      const struct timespec zero = {0, 0};
      int wait_rc;
      do {
        wait_rc = sigtimedwait(&pipe_set, nullptr, &zero);
      } while (wait_rc == -1 && errno == EINTR);
    }
    if (!was_blocked_) {
      sigset_t pipe_set;
      sigemptyset(&pipe_set);
      sigaddset(&pipe_set, SIGPIPE);
      pthread_sigmask(SIG_UNBLOCK, &pipe_set, nullptr);
    }
  }

  ScopedSigPipeSuppressor(const ScopedSigPipeSuppressor&) = delete;
  ScopedSigPipeSuppressor& operator=(const ScopedSigPipeSuppressor&) = delete;

 private:
  bool active_ = false;
  bool was_blocked_ = false;
  bool already_pending_ = false;
};
#else
// Windows has no SIGPIPE; the guard is a no-op.
class ScopedSigPipeSuppressor {};
#endif

}  // namespace

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

using SteadyClock = std::chrono::steady_clock;

/** Wait until the socket direction requested by OpenSSL is ready. */
int WaitForSocket(int fd, bool want_read, SteadyClock::time_point deadline, bool has_deadline) {
  for (;;) {
    int timeout_ms = -1;
    if (has_deadline) {
      auto remaining = deadline - SteadyClock::now();
      if (remaining <= SteadyClock::duration::zero()) return 0;
      auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(remaining).count();
      timeout_ms = static_cast<int>(std::min<int64_t>(std::max<int64_t>(millis, 1), INT_MAX));
    }

    struct pollfd pfd {};
    pfd.fd = fd;
    pfd.events = want_read ? POLLIN : POLLOUT;
#ifdef _WIN32
    int rc = WSAPoll(&pfd, 1, timeout_ms);
    if (rc < 0 && WSAGetLastError() == WSAEINTR) continue;
#else
    int rc = poll(&pfd, 1, timeout_ms);
    if (rc < 0 && errno == EINTR) continue;
#endif
    if (rc <= 0) return rc;
    if ((pfd.revents & pfd.events) != 0) return 1;
    // A peer can send its final TLS record and hang up in the same poll
    // notification. Consume readable/writable data first; report HUP only
    // when the requested direction is not ready.
    if ((pfd.revents & (POLLERR | POLLHUP | POLLNVAL)) != 0) return -1;
  }
}

}  // namespace

// --- Construction / Destruction ---

SocketHandle::SocketHandle() = default;

SocketHandle::~SocketHandle() { Close(); }

SocketHandle::SocketHandle(SocketHandle&& other) noexcept
    : fd_(other.fd_.exchange(-1)),
      ssl_ctx_(other.ssl_ctx_),
      ssl_(other.ssl_),
      tls_active_(other.tls_active_),
      read_timeout_s_(other.read_timeout_s_),
      read_ahead_(other.read_ahead_),
      read_ahead_begin_(other.read_ahead_begin_),
      read_ahead_end_(other.read_ahead_end_) {
  other.ssl_ctx_ = nullptr;
  other.ssl_ = nullptr;
  other.tls_active_ = false;
  other.read_timeout_s_ = 0;
  other.read_ahead_begin_ = 0;
  other.read_ahead_end_ = 0;
}

SocketHandle& SocketHandle::operator=(SocketHandle&& other) noexcept {
  if (this != &other) {
    Close();
    fd_.store(other.fd_.exchange(-1));
    ssl_ctx_ = other.ssl_ctx_;
    ssl_ = other.ssl_;
    tls_active_ = other.tls_active_;
    read_timeout_s_ = other.read_timeout_s_;
    read_ahead_ = other.read_ahead_;
    read_ahead_begin_ = other.read_ahead_begin_;
    read_ahead_end_ = other.read_ahead_end_;
    other.ssl_ctx_ = nullptr;
    other.ssl_ = nullptr;
    other.tls_active_ = false;
    other.read_timeout_s_ = 0;
    other.read_ahead_begin_ = 0;
    other.read_ahead_end_ = 0;
  }
  return *this;
}

// --- Connect ---

mes_error_t SocketHandle::Connect(const char* host, uint16_t port, uint32_t timeout_s) {
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

  const mes_error_t connect_err = ConnectToResolvedAddresses(result, host, port, timeout_s);
  freeaddrinfo(result);
  return connect_err;
}

mes_error_t SocketHandle::ConnectToResolvedAddresses(const struct addrinfo* addresses,
                                                     const char* host, uint16_t port,
                                                     uint32_t timeout_s) {
  // One deadline for the whole call. Applying timeout_s per address would let a
  // dual-stack name block the caller for a multiple of the configured budget.
  const bool has_deadline = timeout_s > 0;
  const auto deadline = SteadyClock::now() + std::chrono::seconds(timeout_s);

  // Try each resolved address until one succeeds or the budget runs out.
  mes_error_t connect_err = MES_ERR_CONNECT;
  for (const struct addrinfo* rp = addresses; rp != nullptr; rp = rp->ai_next) {
    fd_.store(static_cast<int>(socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol)));
    if (fd_.load() < 0) continue;

    if (has_deadline) {
      // Non-blocking connect with timeout via poll().
      if (SetNonBlocking(fd_.load(), true) < 0) {
        CloseSocket(fd_.load());
        fd_.store(-1);
        continue;
      }

      int rc = ::connect(fd_.load(), rp->ai_addr, static_cast<int>(rp->ai_addrlen));
      if (rc < 0) {
#ifdef _WIN32
        int err = WSAGetLastError();
        if (err != WSAEWOULDBLOCK) {
#else
        int err = errno;
        if (err != EINPROGRESS) {
#endif
          CloseSocket(fd_.load());
          fd_.store(-1);
          continue;
        }

        // Wait for connect to complete, using whatever is left of the budget.
        const auto remaining = deadline - SteadyClock::now();
        const int64_t remaining_ms =
            std::chrono::duration_cast<std::chrono::milliseconds>(remaining).count();
        const int timeout_ms =
            remaining_ms <= 0 ? 0 : static_cast<int>(std::min<int64_t>(remaining_ms, INT_MAX));

        struct pollfd pfd {};
        pfd.fd = fd_.load();
        pfd.events = POLLOUT;

#ifdef _WIN32
        rc = WSAPoll(&pfd, 1, timeout_ms);
#else
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
          CloseSocket(fd_.load());
          fd_.store(-1);
          // Remaining candidates would each need a budget that no longer
          // exists; stop rather than overrunning the caller's timeout.
          if (SteadyClock::now() >= deadline) break;
          continue;
        }

        // Check for connect error.
        int sock_err = GetSocketError(fd_.load());
        if (sock_err != 0) {
          CloseSocket(fd_.load());
          fd_.store(-1);
          if (SteadyClock::now() >= deadline) break;
          continue;
        }
      }

      // Restore blocking mode.
      if (SetNonBlocking(fd_.load(), false) < 0) {
        CloseSocket(fd_.load());
        fd_.store(-1);
        continue;
      }
    } else {
      // Blocking connect (no timeout).
      int rc = ::connect(fd_.load(), rp->ai_addr, static_cast<int>(rp->ai_addrlen));
      if (rc < 0) {
        CloseSocket(fd_.load());
        fd_.store(-1);
        continue;
      }
    }

    // Successfully connected.
#if defined(__APPLE__)
    // Prevent SIGPIPE on write to a closed peer socket (macOS).
    int optval = 1;
    setsockopt(fd_.load(), SOL_SOCKET, SO_NOSIGPIPE, &optval, sizeof(optval));
#endif
    connect_err = MES_OK;
    break;
  }

  if (connect_err != MES_OK) {
    StructuredLog()
        .Event("socket_connect_failed")
        .Field("host", host)
        .Field("port", static_cast<int>(port))
        .Error();
    fd_.store(-1);
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

mes_error_t SocketHandle::UpgradeToTLS(uint32_t ssl_mode, const char* ssl_ca, const char* ssl_cert,
                                       const char* ssl_key, const char* hostname) {
  // Mode 0 = disabled: nothing to do.
  if (ssl_mode == 0) return MES_OK;

  if (fd_.load() < 0) return MES_ERR_CONNECT;

  // Create SSL context.
  ssl_ctx_ = SSL_CTX_new(TLS_client_method());
  if (ssl_ctx_ == nullptr) {
    StructuredLog().Event("ssl_ctx_create_failed").Field("error", GetOpenSSLError()).Error();
    return MES_ERR_CONNECT;
  }

  // Require TLS 1.2 as minimum.
  SSL_CTX_set_min_proto_version(ssl_ctx_, TLS1_2_VERSION);

  // Load an explicit CA bundle, or the operating system trust store for
  // verification modes. Without either, SSL_VERIFY_PEER has no anchors and
  // managed services using public CAs cannot be authenticated.
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
  } else if (ssl_mode >= MES_SSL_VERIFY_CA) {
    if (SSL_CTX_set_default_verify_paths(ssl_ctx_) != 1) {
      StructuredLog()
          .Event("ssl_default_verify_paths_failed")
          .Field("error", GetOpenSSLError())
          .Error();
      SSL_CTX_free(ssl_ctx_);
      ssl_ctx_ = nullptr;
      return MES_ERR_CONNECT;
    }
  }

  // Load client certificate if provided.
  if (ssl_cert != nullptr && ssl_cert[0] != '\0') {
    if (SSL_CTX_use_certificate_file(ssl_ctx_, ssl_cert, SSL_FILETYPE_PEM) != 1) {
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
    if (SSL_CTX_use_PrivateKey_file(ssl_ctx_, ssl_key, SSL_FILETYPE_PEM) != 1) {
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

  // Verify that the certificate and private key match.
  if (ssl_cert != nullptr && ssl_cert[0] != '\0' && ssl_key != nullptr && ssl_key[0] != '\0') {
    if (SSL_CTX_check_private_key(ssl_ctx_) != 1) {
      StructuredLog().Event("ssl_keypair_mismatch").Error();
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
    StructuredLog().Event("ssl_new_failed").Field("error", GetOpenSSLError()).Error();
    SSL_CTX_free(ssl_ctx_);
    ssl_ctx_ = nullptr;
    return MES_ERR_CONNECT;
  }

  SSL_set_fd(ssl_, fd_.load());

  // Classify the peer name so verify_identity can bind to the correct SAN
  // entry and SNI is only sent for DNS names (RFC 6066 forbids IP-literal SNI).
  const bool have_host = hostname != nullptr && hostname[0] != '\0';
  bool host_is_ip = false;
  if (have_host) {
    unsigned char addr_buf[sizeof(struct in6_addr)];
    host_is_ip =
        inet_pton(AF_INET, hostname, addr_buf) == 1 || inet_pton(AF_INET6, hostname, addr_buf) == 1;
  }

  // For verify_identity mode, bind the certificate identity check.
  if (ssl_mode >= 4) {
    if (!have_host) {
      // verify_identity cannot be satisfied without a name or address to
      // match against the certificate; CA-only verification would silently
      // accept any valid cert and defeat the requested guarantee.
      StructuredLog().Event("ssl_verify_identity_no_host").Error();
      SSL_free(ssl_);
      ssl_ = nullptr;
      SSL_CTX_free(ssl_ctx_);
      ssl_ctx_ = nullptr;
      return MES_ERR_CONNECT;
    }
    SSL_set_hostflags(ssl_, X509_CHECK_FLAG_NO_PARTIAL_WILDCARDS);
    // IP literals must match an iPAddress SAN, not a DNS name; SSL_set1_host
    // only checks DNS/CN, so route address peers through the IP matcher.
    const int verify_rc = host_is_ip ? X509_VERIFY_PARAM_set1_ip_asc(SSL_get0_param(ssl_), hostname)
                                     : SSL_set1_host(ssl_, hostname);
    if (verify_rc != 1) {
      StructuredLog()
          .Event(host_is_ip ? "ssl_set1_ip_failed" : "ssl_set1_host_failed")
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
  // the correct certificate when hosting multiple virtual hosts. SNI carries
  // DNS hostnames only; IP literals are excluded per RFC 6066.
  if (have_host && !host_is_ip) {
    SSL_set_tlsext_host_name(ssl_, hostname);
  }

  // Perform the TLS handshake under the same deadline already installed for
  // the greeting/authentication path. SSL_connect() otherwise performs an
  // unbounded blocking syscall when a peer accepts TCP but never speaks TLS.
  const bool has_deadline = read_timeout_s_ > 0;
  const auto deadline = SteadyClock::now() + std::chrono::seconds(read_timeout_s_);
  if (has_deadline && SetNonBlocking(fd_.load(), true) != 0) {
    StructuredLog().Event("ssl_handshake_nonblocking_setup_failed").Error();
    SSL_free(ssl_);
    ssl_ = nullptr;
    SSL_CTX_free(ssl_ctx_);
    ssl_ctx_ = nullptr;
    return MES_ERR_CONNECT;
  }
  [[maybe_unused]] ScopedSigPipeSuppressor sigpipe_guard;
  for (;;) {
    int ret = SSL_connect(ssl_);
    if (ret == 1) break;
    const int ssl_err = SSL_get_error(ssl_, ret);
    if (ssl_err == SSL_ERROR_WANT_READ || ssl_err == SSL_ERROR_WANT_WRITE) {
      const int wait_rc =
          WaitForSocket(fd_.load(), ssl_err == SSL_ERROR_WANT_READ, deadline, has_deadline);
      if (wait_rc > 0) continue;
      StructuredLog()
          .Event(wait_rc == 0 ? "ssl_handshake_timeout" : "ssl_handshake_wait_error")
          .Field("timeout_s", static_cast<uint64_t>(read_timeout_s_))
          .Error();
    } else {
      StructuredLog()
          .Event("ssl_handshake_failed")
          .Field("ssl_error", ssl_err)
          .Field("error", GetOpenSSLError())
          .Error();
    }
    SSL_free(ssl_);
    ssl_ = nullptr;
    SSL_CTX_free(ssl_ctx_);
    ssl_ctx_ = nullptr;
    return MES_ERR_CONNECT;
  }

  // For verify modes (verify_ca / verify_identity) the handshake succeeding is
  // not sufficient: OpenSSL records the verification outcome separately, and a
  // missing peer certificate or a failed chain check must be treated as a hard
  // error. SSL_VERIFY_PEER already aborts the handshake on most verification
  // failures, but assert the result explicitly so a configuration that lets a
  // peer through (e.g. no certificate presented) cannot silently pass.
  if (ssl_mode >= MES_SSL_VERIFY_CA) {
    const long verify_result = SSL_get_verify_result(ssl_);
    if (verify_result != X509_V_OK) {
      StructuredLog()
          .Event("ssl_verify_failed")
          .Field("verify_result", static_cast<int64_t>(verify_result))
          .Field("reason", X509_verify_cert_error_string(verify_result))
          .Error();
      SSL_free(ssl_);
      ssl_ = nullptr;
      SSL_CTX_free(ssl_ctx_);
      ssl_ctx_ = nullptr;
      return MES_ERR_CONNECT;
    }

    // SSL_get1_peer_certificate returns a reference that must be freed.
    X509* peer_cert = SSL_get1_peer_certificate(ssl_);
    if (peer_cert == nullptr) {
      StructuredLog().Event("ssl_verify_no_peer_certificate").Error();
      SSL_free(ssl_);
      ssl_ = nullptr;
      SSL_CTX_free(ssl_ctx_);
      ssl_ctx_ = nullptr;
      return MES_ERR_CONNECT;
    }
    X509_free(peer_cert);
  }

  tls_active_ = true;
  StructuredLog().Event("ssl_handshake_complete").Field("protocol", SSL_get_version(ssl_)).Debug();

  return MES_OK;
}

// --- Timeout ---

mes_error_t SocketHandle::SetReadTimeout(uint32_t timeout_s) {
  if (fd_.load() < 0) return MES_ERR_CONNECT;

#ifdef _WIN32
  DWORD tv = (timeout_s > 4294967U) ? MAXDWORD : static_cast<DWORD>(timeout_s) * 1000;
  if (setsockopt(fd_.load(), SOL_SOCKET, SO_RCVTIMEO, reinterpret_cast<const char*>(&tv),
                 sizeof(tv)) != 0) {
    return MES_ERR_CONNECT;
  }
  if (setsockopt(fd_.load(), SOL_SOCKET, SO_SNDTIMEO, reinterpret_cast<const char*>(&tv),
                 sizeof(tv)) != 0) {
    return MES_ERR_CONNECT;
  }
#else
  struct timeval tv {};
  tv.tv_sec = static_cast<time_t>(timeout_s);
  tv.tv_usec = 0;
  if (setsockopt(fd_.load(), SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) != 0) {
    return MES_ERR_CONNECT;
  }
  if (setsockopt(fd_.load(), SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv)) != 0) {
    return MES_ERR_CONNECT;
  }
#endif

  // OpenSSL must not perform an unbounded blocking syscall before it can
  // report WANT_READ/WANT_WRITE. Drive TLS sockets with poll and a monotonic
  // deadline instead of relying on SO_RCVTIMEO/BIO retry behavior.
  if (tls_active_ && SetNonBlocking(fd_.load(), timeout_s > 0) != 0) {
    return MES_ERR_CONNECT;
  }
  read_timeout_s_ = timeout_s;

  return MES_OK;
}

// --- Read / Write ---

mes_error_t SocketHandle::ReadExact(uint8_t* buf, size_t len) {
  if (buf == nullptr) return MES_ERR_NULL_ARG;
  if (fd_.load() < 0) return MES_ERR_STREAM;

  size_t total = 0;
  if (!tls_active_) {
    while (total < len) {
      const size_t available = read_ahead_end_ - read_ahead_begin_;
      if (available > 0) {
        const size_t copied = std::min(available, len - total);
        std::memcpy(buf + total, read_ahead_.data() + read_ahead_begin_, copied);
        read_ahead_begin_ += copied;
        total += copied;
        continue;
      }

      // A remainder the read-ahead buffer could not hold in one piece is
      // received straight into the caller's buffer: staging it would copy every
      // byte twice and cap each recv() at the buffer size. Smaller remainders
      // still fill the buffer, which is what lets packet framing read a header
      // and its payload with a single recv().
      const size_t remaining = len - total;
      const bool read_direct = remaining > read_ahead_.size();
      uint8_t* const dst = read_direct ? buf + total : read_ahead_.data();
      const size_t want =
          read_direct ? std::min(remaining, static_cast<size_t>(INT_MAX)) : read_ahead_.size();
#ifdef _WIN32
      const int n = recv(fd_.load(), reinterpret_cast<char*>(dst), static_cast<int>(want), 0);
#else
      const int n = static_cast<int>(recv(fd_.load(), dst, want, 0));
#endif
      if (n < 0) {
        if (errno == EINTR) continue;
        StructuredLog()
            .Event("socket_read_error")
            .Field("errno", errno)
            .Field("error", strerror(errno))
            .Error();
        return MES_ERR_STREAM;
      }
      if (n == 0) {
        StructuredLog().Event("socket_read_eof").Debug();
        return MES_ERR_STREAM;
      }
      if (read_direct) {
        total += static_cast<size_t>(n);
        continue;
      }
      read_ahead_begin_ = 0;
      read_ahead_end_ = static_cast<size_t>(n);
    }
    return MES_OK;
  }

  const bool has_deadline = tls_active_ && read_timeout_s_ > 0;
  const auto deadline = SteadyClock::now() + std::chrono::seconds(read_timeout_s_);
  // One guard for the whole call rather than one per iteration, so SIGPIPE is
  // never momentarily deliverable between two reads.
  [[maybe_unused]] ScopedSigPipeSuppressor sigpipe_guard;
  while (total < len) {
    const int n = SSL_read(ssl_, buf + total,
                           static_cast<int>(std::min(len - total, static_cast<size_t>(INT_MAX))));
    if (n <= 0) {
      int ssl_err = SSL_get_error(ssl_, n);
      if (ssl_err == SSL_ERROR_WANT_READ || ssl_err == SSL_ERROR_WANT_WRITE) {
        int wait_rc =
            WaitForSocket(fd_.load(), ssl_err == SSL_ERROR_WANT_READ, deadline, has_deadline);
        if (wait_rc > 0) continue;
        StructuredLog()
            .Event(wait_rc == 0 ? "ssl_read_timeout" : "ssl_read_wait_error")
            .Field("timeout_s", static_cast<uint64_t>(read_timeout_s_))
            .Error();
        return MES_ERR_STREAM;
      }
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
    total += static_cast<size_t>(n);
  }

  return MES_OK;
}

mes_error_t SocketHandle::WriteAll(const uint8_t* buf, size_t len) {
  if (buf == nullptr) return MES_ERR_NULL_ARG;
  if (fd_.load() < 0) return MES_ERR_STREAM;

  // The plain send() path is already SIGPIPE-safe (MSG_NOSIGNAL / SO_NOSIGPIPE);
  // guard only the TLS path, where SSL_write() offers no such protection.
  std::optional<ScopedSigPipeSuppressor> sigpipe_guard;
  if (tls_active_) sigpipe_guard.emplace();

  size_t total = 0;
  const bool has_deadline = tls_active_ && read_timeout_s_ > 0;
  const auto deadline = SteadyClock::now() + std::chrono::seconds(read_timeout_s_);
  while (total < len) {
    int n;
    if (tls_active_) {
      n = SSL_write(ssl_, buf + total,
                    static_cast<int>(std::min(len - total, static_cast<size_t>(INT_MAX))));
      if (n <= 0) {
        int ssl_err = SSL_get_error(ssl_, n);
        if (ssl_err == SSL_ERROR_WANT_READ || ssl_err == SSL_ERROR_WANT_WRITE) {
          int wait_rc =
              WaitForSocket(fd_.load(), ssl_err == SSL_ERROR_WANT_READ, deadline, has_deadline);
          if (wait_rc > 0) continue;
          StructuredLog()
              .Event(wait_rc == 0 ? "ssl_write_timeout" : "ssl_write_wait_error")
              .Field("timeout_s", static_cast<uint64_t>(read_timeout_s_))
              .Error();
          return MES_ERR_STREAM;
        }
        StructuredLog()
            .Event("ssl_write_error")
            .Field("ssl_error", ssl_err)
            .Field("error", GetOpenSSLError())
            .Error();
        return MES_ERR_STREAM;
      }
    } else {
#ifdef _WIN32
      n = send(fd_.load(), reinterpret_cast<const char*>(buf + total),
               static_cast<int>(std::min(len - total, static_cast<size_t>(INT_MAX))), 0);
#elif defined(__linux__)
      n = static_cast<int>(send(fd_.load(), buf + total, len - total, MSG_NOSIGNAL));
#else
      n = static_cast<int>(send(fd_.load(), buf + total, len - total, 0));
#endif
      if (n < 0) {
        if (errno == EINTR) continue;
        StructuredLog()
            .Event("socket_write_error")
            .Field("errno", errno)
            .Field("error", strerror(errno))
            .Error();
        return MES_ERR_STREAM;
      }
      if (n == 0) {
        StructuredLog().Event("socket_write_zero").Warn();
        return MES_ERR_STREAM;
      }
    }
    total += static_cast<size_t>(n);
  }

  return MES_OK;
}

// --- Shutdown / Close ---

void SocketHandle::Shutdown() {
  std::lock_guard<std::mutex> lock(lifecycle_mutex_);
  const int fd = fd_.load();
  if (fd >= 0) {
#ifdef _WIN32
    shutdown(fd, SD_BOTH);
#else
    shutdown(fd, SHUT_RDWR);
#endif
  }
}

bool SocketHandle::IsValid() const { return fd_.load() >= 0; }

bool SocketHandle::IsTlsActive() const { return tls_active_; }

void SocketHandle::Poison() {
  // Interrupt any protocol I/O first. Close() then releases the descriptor and
  // TLS state, making accidental reuse impossible.
  Shutdown();
  Close();
}

void SocketHandle::Close() {
  std::lock_guard<std::mutex> lock(lifecycle_mutex_);
  if (ssl_ != nullptr) {
    // Attempt a clean TLS shutdown; ignore errors (we are tearing down).
    // SSL_shutdown() writes close_notify and can raise SIGPIPE if the peer has
    // already gone, so guard it the same way as WriteAll().
    {
      [[maybe_unused]] ScopedSigPipeSuppressor sigpipe_guard;
      SSL_shutdown(ssl_);
    }
    SSL_free(ssl_);
    ssl_ = nullptr;
  }

  if (ssl_ctx_ != nullptr) {
    SSL_CTX_free(ssl_ctx_);
    ssl_ctx_ = nullptr;
  }

  tls_active_ = false;
  read_timeout_s_ = 0;
  read_ahead_begin_ = 0;
  read_ahead_end_ = 0;

  const int fd = fd_.exchange(-1);
  if (fd >= 0) {
    CloseSocket(fd);
  }
}

}  // namespace mes::protocol
