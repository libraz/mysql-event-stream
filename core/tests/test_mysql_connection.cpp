#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "client/binlog_client.h"
#include "protocol/mysql_connection.h"
#include "protocol/mysql_socket.h"

#ifndef _WIN32
#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

namespace mes::protocol {
namespace {

#ifdef _WIN32
TEST(MysqlConnection, HandshakeTimeoutAndStop) {
  GTEST_SKIP() << "The dummy TCP peer is POSIX-only";
}
#else

class SilentTcpPeer {
 public:
  SilentTcpPeer() {
    listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    EXPECT_GE(listen_fd_, 0);

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = 0;
    EXPECT_EQ(bind(listen_fd_, reinterpret_cast<const sockaddr*>(&address), sizeof(address)), 0);
    EXPECT_EQ(listen(listen_fd_, 1), 0);

    socklen_t length = sizeof(address);
    EXPECT_EQ(getsockname(listen_fd_, reinterpret_cast<sockaddr*>(&address), &length), 0);
    port_ = ntohs(address.sin_port);
    thread_ = std::thread([this] {
      const int client = accept(listen_fd_, nullptr, nullptr);
      if (client >= 0) {
        accepted_.store(true, std::memory_order_release);
        while (!release_.load(std::memory_order_acquire)) {
          std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
        close(client);
      }
    });
  }

  ~SilentTcpPeer() {
    release_.store(true, std::memory_order_release);
    if (thread_.joinable()) thread_.join();
    if (listen_fd_ >= 0) close(listen_fd_);
  }

  uint16_t port() const { return port_; }

  bool WaitForAccept(std::chrono::milliseconds timeout) const {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
      if (accepted_.load(std::memory_order_acquire)) return true;
      std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    return accepted_.load(std::memory_order_acquire);
  }

 private:
  int listen_fd_ = -1;
  uint16_t port_ = 0;
  std::atomic<bool> accepted_{false};
  std::atomic<bool> release_{false};
  std::thread thread_;
};

TEST(MysqlConnection, HandshakeReadUsesConfiguredTimeout) {
  SilentTcpPeer peer;
  MysqlConnection connection;

  const auto start = std::chrono::steady_clock::now();
  EXPECT_EQ(connection.Connect("127.0.0.1", peer.port(), "user", "password", 1, 1, 0, "", "", ""),
            MES_ERR_CONNECT);
  const auto elapsed = std::chrono::steady_clock::now() - start;

  EXPECT_TRUE(peer.WaitForAccept(std::chrono::milliseconds(100)));
  EXPECT_GE(std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count(), 700);
  EXPECT_LT(std::chrono::duration_cast<std::chrono::seconds>(elapsed).count(), 3);
}

/**
 * @brief Loopback peer that greets with a scripted Initial Handshake v10.
 *
 * Sends the handshake, then hands the socket to the responder so a test can
 * continue the authentication exchange or simply stay silent.
 */
class HandshakePeer {
 public:
  HandshakePeer(const std::string& auth_plugin, std::function<void(int)> respond) {
    listener_ = socket(AF_INET, SOCK_STREAM, 0);
    EXPECT_GE(listener_, 0);
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = 0;
    EXPECT_EQ(bind(listener_, reinterpret_cast<const sockaddr*>(&address), sizeof(address)), 0);
    EXPECT_EQ(listen(listener_, 1), 0);
    socklen_t length = sizeof(address);
    EXPECT_EQ(getsockname(listener_, reinterpret_cast<sockaddr*>(&address), &length), 0);
    port_ = ntohs(address.sin_port);

    thread_ = std::thread([this, plugin = auth_plugin, responder = std::move(respond)] {
      const int peer = accept(listener_, nullptr, nullptr);
      if (peer < 0) return;
      if (SendPacket(peer, 0, BuildHandshake(plugin))) responder(peer);
      close(peer);
    });
  }

  ~HandshakePeer() {
    if (thread_.joinable()) thread_.join();
    if (listener_ >= 0) close(listener_);
  }

  HandshakePeer(const HandshakePeer&) = delete;
  HandshakePeer& operator=(const HandshakePeer&) = delete;

  uint16_t port() const { return port_; }

  static bool SendPacket(int peer, uint8_t sequence, const std::vector<uint8_t>& payload) {
    const size_t size = payload.size();
    std::vector<uint8_t> packet = {static_cast<uint8_t>(size), static_cast<uint8_t>(size >> 8),
                                   static_cast<uint8_t>(size >> 16), sequence};
    packet.insert(packet.end(), payload.begin(), payload.end());
    return send(peer, packet.data(), packet.size(), 0) == static_cast<ssize_t>(packet.size());
  }

  /** @brief Read and discard one wire packet; false if the peer went away. */
  static bool ConsumePacket(int peer) {
    uint8_t header[4]{};
    if (recv(peer, header, sizeof(header), MSG_WAITALL) != static_cast<ssize_t>(sizeof(header))) {
      return false;
    }
    const size_t size = static_cast<size_t>(header[0]) | (static_cast<size_t>(header[1]) << 8) |
                        (static_cast<size_t>(header[2]) << 16);
    std::vector<uint8_t> body(size);
    return recv(peer, body.data(), body.size(), MSG_WAITALL) == static_cast<ssize_t>(body.size());
  }

  /** @brief Block until the client hangs up, so the peer outlives the test call. */
  static void WaitForClose(int peer) {
    uint8_t byte = 0;
    recv(peer, &byte, 1, 0);
  }

 private:
  // Protocol41 + SecureConnection in the lower half, PluginAuth in the upper.
  // CLIENT_SSL is deliberately absent: these tests exercise the plaintext path.
  static std::vector<uint8_t> BuildHandshake(const std::string& plugin) {
    std::vector<uint8_t> payload;
    payload.push_back(10);
    const std::string version = "8.4.0";
    payload.insert(payload.end(), version.begin(), version.end());
    payload.push_back(0);
    payload.insert(payload.end(), 4, 1);  // connection id
    for (uint8_t i = 0; i < 8; ++i) payload.push_back(static_cast<uint8_t>('a' + i));
    payload.push_back(0);     // filler
    payload.push_back(0x00);  // capabilities lower (0x8200)
    payload.push_back(0x82);
    payload.push_back(45);    // charset
    payload.push_back(0x02);  // status flags
    payload.push_back(0x00);
    payload.push_back(0x08);  // capabilities upper (PluginAuth)
    payload.push_back(0x00);
    payload.push_back(21);                 // auth plugin data length
    payload.insert(payload.end(), 10, 0);  // reserved
    for (uint8_t i = 0; i < 12; ++i) payload.push_back(static_cast<uint8_t>('A' + i));
    payload.push_back(0);  // scramble terminator
    payload.insert(payload.end(), plugin.begin(), plugin.end());
    payload.push_back(0);
    return payload;
  }

  int listener_ = -1;
  uint16_t port_ = 0;
  std::thread thread_;
};

TEST(MysqlConnection, UnknownAuthPluginIsRejectedEvenForAnEmptyPassword) {
  HandshakePeer peer("sha256_password", [](int fd) { HandshakePeer::WaitForClose(fd); });

  MysqlConnection connection;
  EXPECT_EQ(connection.Connect("127.0.0.1", peer.port(), "user", "", 1, 5, 0, "", "", ""),
            MES_ERR_AUTH);
  // An empty password must not skip the plugin allow-list: without the check
  // the client would answer with an empty response and wait for the server.
  EXPECT_EQ(connection.GetLastError(), "Unsupported auth plugin: sha256_password");
}

TEST(MysqlConnection, FullAuthWithoutVerifiedTlsNamesBothRemedies) {
  HandshakePeer peer("caching_sha2_password", [](int fd) {
    if (!HandshakePeer::ConsumePacket(fd)) return;  // client handshake response
    // AuthMoreData: full authentication required.
    if (!HandshakePeer::SendPacket(fd, 2, {0x01, 0x04})) return;
    HandshakePeer::WaitForClose(fd);
  });

  MysqlConnection connection;
  EXPECT_EQ(connection.Connect("127.0.0.1", peer.port(), "user", "password", 1, 5, 0, "", "", ""),
            MES_ERR_AUTH);
  const std::string& error = connection.GetLastError();
  EXPECT_NE(error.find("verify_ca"), std::string::npos) << error;
  EXPECT_NE(error.find("allow_public_key_retrieval"), std::string::npos) << error;
}

TEST(MysqlConnection, ConnectSpendsOneTimeoutBudgetAcrossResolvedAddresses) {
  // A listener whose accept queue is never drained: once the backlog is full
  // the kernel drops further SYNs rather than refusing them, which is what
  // makes connect() block long enough to observe the budget.
  const int listener = socket(AF_INET, SOCK_STREAM, 0);
  ASSERT_GE(listener, 0);
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  address.sin_port = 0;
  ASSERT_EQ(bind(listener, reinterpret_cast<const sockaddr*>(&address), sizeof(address)), 0);
  ASSERT_EQ(listen(listener, 1), 0);
  socklen_t length = sizeof(address);
  ASSERT_EQ(getsockname(listener, reinterpret_cast<sockaddr*>(&address), &length), 0);

  const auto start_pending_connect = [&address] {
    const int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return fd;
    fcntl(fd, F_SETFL, fcntl(fd, F_GETFL, 0) | O_NONBLOCK);
    connect(fd, reinterpret_cast<const sockaddr*>(&address), sizeof(address));
    return fd;
  };

  std::vector<int> pending;
  for (int i = 0; i < 32; ++i) {
    const int fd = start_pending_connect();
    if (fd < 0) break;
    pending.push_back(fd);
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  const int probe = start_pending_connect();
  ASSERT_GE(probe, 0);
  struct pollfd probe_poll {};
  probe_poll.fd = probe;
  probe_poll.events = POLLOUT;
  const bool blackholed = poll(&probe_poll, 1, 300) == 0;
  close(probe);

  if (!blackholed) {
    for (const int fd : pending) close(fd);
    close(listener);
    GTEST_SKIP() << "the platform refuses rather than drops connections past the backlog";
  }

  // Three candidates for the same unanswering endpoint stand in for a
  // dual-stack name whose addresses all fail to answer.
  struct addrinfo candidates[3]{};
  for (int i = 0; i < 3; ++i) {
    candidates[i].ai_family = AF_INET;
    candidates[i].ai_socktype = SOCK_STREAM;
    candidates[i].ai_protocol = IPPROTO_TCP;
    candidates[i].ai_addr = reinterpret_cast<struct sockaddr*>(&address);
    candidates[i].ai_addrlen = sizeof(address);
    candidates[i].ai_next = (i < 2) ? &candidates[i + 1] : nullptr;
  }

  SocketHandle handle;
  const auto start = std::chrono::steady_clock::now();
  const mes_error_t rc =
      handle.ConnectToResolvedAddresses(candidates, "127.0.0.1", ntohs(address.sin_port), 1);
  const auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                              std::chrono::steady_clock::now() - start)
                              .count();

  EXPECT_EQ(rc, MES_ERR_CONNECT);
  EXPECT_GE(elapsed_ms, 700);
  // Three addresses must still cost one timeout, not three.
  EXPECT_LT(elapsed_ms, 2000);

  for (const int fd : pending) close(fd);
  close(listener);
}

TEST(MysqlConnection, StopInterruptsHandshakeBeforeLogicalConnection) {
  SilentTcpPeer peer;
  mes::BinlogClient client;
  mes::BinlogClientConfig config;
  config.host = "127.0.0.1";
  config.port = peer.port();
  config.user = "user";
  config.password = "password";
  config.connect_timeout_s = 1;
  config.read_timeout_s = 10;

  mes_error_t connect_result = MES_OK;
  std::thread connector([&] { connect_result = client.Connect(config); });
  ASSERT_TRUE(peer.WaitForAccept(std::chrono::seconds(1)));

  const auto start = std::chrono::steady_clock::now();
  client.Stop();
  connector.join();
  const auto elapsed = std::chrono::steady_clock::now() - start;

  EXPECT_EQ(connect_result, MES_ERR_CONNECT);
  EXPECT_LT(std::chrono::duration_cast<std::chrono::seconds>(elapsed).count(), 2);
}

#endif

// --- Initial Handshake Packet v10 parsing ---

// Capability bits the handshake parser branches on. Redeclared here because
// mysql_connection.cpp keeps its capability table private.
constexpr uint32_t kCapSecureConnection = 0x00008000;
constexpr uint32_t kCapPluginAuth = 0x00080000;

// A handshake as MySQL 8.4 sends it. Individual tests move one field at a time
// to reach a specific boundary in the parser.
struct HandshakeSpec {
  uint8_t protocol_version = 10;
  std::string server_version = "8.4.0";
  uint32_t connection_id = 0x01020304;
  uint32_t capabilities = kCapSecureConnection | kCapPluginAuth;
  uint8_t charset = 45;
  uint16_t status_flags = 0x0002;
  uint8_t auth_plugin_data_len = 21;
  std::string auth_plugin_name = "caching_sha2_password";
};

std::vector<uint8_t> BuildHandshake(const HandshakeSpec& spec) {
  std::vector<uint8_t> packet;
  const auto push_u16 = [&packet](uint16_t v) {
    packet.push_back(static_cast<uint8_t>(v));
    packet.push_back(static_cast<uint8_t>(v >> 8));
  };
  const auto push_u32 = [&packet](uint32_t v) {
    for (int i = 0; i < 4; ++i) packet.push_back(static_cast<uint8_t>(v >> (i * 8)));
  };

  packet.push_back(spec.protocol_version);
  packet.insert(packet.end(), spec.server_version.begin(), spec.server_version.end());
  packet.push_back(0);
  push_u32(spec.connection_id);
  for (uint8_t i = 0; i < 8; ++i) packet.push_back(static_cast<uint8_t>(0x11 + i));
  packet.push_back(0);  // filler
  push_u16(static_cast<uint16_t>(spec.capabilities & 0xFFFF));
  packet.push_back(spec.charset);
  push_u16(spec.status_flags);
  push_u16(static_cast<uint16_t>(spec.capabilities >> 16));
  packet.push_back(spec.auth_plugin_data_len);
  packet.insert(packet.end(), 10, 0);  // reserved

  if (spec.capabilities & kCapSecureConnection) {
    size_t part2 = 13;
    if (spec.auth_plugin_data_len > 8) {
      part2 = std::max<size_t>(13, static_cast<size_t>(spec.auth_plugin_data_len) - 8);
    }
    for (size_t i = 0; i + 1 < part2; ++i) packet.push_back(static_cast<uint8_t>(0x21 + i));
    packet.push_back(0);  // scramble terminator
  }
  if (spec.capabilities & kCapPluginAuth) {
    packet.insert(packet.end(), spec.auth_plugin_name.begin(), spec.auth_plugin_name.end());
    packet.push_back(0);
  }
  return packet;
}

// Byte offset of the auth_plugin_data_len field for a given server version.
size_t AuthPluginDataLenOffset(const HandshakeSpec& spec) {
  return 1 + spec.server_version.size() + 1 + 4 + 8 + 1 + 2 + 1 + 2 + 2;
}

TEST(MysqlConnectionHandshake, ParsesAServerHandshake) {
  const HandshakeSpec spec;
  const auto packet = BuildHandshake(spec);

  ServerHandshake out;
  std::string error;
  ASSERT_EQ(detail::ParseServerHandshakePayload(packet.data(), packet.size(), &out, &error), MES_OK)
      << error;
  EXPECT_TRUE(error.empty());
  EXPECT_EQ(out.protocol_version, 10);
  EXPECT_EQ(out.server_version, "8.4.0");
  EXPECT_EQ(out.connection_id, 0x01020304u);
  EXPECT_EQ(out.charset, 45);
  EXPECT_EQ(out.status_flags, 0x0002);
  EXPECT_EQ(out.server_capabilities, kCapSecureConnection | kCapPluginAuth);
  EXPECT_EQ(out.auth_plugin_name, "caching_sha2_password");
  // Salt = 8 bytes of part 1 plus (auth_plugin_data_len - 8 - 1) scramble
  // bytes, excluding part 2's trailing NUL.
  ASSERT_EQ(out.auth_data.size(), 20u);
  EXPECT_EQ(out.auth_data[0], 0x11);
  EXPECT_EQ(out.auth_data[7], 0x18);
  EXPECT_EQ(out.auth_data[8], 0x21);
  EXPECT_EQ(out.auth_data[19], 0x2C);
}

TEST(MysqlConnectionHandshake, TruncationAtEveryOffsetFailsSafely) {
  const HandshakeSpec spec;
  const auto packet = BuildHandshake(spec);
  // Every field up to the auth-plugin name is mandatory. The name itself is
  // optional: the parser falls back to caching_sha2_password when it is absent.
  const size_t required = packet.size() - (spec.auth_plugin_name.size() + 1);

  for (size_t len = 0; len < packet.size(); ++len) {
    ServerHandshake out;
    std::string error;
    const mes_error_t rc = detail::ParseServerHandshakePayload(packet.data(), len, &out, &error);
    if (len < required) {
      EXPECT_EQ(rc, MES_ERR_AUTH) << "length " << len;
      EXPECT_FALSE(error.empty()) << "length " << len;
    } else {
      EXPECT_EQ(rc, MES_OK) << "length " << len << ": " << error;
    }
  }
}

TEST(MysqlConnectionHandshake, EveryAuthPluginDataLengthIsAccepted) {
  // A hostile server can put any of 256 values in this byte. None may produce
  // an out-of-bounds read or a salt longer than the bytes actually present.
  for (int declared = 0; declared <= 255; ++declared) {
    HandshakeSpec spec;
    spec.auth_plugin_data_len = static_cast<uint8_t>(declared);
    const auto packet = BuildHandshake(spec);

    ServerHandshake out;
    std::string error;
    ASSERT_EQ(detail::ParseServerHandshakePayload(packet.data(), packet.size(), &out, &error),
              MES_OK)
        << "auth_plugin_data_len " << declared << ": " << error;
    const size_t scramble = declared > 9 ? static_cast<size_t>(declared) - 9 : 0;
    EXPECT_EQ(out.auth_data.size(), 8u + scramble) << "auth_plugin_data_len " << declared;
    EXPECT_EQ(out.auth_plugin_name, "caching_sha2_password") << "auth_plugin_data_len " << declared;
  }
}

TEST(MysqlConnectionHandshake, OverstatedAuthDataLengthIsRejectedRatherThanOverread) {
  const HandshakeSpec spec;
  auto packet = BuildHandshake(spec);
  const size_t offset = AuthPluginDataLenOffset(spec);
  ASSERT_LT(offset, packet.size());
  ASSERT_EQ(packet[offset], spec.auth_plugin_data_len);
  // Claim a 247-byte scramble in a packet that carries 13 part-2 bytes.
  packet[offset] = 255;

  ServerHandshake out;
  std::string error;
  EXPECT_EQ(detail::ParseServerHandshakePayload(packet.data(), packet.size(), &out, &error),
            MES_ERR_AUTH);
  EXPECT_NE(error.find("auth data part 2"), std::string::npos) << error;
}

TEST(MysqlConnectionHandshake, RejectsUnsupportedProtocolVersion) {
  HandshakeSpec spec;
  spec.protocol_version = 9;
  const auto packet = BuildHandshake(spec);

  ServerHandshake out;
  std::string error;
  EXPECT_EQ(detail::ParseServerHandshakePayload(packet.data(), packet.size(), &out, &error),
            MES_ERR_AUTH);
  EXPECT_NE(error.find("Unsupported protocol version"), std::string::npos) << error;
}

TEST(MysqlConnectionHandshake, RejectsAnUnterminatedServerVersion) {
  std::vector<uint8_t> packet(64, 'x');
  packet[0] = 10;

  ServerHandshake out;
  std::string error;
  EXPECT_EQ(detail::ParseServerHandshakePayload(packet.data(), packet.size(), &out, &error),
            MES_ERR_AUTH);
  EXPECT_NE(error.find("server version terminator"), std::string::npos) << error;
}

TEST(MysqlConnectionHandshake, ArbitraryRemoteBytesAreRejectedWithADiagnostic) {
  // Whatever a peer puts on the wire, the parser reports a reason instead of
  // reading past the payload. Run under ASan/UBSan in the safety suite.
  const std::vector<std::vector<uint8_t>> inputs = {
      {},
      {0x00},
      {0x0A},
      {0x0A, 0x00},
      {0x0A, 0x00, 0x00, 0x00},
      std::vector<uint8_t>(128, 0x00),
      std::vector<uint8_t>(128, 0xFF),
      std::vector<uint8_t>(1024, 0x0A),
  };

  for (const auto& input : inputs) {
    ServerHandshake out;
    std::string error;
    EXPECT_EQ(detail::ParseServerHandshakePayload(input.empty() ? nullptr : input.data(),
                                                  input.size(), &out, &error),
              MES_ERR_AUTH)
        << "size " << input.size();
    EXPECT_FALSE(error.empty()) << "size " << input.size();
  }
}

TEST(MysqlConnectionHandshake, ToleratesAMissingErrorSinkAndOutput) {
  const HandshakeSpec spec;
  const auto packet = BuildHandshake(spec);

  ServerHandshake out;
  EXPECT_EQ(detail::ParseServerHandshakePayload(packet.data(), packet.size(), &out, nullptr),
            MES_OK);
  EXPECT_EQ(detail::ParseServerHandshakePayload(packet.data(), packet.size(), nullptr, nullptr),
            MES_ERR_AUTH);
}

}  // namespace
}  // namespace mes::protocol
