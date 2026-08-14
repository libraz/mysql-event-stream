// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <functional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#ifndef _WIN32
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

#include "protocol/mysql_query.h"
#include "protocol/mysql_socket.h"

namespace mes::protocol {
namespace {

#ifndef _WIN32

// These tests deliberately keep sending after the client has poisoned the
// socket, so writes to a closed peer must report EPIPE instead of raising
// SIGPIPE and killing the test binary. Linux needs the send flag; macOS/BSD use
// SO_NOSIGPIPE on the accepted socket.
#ifdef MSG_NOSIGNAL
constexpr int kSendFlags = MSG_NOSIGNAL;
#else
constexpr int kSendFlags = 0;
#endif

/** @brief Send one MySQL wire packet, looping over partial writes. */
bool SendWirePacket(int peer, uint8_t sequence, const std::vector<uint8_t>& payload) {
  const size_t size = payload.size();
  std::vector<uint8_t> packet = {static_cast<uint8_t>(size), static_cast<uint8_t>(size >> 8),
                                 static_cast<uint8_t>(size >> 16), sequence};
  packet.insert(packet.end(), payload.begin(), payload.end());
  size_t offset = 0;
  while (offset < packet.size()) {
    const ssize_t sent = send(peer, packet.data() + offset, packet.size() - offset, kSendFlags);
    if (sent <= 0) return false;
    offset += static_cast<size_t>(sent);
  }
  return true;
}

/**
 * @brief Loopback peer that answers a single COM_QUERY with scripted bytes.
 *
 * Accepts one connection, consumes the request packet, then hands the peer
 * socket to the supplied responder so each test can shape its own reply.
 */
class QueryPeer {
 public:
  explicit QueryPeer(std::function<void(int)> respond) {
    listener_ = socket(AF_INET, SOCK_STREAM, 0);
    EXPECT_GE(listener_, 0);
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = 0;
    EXPECT_EQ(bind(listener_, reinterpret_cast<const sockaddr*>(&address), sizeof(address)), 0);
    EXPECT_EQ(listen(listener_, 1), 0);
    socklen_t address_len = sizeof(address);
    EXPECT_EQ(getsockname(listener_, reinterpret_cast<sockaddr*>(&address), &address_len), 0);
    port_ = ntohs(address.sin_port);

    thread_ = std::thread([this, responder = std::move(respond)] {
      const int peer = accept(listener_, nullptr, nullptr);
      if (peer < 0) return;
#if defined(SO_NOSIGPIPE)
      const int nosigpipe = 1;
      setsockopt(peer, SOL_SOCKET, SO_NOSIGPIPE, &nosigpipe, sizeof(nosigpipe));
#endif
      if (ConsumeRequest(peer)) responder(peer);
      close(peer);
    });
  }

  ~QueryPeer() {
    if (thread_.joinable()) thread_.join();
    if (listener_ >= 0) close(listener_);
  }

  QueryPeer(const QueryPeer&) = delete;
  QueryPeer& operator=(const QueryPeer&) = delete;

  uint16_t port() const { return port_; }

 private:
  static bool ConsumeRequest(int peer) {
    uint8_t header[4]{};
    if (recv(peer, header, sizeof(header), MSG_WAITALL) != static_cast<ssize_t>(sizeof(header))) {
      return false;
    }
    const size_t size = static_cast<size_t>(header[0]) | (static_cast<size_t>(header[1]) << 8) |
                        (static_cast<size_t>(header[2]) << 16);
    std::vector<uint8_t> request(size);
    return recv(peer, request.data(), request.size(), MSG_WAITALL) ==
           static_cast<ssize_t>(request.size());
  }

  int listener_ = -1;
  uint16_t port_ = 0;
  std::thread thread_;
};

/** @brief Minimal one-column definition packet naming the column "x". */
const std::vector<uint8_t> kSingleColumnDefinition = {0, 0, 0, 0, 1, 'x'};

#endif  // _WIN32

TEST(QueryResultTest, ConstructionAndAccess) {
  QueryResult result;
  result.column_names = {"id", "name"};

  QueryResultRow row;
  row.values = {"1", "alice"};
  row.is_null = {false, false};
  result.rows.push_back(row);

  EXPECT_EQ(result.column_names.size(), 2u);
  EXPECT_EQ(result.rows.size(), 1u);
  EXPECT_EQ(result.rows[0].values[0], "1");
  EXPECT_EQ(result.rows[0].values[1], "alice");
  EXPECT_FALSE(result.rows[0].is_null[0]);
}

TEST(QueryResultTest, NullValues) {
  QueryResultRow row;
  row.values = {"", ""};
  row.is_null = {true, false};

  EXPECT_TRUE(row.is_null[0]);
  EXPECT_FALSE(row.is_null[1]);
}

TEST(QueryResultTest, EmptyResult) {
  QueryResult result;
  EXPECT_TRUE(result.column_names.empty());
  EXPECT_TRUE(result.rows.empty());
}

TEST(QueryExecutionTest, MalformedResultPoisonsSocketBeforeASecondQueryCanDesync) {
#ifdef _WIN32
  GTEST_SKIP() << "local socket test is POSIX-only";
#else
  std::atomic<bool> peer_observed_close{false};
  QueryPeer peer([&](int fd) {
    // A one-column result set followed by a row whose length prefix exceeds
    // the bytes in that packet. The server intentionally leaves its terminal
    // packet unsent: the client must close rather than reuse this connection.
    if (!SendWirePacket(fd, 1, {1}) || !SendWirePacket(fd, 2, kSingleColumnDefinition) ||
        !SendWirePacket(fd, 3, {5, 'x'})) {
      return;
    }
    uint8_t byte = 0;
    peer_observed_close.store(recv(fd, &byte, 1, 0) == 0, std::memory_order_release);
  });

  SocketHandle socket;
  ASSERT_EQ(socket.Connect("127.0.0.1", peer.port(), 1), MES_OK);
  QueryResult result;
  std::string error;
  EXPECT_EQ(ExecuteQuery(&socket, "SELECT malformed", &result, &error), MES_ERR_STREAM);
  EXPECT_EQ(error, "Truncated result-set row");
  EXPECT_FALSE(socket.IsValid());
#endif
}

TEST(QueryExecutionTest, ResultRowLimitPoisonsSocket) {
#ifdef _WIN32
  GTEST_SKIP() << "local socket test is POSIX-only";
#else
  QueryPeer peer([](int fd) {
    if (!SendWirePacket(fd, 1, {1}) || !SendWirePacket(fd, 2, kSingleColumnDefinition)) return;
    for (size_t row = 0; row <= 100000; ++row) {
      if (!SendWirePacket(fd, static_cast<uint8_t>(3 + row), {1, 'x'})) return;
    }
  });

  SocketHandle socket;
  ASSERT_EQ(socket.Connect("127.0.0.1", peer.port(), 1), MES_OK);
  QueryResult result;
  std::string error;
  EXPECT_EQ(ExecuteQuery(&socket, "SELECT too_many_rows", &result, &error), MES_ERR_QUEUE_FULL);
  EXPECT_EQ(error, "Result set row count exceeds maximum (100000)");
  EXPECT_FALSE(socket.IsValid());
#endif
}

TEST(QueryExecutionTest, ColumnCountLimitReportsStreamFailure) {
#ifdef _WIN32
  GTEST_SKIP() << "local socket test is POSIX-only";
#else
  QueryPeer peer([](int fd) {
    // Length-encoded 8193 columns, past the 4096 ceiling.
    SendWirePacket(fd, 1, {0xFC, 0x01, 0x20});
  });

  SocketHandle socket;
  ASSERT_EQ(socket.Connect("127.0.0.1", peer.port(), 1), MES_OK);
  QueryResult result;
  std::string error;
  EXPECT_EQ(ExecuteQuery(&socket, "SELECT too_many_columns", &result, &error), MES_ERR_STREAM);
  EXPECT_EQ(error, "Column count exceeds maximum (8193)");
  EXPECT_FALSE(socket.IsValid());
#endif
}

TEST(QueryExecutionTest, OversizedColumnDefinitionPacketIsRejected) {
#ifdef _WIN32
  GTEST_SKIP() << "local socket test is POSIX-only";
#else
  QueryPeer peer([](int fd) {
    if (!SendWirePacket(fd, 1, {1})) return;
    // A column definition far past anything six identifiers can produce.
    std::vector<uint8_t> definition = {0, 0, 0, 0, 0xFD, 0x00, 0x20, 0x01};
    definition.resize(definition.size() + 0x012000, 'x');
    SendWirePacket(fd, 2, definition);
  });

  SocketHandle socket;
  ASSERT_EQ(socket.Connect("127.0.0.1", peer.port(), 1), MES_OK);
  QueryResult result;
  std::string error;
  EXPECT_EQ(ExecuteQuery(&socket, "SELECT huge_column_def", &result, &error), MES_ERR_STREAM);
  EXPECT_EQ(error, "Failed to read column definition");
  EXPECT_FALSE(socket.IsValid());
#endif
}

TEST(QueryExecutionTest, ColumnNamesCountAgainstTheResultByteBudget) {
#ifdef _WIN32
  GTEST_SKIP() << "local socket test is POSIX-only";
#else
  // Each definition carries a 64000-byte column name, so the 64 MiB retained
  // byte budget is reached long before the 4096-column ceiling. Without column
  // names participating in the budget, the client would hold all of them.
  constexpr size_t kNameSize = 64000;
  QueryPeer peer([](int fd) {
    if (!SendWirePacket(fd, 1, {0xFC, 0x00, 0x10})) return;  // 4096 columns
    std::vector<uint8_t> definition = {0,
                                       0,
                                       0,
                                       0,
                                       0xFD,
                                       static_cast<uint8_t>(kNameSize & 0xFF),
                                       static_cast<uint8_t>((kNameSize >> 8) & 0xFF),
                                       0};
    definition.resize(definition.size() + kNameSize, 'x');
    for (size_t i = 0; i < 4096; ++i) {
      if (!SendWirePacket(fd, static_cast<uint8_t>(2 + i), definition)) return;
    }
  });

  SocketHandle socket;
  ASSERT_EQ(socket.Connect("127.0.0.1", peer.port(), 1), MES_OK);
  QueryResult result;
  std::string error;
  EXPECT_EQ(ExecuteQuery(&socket, "SELECT wide_column_names", &result, &error), MES_ERR_QUEUE_FULL);
  EXPECT_EQ(error, "Result set byte size exceeds maximum (67108864)");
  EXPECT_FALSE(socket.IsValid());
#endif
}

TEST(QueryExecutionTest, EmptyRowPacketsTerminateAfterABoundedCount) {
#ifdef _WIN32
  GTEST_SKIP() << "local socket test is POSIX-only";
#else
  // A server that only ever sends zero-length packets keeps the socket
  // readable, so the read timeout never fires: the row loop itself has to
  // give up.
  QueryPeer peer([](int fd) {
    if (!SendWirePacket(fd, 1, {1}) || !SendWirePacket(fd, 2, kSingleColumnDefinition)) return;
    for (size_t i = 0; i < 4096; ++i) {
      if (!SendWirePacket(fd, static_cast<uint8_t>(3 + i), {})) return;
    }
    // Stay open so a client that never gives up trips its read timeout rather
    // than seeing a clean EOF, which would look like an ordinary stream error.
    uint8_t byte = 0;
    recv(fd, &byte, 1, 0);
  });

  SocketHandle socket;
  ASSERT_EQ(socket.Connect("127.0.0.1", peer.port(), 1), MES_OK);
  ASSERT_EQ(socket.SetReadTimeout(2), MES_OK);
  QueryResult result;
  std::string error;
  EXPECT_EQ(ExecuteQuery(&socket, "SELECT empty_packets", &result, &error), MES_ERR_STREAM);
  EXPECT_EQ(error, "Server sent only empty packets while reading result-set rows");
  EXPECT_FALSE(socket.IsValid());
#endif
}

}  // namespace
}  // namespace mes::protocol
