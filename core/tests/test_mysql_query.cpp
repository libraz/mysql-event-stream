// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#include <gtest/gtest.h>

#include <atomic>
#include <thread>
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
  const int listener = socket(AF_INET, SOCK_STREAM, 0);
  ASSERT_GE(listener, 0);
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  address.sin_port = 0;
  ASSERT_EQ(bind(listener, reinterpret_cast<const sockaddr*>(&address), sizeof(address)), 0);
  ASSERT_EQ(listen(listener, 1), 0);
  socklen_t address_len = sizeof(address);
  ASSERT_EQ(getsockname(listener, reinterpret_cast<sockaddr*>(&address), &address_len), 0);

  std::atomic<bool> peer_observed_close{false};
  std::thread server([&] {
    const int peer = accept(listener, nullptr, nullptr);
    if (peer < 0) return;

    uint8_t request_header[4]{};
    if (recv(peer, request_header, sizeof(request_header), MSG_WAITALL) !=
        static_cast<ssize_t>(sizeof(request_header))) {
      close(peer);
      return;
    }
    const size_t request_size = static_cast<size_t>(request_header[0]) |
                                (static_cast<size_t>(request_header[1]) << 8) |
                                (static_cast<size_t>(request_header[2]) << 16);
    std::vector<uint8_t> request(request_size);
    if (recv(peer, request.data(), request.size(), MSG_WAITALL) !=
        static_cast<ssize_t>(request.size())) {
      close(peer);
      return;
    }

    const auto send_packet = [peer](uint8_t sequence, const std::vector<uint8_t>& payload) {
      const size_t size = payload.size();
      std::vector<uint8_t> packet = {static_cast<uint8_t>(size), static_cast<uint8_t>(size >> 8),
                                     static_cast<uint8_t>(size >> 16), sequence};
      packet.insert(packet.end(), payload.begin(), payload.end());
      return send(peer, packet.data(), packet.size(), 0) == static_cast<ssize_t>(packet.size());
    };

    // A one-column result set followed by a row whose length prefix exceeds
    // the bytes in that packet. The server intentionally leaves its terminal
    // packet unsent: the client must close rather than reuse this connection.
    if (!send_packet(1, {1}) || !send_packet(2, {0, 0, 0, 0, 1, 'x'}) ||
        !send_packet(3, {5, 'x'})) {
      close(peer);
      return;
    }
    uint8_t byte = 0;
    peer_observed_close.store(recv(peer, &byte, 1, 0) == 0, std::memory_order_release);
    close(peer);
  });

  SocketHandle socket;
  ASSERT_EQ(socket.Connect("127.0.0.1", ntohs(address.sin_port), 1), MES_OK);
  QueryResult result;
  std::string error;
  EXPECT_EQ(ExecuteQuery(&socket, "SELECT malformed", &result, &error), MES_ERR_PARSE);
  EXPECT_EQ(error, "Truncated result-set row");
  EXPECT_FALSE(socket.IsValid());

  server.join();
  EXPECT_TRUE(peer_observed_close.load(std::memory_order_acquire));
  close(listener);
#endif
}

TEST(QueryExecutionTest, ResultRowLimitPoisonsSocket) {
#ifdef _WIN32
  GTEST_SKIP() << "local socket test is POSIX-only";
#else
  const int listener = socket(AF_INET, SOCK_STREAM, 0);
  ASSERT_GE(listener, 0);
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  address.sin_port = 0;
  ASSERT_EQ(bind(listener, reinterpret_cast<const sockaddr*>(&address), sizeof(address)), 0);
  ASSERT_EQ(listen(listener, 1), 0);
  socklen_t address_len = sizeof(address);
  ASSERT_EQ(getsockname(listener, reinterpret_cast<sockaddr*>(&address), &address_len), 0);

  std::thread server([&] {
    const int peer = accept(listener, nullptr, nullptr);
    if (peer < 0) return;
    uint8_t request_header[4]{};
    if (recv(peer, request_header, sizeof(request_header), MSG_WAITALL) !=
        static_cast<ssize_t>(sizeof(request_header))) {
      close(peer);
      return;
    }
    const size_t request_size = static_cast<size_t>(request_header[0]) |
                                (static_cast<size_t>(request_header[1]) << 8) |
                                (static_cast<size_t>(request_header[2]) << 16);
    std::vector<uint8_t> request(request_size);
    if (recv(peer, request.data(), request.size(), MSG_WAITALL) !=
        static_cast<ssize_t>(request.size())) {
      close(peer);
      return;
    }
    const auto send_packet = [peer](uint8_t sequence, const std::vector<uint8_t>& payload) {
      const size_t size = payload.size();
      std::vector<uint8_t> packet = {static_cast<uint8_t>(size), static_cast<uint8_t>(size >> 8),
                                     static_cast<uint8_t>(size >> 16), sequence};
      packet.insert(packet.end(), payload.begin(), payload.end());
      size_t offset = 0;
      while (offset < packet.size()) {
        const ssize_t sent = send(peer, packet.data() + offset, packet.size() - offset, 0);
        if (sent <= 0) return false;
        offset += static_cast<size_t>(sent);
      }
      return true;
    };

    if (!send_packet(1, {1}) || !send_packet(2, {0, 0, 0, 0, 1, 'x'})) {
      close(peer);
      return;
    }
    for (size_t row = 0; row <= 100000; ++row) {
      if (!send_packet(static_cast<uint8_t>(3 + row), {1, 'x'})) break;
    }
    close(peer);
  });

  SocketHandle socket;
  ASSERT_EQ(socket.Connect("127.0.0.1", ntohs(address.sin_port), 1), MES_OK);
  QueryResult result;
  std::string error;
  EXPECT_EQ(ExecuteQuery(&socket, "SELECT too_many_rows", &result, &error), MES_ERR_QUEUE_FULL);
  EXPECT_EQ(error, "Result set row count exceeds maximum (100000)");
  EXPECT_FALSE(socket.IsValid());

  server.join();
  close(listener);
#endif
}

}  // namespace
}  // namespace mes::protocol
