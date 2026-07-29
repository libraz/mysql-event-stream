#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <thread>

#include "client/binlog_client.h"
#include "protocol/mysql_connection.h"

#ifndef _WIN32
#include <arpa/inet.h>
#include <netinet/in.h>
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

}  // namespace
}  // namespace mes::protocol
