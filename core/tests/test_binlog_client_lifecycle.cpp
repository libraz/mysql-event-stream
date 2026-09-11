// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * @file test_binlog_client_lifecycle.cpp
 * @brief BinlogClient startup/shutdown behaviour against a scripted MySQL peer
 *
 * The peer answers just enough of the wire protocol to carry Connect() and
 * StartStream() to completion, which makes it possible to assert two things a
 * live server cannot be made to do on demand: stalling in the middle of stream
 * setup, and delivering an event of exactly max_event_size.
 */

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <map>
#include <string>
#include <thread>
#include <vector>

#include "client/binlog_client.h"
#include "client/event_queue.h"

#ifndef _WIN32
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

namespace mes {

/** @brief Reads the credential the client retains after Connect(). */
class BinlogClientTestAccess {
 public:
  static const std::string& RetainedPassword(const BinlogClient& client) {
    return client.config_.password;
  }
};

namespace {

#ifdef _WIN32
TEST(BinlogClientLifecycle, ScriptedPeerIsPosixOnly) {
  GTEST_SKIP() << "The scripted MySQL peer is POSIX-only";
}
#else

using std::chrono::milliseconds;
using std::chrono::seconds;
using std::chrono::steady_clock;

constexpr uint8_t kComQuery = 0x03;
constexpr uint8_t kComBinlogDump = 0x12;
constexpr uint8_t kComBinlogDumpGtid = 0x1E;

/** @brief Server variables that satisfy ConnectionValidator for MySQL. */
const std::map<std::string, std::string>& ValidatedVariables() {
  static const std::map<std::string, std::string> values = {
      {"log_bin", "ON"},
      {"gtid_mode", "ON"},
      {"binlog_format", "ROW"},
      {"binlog_row_image", "FULL"},
      {"binlog_transaction_compression", "OFF"},
      {"binlog_row_value_options", ""},
  };
  return values;
}

void AppendLenEncString(std::vector<uint8_t>* out, const std::string& value) {
  // Every string this peer sends is far below 251 bytes, so the one-byte
  // length-encoded form is always the right one.
  out->push_back(static_cast<uint8_t>(value.size()));
  out->insert(out->end(), value.begin(), value.end());
}

/**
 * @brief A loopback MySQL server that follows a fixed script.
 *
 * Sends the initial handshake, accepts any credentials, answers the
 * configuration queries issued by Connect() and StartStream(), and finally
 * either delivers one binlog event or goes silent, depending on @p mode.
 */
class ScriptedMysqlPeer {
 public:
  enum class Mode {
    /// Stop answering once StartStream() issues its first query.
    kStallDuringStartStream,
    /// Answer the whole start sequence, then stream `stream_event`.
    kStreamOneEvent,
  };

  ScriptedMysqlPeer(Mode mode, std::vector<uint8_t> stream_event = {}) {
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

    thread_ = std::thread([this, mode, event = std::move(stream_event)] { Serve(mode, event); });
  }

  ~ScriptedMysqlPeer() {
    if (thread_.joinable()) thread_.join();
    if (listener_ >= 0) close(listener_);
  }

  ScriptedMysqlPeer(const ScriptedMysqlPeer&) = delete;
  ScriptedMysqlPeer& operator=(const ScriptedMysqlPeer&) = delete;

  uint16_t port() const { return port_; }

  /** @brief Wait until the peer has decided to stop answering the client. */
  bool WaitUntilStalled(milliseconds timeout) const {
    const auto deadline = steady_clock::now() + timeout;
    while (steady_clock::now() < deadline) {
      if (stalled_.load(std::memory_order_acquire)) return true;
      std::this_thread::sleep_for(milliseconds(5));
    }
    return stalled_.load(std::memory_order_acquire);
  }

 private:
  void Serve(Mode mode, const std::vector<uint8_t>& stream_event) {
    const int peer = accept(listener_, nullptr, nullptr);
    if (peer < 0) return;
    if (!SendPacket(peer, 0, BuildHandshake())) {
      close(peer);
      return;
    }

    std::vector<uint8_t> command;
    // Handshake response; any credentials are accepted.
    if (!ReadPacket(peer, &command) ||
        !SendPacket(peer, 2, {0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00})) {
      close(peer);
      return;
    }

    bool connect_phase = true;
    while (ReadPacket(peer, &command)) {
      if (command.empty()) break;
      const uint8_t command_byte = command[0];
      if (command_byte == kComBinlogDump || command_byte == kComBinlogDumpGtid) {
        std::vector<uint8_t> packet{0x00};  // replication OK marker
        packet.insert(packet.end(), stream_event.begin(), stream_event.end());
        SendPacket(peer, 1, packet);
        Stall(peer);
        break;
      }
      if (command_byte != kComQuery) break;

      const std::string query(command.begin() + 1, command.end());
      // SHOW VARIABLES is issued only by Connect(); the first query that is not
      // one marks the start of stream setup.
      const bool is_validation_query = query.rfind("SHOW VARIABLES", 0) == 0;
      if (connect_phase && !is_validation_query) {
        connect_phase = false;
        if (mode == Mode::kStallDuringStartStream) {
          Stall(peer);
          break;
        }
      }

      if (is_validation_query) {
        SendVariableRow(peer, query);
      } else if (query.rfind("SET ", 0) == 0) {
        SendPacket(peer, 1, {0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00});
      } else {
        SendSingleValueRow(peer, SelectResponse(query));
      }
    }
    close(peer);
  }

  /** @brief Stop answering and hold the connection until the client hangs up. */
  void Stall(int peer) {
    stalled_.store(true, std::memory_order_release);
    uint8_t byte = 0;
    recv(peer, &byte, 1, 0);
  }

  static std::string SelectResponse(const std::string& query) {
    // binlog_checksum=NONE keeps the reader from expecting a CRC32 trailer on
    // the synthetic event; an empty purged set skips the preflight comparison.
    if (query.find("binlog_checksum") != std::string::npos) return "NONE";
    return "";
  }

  static void SendVariableRow(int peer, const std::string& query) {
    const size_t close_quote = query.find_last_of('\'');
    const size_t open_quote = query.find_last_of('\'', close_quote - 1);
    const std::string name = query.substr(open_quote + 1, close_quote - open_quote - 1);
    const auto& values = ValidatedVariables();
    const auto it = values.find(name);
    std::vector<std::string> row{name, it == values.end() ? std::string() : it->second};
    SendResultSet(peer, {"Variable_name", "Value"}, row);
  }

  static void SendSingleValueRow(int peer, const std::string& value) {
    SendResultSet(peer, {"Value"}, {value});
  }

  static void SendResultSet(int peer, const std::vector<std::string>& columns,
                            const std::vector<std::string>& row) {
    uint8_t sequence = 1;
    if (!SendPacket(peer, sequence++, {static_cast<uint8_t>(columns.size())})) return;
    for (const std::string& column : columns) {
      // ParseColumnName reads catalog/schema/table/org_table before the name.
      std::vector<uint8_t> definition;
      AppendLenEncString(&definition, "def");
      AppendLenEncString(&definition, "");
      AppendLenEncString(&definition, "");
      AppendLenEncString(&definition, "");
      AppendLenEncString(&definition, column);
      if (!SendPacket(peer, sequence++, definition)) return;
    }
    if (!SendPacket(peer, sequence++, {0xFE, 0x00, 0x00, 0x02, 0x00})) return;
    std::vector<uint8_t> row_payload;
    for (const std::string& value : row) AppendLenEncString(&row_payload, value);
    if (!SendPacket(peer, sequence++, row_payload)) return;
    SendPacket(peer, sequence, {0xFE, 0x00, 0x00, 0x02, 0x00});
  }

  static bool SendPacket(int peer, uint8_t sequence, const std::vector<uint8_t>& payload) {
    const size_t size = payload.size();
    std::vector<uint8_t> packet = {static_cast<uint8_t>(size), static_cast<uint8_t>(size >> 8),
                                   static_cast<uint8_t>(size >> 16), sequence};
    packet.insert(packet.end(), payload.begin(), payload.end());
    return SendAll(peer, packet);
  }

  static bool SendAll(int peer, const std::vector<uint8_t>& bytes) {
    size_t sent = 0;
    while (sent < bytes.size()) {
      const ssize_t written = send(peer, bytes.data() + sent, bytes.size() - sent, 0);
      if (written <= 0) return false;
      sent += static_cast<size_t>(written);
    }
    return true;
  }

  static bool ReadPacket(int peer, std::vector<uint8_t>* payload) {
    uint8_t header[4]{};
    if (recv(peer, header, sizeof(header), MSG_WAITALL) != static_cast<ssize_t>(sizeof(header))) {
      return false;
    }
    const size_t size = static_cast<size_t>(header[0]) | (static_cast<size_t>(header[1]) << 8) |
                        (static_cast<size_t>(header[2]) << 16);
    payload->assign(size, 0);
    if (size == 0) return true;
    return recv(peer, payload->data(), size, MSG_WAITALL) == static_cast<ssize_t>(size);
  }

  // Protocol41 + SecureConnection in the lower half, PluginAuth in the upper.
  // CLIENT_SSL is deliberately absent: this peer speaks plaintext only.
  static std::vector<uint8_t> BuildHandshake() {
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
    const std::string plugin = "mysql_native_password";
    payload.insert(payload.end(), plugin.begin(), plugin.end());
    payload.push_back(0);
    return payload;
  }

  int listener_ = -1;
  uint16_t port_ = 0;
  std::atomic<bool> stalled_{false};
  std::thread thread_;
};

BinlogClientConfig PeerConfig(const ScriptedMysqlPeer& peer, uint32_t read_timeout_s) {
  BinlogClientConfig config;
  config.host = "127.0.0.1";
  config.port = peer.port();
  config.user = "repl";
  config.password = "repl";
  config.server_id = 4242;
  config.start_at_current = false;
  config.start_gtid = "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5";
  config.connect_timeout_s = 2;
  config.read_timeout_s = read_timeout_s;
  return config;
}

/** @brief A well-formed binlog event header followed by filler bytes. */
std::vector<uint8_t> MakeWireEvent(uint32_t event_length) {
  std::vector<uint8_t> event(event_length, 0x5A);
  event[0] = 0;
  event[1] = 0;
  event[2] = 0;
  event[3] = 0;                              // timestamp
  event[4] = 30;                             // WRITE_ROWS_EVENT
  for (int i = 5; i < 9; ++i) event[i] = 0;  // server id
  event[9] = static_cast<uint8_t>(event_length);
  event[10] = static_cast<uint8_t>(event_length >> 8);
  event[11] = static_cast<uint8_t>(event_length >> 16);
  event[12] = static_cast<uint8_t>(event_length >> 24);
  for (int i = 13; i < 19; ++i) event[i] = 0;  // next position + flags
  return event;
}

TEST(BinlogClientLifecycle, StopInterruptsStartStreamBlockedOnTheServer) {
  // A read timeout far longer than the assertion window: if Stop() had to wait
  // for the lock StartStream() used to hold across its round trips, the call
  // could only return once the socket read timed out.
  ScriptedMysqlPeer peer(ScriptedMysqlPeer::Mode::kStallDuringStartStream);
  BinlogClient client;
  ASSERT_EQ(client.Connect(PeerConfig(peer, 60)), MES_OK) << client.GetLastError();

  std::atomic<bool> stop_returned{false};
  std::thread stopper([&] {
    ASSERT_TRUE(peer.WaitUntilStalled(seconds(3)));
    const auto stop_started = steady_clock::now();
    client.Stop();
    EXPECT_LT(steady_clock::now() - stop_started, seconds(3));
    stop_returned.store(true, std::memory_order_release);
  });

  const auto start = steady_clock::now();
  const mes_error_t rc = client.StartStream();
  const auto elapsed = steady_clock::now() - start;
  stopper.join();

  EXPECT_EQ(rc, MES_ERR_DISCONNECTED) << client.GetLastError();
  EXPECT_TRUE(stop_returned.load(std::memory_order_acquire));
  EXPECT_LT(elapsed, seconds(5));
  EXPECT_FALSE(client.IsStreaming());
}

TEST(BinlogClientLifecycle, StartStreamRejectsAQueueBudgetBelowOneMaxSizedEvent) {
  ScriptedMysqlPeer peer(ScriptedMysqlPeer::Mode::kStallDuringStartStream);
  BinlogClient client;
  ASSERT_EQ(client.Connect(PeerConfig(peer, 2)), MES_OK) << client.GetLastError();

  client.SetMaxEventSize(4096);
  client.SetMaxQueueBytes(MinQueueBytesForEvent(client.MaxEventSize()) - 1);

  // Rejected before any round trip, so the peer never even sees a query.
  EXPECT_EQ(client.StartStream(), MES_ERR_INVALID_ARG);
  EXPECT_FALSE(client.IsStreaming());
  client.Disconnect();
}

TEST(BinlogClientLifecycle, PollAfterARejectedStartReturnsInsteadOfWaitingForAProducer) {
  ScriptedMysqlPeer peer(ScriptedMysqlPeer::Mode::kStallDuringStartStream);
  BinlogClient client;
  ASSERT_EQ(client.Connect(PeerConfig(peer, 2)), MES_OK) << client.GetLastError();

  client.SetMaxEventSize(4096);
  client.SetMaxQueueBytes(MinQueueBytesForEvent(client.MaxEventSize()) - 1);
  ASSERT_EQ(client.StartStream(), MES_ERR_INVALID_ARG);

  // A start that never reached the reader must leave the client not streaming:
  // Poll() has to report that rather than block in the queue waiting for an
  // event no thread will ever push.
  EXPECT_FALSE(client.IsStreaming());
  const auto poll_started = steady_clock::now();
  const PollResult result = client.Poll();
  EXPECT_EQ(result.error, MES_ERR_DISCONNECTED);
  EXPECT_LT(steady_clock::now() - poll_started, seconds(1));
  client.Disconnect();
}

TEST(BinlogClientLifecycle, ConnectDoesNotRetainThePlaintextPassword) {
  ScriptedMysqlPeer peer(ScriptedMysqlPeer::Mode::kStallDuringStartStream);
  BinlogClient client;
  const BinlogClientConfig config = PeerConfig(peer, 2);
  ASSERT_FALSE(config.password.empty());
  ASSERT_EQ(client.Connect(config), MES_OK) << client.GetLastError();

  // Authentication is over, so the client keeps no plaintext copy of the
  // credential it was handed.
  EXPECT_TRUE(BinlogClientTestAccess::RetainedPassword(client).empty());
  client.Disconnect();
}

TEST(BinlogClientLifecycle, QueuedBytesIsSampledWhileTheStreamIsRestarted) {
  BinlogClient client;
  std::atomic<bool> sampling{true};
  std::atomic<uint64_t> samples{0};
  // A monitoring thread, which the C ABI documents as a supported caller of
  // this accessor, reading the queue pointer while the owner thread below
  // replaces it and destroys the queue it pointed at.
  std::thread monitor([&] {
    while (sampling.load(std::memory_order_acquire)) {
      client.QueuedBytes();
      samples.fetch_add(1, std::memory_order_relaxed);
    }
  });

  for (int restart = 0; restart < 3; ++restart) {
    ScriptedMysqlPeer peer(ScriptedMysqlPeer::Mode::kStreamOneEvent, MakeWireEvent(256));
    ASSERT_EQ(client.Connect(PeerConfig(peer, 10)), MES_OK) << client.GetLastError();
    ASSERT_EQ(client.StartStream(), MES_OK) << client.GetLastError();
    // Polling the event proves the restarted stream has a live reader behind
    // the queue the sampler is reading from.
    const PollResult result = client.Poll();
    EXPECT_EQ(result.error, MES_OK) << client.GetLastError();
    EXPECT_EQ(result.size, 256u);
    client.Stop();
    client.Disconnect();
  }

  sampling.store(false, std::memory_order_release);
  monitor.join();
  EXPECT_GT(samples.load(std::memory_order_relaxed), 0u);
  EXPECT_EQ(client.QueuedBytes(), 0u);
}

TEST(BinlogClientLifecycle, EventAtMaxEventSizeSurvivesTheMinimumQueueBudget) {
  constexpr uint32_t kMaxEventSize = 4096;
  ScriptedMysqlPeer peer(ScriptedMysqlPeer::Mode::kStreamOneEvent, MakeWireEvent(kMaxEventSize));
  BinlogClient client;
  ASSERT_EQ(client.Connect(PeerConfig(peer, 10)), MES_OK) << client.GetLastError();

  client.SetMaxEventSize(kMaxEventSize);
  client.SetMaxQueueBytes(MinQueueBytesForEvent(kMaxEventSize));
  ASSERT_EQ(client.MaxQueueBytes(), MinQueueBytesForEvent(kMaxEventSize));

  ASSERT_EQ(client.StartStream(), MES_OK) << client.GetLastError();

  const PollResult result = client.Poll();
  EXPECT_EQ(result.error, MES_OK) << client.GetLastError();
  ASSERT_NE(result.data, nullptr);
  EXPECT_EQ(result.size, kMaxEventSize);

  client.Stop();
  client.Disconnect();
}

#endif

}  // namespace
}  // namespace mes
