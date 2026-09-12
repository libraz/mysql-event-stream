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

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <map>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "binary_util.h"
#include "client/binlog_client.h"
#include "client/event_queue.h"
#include "client/gtid_encoder.h"
#include "event_header.h"
#include "protocol/mysql_binlog_stream.h"

#ifndef _WIN32
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

namespace mes {

/** @brief Reads client state the public surface deliberately does not expose. */
class BinlogClientTestAccess {
 public:
  static const std::string& RetainedPassword(const BinlogClient& client) {
    return client.config_.password;
  }

  /** @brief Payload bytes still held by events already handed to the consumer. */
  static size_t RetainedEventBytes(const BinlogClient& client) {
    size_t bytes = client.current_event_.data.size();
    for (const QueuedEvent& event : client.batch_events_) bytes += event.data.size();
    return bytes;
  }

  /** @brief Events the reader has buffered but the consumer has not drained. */
  static size_t QueuedEvents(const BinlogClient& client) {
    return client.event_queue_ ? client.event_queue_->Size() : 0;
  }

  /** @brief Whether the connection underneath still holds a usable transport. */
  static bool TransportUsable(const BinlogClient& client) { return client.conn_.IsConnected(); }
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

/// Handshake server-version strings, one per flavour DetectServerFlavor()
/// recognizes. This field is the only input that selects the flavour, and with
/// it the whole stream-setup branch, so the peer advertises it verbatim.
constexpr char kMySQLVersion[] = "8.4.0";
constexpr char kMariaDBVersion[] = "10.11.6-MariaDB";

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
    /// Answer StartStream()'s first query with a packet that carries no
    /// payload, which no result-set framing can consume.
    kMalformedReplyDuringStartStream,
    /// Answer the whole start sequence, then stream `stream_event`.
    kStreamOneEvent,
    /// Stream `stream_event` and a heartbeat, then end the dump with a server
    /// error and go on answering commands. That is the state a source leaves a
    /// client in when the dump dies but the session survives, which is what
    /// makes a restart possible without reconnecting.
    kStreamThenServerError,
    /// Report binlog_checksum=CRC32, then stream `stream_event` -- whose
    /// trailer does not match its bytes -- followed by more events. A source
    /// never learns that a client stopped reading, so the dump keeps arriving
    /// after the client has given up on it.
    kStreamCorruptedEventThenKeepStreaming,
  };

  /// ER_SERVER_SHUTDOWN: ends a dump without implying anything about the
  /// requested position, so a restart over the same session stays valid.
  static constexpr uint16_t kDefaultDumpError = 1053;

  /** @brief One command as the peer received it, in arrival order. */
  struct ReceivedCommand {
    uint8_t command = 0;           ///< COM_QUERY or COM_BINLOG_DUMP[_GTID]
    std::string query;             ///< Statement text of a COM_QUERY
    std::vector<uint8_t> payload;  ///< Whole payload of a dump request
  };

  ScriptedMysqlPeer(Mode mode, std::vector<uint8_t> stream_event = {},
                    uint16_t dump_error = kDefaultDumpError,
                    const std::string& server_version = kMySQLVersion) {
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

    thread_ = std::thread([this, mode, event = std::move(stream_event), dump_error,
                           version = server_version] { Serve(mode, event, dump_error, version); });
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

  /**
   * @brief Every command received so far, in the order they arrived.
   *
   * Queries and dump requests share one list because where a setup query sits
   * relative to the dump request is itself part of what the stream setup owes
   * the server: a session variable the dump depends on is worthless afterwards.
   */
  std::vector<ReceivedCommand> Commands() const {
    std::lock_guard<std::mutex> lock(command_mutex_);
    return commands_;
  }

  /** @brief Every COM_QUERY statement received so far, in order. */
  std::vector<std::string> Queries() const {
    std::vector<std::string> queries;
    for (const ReceivedCommand& command : Commands()) {
      if (command.command == kComQuery) queries.push_back(command.query);
    }
    return queries;
  }

  /** @brief Every COM_BINLOG_DUMP[_GTID] payload received so far, in order. */
  std::vector<std::vector<uint8_t> > DumpRequests() const {
    std::vector<std::vector<uint8_t> > requests;
    for (const ReceivedCommand& command : Commands()) {
      if (command.command != kComQuery) requests.push_back(command.payload);
    }
    return requests;
  }

  /** @brief Wait until @p count dump requests have been received. */
  bool WaitForDumpRequests(size_t count, milliseconds timeout) const {
    const auto deadline = steady_clock::now() + timeout;
    for (;;) {
      if (DumpRequests().size() >= count) return true;
      if (steady_clock::now() >= deadline) return false;
      std::this_thread::sleep_for(milliseconds(5));
    }
  }

 private:
  void Serve(Mode mode, const std::vector<uint8_t>& stream_event, uint16_t dump_error,
             const std::string& server_version) {
    const int peer = accept(listener_, nullptr, nullptr);
    if (peer < 0) return;
    if (!SendPacket(peer, 0, BuildHandshake(server_version))) {
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
        const size_t dumps = RecordDumpRequest(command);
        if (mode == Mode::kStreamThenServerError) {
          if (dumps == 1) {
            std::vector<uint8_t> packet{0x00};  // replication OK marker
            packet.insert(packet.end(), stream_event.begin(), stream_event.end());
            SendPacket(peer, 1, packet);
            SendPacket(peer, 2, {0x00});  // OK marker with no event: a heartbeat
          }
          // An ERR packet ends the dump without closing the session, so the
          // loop goes back to serving commands and the client can start a
          // replacement stream over this same connection.
          SendPacket(peer, 3, BuildStreamError(dump_error));
          continue;
        }
        if (mode == Mode::kStreamCorruptedEventThenKeepStreaming) {
          std::vector<uint8_t> packet{0x00};  // replication OK marker
          packet.insert(packet.end(), stream_event.begin(), stream_event.end());
          // Three events go out back to back, so the two after the one the
          // client rejects are left unread in the socket. Then back to serving
          // commands: a client that keeps using this connection gets those
          // replication packets as its query responses.
          for (uint8_t sequence = 1; sequence <= 3; ++sequence) {
            if (!SendPacket(peer, sequence, packet)) break;
          }
          continue;
        }
        std::vector<uint8_t> packet{0x00};  // replication OK marker
        packet.insert(packet.end(), stream_event.begin(), stream_event.end());
        SendPacket(peer, 1, packet);
        Stall(peer);
        break;
      }
      if (command_byte != kComQuery) break;

      const std::string query(command.begin() + 1, command.end());
      RecordQuery(query);
      // SHOW VARIABLES is issued only by Connect(); the first query that is not
      // one marks the start of stream setup.
      const bool is_validation_query = query.rfind("SHOW VARIABLES", 0) == 0;
      if (connect_phase && !is_validation_query) {
        connect_phase = false;
        if (mode == Mode::kStallDuringStartStream) {
          Stall(peer);
          break;
        }
        if (mode == Mode::kMalformedReplyDuringStartStream) {
          SendPacket(peer, 1, {});
          Stall(peer);
          break;
        }
      }

      if (is_validation_query) {
        SendVariableRow(peer, query);
      } else if (query.rfind("SET ", 0) == 0) {
        SendPacket(peer, 1, {0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00});
      } else {
        SendSingleValueRow(peer, SelectResponse(mode, query));
      }
    }
    close(peer);
  }

  void RecordQuery(const std::string& query) {
    std::lock_guard<std::mutex> lock(command_mutex_);
    commands_.push_back(ReceivedCommand{kComQuery, query, {}});
  }

  /** @return The number of dump requests received, @p request included. */
  size_t RecordDumpRequest(const std::vector<uint8_t>& request) {
    std::lock_guard<std::mutex> lock(command_mutex_);
    commands_.push_back(ReceivedCommand{request[0], std::string(), request});
    size_t dumps = 0;
    for (const ReceivedCommand& command : commands_) {
      if (command.command != kComQuery) ++dumps;
    }
    return dumps;
  }

  /// ERR packet for a source that ends the dump but keeps the session.
  static std::vector<uint8_t> BuildStreamError(uint16_t error_code) {
    std::vector<uint8_t> payload{0xFF, static_cast<uint8_t>(error_code),
                                 static_cast<uint8_t>(error_code >> 8), '#'};
    const std::string sql_state = "08S01";
    payload.insert(payload.end(), sql_state.begin(), sql_state.end());
    const std::string message = "the source ended the dump";
    payload.insert(payload.end(), message.begin(), message.end());
    return payload;
  }

  /** @brief Stop answering and hold the connection until the client hangs up. */
  void Stall(int peer) {
    stalled_.store(true, std::memory_order_release);
    uint8_t byte = 0;
    recv(peer, &byte, 1, 0);
  }

  static std::string SelectResponse(Mode mode, const std::string& query) {
    // binlog_checksum=NONE keeps the reader from expecting a CRC32 trailer on
    // the synthetic event; an empty purged set skips the preflight comparison.
    // The corrupted-event script needs the opposite: the trailer is only read,
    // and only found wrong, when the source reports CRC32.
    if (query.find("binlog_checksum") != std::string::npos) {
      return mode == Mode::kStreamCorruptedEventThenKeepStreaming ? "CRC32" : "NONE";
    }
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
  static std::vector<uint8_t> BuildHandshake(const std::string& version) {
    std::vector<uint8_t> payload;
    payload.push_back(10);
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
  // Written by the peer thread, read by the test thread.
  mutable std::mutex command_mutex_;
  std::vector<ReceivedCommand> commands_;
  std::thread thread_;
};

/// Start position PeerConfig() asks for, and the wider set the scripted stream
/// publishes as a checkpoint before the dump drops.
constexpr char kConfiguredGtid[] = "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5";
constexpr char kDeliveredGtid[] = "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-9";
/// The same start position in MariaDB's domain-server-sequence form, which is
/// the only form its GTID parsing and its session variables accept.
constexpr char kMariaDBConfiguredGtid[] = "0-1-5";

BinlogClientConfig PeerConfig(const ScriptedMysqlPeer& peer, uint32_t read_timeout_s) {
  BinlogClientConfig config;
  config.host = "127.0.0.1";
  config.port = peer.port();
  config.user = "repl";
  config.password = "repl";
  config.server_id = 4242;
  config.start_at_current = false;
  config.start_gtid = kConfiguredGtid;
  config.connect_timeout_s = 2;
  config.read_timeout_s = read_timeout_s;
  return config;
}

/** @brief Write the event's own byte count into its header length field. */
void SetEventLength(std::vector<uint8_t>* event) {
  const uint32_t length = static_cast<uint32_t>(event->size());
  (*event)[9] = static_cast<uint8_t>(length);
  (*event)[10] = static_cast<uint8_t>(length >> 8);
  (*event)[11] = static_cast<uint8_t>(length >> 16);
  (*event)[12] = static_cast<uint8_t>(length >> 24);
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
  SetEventLength(&event);
  for (int i = 13; i < 19; ++i) event[i] = 0;  // next position + flags
  return event;
}

/**
 * @brief A PREVIOUS_GTIDS_EVENT advertising @p gtid_set as the baseline.
 *
 * One such event is enough to produce a checkpoint: the tracker merges the
 * baseline into its set and hands the result back immediately, where a
 * transaction would need a GTID event plus its commit boundary.
 */
std::vector<uint8_t> MakePreviousGtidsEvent(const std::string& gtid_set) {
  std::vector<uint8_t> encoded;
  EXPECT_EQ(GtidEncoder::Encode(gtid_set.c_str(), &encoded), MES_OK);
  std::vector<uint8_t> event(kEventHeaderSize + encoded.size(), 0);
  event[4] = static_cast<uint8_t>(BinlogEventType::kPreviousGtidsEvent);
  SetEventLength(&event);
  std::copy(encoded.begin(), encoded.end(), event.begin() + kEventHeaderSize);
  return event;
}

/** @brief The encoded GTID set carried by a captured dump request. */
std::vector<uint8_t> DumpRequestGtidData(const std::vector<uint8_t>& request) {
  // COM_BINLOG_DUMP_GTID: command, flags, server id, filename length, filename,
  // position, GTID data length, GTID data.
  size_t offset = 1 + 2 + 4;
  if (request.size() < offset + 4) return {};
  const uint32_t filename_length = binary::ReadU32Le(request.data() + offset);
  offset += 4 + filename_length + 8;
  if (request.size() < offset + 4) return {};
  const uint32_t gtid_length = binary::ReadU32Le(request.data() + offset);
  offset += 4;
  if (request.size() < offset + gtid_length) return {};
  return std::vector<uint8_t>(request.data() + offset, request.data() + offset + gtid_length);
}

/// Offset of the 2-byte flags field in a COM_BINLOG_DUMP payload: the command
/// byte, then the 4-byte position.
constexpr size_t kComBinlogDumpFlagsOffset = 5;

/** @brief Position of @p statement among @p commands, or npos if never issued. */
size_t IndexOfQuery(const std::vector<ScriptedMysqlPeer::ReceivedCommand>& commands,
                    const std::string& statement) {
  for (size_t i = 0; i < commands.size(); ++i) {
    if (commands[i].command == kComQuery && commands[i].query == statement) return i;
  }
  return std::string::npos;
}

/** @brief Position of the first dump request among @p commands, or npos. */
size_t IndexOfDumpRequest(const std::vector<ScriptedMysqlPeer::ReceivedCommand>& commands) {
  for (size_t i = 0; i < commands.size(); ++i) {
    if (commands[i].command != kComQuery) return i;
  }
  return std::string::npos;
}

/**
 * @brief Wait until the reader has buffered @p count events.
 *
 * PollBatch() drains only what is already queued, so a test that needs a whole
 * scripted stream in one batch has to wait for the reader rather than race it.
 */
bool WaitForQueuedEvents(const BinlogClient& client, size_t count, milliseconds timeout) {
  const auto deadline = steady_clock::now() + timeout;
  while (BinlogClientTestAccess::QueuedEvents(client) < count) {
    if (steady_clock::now() >= deadline) return false;
    std::this_thread::sleep_for(milliseconds(5));
  }
  return true;
}

/** @brief Binary form of @p gtid_set as the dump request carries it. */
std::vector<uint8_t> EncodedGtidSet(const std::string& gtid_set) {
  std::vector<uint8_t> encoded;
  EXPECT_EQ(GtidEncoder::Encode(gtid_set.c_str(), &encoded), MES_OK);
  return encoded;
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

/**
 * @brief A stream setup that breaks the protocol stops reporting connected.
 *
 * Stream setup spends several queries on the connection, and each of them
 * closes the descriptor if the response cannot be framed. A client that keeps
 * answering "connected" there defeats the documented supervisor pattern: the
 * caller skips its reconnect and spends another whole start sequence on a dead
 * descriptor. Two independent failures are driven because setup queries fail
 * for more than one reason.
 */
TEST(BinlogClientLifecycle, AStreamSetupFailureThatClosesTheSocketReportsDisconnected) {
  struct Failure {
    const char* description;
    ScriptedMysqlPeer::Mode mode;
    uint32_t read_timeout_s;
  };
  const Failure failures[] = {
      {"setup query left unanswered", ScriptedMysqlPeer::Mode::kStallDuringStartStream, 1},
      {"setup query answered with an unframeable packet",
       ScriptedMysqlPeer::Mode::kMalformedReplyDuringStartStream, 10},
  };

  for (const Failure& failure : failures) {
    SCOPED_TRACE(failure.description);
    ScriptedMysqlPeer peer(failure.mode);
    BinlogClient client;
    ASSERT_EQ(client.Connect(PeerConfig(peer, failure.read_timeout_s)), MES_OK)
        << client.GetLastError();
    // Without this the assertions below would also hold for a client that
    // never connected at all.
    ASSERT_TRUE(client.IsConnected());

    EXPECT_EQ(client.StartStream(), MES_ERR_STREAM) << client.GetLastError();
    EXPECT_FALSE(client.IsStreaming());
    EXPECT_FALSE(BinlogClientTestAccess::TransportUsable(client))
        << "the failure under test did not close the socket";
    EXPECT_FALSE(client.IsConnected());
    // The consequence that matters: the next start is refused instead of
    // spending the whole setup sequence on a descriptor that is gone.
    EXPECT_EQ(client.StartStream(), MES_ERR_DISCONNECTED) << client.GetLastError();

    client.Disconnect();
  }
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

TEST(BinlogClientLifecycle, RestartAfterAStreamErrorResumesFromTheDeliveredCheckpoint) {
  ScriptedMysqlPeer peer(ScriptedMysqlPeer::Mode::kStreamThenServerError,
                         MakePreviousGtidsEvent(kDeliveredGtid));
  BinlogClient client;
  ASSERT_EQ(client.Connect(PeerConfig(peer, 10)), MES_OK) << client.GetLastError();
  ASSERT_EQ(client.StartStream(), MES_OK) << client.GetLastError();

  // The baseline event carries the checkpoint; requesting the following event is
  // the acknowledgement that publishes it.
  const PollResult delivered = client.Poll();
  ASSERT_EQ(delivered.error, MES_OK) << client.GetLastError();
  ASSERT_NE(delivered.data, nullptr);
  const PollResult heartbeat = client.Poll();
  ASSERT_EQ(heartbeat.error, MES_OK) << client.GetLastError();
  ASSERT_TRUE(heartbeat.is_heartbeat);
  ASSERT_EQ(std::string(client.GetCurrentGtid()), kDeliveredGtid);

  // The dump dies with a server error while the session stays usable, which is
  // the state the documented recovery path starts from.
  const PollResult dropped = client.Poll();
  EXPECT_EQ(dropped.error, MES_ERR_STREAM) << client.GetLastError();
  EXPECT_FALSE(client.IsStreaming());

  ASSERT_EQ(client.StartStream(), MES_OK) << client.GetLastError();
  ASSERT_TRUE(peer.WaitForDumpRequests(2, seconds(3)));
  const std::vector<std::vector<uint8_t> > requests = peer.DumpRequests();
  ASSERT_EQ(requests.size(), 2u);

  EXPECT_EQ(DumpRequestGtidData(requests[0]), EncodedGtidSet(kConfiguredGtid));
  // The replacement stream asks for the checkpoint this client published, not
  // the configured anchor: requesting that again would re-deliver everything
  // after it, and asking the source for its position now would skip whatever
  // committed while the stream was down.
  EXPECT_EQ(DumpRequestGtidData(requests[1]), EncodedGtidSet(kDeliveredGtid));
  EXPECT_NE(DumpRequestGtidData(requests[1]), DumpRequestGtidData(requests[0]));
  // Establishing the replacement stream may not walk the checkpoint backwards.
  EXPECT_EQ(std::string(client.GetCurrentGtid()), kDeliveredGtid);

  client.Stop();
  client.Disconnect();
}

/**
 * @brief The source's fatal binlog error is unrecoverable only for a GTID
 *        start; a file/position dump reports it as a retryable stream error.
 *
 * One error code covers both a purged GTID interval and a stale file offset,
 * and the offset case is fixed by restarting from a valid one. Reporting it as
 * a purged position would send the consumer to the one classification its
 * reconnect logic refuses to retry.
 */
TEST(BinlogClientLifecycle, FatalDumpErrorIsUnrecoverableOnlyForAGtidStart) {
  constexpr uint16_t kFatalErrorReadingBinlog = 1236;
  struct StartMode {
    bool from_file_position;
    uint8_t dump_command;
    mes_error_t expected;
  };
  const StartMode start_modes[] = {
      {false, kComBinlogDumpGtid, MES_ERR_GTID_PURGED},
      {true, kComBinlogDump, MES_ERR_STREAM},
  };

  for (const StartMode& start_mode : start_modes) {
    SCOPED_TRACE(start_mode.from_file_position ? "file/position start" : "GTID start");
    ScriptedMysqlPeer peer(ScriptedMysqlPeer::Mode::kStreamThenServerError, {},
                           kFatalErrorReadingBinlog);
    BinlogClient client;
    BinlogClientConfig config = PeerConfig(peer, 10);
    if (start_mode.from_file_position) {
      config.start_gtid.clear();
      config.start_at_file_position = true;
      config.binlog_file = "binlog.000001";
      config.binlog_position = kBinlogMagicOffset;
    }
    ASSERT_EQ(client.Connect(config), MES_OK) << client.GetLastError();
    ASSERT_EQ(client.StartStream(), MES_OK) << client.GetLastError();

    // The start mode has to be visible in the request that reached the peer,
    // otherwise both rows of this table would be driving the same dump.
    ASSERT_TRUE(peer.WaitForDumpRequests(1, seconds(3)));
    const std::vector<std::vector<uint8_t> > requests = peer.DumpRequests();
    ASSERT_FALSE(requests.empty());
    ASSERT_FALSE(requests[0].empty());
    EXPECT_EQ(requests[0][0], start_mode.dump_command);

    // The scripted dump carries no event, so the replication OK markers ahead
    // of the error arrive as heartbeats.
    PollResult result{};
    bool terminal = false;
    for (int poll = 0; poll < 8; ++poll) {
      result = client.Poll();
      if (result.error != MES_OK || !result.is_heartbeat) {
        terminal = true;
        break;
      }
    }
    ASSERT_TRUE(terminal) << "the scripted dump never delivered its terminal error";
    EXPECT_EQ(result.error, start_mode.expected) << client.GetLastError();

    client.Stop();
    client.Disconnect();
  }
}

/**
 * @brief A MariaDB source has its session negotiated before the dump is asked
 *        for, and the dump request asks for annotated rows.
 *
 * Every statement asserted here fails silently when it is dropped, and two of
 * them cost data rather than an error: without slave capability 4 the source
 * falls back to the legacy replication format, whose stream carries no
 * per-transaction GTID events, and without kBinlogSendAnnotateRows it withholds
 * the ANNOTATE_ROWS events published as mes_event_t.source_sql, leaving that
 * field permanently empty. The ordering is equally load-bearing: a session
 * variable the dump depends on has no effect once the dump has been requested.
 */
TEST(BinlogClientLifecycle, MariaDBStreamSetupNegotiatesTheSessionBeforeRequestingTheDump) {
  ScriptedMysqlPeer peer(ScriptedMysqlPeer::Mode::kStreamOneEvent, MakeWireEvent(256),
                         ScriptedMysqlPeer::kDefaultDumpError, kMariaDBVersion);
  BinlogClient client;
  BinlogClientConfig config = PeerConfig(peer, 10);
  config.start_gtid = kMariaDBConfiguredGtid;
  ASSERT_EQ(client.Connect(config), MES_OK) << client.GetLastError();
  ASSERT_EQ(client.StartStream(), MES_OK) << client.GetLastError();

  // Draining the scripted event proves the negotiated sequence carried a live
  // dump, rather than only a run of statements the peer happened to accept.
  const PollResult result = client.Poll();
  ASSERT_EQ(result.error, MES_OK) << client.GetLastError();
  EXPECT_EQ(result.size, 256u);

  const std::vector<ScriptedMysqlPeer::ReceivedCommand> commands = peer.Commands();
  const size_t dump = IndexOfDumpRequest(commands);
  ASSERT_NE(dump, std::string::npos) << "no dump was ever requested";

  const std::string negotiated[] = {
      // Capability 4 is what makes the source emit GTID events at all.
      "SET @mariadb_slave_capability = 4",
      // MariaDB reads its own checksum variable; the MySQL one is accepted and
      // then ignored, which would leave the trailer mode unnegotiated.
      "SET @master_binlog_checksum = @@global.binlog_checksum",
      // A gap in the requested GTID range has to fail the stream rather than be
      // skipped, and a duplicate has to be delivered rather than dropped.
      "SET @slave_gtid_strict_mode = 1",
      "SET @slave_gtid_ignore_duplicates = 0",
      // The start position itself: MariaDB takes it from the session, because
      // its COM_BINLOG_DUMP request has no field to carry a GTID set.
      std::string("SET @slave_connect_state = '") + kMariaDBConfiguredGtid + "'",
  };
  for (const std::string& statement : negotiated) {
    SCOPED_TRACE(statement);
    const size_t index = IndexOfQuery(commands, statement);
    ASSERT_NE(index, std::string::npos) << "the statement was never issued";
    EXPECT_LT(index, dump) << "the statement was issued after the dump it configures";
  }
  EXPECT_EQ(IndexOfQuery(commands, "SET @source_binlog_checksum='CRC32'"), std::string::npos);

  const std::vector<uint8_t>& request = commands[dump].payload;
  ASSERT_GE(request.size(), kComBinlogDumpFlagsOffset + 2);
  EXPECT_EQ(request[0], kComBinlogDump);
  EXPECT_EQ(binary::ReadU16Le(request.data() + kComBinlogDumpFlagsOffset),
            protocol::kBinlogSendAnnotateRows);

  client.Stop();
  client.Disconnect();
}

/**
 * @brief The MariaDB session negotiation and dump flag are flavour-gated.
 *
 * Both rows request the same start mode, so both reach the source through
 * COM_BINLOG_DUMP and the flags field sits at the same offset of the same
 * command: the server version advertised in the handshake is the only input
 * that differs. That makes this the discriminator the MariaDB assertions above
 * need, since they would hold just as well for a client that negotiated the
 * capability and asked for annotated rows unconditionally -- which a MySQL
 * source answers with an error rather than a stream.
 */
TEST(BinlogClientLifecycle, TheMariaDBCapabilityAndDumpFlagAreSentToMariaDBOnly) {
  struct Flavour {
    const char* server_version;
    bool negotiates_mariadb_capability;
    uint16_t expected_dump_flags;
  };
  const Flavour flavours[] = {
      {kMySQLVersion, false, 0},
      {kMariaDBVersion, true, protocol::kBinlogSendAnnotateRows},
  };

  for (const Flavour& flavour : flavours) {
    SCOPED_TRACE(flavour.server_version);
    ScriptedMysqlPeer peer(ScriptedMysqlPeer::Mode::kStreamOneEvent, MakeWireEvent(256),
                           ScriptedMysqlPeer::kDefaultDumpError, flavour.server_version);
    BinlogClient client;
    BinlogClientConfig config = PeerConfig(peer, 10);
    config.start_gtid.clear();
    config.start_at_file_position = true;
    config.binlog_file = "binlog.000001";
    config.binlog_position = kBinlogMagicOffset;
    ASSERT_EQ(client.Connect(config), MES_OK) << client.GetLastError();
    ASSERT_EQ(client.StartStream(), MES_OK) << client.GetLastError();
    ASSERT_TRUE(peer.WaitForDumpRequests(1, seconds(3)));

    const std::vector<ScriptedMysqlPeer::ReceivedCommand> commands = peer.Commands();
    EXPECT_EQ(IndexOfQuery(commands, "SET @mariadb_slave_capability = 4") != std::string::npos,
              flavour.negotiates_mariadb_capability);

    const size_t dump = IndexOfDumpRequest(commands);
    ASSERT_NE(dump, std::string::npos) << "no dump was ever requested";
    const std::vector<uint8_t>& request = commands[dump].payload;
    ASSERT_EQ(request.size(), 1u + 4u + 2u + 4u + config.binlog_file.size());
    EXPECT_EQ(request[0], kComBinlogDump);
    EXPECT_EQ(binary::ReadU16Le(request.data() + kComBinlogDumpFlagsOffset),
              flavour.expected_dump_flags);

    client.Stop();
    client.Disconnect();
  }
}

/**
 * @brief A MySQL-format start position is refused before a MariaDB source ever
 *        sees it.
 *
 * MariaDB expresses a position as domain-server-sequence and receives it
 * through @\@slave_connect_state, a statement built by interpolating the
 * configured set. A uuid:range set is not a position it can honour, and those
 * same characters are what an injected statement would arrive as, so the
 * mismatch has to end the start rather than reach the wire.
 */
TEST(BinlogClientLifecycle, AMySQLFormatStartGtidIsRefusedForAMariaDBSource) {
  ScriptedMysqlPeer peer(ScriptedMysqlPeer::Mode::kStreamOneEvent, MakeWireEvent(256),
                         ScriptedMysqlPeer::kDefaultDumpError, kMariaDBVersion);
  BinlogClient client;
  const BinlogClientConfig config = PeerConfig(peer, 10);
  ASSERT_EQ(config.start_gtid, kConfiguredGtid);
  ASSERT_EQ(client.Connect(config), MES_OK) << client.GetLastError();

  EXPECT_EQ(client.StartStream(), MES_ERR_INVALID_ARG) << client.GetLastError();
  EXPECT_FALSE(client.IsStreaming());
  // That code also covers the queue-budget rejection, which is refused before
  // any round trip, so the diagnostic is what tells the two apart.
  EXPECT_NE(std::string(client.GetLastError()).find("MariaDB GTID"), std::string::npos)
      << client.GetLastError();
  // No dump was requested, and no statement carried the rejected set: a
  // position this source cannot parse must not be interpolated into its
  // session at all.
  EXPECT_TRUE(peer.DumpRequests().empty());
  for (const std::string& query : peer.Queries()) {
    EXPECT_EQ(query.find(kConfiguredGtid), std::string::npos) << query;
  }

  client.Disconnect();
}

/**
 * @brief A reader that abandons a live dump retires the transport with it.
 *
 * A corrupt event stops the reader while the source is still streaming, and
 * the packets already in flight can never be matched to a command's response:
 * every one of them opens with the same marker byte as a command-phase OK
 * packet. The consequence that matters is the next start -- refused, rather
 * than issuing COM_QUERY into the middle of a replication stream.
 */
TEST(BinlogClientLifecycle, AReaderThatAbandonsALiveDumpRefusesTheNextStart) {
  ScriptedMysqlPeer peer(ScriptedMysqlPeer::Mode::kStreamCorruptedEventThenKeepStreaming,
                         MakeWireEvent(256));
  BinlogClient client;
  ASSERT_EQ(client.Connect(PeerConfig(peer, 10)), MES_OK) << client.GetLastError();
  ASSERT_EQ(client.StartStream(), MES_OK) << client.GetLastError();
  ASSERT_TRUE(client.IsConnected());

  const PollResult corrupted = client.Poll();
  EXPECT_EQ(corrupted.error, MES_ERR_CHECKSUM) << client.GetLastError();
  EXPECT_FALSE(client.IsStreaming());
  EXPECT_FALSE(BinlogClientTestAccess::TransportUsable(client))
      << "a socket still holding unread replication packets is reported as usable";
  EXPECT_FALSE(client.IsConnected());

  const size_t queries_before_restart = peer.Queries().size();
  EXPECT_EQ(client.StartStream(), MES_ERR_DISCONNECTED) << client.GetLastError();
  // The refusal has to come before the wire: a setup query sent here would be
  // answered by whatever the dump still had in flight.
  EXPECT_EQ(peer.Queries().size(), queries_before_restart);

  client.Disconnect();
}

TEST(BinlogClientLifecycle, RestartDoesNotPublishABatchCheckpointFromThePreviousStream) {
  ScriptedMysqlPeer peer(ScriptedMysqlPeer::Mode::kStreamThenServerError,
                         MakePreviousGtidsEvent(kDeliveredGtid));
  BinlogClient client;
  ASSERT_EQ(client.Connect(PeerConfig(peer, 10)), MES_OK) << client.GetLastError();
  ASSERT_EQ(client.StartStream(), MES_OK) << client.GetLastError();

  // One batch drains the event, the heartbeat and the terminal error. Its
  // checkpoint is only published by the following poll, so the client is holding
  // an unpublished checkpoint of this stream when it is restarted below.
  ASSERT_TRUE(WaitForQueuedEvents(client, 3, seconds(3)));
  std::vector<PollResult> batch;
  ASSERT_EQ(client.PollBatch(8, &batch), 3u);
  ASSERT_NE(batch.front().data, nullptr);
  ASSERT_EQ(batch.back().error, MES_ERR_STREAM);
  ASSERT_EQ(std::string(client.GetCurrentGtid()), kConfiguredGtid);
  ASSERT_FALSE(client.IsStreaming());

  ASSERT_EQ(client.StartStream(), MES_OK) << client.GetLastError();
  const PollResult after_restart = client.Poll();
  EXPECT_EQ(after_restart.error, MES_ERR_STREAM) << client.GetLastError();
  // Nothing has been delivered since the restart, so the first poll of the new
  // stream must not publish a checkpoint the previous one left behind.
  EXPECT_EQ(std::string(client.GetCurrentGtid()), kConfiguredGtid);

  client.Stop();
  client.Disconnect();
}

TEST(BinlogClientLifecycle, PollWithoutDataReleasesThePreviousEventBuffer) {
  constexpr uint32_t kEventSize = 4096;
  ScriptedMysqlPeer peer(ScriptedMysqlPeer::Mode::kStreamThenServerError,
                         MakeWireEvent(kEventSize));
  BinlogClient client;
  ASSERT_EQ(client.Connect(PeerConfig(peer, 10)), MES_OK) << client.GetLastError();
  ASSERT_EQ(client.StartStream(), MES_OK) << client.GetLastError();

  const PollResult delivered = client.Poll();
  ASSERT_EQ(delivered.error, MES_OK) << client.GetLastError();
  ASSERT_NE(delivered.data, nullptr);
  EXPECT_GE(BinlogClientTestAccess::RetainedEventBytes(client), kEventSize);

  // A heartbeat hands back no data, so the payload of the previous event is
  // released rather than kept for the rest of the client's life: an idle client
  // must not sit on the largest event it ever received.
  const PollResult heartbeat = client.Poll();
  ASSERT_TRUE(heartbeat.is_heartbeat);
  EXPECT_EQ(BinlogClientTestAccess::RetainedEventBytes(client), 0u);

  const PollResult dropped = client.Poll();
  EXPECT_EQ(dropped.error, MES_ERR_STREAM) << client.GetLastError();
  EXPECT_EQ(BinlogClientTestAccess::RetainedEventBytes(client), 0u);

  client.Stop();
  client.Disconnect();
  EXPECT_EQ(BinlogClientTestAccess::RetainedEventBytes(client), 0u);
}

TEST(BinlogClientLifecycle, PollAfterStopNamesReconnectInsteadOfStart) {
  ScriptedMysqlPeer peer(ScriptedMysqlPeer::Mode::kStallDuringStartStream);
  BinlogClient client;
  ASSERT_EQ(client.Connect(PeerConfig(peer, 2)), MES_OK) << client.GetLastError();

  // Never started: start() is precisely what this caller is missing.
  EXPECT_EQ(client.Poll().error, MES_ERR_DISCONNECTED);
  EXPECT_NE(std::string(client.GetLastError()).find("start()"), std::string::npos)
      << client.GetLastError();

  client.Stop();
  EXPECT_EQ(client.Poll().error, MES_ERR_DISCONNECTED);
  // A stop latches until a reconnect, so every later start is refused. The
  // message may not send the caller to the one action already ruled out.
  const std::string message = client.GetLastError();
  EXPECT_NE(message.find("reconnect"), std::string::npos) << message;
  EXPECT_EQ(message.find("start()"), std::string::npos) << message;
  EXPECT_EQ(client.StartStream(), MES_ERR_DISCONNECTED) << client.GetLastError();

  client.Disconnect();
}

/**
 * @brief Stop() from another thread releases a Poll() already waiting on the
 *        queue.
 *
 * The header names stop() as the one entry point a thread other than the
 * client's owner may call, and releasing a parked consumer is what that is
 * for: a poll waiting on a source that has gone quiet has no other way out,
 * and a caller shutting down cannot be made to sit through the read timeout
 * first.
 */
TEST(BinlogClientLifecycle, StopFromAnotherThreadReleasesAPollWaitingOnTheQueue) {
  // The scripted peer goes silent after its one event, so the reader parks in
  // recv() and nothing else is ever queued. The read timeout is what the poll
  // below would otherwise be waiting for, and it is set far past the bound this
  // test asserts so that only the stop can account for the poll returning.
  ScriptedMysqlPeer peer(ScriptedMysqlPeer::Mode::kStreamOneEvent, MakeWireEvent(256));
  BinlogClient client;
  ASSERT_EQ(client.Connect(PeerConfig(peer, 60)), MES_OK) << client.GetLastError();
  ASSERT_EQ(client.StartStream(), MES_OK) << client.GetLastError();

  // Draining the one scripted event is what leaves the queue empty, which is
  // the state in which the next poll genuinely waits.
  const PollResult delivered = client.Poll();
  ASSERT_EQ(delivered.error, MES_OK) << client.GetLastError();
  ASSERT_NE(delivered.data, nullptr);
  ASSERT_TRUE(peer.WaitUntilStalled(seconds(3)));

  // The stop is held back so the poll is measurably waiting when it arrives: a
  // stop that landed first would be answered by Poll()'s not-streaming
  // fast path instead, which is a different path and proves nothing here.
  constexpr milliseconds kStopDelay(300);
  std::atomic<bool> poll_entered{false};
  std::thread stopper([&] {
    while (!poll_entered.load(std::memory_order_acquire)) {
      std::this_thread::sleep_for(milliseconds(1));
    }
    std::this_thread::sleep_for(kStopDelay);
    client.Stop();
  });

  poll_entered.store(true, std::memory_order_release);
  const auto poll_started = steady_clock::now();
  const PollResult interrupted = client.Poll();
  const auto elapsed = steady_clock::now() - poll_started;
  stopper.join();

  EXPECT_EQ(interrupted.error, MES_ERR_DISCONNECTED) << client.GetLastError();
  EXPECT_EQ(interrupted.data, nullptr);
  // The poll waited for the stop rather than returning ahead of it,
  EXPECT_GE(elapsed, kStopDelay / 2);
  // and the stop is what ended the wait, not the configured read timeout.
  EXPECT_LT(elapsed, seconds(5));
  EXPECT_FALSE(client.IsStreaming());

  client.Disconnect();
}

/**
 * @brief A poll after a stop never hands out an event the stopped stream had
 *        already queued.
 *
 * Stop() latches until a reconnect, so everything the reader buffered belongs
 * to a stream the caller has abandoned. Delivering one of those afterwards
 * would hand a consumer a row change from a position it has stopped tracking,
 * and would do it with an error state already set.
 */
TEST(BinlogClientLifecycle, PollAfterStopDoesNotHandOutAnEventTheStreamHadQueued) {
  ScriptedMysqlPeer peer(ScriptedMysqlPeer::Mode::kStreamOneEvent, MakeWireEvent(256));
  BinlogClient client;
  ASSERT_EQ(client.Connect(PeerConfig(peer, 10)), MES_OK) << client.GetLastError();
  ASSERT_EQ(client.StartStream(), MES_OK) << client.GetLastError();

  // Nothing is polled first: the scripted event has to be sitting in the queue
  // when the stop arrives, otherwise there is nothing that could be handed out
  // wrongly and the assertions below would hold for any stopped client.
  ASSERT_TRUE(WaitForQueuedEvents(client, 1, seconds(3)));
  ASSERT_EQ(BinlogClientTestAccess::QueuedEvents(client), 1u);

  client.Stop();

  const PollResult after_stop = client.Poll();
  EXPECT_EQ(after_stop.error, MES_ERR_DISCONNECTED) << client.GetLastError();
  EXPECT_EQ(after_stop.data, nullptr);
  EXPECT_EQ(after_stop.size, 0u);
  EXPECT_FALSE(client.IsStreaming());
  // Released as well as withheld: the abandoned stream's payload must not stay
  // resident for the rest of the client's life just because it was never read.
  EXPECT_EQ(BinlogClientTestAccess::QueuedEvents(client), 0u);
  EXPECT_EQ(client.QueuedBytes(), 0u);

  client.Disconnect();
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

/**
 * @brief The timeout a C caller sets is the timeout the stream is configured
 *        with, resolved at the C ABI boundary and nowhere else.
 *
 * The negotiated heartbeat period is half the read timeout, capped at three
 * seconds, so a timeout below six seconds reaches the peer as the statement
 * that configures it. That makes the resolved value observable on the wire:
 * a boundary that dropped the caller's value, or that substituted the default
 * for a non-zero one, would negotiate the capped period instead.
 */
TEST(BinlogClientLifecycle, CApiReadTimeoutReachesTheNegotiatedHeartbeat) {
  ScriptedMysqlPeer peer(ScriptedMysqlPeer::Mode::kStreamOneEvent, MakeWireEvent(256));
  mes_client_t* client = mes_client_create();
  ASSERT_NE(client, nullptr);

  mes_client_config_t config{};
  config.host = "127.0.0.1";
  config.port = peer.port();
  config.user = "repl";
  config.password = "repl";
  config.server_id = 4242;
  config.start_position_mode = MES_START_AT_GTID;
  config.start_gtid = kConfiguredGtid;
  config.connect_timeout_s = 2;
  config.read_timeout_s = 2;

  ASSERT_EQ(mes_client_connect(client, &config), MES_OK) << mes_client_last_error(client);
  ASSERT_EQ(mes_client_start(client), MES_OK) << mes_client_last_error(client);

  const std::vector<std::string> queries = peer.Queries();
  const std::string expected = "SET @master_heartbeat_period = 1000000000";
  EXPECT_NE(std::find(queries.begin(), queries.end(), expected), queries.end())
      << "the stream negotiated a heartbeat that is not half of the configured read timeout";

  mes_client_stop(client);
  mes_client_destroy(client);
}

#endif

/**
 * @brief The two cross-thread accessors share one buffer, not one per caller.
 *
 * GetLastError() and GetCurrentGtid() are what a binding exposes as properties
 * readable at any moment, so the header states they are safe to call from any
 * thread and that the pointer each returns survives only until the next call to
 * the same accessor -- from any thread, not merely the caller's own. That second
 * half is a property of the shared snapshot buffer they copy into, and this is
 * what observes it: a call made on another thread comes back with the address
 * the first call already handed out. Given per-caller storage instead, the
 * documented caveat would be stricter than the code, and a binding author would
 * be copying strings to escape a hazard that no longer existed.
 *
 * Placed outside the scripted-peer guard: refusing a start needs no socket.
 */
TEST(BinlogClientLifecycle, CrossThreadAccessorsShareOneBufferAcrossThreads) {
  BinlogClient client;

  // Refused locally, before any transport exists, which is enough to put a real
  // message in the buffer rather than exercising an empty one.
  ASSERT_EQ(client.StartStream(), MES_ERR_DISCONNECTED);

  const char* error_here = client.GetLastError();
  ASSERT_NE(error_here, nullptr);
  ASSERT_STRNE(error_here, "");
  const char* gtid_here = client.GetCurrentGtid();
  ASSERT_NE(gtid_here, nullptr);

  const char* error_there = nullptr;
  const char* gtid_there = nullptr;
  // Joined rather than overlapped: what is under test is where the second call
  // writes, which does not require the two calls to be concurrent.
  std::thread other([&] {
    error_there = client.GetLastError();
    gtid_there = client.GetCurrentGtid();
  });
  other.join();

  EXPECT_EQ(error_there, error_here);
  EXPECT_EQ(gtid_there, gtid_here);
  // And the address the other thread wrote through still holds a whole string.
  EXPECT_STRNE(error_here, "");
}

}  // namespace
}  // namespace mes
