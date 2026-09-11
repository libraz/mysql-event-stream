// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <functional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#ifndef _WIN32
#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

#include "protocol/mysql_packet.h"
#include "protocol/mysql_socket.h"
#include "source_scan.h"

#if defined(__has_feature)
#if __has_feature(address_sanitizer)
#define MES_TEST_CONTAINER_ANNOTATIONS 1
#endif
#elif defined(__SANITIZE_ADDRESS__)
#define MES_TEST_CONTAINER_ANNOTATIONS 1
#endif

#if defined(MES_TEST_CONTAINER_ANNOTATIONS)
extern "C" void __sanitizer_annotate_contiguous_container(const void* beg, const void* end,
                                                          const void* old_mid, const void* new_mid);
#endif

namespace mes::protocol {
namespace {

/**
 * @brief Treats a container's whole allocation as valid for the object's scope.
 *
 * A sanitizer-instrumented std::vector poisons the bytes between size() and
 * capacity(), which is precisely the region a scrubbing test has to read. This
 * lifts that poison for the read and restores it afterwards, so the rest of
 * the suite keeps full container-overflow detection. Without a sanitizer it
 * compiles away.
 */
class ScopedContiguousContainerFullyValid {
 public:
  ScopedContiguousContainerFullyValid(const void* begin, size_t capacity, size_t valid)
      : begin_(static_cast<const uint8_t*>(begin)), capacity_(capacity), valid_(valid) {
    Annotate(valid_, capacity_);
  }

  ~ScopedContiguousContainerFullyValid() { Annotate(capacity_, valid_); }

  ScopedContiguousContainerFullyValid(const ScopedContiguousContainerFullyValid&) = delete;
  ScopedContiguousContainerFullyValid& operator=(const ScopedContiguousContainerFullyValid&) =
      delete;

 private:
  void Annotate([[maybe_unused]] size_t old_mid, [[maybe_unused]] size_t new_mid) const {
#if defined(MES_TEST_CONTAINER_ANNOTATIONS)
    if (begin_ == nullptr || capacity_ == 0) return;
    __sanitizer_annotate_contiguous_container(begin_, begin_ + capacity_, begin_ + old_mid,
                                              begin_ + new_mid);
#endif
  }

  const uint8_t* begin_;
  size_t capacity_;
  size_t valid_;
};

// --- ReadFixedInt / WriteFixedInt round-trip ---

TEST(FixedIntTest, RoundTrip1Byte) {
  std::vector<uint8_t> buf;
  WriteFixedInt(&buf, 0xAB, 1);
  EXPECT_EQ(buf.size(), 1u);
  EXPECT_EQ(ReadFixedInt(buf.data(), 1), 0xABu);
}

TEST(FixedIntTest, RoundTrip2Bytes) {
  std::vector<uint8_t> buf;
  WriteFixedInt(&buf, 0xBEEF, 2);
  EXPECT_EQ(buf.size(), 2u);
  EXPECT_EQ(ReadFixedInt(buf.data(), 2), 0xBEEFu);
}

TEST(FixedIntTest, RoundTrip3Bytes) {
  std::vector<uint8_t> buf;
  WriteFixedInt(&buf, 0xABCDEF, 3);
  EXPECT_EQ(buf.size(), 3u);
  EXPECT_EQ(ReadFixedInt(buf.data(), 3), 0xABCDEFu);
}

TEST(FixedIntTest, RoundTrip4Bytes) {
  std::vector<uint8_t> buf;
  WriteFixedInt(&buf, 0xDEADBEEF, 4);
  EXPECT_EQ(buf.size(), 4u);
  EXPECT_EQ(ReadFixedInt(buf.data(), 4), 0xDEADBEEFu);
}

TEST(FixedIntTest, RoundTrip8Bytes) {
  std::vector<uint8_t> buf;
  uint64_t val = 0x0102030405060708ULL;
  WriteFixedInt(&buf, val, 8);
  EXPECT_EQ(buf.size(), 8u);
  EXPECT_EQ(ReadFixedInt(buf.data(), 8), val);
}

// --- ReadLenEncInt / WriteLenEncInt round-trip ---

TEST(LenEncIntTest, SmallValues) {
  for (uint64_t v : {uint64_t{0}, uint64_t{1}, uint64_t{250}}) {
    std::vector<uint8_t> buf;
    WriteLenEncInt(&buf, v);
    EXPECT_EQ(buf.size(), 1u);
    size_t pos = 0;
    EXPECT_EQ(ReadLenEncInt(buf.data(), buf.size(), &pos), v);
    EXPECT_EQ(pos, 1u);
  }
}

TEST(LenEncIntTest, TwoByteValues) {
  for (uint64_t v : {uint64_t{252}, uint64_t{65535}}) {
    std::vector<uint8_t> buf;
    WriteLenEncInt(&buf, v);
    EXPECT_EQ(buf.size(), 3u);  // 0xFC marker + 2 bytes
    size_t pos = 0;
    EXPECT_EQ(ReadLenEncInt(buf.data(), buf.size(), &pos), v);
    EXPECT_EQ(pos, 3u);
  }
}

TEST(LenEncIntTest, ThreeByteValues) {
  for (uint64_t v : {uint64_t{65536}, uint64_t{0xFFFFFF}}) {
    std::vector<uint8_t> buf;
    WriteLenEncInt(&buf, v);
    EXPECT_EQ(buf.size(), 4u);  // 0xFD marker + 3 bytes
    size_t pos = 0;
    EXPECT_EQ(ReadLenEncInt(buf.data(), buf.size(), &pos), v);
    EXPECT_EQ(pos, 4u);
  }
}

TEST(LenEncIntTest, EightByteValues) {
  for (uint64_t v : {uint64_t{0x1000000}, uint64_t{0xFFFFFFFFFFFFFFFF}}) {
    std::vector<uint8_t> buf;
    WriteLenEncInt(&buf, v);
    EXPECT_EQ(buf.size(), 9u);  // 0xFE marker + 8 bytes
    size_t pos = 0;
    EXPECT_EQ(ReadLenEncInt(buf.data(), buf.size(), &pos), v);
    EXPECT_EQ(pos, 9u);
  }
}

// --- WriteLenEncString ---

TEST(LenEncStringTest, ContainsLengthPrefixAndData) {
  std::vector<uint8_t> buf;
  std::string s = "hello";
  WriteLenEncString(&buf, s);

  // First byte should be the string length (5, fits in 1 byte)
  EXPECT_EQ(buf[0], 5u);
  // Remaining bytes should be the string data
  EXPECT_EQ(buf.size(), 1u + s.size());
  EXPECT_EQ(std::memcmp(buf.data() + 1, s.data(), s.size()), 0);
}

TEST(LenEncStringTest, EmptyString) {
  std::vector<uint8_t> buf;
  WriteLenEncString(&buf, "");
  EXPECT_EQ(buf.size(), 1u);
  EXPECT_EQ(buf[0], 0u);
}

// --- PacketBuffer::WritePacket ---

TEST(PacketBufferTest, NormalPacketHeaderAndPayload) {
  PacketBuffer pb;
  uint8_t payload[] = {0x01, 0x02, 0x03};
  uint8_t seq = 0;
  pb.WritePacket(payload, sizeof(payload), &seq);

  // 4-byte header + 3-byte payload = 7 bytes total
  EXPECT_EQ(pb.Size(), 7u);

  const uint8_t* data = pb.Data();
  // 3-byte LE length = 3
  EXPECT_EQ(data[0], 3u);
  EXPECT_EQ(data[1], 0u);
  EXPECT_EQ(data[2], 0u);
  // sequence_id = 0
  EXPECT_EQ(data[3], 0u);
  // payload
  EXPECT_EQ(data[4], 0x01u);
  EXPECT_EQ(data[5], 0x02u);
  EXPECT_EQ(data[6], 0x03u);
}

TEST(PacketBufferTest, SequenceIdIncremented) {
  PacketBuffer pb;
  uint8_t payload[] = {0xAA};
  uint8_t seq = 5;
  pb.WritePacket(payload, sizeof(payload), &seq);

  // sequence_id should be incremented after writing
  EXPECT_EQ(seq, 6u);

  const uint8_t* data = pb.Data();
  // The header should contain the original sequence_id (5)
  EXPECT_EQ(data[3], 5u);
}

// --- PacketBuffer multi-packet splitting ---

TEST(PacketBufferTest, ExactMaxPayloadProducesTwoPackets) {
  PacketBuffer pb;
  constexpr size_t kMaxPayload = 0xFFFFFF;
  std::vector<uint8_t> payload(kMaxPayload, 0x42);
  uint8_t seq = 0;
  pb.WritePacket(payload.data(), payload.size(), &seq);

  // Should produce 2 packets: full 0xFFFFFF + trailing 0-length packet
  // = (4 + 0xFFFFFF) + (4 + 0) = 0xFFFFFF + 8
  EXPECT_EQ(pb.Size(), kMaxPayload + 4 + 4);
  // sequence_id should be incremented twice (0 -> 2)
  EXPECT_EQ(seq, 2u);
}

TEST(PacketBufferTest, MaxPayloadPlusOneProducesTwoPackets) {
  PacketBuffer pb;
  constexpr size_t kMaxPayload = 0xFFFFFF;
  std::vector<uint8_t> payload(kMaxPayload + 1, 0x42);
  uint8_t seq = 0;
  pb.WritePacket(payload.data(), payload.size(), &seq);

  // Should produce 2 packets: full 0xFFFFFF + 1-byte remainder
  // = (4 + 0xFFFFFF) + (4 + 1)
  EXPECT_EQ(pb.Size(), kMaxPayload + 4 + 4 + 1);
  EXPECT_EQ(seq, 2u);
}

// --- PacketBuffer::Clear ---

TEST(PacketBufferTest, ClearResetsSizeToZero) {
  PacketBuffer pb;
  uint8_t payload[] = {0x01};
  uint8_t seq = 0;
  pb.WritePacket(payload, sizeof(payload), &seq);
  EXPECT_GT(pb.Size(), 0u);

  pb.Clear();
  EXPECT_EQ(pb.Size(), 0u);
}

TEST(PacketBufferTest, ClearWipesPayloadBytesInPlace) {
  // Handshake and cleartext-password packets pass through PacketBuffer, so the
  // bytes must be scrubbed rather than merely forgotten. Clear() keeps the
  // capacity, which lets the test inspect the very storage that held them.
  PacketBuffer pb;
  const std::string secret = "correct horse battery staple";
  uint8_t seq = 0;
  pb.WritePacket(reinterpret_cast<const uint8_t*>(secret.data()), secret.size(), &seq);

  const uint8_t* storage = pb.Data();
  const size_t written = pb.Size();
  const size_t capacity = pb.Capacity();
  ASSERT_EQ(written, 4u + secret.size());
  ASSERT_LE(written, capacity);
  ASSERT_EQ(std::memcmp(storage + 4, secret.data(), secret.size()), 0);

  pb.Clear();
  EXPECT_EQ(pb.Size(), 0u);
  EXPECT_EQ(pb.Capacity(), capacity) << "Clear() must keep the storage it scrubbed";

  // The scrubbed bytes now live between size() and capacity(), which a
  // container-overflow-aware sanitizer poisons. Lift the annotation for the
  // read: this region is exactly what the assertion is about.
  ScopedContiguousContainerFullyValid unpoisoned(storage, capacity, 0);
  for (size_t i = 0; i < written; ++i) {
    EXPECT_EQ(storage[i], 0) << "byte " << i << " survived Clear()";
  }
}

// --- ReadFixedInt width guard ---

TEST(FixedIntTest, WidthGreaterThan8ReturnsZero) {
  uint8_t data[] = {0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF};
  EXPECT_EQ(ReadFixedInt(data, 9), 0u);
  EXPECT_EQ(ReadFixedInt(data, 16), 0u);
  EXPECT_EQ(ReadFixedInt(data, 100), 0u);
}

// --- ReadFixedIntChecked length guard ---

TEST(FixedIntCheckedTest, ReadsWithinBounds) {
  uint8_t data[] = {0x11, 0x22, 0x33, 0x44};
  size_t pos = 0;
  uint64_t val = 0;
  EXPECT_TRUE(ReadFixedIntChecked(data, sizeof(data), &pos, 4, &val));
  EXPECT_EQ(val, 0x44332211u);
  EXPECT_EQ(pos, 4u);
}

TEST(FixedIntCheckedTest, AdvancesPositionSequentially) {
  uint8_t data[] = {0xAA, 0xBB, 0xCC};
  size_t pos = 0;
  uint64_t v1 = 0;
  uint64_t v2 = 0;
  EXPECT_TRUE(ReadFixedIntChecked(data, sizeof(data), &pos, 1, &v1));
  EXPECT_EQ(v1, 0xAAu);
  EXPECT_EQ(pos, 1u);
  EXPECT_TRUE(ReadFixedIntChecked(data, sizeof(data), &pos, 2, &v2));
  EXPECT_EQ(v2, 0xCCBBu);
  EXPECT_EQ(pos, 3u);
}

TEST(FixedIntCheckedTest, InsufficientBytesFailsAndLeavesPosUnchanged) {
  uint8_t data[] = {0x01, 0x02, 0x03};
  size_t pos = 2;
  uint64_t val = 0xDEAD;
  // Only 1 byte remains but 2 requested.
  EXPECT_FALSE(ReadFixedIntChecked(data, sizeof(data), &pos, 2, &val));
  EXPECT_EQ(pos, 2u);       // unchanged
  EXPECT_EQ(val, 0xDEADu);  // unchanged
}

TEST(FixedIntCheckedTest, PosAtEndFails) {
  uint8_t data[] = {0x01};
  size_t pos = 1;
  uint64_t val = 0;
  EXPECT_FALSE(ReadFixedIntChecked(data, sizeof(data), &pos, 1, &val));
  EXPECT_EQ(pos, 1u);
}

TEST(FixedIntCheckedTest, WidthZeroOrTooLargeFails) {
  uint8_t data[] = {0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09};
  size_t pos = 0;
  uint64_t val = 0;
  EXPECT_FALSE(ReadFixedIntChecked(data, sizeof(data), &pos, 0, &val));
  EXPECT_EQ(pos, 0u);
  EXPECT_FALSE(ReadFixedIntChecked(data, sizeof(data), &pos, 9, &val));
  EXPECT_EQ(pos, 0u);
}

// --- ParseErrPacketPayload ---

TEST(ParseErrPacketPayloadTest, StandardErrPacketWithSqlState) {
  // Build: 0xFF + error_code(2 LE) + '#' + sql_state(5) + message
  std::vector<uint8_t> packet = {
      0xFF,                       // marker
      0xE8, 0x03,                 // error_code = 1000 (LE)
      '#',                        // sql_state_marker
      'H',  'Y',  '0', '0', '0',  // sql_state
      'T',  'e',  's', 't',       // message
  };
  uint16_t code = 0;
  std::string msg;
  ParseErrPacketPayload(packet.data(), packet.size(), &code, &msg);
  EXPECT_EQ(code, 1000u);
  EXPECT_EQ(msg, "Test");
}

TEST(ParseErrPacketPayloadTest, ErrPacketWithoutSqlState) {
  // Build: 0xFF + error_code(2 LE) + message (no '#' marker)
  std::vector<uint8_t> packet = {
      0xFF, 0x15, 0x04,  // error_code = 1045 (LE)
      'A',  'c',  'c',  'e', 's', 's',
  };
  uint16_t code = 0;
  std::string msg;
  ParseErrPacketPayload(packet.data(), packet.size(), &code, &msg);
  EXPECT_EQ(code, 1045u);
  EXPECT_EQ(msg, "Access");
}

TEST(ParseErrPacketPayloadTest, TruncatedPacketTooShort) {
  // Only 2 bytes, less than minimum 3
  std::vector<uint8_t> packet = {0xFF, 0x01};
  uint16_t code = 0;
  std::string msg;
  ParseErrPacketPayload(packet.data(), packet.size(), &code, &msg);
  EXPECT_EQ(code, 0u);
  EXPECT_EQ(msg, "Unknown MySQL error");
}

TEST(ParseErrPacketPayloadTest, MinimalErrPacketNoMessage) {
  // 0xFF + error_code only, no message
  std::vector<uint8_t> packet = {0xFF, 0x01, 0x00};
  uint16_t code = 0;
  std::string msg;
  ParseErrPacketPayload(packet.data(), packet.size(), &code, &msg);
  EXPECT_EQ(code, 1u);
  EXPECT_TRUE(msg.empty());
}

TEST(ParseErrPacketPayloadTest, ErrPacketWithSqlStateNoMessage) {
  // 0xFF + error_code + '#' + sql_state, but no message after
  std::vector<uint8_t> packet = {
      0xFF, 0x01, 0x00, '#', 'H', 'Y', '0', '0', '0',
  };
  uint16_t code = 0;
  std::string msg;
  ParseErrPacketPayload(packet.data(), packet.size(), &code, &msg);
  EXPECT_EQ(code, 1u);
  EXPECT_TRUE(msg.empty());
}

// --- ReadPacket multi-packet reassembly ---

#ifndef _WIN32

/// Payload length that marks a packet as continued by the next one.
constexpr size_t kMaxPacketPayload = 0xFFFFFF;

// These tests leave the peer writing after the reader has given up, so a write
// to a closed peer must report EPIPE instead of raising SIGPIPE and killing the
// test binary. Linux needs the send flag; macOS/BSD use SO_NOSIGPIPE on the
// accepted socket.
#ifdef MSG_NOSIGNAL
constexpr int kSendFlags = MSG_NOSIGNAL;
#else
constexpr int kSendFlags = 0;
#endif

/** @brief Write every byte of a buffer to a socket, looping over partial sends. */
bool SendAll(int peer, const uint8_t* data, size_t len) {
  size_t offset = 0;
  while (offset < len) {
    const ssize_t sent = send(peer, data + offset, len - offset, kSendFlags);
    if (sent <= 0) return false;
    offset += static_cast<size_t>(sent);
  }
  return true;
}

/** @brief Build a packet header: 3-byte LE payload length plus sequence ID. */
std::vector<uint8_t> PacketHeader(size_t len, uint8_t sequence) {
  return {static_cast<uint8_t>(len), static_cast<uint8_t>(len >> 8),
          static_cast<uint8_t>(len >> 16), sequence};
}

/** @brief Send one MySQL wire packet: header followed by its payload. */
bool SendWirePacket(int peer, uint8_t sequence, const uint8_t* payload, size_t len) {
  const std::vector<uint8_t> header = PacketHeader(len, sequence);
  if (!SendAll(peer, header.data(), header.size())) return false;
  return len == 0 || SendAll(peer, payload, len);
}

bool SendWirePacket(int peer, uint8_t sequence, const std::vector<uint8_t>& payload) {
  return SendWirePacket(peer, sequence, payload.data(), payload.size());
}

/**
 * @brief Send a packet header one byte per write, pausing between bytes.
 *
 * The pause is what puts each header byte in its own recv() return: the reader
 * is already blocked on the previous byte by the time the next one arrives.
 */
bool SendDribbledHeader(int peer, uint8_t sequence, size_t len) {
  for (uint8_t byte : PacketHeader(len, sequence)) {
    if (!SendAll(peer, &byte, 1)) return false;
    std::this_thread::sleep_for(std::chrono::milliseconds(2));
  }
  return true;
}

/** @brief Deterministic byte pattern; @p seed distinguishes one region from another. */
std::vector<uint8_t> PatternBytes(size_t len, uint8_t seed) {
  std::vector<uint8_t> bytes(len);
  for (size_t i = 0; i < len; ++i) {
    bytes[i] = static_cast<uint8_t>((i * 31u) + seed);
  }
  return bytes;
}

/**
 * @brief Loopback peer that writes scripted wire bytes to one accepted client.
 *
 * ReadPacket() is driven directly, so nothing is expected on the wire from the
 * client and the peer starts writing as soon as the connection is accepted.
 * Nagle is disabled so a byte-at-a-time script is not coalesced back into one
 * segment before it reaches the reader.
 */
class PacketPeer {
 public:
  explicit PacketPeer(std::function<void(int)> respond) {
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
      const int nodelay = 1;
      setsockopt(peer, IPPROTO_TCP, TCP_NODELAY, &nodelay, sizeof(nodelay));
      responder(peer);
      close(peer);
    });
  }

  ~PacketPeer() {
    if (thread_.joinable()) thread_.join();
    if (listener_ >= 0) close(listener_);
  }

  PacketPeer(const PacketPeer&) = delete;
  PacketPeer& operator=(const PacketPeer&) = delete;

  uint16_t port() const { return port_; }

 private:
  int listener_ = -1;
  uint16_t port_ = 0;
  std::thread thread_;
};

/**
 * @brief Block until the reader closes, so the script's last packet stays unread.
 *
 * A peer that returned immediately would close the connection, and a reader
 * that consumed one packet too many would then see EOF rather than the packet
 * the assertions are about.
 */
void WaitForReaderClose(int peer) {
  uint8_t byte = 0;
  recv(peer, &byte, 1, 0);
}

/**
 * @brief Hold the script until the reader announces it has consumed what is sent.
 *
 * A read-ahead buffer can only be left in a known state if nothing beyond the
 * bytes under test is in flight, so the peer has to wait for the reader rather
 * than write the whole script up front.
 */
bool WaitForReaderRequest(int peer) {
  uint8_t byte = 0;
  return recv(peer, &byte, 1, 0) == 1;
}

/** @brief Tell a peer blocked in WaitForReaderRequest() to send the next region. */
bool RequestNextRegion(SocketHandle* socket) {
  const uint8_t byte = 0xA5;
  return socket->WriteAll(&byte, 1) == MES_OK;
}

// Bigger than any plausible read-ahead staging buffer, so a read of this size
// cannot be satisfied by a single staged recv() however the buffer is sized.
constexpr size_t kBeyondReadAhead = 256u * 1024u;

#endif  // _WIN32

TEST(ReadPacketTest, AZeroLengthContinuationEndsTheChainAndLeavesTheNextPacketUnread) {
#ifdef _WIN32
  GTEST_SKIP() << "local socket test is POSIX-only";
#else
  // The terminator of a maximum-length packet is allowed to carry no payload at
  // all. Stopping at the full packet would leave the empty one in the socket and
  // desynchronise every later read; consuming past it would swallow the next
  // packet outright.
  const std::vector<uint8_t> chained = PatternBytes(kMaxPacketPayload, 0x11);
  const std::vector<uint8_t> following = PatternBytes(64, 0x77);
  PacketPeer peer([&](int fd) {
    if (!SendWirePacket(fd, 1, chained) || !SendWirePacket(fd, 2, {}) ||
        !SendWirePacket(fd, 3, following)) {
      return;
    }
    WaitForReaderClose(fd);
  });

  SocketHandle socket;
  ASSERT_EQ(socket.Connect("127.0.0.1", peer.port(), 1), MES_OK);
  ASSERT_EQ(socket.SetReadTimeout(2), MES_OK);

  std::vector<uint8_t> payload;
  uint8_t sequence = 0;
  ASSERT_EQ(ReadPacket(&socket, &payload, &sequence), MES_OK);
  ASSERT_EQ(payload.size(), kMaxPacketPayload);
  EXPECT_EQ(payload, chained);
  // The empty packet supplied the last header that was read.
  EXPECT_EQ(sequence, 2u);

  ASSERT_EQ(ReadPacket(&socket, &payload, &sequence), MES_OK);
  EXPECT_EQ(payload, following);
  EXPECT_EQ(sequence, 3u);
#endif
}

TEST(ReadPacketTest, APayloadAtTheMaximumIsCompletedByItsShortContinuation) {
#ifdef _WIN32
  GTEST_SKIP() << "local socket test is POSIX-only";
#else
  // The ordinary split: one full packet plus a remainder. The reassembled
  // payload is the concatenation of both, with no header bytes in between.
  const std::vector<uint8_t> chained = PatternBytes(kMaxPacketPayload, 0x11);
  const std::vector<uint8_t> remainder = PatternBytes(10, 0xA0);
  PacketPeer peer([&](int fd) {
    if (!SendWirePacket(fd, 1, chained) || !SendWirePacket(fd, 2, remainder)) return;
    WaitForReaderClose(fd);
  });

  SocketHandle socket;
  ASSERT_EQ(socket.Connect("127.0.0.1", peer.port(), 1), MES_OK);
  ASSERT_EQ(socket.SetReadTimeout(2), MES_OK);

  std::vector<uint8_t> payload;
  uint8_t sequence = 0;
  ASSERT_EQ(ReadPacket(&socket, &payload, &sequence), MES_OK);
  ASSERT_EQ(payload.size(), kMaxPacketPayload + remainder.size());
  EXPECT_TRUE(std::equal(chained.begin(), chained.end(), payload.begin()));
  EXPECT_TRUE(std::equal(remainder.begin(), remainder.end(), payload.begin() + kMaxPacketPayload));
  EXPECT_EQ(sequence, 2u);
#endif
}

TEST(ReadPacketTest, TwoMaximumPacketsAccumulateBeforeTheShortContinuationEndsTheChain) {
#ifdef _WIN32
  GTEST_SKIP() << "local socket test is POSIX-only";
#else
  // A payload past 32 MB chains three packets. Reassembly that handled only a
  // single continuation would return the first two thirds and leave the rest in
  // the socket.
  const std::vector<uint8_t> chained = PatternBytes(kMaxPacketPayload * 2, 0x11);
  const std::vector<uint8_t> remainder = PatternBytes(7, 0xC0);
  PacketPeer peer([&](int fd) {
    if (!SendWirePacket(fd, 1, chained.data(), kMaxPacketPayload) ||
        !SendWirePacket(fd, 2, chained.data() + kMaxPacketPayload, kMaxPacketPayload) ||
        !SendWirePacket(fd, 3, remainder)) {
      return;
    }
    WaitForReaderClose(fd);
  });

  SocketHandle socket;
  ASSERT_EQ(socket.Connect("127.0.0.1", peer.port(), 1), MES_OK);
  ASSERT_EQ(socket.SetReadTimeout(2), MES_OK);

  std::vector<uint8_t> payload;
  uint8_t sequence = 0;
  ASSERT_EQ(ReadPacket(&socket, &payload, &sequence), MES_OK);
  ASSERT_EQ(payload.size(), chained.size() + remainder.size());
  EXPECT_TRUE(std::equal(chained.begin(), chained.end(), payload.begin()));
  EXPECT_TRUE(std::equal(remainder.begin(), remainder.end(), payload.begin() + chained.size()));
  EXPECT_EQ(sequence, 3u);
#endif
}

TEST(ReadPacketTest, HeadersArrivingOneByteAtATimeFrameTheSamePayload) {
#ifdef _WIN32
  GTEST_SKIP() << "local socket test is POSIX-only";
#else
  // A header is four bytes of a byte stream, not an atomic unit: a slow or
  // heavily fragmenting peer can deliver each byte in its own recv() return,
  // including the header of a continuation packet. Framing must not depend on
  // where the stream happens to be broken.
  const std::vector<uint8_t> chained = PatternBytes(kMaxPacketPayload, 0x33);
  const std::vector<uint8_t> remainder = PatternBytes(5, 0x55);
  PacketPeer peer([&](int fd) {
    if (!SendDribbledHeader(fd, 1, kMaxPacketPayload) ||
        !SendAll(fd, chained.data(), chained.size()) ||
        !SendDribbledHeader(fd, 2, remainder.size()) ||
        !SendAll(fd, remainder.data(), remainder.size())) {
      return;
    }
    WaitForReaderClose(fd);
  });

  SocketHandle socket;
  ASSERT_EQ(socket.Connect("127.0.0.1", peer.port(), 1), MES_OK);
  ASSERT_EQ(socket.SetReadTimeout(2), MES_OK);

  std::vector<uint8_t> payload;
  uint8_t sequence = 0;
  ASSERT_EQ(ReadPacket(&socket, &payload, &sequence), MES_OK);
  ASSERT_EQ(payload.size(), kMaxPacketPayload + remainder.size());
  EXPECT_TRUE(std::equal(chained.begin(), chained.end(), payload.begin()));
  EXPECT_TRUE(std::equal(remainder.begin(), remainder.end(), payload.begin() + kMaxPacketPayload));
  EXPECT_EQ(sequence, 2u);
#endif
}

TEST(ReadPacketTest, TheMaximumPayloadSizeBoundsTheWholeChainNotEachPacket) {
#ifdef _WIN32
  GTEST_SKIP() << "local socket test is POSIX-only";
#else
  // Every packet of a chain is within the per-packet maximum by construction, so
  // a cap tested against the individual packet would never reject anything. The
  // continuation here is small, yet it takes the reassembled payload past the
  // caller's limit and must be refused rather than appended.
  constexpr size_t kLimit = kMaxPacketPayload + 5;
  const std::vector<uint8_t> chained = PatternBytes(kMaxPacketPayload, 0x11);
  const std::vector<uint8_t> remainder = PatternBytes(10, 0xA0);
  PacketPeer peer([&](int fd) {
    if (!SendWirePacket(fd, 1, chained) || !SendWirePacket(fd, 2, remainder)) return;
    WaitForReaderClose(fd);
  });

  SocketHandle socket;
  ASSERT_EQ(socket.Connect("127.0.0.1", peer.port(), 1), MES_OK);
  ASSERT_EQ(socket.SetReadTimeout(2), MES_OK);

  std::vector<uint8_t> payload;
  uint8_t sequence = 0;
  EXPECT_EQ(ReadPacket(&socket, &payload, &sequence, kLimit), MES_ERR_STREAM);
  EXPECT_LE(payload.size(), kLimit);
#endif
}

// --- ReadExact read-ahead handling ---

TEST(ReadExactTest, AReadLargerThanTheReadAheadBufferDeliversEveryByteInOrder) {
#ifdef _WIN32
  GTEST_SKIP() << "local socket test is POSIX-only";
#else
  // A payload wider than the staging buffer is received in several pieces
  // whichever way it reaches the caller's buffer, so the pieces have to be
  // placed end to end: a read that returns the right byte count in the wrong
  // order desynchronises every later read.
  const std::vector<uint8_t> region = PatternBytes(kBeyondReadAhead, 0x2B);
  PacketPeer peer([&](int fd) {
    if (!SendAll(fd, region.data(), region.size())) return;
    WaitForReaderClose(fd);
  });

  SocketHandle socket;
  ASSERT_EQ(socket.Connect("127.0.0.1", peer.port(), 1), MES_OK);
  ASSERT_EQ(socket.SetReadTimeout(2), MES_OK);

  std::vector<uint8_t> received(region.size(), 0);
  ASSERT_EQ(socket.ReadExact(received.data(), received.size()), MES_OK);
  EXPECT_EQ(received, region);
#endif
}

TEST(ReadExactTest, ALargeReadConsumesTheAlreadyBufferedBytesBeforeTheSocket) {
#ifdef _WIN32
  GTEST_SKIP() << "local socket test is POSIX-only";
#else
  // A short read leaves the rest of what arrived with it staged in the
  // read-ahead buffer. Those bytes precede everything still on the wire, so a
  // following large read owes them first, in their original order, however it
  // receives the remainder.
  const std::vector<uint8_t> staged = PatternBytes(16, 0x40);
  const std::vector<uint8_t> region = PatternBytes(kBeyondReadAhead, 0x91);
  PacketPeer peer([&](int fd) {
    if (!SendAll(fd, staged.data(), staged.size()) || !WaitForReaderRequest(fd)) return;
    if (!SendAll(fd, region.data(), region.size())) return;
    WaitForReaderClose(fd);
  });

  SocketHandle socket;
  ASSERT_EQ(socket.Connect("127.0.0.1", peer.port(), 1), MES_OK);
  ASSERT_EQ(socket.SetReadTimeout(2), MES_OK);

  uint8_t first = 0;
  ASSERT_EQ(socket.ReadExact(&first, 1), MES_OK);
  EXPECT_EQ(first, staged.front());
  ASSERT_TRUE(RequestNextRegion(&socket));

  std::vector<uint8_t> received(staged.size() - 1 + region.size(), 0);
  ASSERT_EQ(socket.ReadExact(received.data(), received.size()), MES_OK);
  EXPECT_TRUE(std::equal(staged.begin() + 1, staged.end(), received.begin()));
  EXPECT_TRUE(std::equal(region.begin(), region.end(), received.begin() + (staged.size() - 1)));
#endif
}

TEST(ReadExactTest, OneReadCrossingTheReadAheadSizeInBothDirectionsStaysInOrder) {
#ifdef _WIN32
  GTEST_SKIP() << "local socket test is POSIX-only";
#else
  // Within a single read the remainder shrinks past the staging buffer's size:
  // it starts staged, then exceeds the buffer, then falls back under it. The
  // decision belongs to each iteration, so the tail region is as much a part of
  // the read as the wide one before it.
  const std::vector<uint8_t> staged = PatternBytes(16, 0x0C);
  const std::vector<uint8_t> wide = PatternBytes(kBeyondReadAhead, 0x63);
  const std::vector<uint8_t> tail = PatternBytes(1000, 0xD7);
  PacketPeer peer([&](int fd) {
    if (!SendAll(fd, staged.data(), staged.size()) || !WaitForReaderRequest(fd)) return;
    if (!SendAll(fd, wide.data(), wide.size())) return;
    // The reader is blocked on the wide region by the time the tail is sent, so
    // the tail cannot be swallowed by the same recv() that took the last of it.
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    if (!SendAll(fd, tail.data(), tail.size())) return;
    WaitForReaderClose(fd);
  });

  SocketHandle socket;
  ASSERT_EQ(socket.Connect("127.0.0.1", peer.port(), 1), MES_OK);
  ASSERT_EQ(socket.SetReadTimeout(2), MES_OK);

  uint8_t first = 0;
  ASSERT_EQ(socket.ReadExact(&first, 1), MES_OK);
  EXPECT_EQ(first, staged.front());
  ASSERT_TRUE(RequestNextRegion(&socket));

  std::vector<uint8_t> expected(staged.begin() + 1, staged.end());
  expected.insert(expected.end(), wide.begin(), wide.end());
  expected.insert(expected.end(), tail.begin(), tail.end());

  std::vector<uint8_t> received(expected.size(), 0);
  ASSERT_EQ(socket.ReadExact(received.data(), received.size()), MES_OK);
  EXPECT_EQ(received, expected);
#endif
}

// --- ReadExact staging decision scope ---

/**
 * @brief The choice between staging a read and receiving it directly is per iteration.
 *
 * A read wider than the staging buffer belongs in the caller's buffer, and the
 * remainder that decides it shrinks as the read progresses: a decision taken once
 * on entry is wrong for every later iteration, and one taken on entry only is
 * wrong for a read that starts by draining staged bytes. Neither mistake is
 * observable from a test that drives the socket, because both routes deliver the
 * same byte stream in the same order and SocketHandle exposes neither its
 * descriptor nor its staging state — observing the destination of a recv() would
 * mean widening that surface for the benefit of a test. Where the decision is
 * written is observable, so that is what this asserts; the byte stream itself is
 * pinned by ReadExactTest.
 */
TEST(ReadExactStagingDecisionTest, IsTakenInsideTheReadLoopRatherThanOnEntry) {
  const std::filesystem::path source =
      source_scan::RepoRoot() / "core" / "src" / "protocol" / "mysql_socket.cpp";
  const std::string text = source_scan::ReadCollapsed(source);
  ASSERT_FALSE(text.empty()) << "cannot read " << source;

  const std::string decision = std::string("const bool read_") + "direct = ";
  const std::string read_loop = std::string("while (total") + " < len) {";

  // A renamed or restructured decision leaves nothing to compare positions
  // against, which is a gap in the pin rather than a property that holds.
  ASSERT_EQ(source_scan::CountOccurrences(text, decision), 1)
      << decision << " is not written exactly once in " << source;
  ASSERT_GT(source_scan::CountOccurrences(text, read_loop), 0)
      << read_loop << " not found in " << source;

  const size_t decision_at = text.find(decision);
  ASSERT_NE(decision_at, std::string::npos) << decision << " not found in " << source;

  // A decision taken on entry has no read loop open ahead of it, so a search
  // bounded by the decision finds a loop only while the decision sits inside one.
  EXPECT_NE(text.rfind(read_loop, decision_at), std::string::npos)
      << "the staging decision is taken before the read loop is entered";
}

// --- TLS read SIGPIPE guard scope ---

/**
 * @brief SIGPIPE stays blocked for a whole TLS read, not one SSL_read at a time.
 *
 * SSL_read() can write to the socket, so a peer that has gone away turns a read
 * into a SIGPIPE. The suppressor blocks the signal for its scope and drains only
 * what was raised inside it, which is a correct guard however narrow its scope
 * is: a guard rebuilt per iteration leaves SIGPIPE momentarily deliverable
 * between two reads, and nothing a caller can observe distinguishes that window
 * from the call being covered end to end. Only where the guard is declared does,
 * which is what this asserts. The mask and pending-signal behaviour itself is a
 * property of the suppressor, and it applies to the platforms where the signal
 * exists, so a test driving a read cannot observe it on the others at all.
 */
TEST(TlsReadSigPipeGuardTest, IsEnteredOncePerCallRatherThanPerRead) {
  const std::filesystem::path source =
      source_scan::RepoRoot() / "core" / "src" / "protocol" / "mysql_socket.cpp";
  const std::string text = source_scan::ReadCollapsed(source);
  ASSERT_FALSE(text.empty()) << "cannot read " << source;

  const std::string guard = std::string("ScopedSigPipeSuppressor ") + "sigpipe_guard;";
  const std::string read_call = std::string("SSL_") + "read(ssl_,";
  const std::string read_loop = std::string("while (total") + " < len) {";

  const size_t read_at = text.find(read_call);
  ASSERT_NE(read_at, std::string::npos) << read_call << " not found in " << source;
  // The loop and the guard that cover the read are the last of each to open
  // before it.
  const size_t loop_at = text.rfind(read_loop, read_at);
  ASSERT_NE(loop_at, std::string::npos) << read_loop << " not found before " << read_call;
  const size_t guard_at = text.rfind(guard, read_at);
  ASSERT_NE(guard_at, std::string::npos) << guard << " not found before " << read_call;

  EXPECT_LT(guard_at, loop_at) << "the TLS read loop enters the SIGPIPE guard per iteration";
}

}  // namespace
}  // namespace mes::protocol
