// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#include <gtest/gtest.h>

#include <cstdint>
#include <cstring>
#include <vector>

#include "protocol/mysql_packet.h"

namespace mes::protocol {
namespace {

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

}  // namespace
}  // namespace mes::protocol
