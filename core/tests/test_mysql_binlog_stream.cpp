// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#include <gtest/gtest.h>

#include "protocol/mysql_binlog_stream.h"
#include "protocol/mysql_packet.h"

namespace mes::protocol {
namespace {

TEST(BinlogStreamConfigTest, DefaultValues) {
  BinlogStreamConfig config;
  EXPECT_EQ(config.server_id, 1u);
  EXPECT_TRUE(config.binlog_filename.empty());
  EXPECT_EQ(config.binlog_position, 4u);
  EXPECT_TRUE(config.gtid_encoded.empty());
  EXPECT_EQ(config.flags, 0u);
}

TEST(BinlogEventPacketTest, DefaultValues) {
  BinlogEventPacket packet;
  EXPECT_EQ(packet.data, nullptr);
  EXPECT_EQ(packet.size, 0u);
  EXPECT_EQ(packet.data_offset, 0u);
  EXPECT_FALSE(packet.is_heartbeat);
}

TEST(BinlogStreamPacketLimitTest, EventAt64MiBBoundaryIncludesOkPrefix) {
  constexpr size_t kEventLimit = 64u * 1024u * 1024u;
  constexpr size_t kPacketLimit = BinlogPacketPayloadLimit(kEventLimit);

  EXPECT_EQ(kPacketLimit, kEventLimit + 1U);
  EXPECT_TRUE(PacketPayloadAppendFits(0, kPacketLimit, kPacketLimit));
  EXPECT_FALSE(PacketPayloadAppendFits(0, kPacketLimit + 1U, kPacketLimit));
}

TEST(BinlogStreamPacketLimitTest, MultiPacketAppendCheckIsOverflowSafe) {
  constexpr size_t kEventLimit = 64u * 1024u * 1024u;
  constexpr size_t kPacketLimit = BinlogPacketPayloadLimit(kEventLimit);
  constexpr size_t kMysqlChunk = 0xFFFFFFU;

  EXPECT_TRUE(PacketPayloadAppendFits(kMysqlChunk * 4U, 5U, kPacketLimit));
  EXPECT_FALSE(PacketPayloadAppendFits(kMysqlChunk * 4U, 6U, kPacketLimit));
  EXPECT_FALSE(PacketPayloadAppendFits(kPacketLimit + 1U, 0U, kPacketLimit));
  EXPECT_FALSE(PacketPayloadAppendFits(SIZE_MAX, 1U, kPacketLimit));
}

TEST(BinlogStreamPacketTest, EncodedEmptySetUsesThroughGtidAndEightByteSet) {
  BinlogStreamConfig config;
  config.server_id = 42;
  config.gtid_encoded.assign(8, 0);

  auto payload = BuildComBinlogDumpGtidPayload(config);

  ASSERT_EQ(payload.size(), 1u + 2u + 4u + 4u + 8u + 4u + 8u);
  EXPECT_EQ(payload[0], 0x1Eu);
  EXPECT_EQ(ReadFixedInt(payload.data() + 1, 2), 0x04u);
  EXPECT_EQ(ReadFixedInt(payload.data() + 3, 4), 42u);
  EXPECT_EQ(ReadFixedInt(payload.data() + 7, 4), 0u);
  EXPECT_EQ(ReadFixedInt(payload.data() + 11, 8), 4u);
  EXPECT_EQ(ReadFixedInt(payload.data() + 19, 4), 8u);
  EXPECT_EQ(ReadFixedInt(payload.data() + 23, 8), 0u);
}

TEST(BinlogStreamPacketTest, EncodedCurrentSetIsIncludedExactly) {
  BinlogStreamConfig config;
  config.server_id = 7;
  config.binlog_filename = "binlog.000123";
  config.binlog_position = 99;
  config.gtid_encoded = {1, 2, 3, 4};

  auto payload = BuildComBinlogDumpGtidPayload(config);
  const size_t filename_offset = 11;
  const size_t position_offset = filename_offset + config.binlog_filename.size();
  const size_t gtid_length_offset = position_offset + 8;
  const size_t gtid_offset = gtid_length_offset + 4;

  EXPECT_EQ(ReadFixedInt(payload.data() + 1, 2), 0x04u);
  EXPECT_EQ(ReadFixedInt(payload.data() + 7, 4), config.binlog_filename.size());
  EXPECT_EQ(std::string(reinterpret_cast<const char*>(payload.data() + filename_offset),
                        config.binlog_filename.size()),
            config.binlog_filename);
  EXPECT_EQ(ReadFixedInt(payload.data() + position_offset, 8), 99u);
  EXPECT_EQ(ReadFixedInt(payload.data() + gtid_length_offset, 4), 4u);
  EXPECT_EQ(std::vector<uint8_t>(payload.begin() + gtid_offset, payload.end()),
            config.gtid_encoded);
}

TEST(BinlogStreamPacketTest, EmptyVectorDoesNotReusePriorEncodedState) {
  BinlogStreamConfig reused;
  reused.gtid_encoded = {9, 8, 7};
  auto first = BuildComBinlogDumpGtidPayload(reused);
  EXPECT_EQ(ReadFixedInt(first.data() + 19, 4), 3u);

  reused.gtid_encoded.clear();
  auto second = BuildComBinlogDumpGtidPayload(reused);
  EXPECT_EQ(ReadFixedInt(second.data() + 1, 2), 0u);
  EXPECT_EQ(ReadFixedInt(second.data() + 19, 4), 0u);
  EXPECT_EQ(second.size(), 23u);
}

}  // namespace
}  // namespace mes::protocol
