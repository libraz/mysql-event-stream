// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#include <gtest/gtest.h>

#include "protocol/mysql_binlog_stream.h"

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

}  // namespace
}  // namespace mes::protocol
