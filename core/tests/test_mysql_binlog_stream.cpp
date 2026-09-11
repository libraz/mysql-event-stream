// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#include <gtest/gtest.h>

#include <filesystem>
#include <string>
#include <thread>
#include <vector>

#ifndef _WIN32
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

#include "protocol/mysql_binlog_stream.h"
#include "protocol/mysql_packet.h"
#include "protocol/mysql_socket.h"
#include "source_scan.h"

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

/**
 * @brief The binlog magic offset has one definition, and every structure
 *        defaulting a start offset reads it rather than restating the value.
 *
 * A second constant holding the same offset changes no behaviour until one of
 * the two is edited, so no test that drives the stream can observe it: the
 * default and the lowest accepted offset would simply stop agreeing, and a
 * dump would then be requested from inside the file's magic number. Scanning
 * the sources is what proves the duplicate is absent.
 *
 * The value itself is pinned by BinlogStreamConfigTest.DefaultValues and by
 * the client's own range check, so this test deliberately asserts uniqueness
 * only. Patterns are assembled from fragments so that this file does not
 * become a hit in the search it exists to keep empty.
 */
TEST(BinlogMagicOffsetTest, IsDefinedOnceAndReadByEveryStartOffset) {
  const std::string constant_name = std::string("kBinlogMagic") + "Offset";
  const std::string definition_keyword = std::string("const") + "expr";
  const std::string offset_field = std::string("binlog_") + "position";
  const std::string inline_value = std::string("= ") + "4";

  const std::filesystem::path root = source_scan::RepoRoot();
  const std::filesystem::path definition_file = root / "core" / "src" / "event_header.h";
  // Every structure that carries a start offset, plus the one range check that
  // refuses an offset below it.
  const std::filesystem::path readers[] = {
      root / "core" / "src" / "protocol" / "mysql_binlog_stream.h",
      root / "core" / "src" / "client" / "binlog_client.h",
      root / "core" / "src" / "client" / "binlog_client.cpp",
  };

  const std::filesystem::path scanned_root = root / "core" / "src";
  const std::vector<std::filesystem::path> files = source_scan::FilesUnder(scanned_root);
  ASSERT_FALSE(files.empty()) << "no files found under " << scanned_root;

  int definitions = 0;
  std::vector<std::string> restated;
  for (const std::filesystem::path& file : files) {
    const int definition_hits =
        source_scan::CountLinesContainingBoth(file, definition_keyword, constant_name);
    const int restated_hits =
        source_scan::CountLinesContainingBoth(file, offset_field, inline_value);
    ASSERT_GE(definition_hits, 0) << "cannot read " << file;
    ASSERT_GE(restated_hits, 0) << "cannot read " << file;
    definitions += definition_hits;
    if (restated_hits > 0) {
      restated.push_back(file.string());
    }
  }

  // The scan reaches the file that is supposed to hold the definition, so a
  // zero count above would mean the shape is gone rather than unfound.
  ASSERT_GT(source_scan::CountLinesContaining(definition_file, constant_name), 0)
      << constant_name << " not found in " << definition_file;

  EXPECT_EQ(definitions, 1) << "the binlog magic offset is defined more than once";
  EXPECT_EQ(restated, std::vector<std::string>{})
      << "a start offset is defaulted to the magic offset's value instead of the constant";

  for (const std::filesystem::path& reader : readers) {
    EXPECT_GT(source_scan::CountLinesContaining(reader, constant_name), 0)
        << reader << " does not read " << constant_name;
  }
}

TEST(BinlogEventPacketTest, DefaultValues) {
  BinlogEventPacket packet;
  EXPECT_EQ(packet.data, nullptr);
  EXPECT_EQ(packet.size, 0u);
  EXPECT_EQ(packet.data_offset, 0u);
  EXPECT_FALSE(packet.is_heartbeat);
  EXPECT_EQ(packet.server_error_code, 0u);
  EXPECT_TRUE(packet.error_message.empty());
}

TEST(BinlogStreamPacketTest, ServerPurgeErrorPreservesCodeAndMessage) {
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

  const std::string server_message = "The requested GTID has been purged";
  std::thread server([&] {
    const int peer = accept(listener, nullptr, nullptr);
    if (peer < 0) return;
    std::vector<uint8_t> payload = {0xFF, 0xD4, 0x04, '#', 'H', 'Y', '0', '0', '0'};
    payload.insert(payload.end(), server_message.begin(), server_message.end());
    const uint32_t payload_size = static_cast<uint32_t>(payload.size());
    std::vector<uint8_t> packet = {static_cast<uint8_t>(payload_size),
                                   static_cast<uint8_t>(payload_size >> 8),
                                   static_cast<uint8_t>(payload_size >> 16), 1};
    packet.insert(packet.end(), payload.begin(), payload.end());
    send(peer, packet.data(), packet.size(), 0);
    close(peer);
  });

  SocketHandle socket;
  ASSERT_EQ(socket.Connect("127.0.0.1", ntohs(address.sin_port), 1), MES_OK);
  BinlogStream stream;
  std::vector<uint8_t> buffer;
  BinlogEventPacket result;
  EXPECT_EQ(stream.FetchEvent(&socket, &buffer, &result, 1024), MES_ERR_GTID_PURGED);
  EXPECT_EQ(result.server_error_code, 1236u);
  EXPECT_EQ(result.error_message, "MySQL server error 1236: " + server_message);
  server.join();
  close(listener);
#endif
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

TEST(BinlogStreamPacketTest, PositionStartSendsRequestedFileAndOffset) {
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

  std::vector<uint8_t> received;
  std::thread server([&] {
    const int peer = accept(listener, nullptr, nullptr);
    if (peer < 0) return;
    uint8_t header[4]{};
    if (recv(peer, header, sizeof(header), MSG_WAITALL) != static_cast<ssize_t>(sizeof(header))) {
      close(peer);
      return;
    }
    const size_t payload_size = ReadFixedInt(header, 3);
    received.resize(payload_size);
    recv(peer, received.data(), received.size(), MSG_WAITALL);
    close(peer);
  });

  SocketHandle socket;
  ASSERT_EQ(socket.Connect("127.0.0.1", ntohs(address.sin_port), 1), MES_OK);
  BinlogStreamConfig config;
  config.server_id = 42;
  config.binlog_filename = "binlog.000123";
  config.binlog_position = 9876;
  BinlogStream stream;
  EXPECT_EQ(stream.StartComBinlogDump(&socket, config), MES_OK);
  server.join();
  close(listener);

  ASSERT_EQ(received.size(), 1u + 4u + 2u + 4u + config.binlog_filename.size());
  EXPECT_EQ(received[0], 0x12u);
  EXPECT_EQ(ReadFixedInt(received.data() + 1, 4), config.binlog_position);
  EXPECT_EQ(ReadFixedInt(received.data() + 5, 2), 0u);
  EXPECT_EQ(ReadFixedInt(received.data() + 7, 4), config.server_id);
  EXPECT_EQ(std::string(reinterpret_cast<const char*>(received.data() + 11),
                        config.binlog_filename.size()),
            config.binlog_filename);
#endif
}

TEST(BinlogStreamPacketTest, AnnotateRowsFlagMatchesTheMariaDBWireValue) {
  EXPECT_EQ(kBinlogSendAnnotateRows, 0x0002u);
}

TEST(BinlogStreamPacketTest, DumpRequestCarriesRequestedFlags) {
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

  std::vector<uint8_t> received;
  std::thread server([&] {
    const int peer = accept(listener, nullptr, nullptr);
    if (peer < 0) return;
    uint8_t header[4]{};
    if (recv(peer, header, sizeof(header), MSG_WAITALL) != static_cast<ssize_t>(sizeof(header))) {
      close(peer);
      return;
    }
    const size_t payload_size = ReadFixedInt(header, 3);
    received.resize(payload_size);
    recv(peer, received.data(), received.size(), MSG_WAITALL);
    close(peer);
  });

  SocketHandle socket;
  ASSERT_EQ(socket.Connect("127.0.0.1", ntohs(address.sin_port), 1), MES_OK);
  BinlogStreamConfig config;
  config.server_id = 42;
  config.binlog_position = 4;
  config.flags = kBinlogSendAnnotateRows;
  BinlogStream stream;
  EXPECT_EQ(stream.StartComBinlogDump(&socket, config), MES_OK);
  server.join();
  close(listener);

  ASSERT_GE(received.size(), 11u);
  EXPECT_EQ(ReadFixedInt(received.data() + 5, 2), kBinlogSendAnnotateRows);
#endif
}

}  // namespace
}  // namespace mes::protocol
