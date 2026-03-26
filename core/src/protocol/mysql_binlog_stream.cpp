// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#include "protocol/mysql_binlog_stream.h"

#include <cstdint>
#include <string>
#include <vector>

#include "protocol/mysql_packet.h"
#include "protocol/mysql_socket.h"

namespace mes::protocol {

namespace {

/// HEARTBEAT_LOG_EVENT type ID in the binlog event header
constexpr uint8_t kHeartbeatLogEvent = 27;

/// COM_BINLOG_DUMP_GTID command byte
constexpr uint8_t kComBinlogDumpGtid = 0x1E;

/// Flag to indicate GTID-based positioning
constexpr uint16_t kBinlogThroughGtid = 0x04;

}  // namespace

mes_error_t BinlogStream::Start(SocketHandle* sock,
                                const BinlogStreamConfig& config) {
  // Build COM_BINLOG_DUMP_GTID payload:
  //   [1] command byte
  //   [2] flags (LE)
  //   [4] server_id (LE)
  //   [4] filename length (LE)
  //   [N] filename bytes
  //   [8] position (LE)
  //   [4] GTID data length (LE)
  //   [M] GTID data bytes
  std::vector<uint8_t> payload;

  uint16_t flags = config.flags;
  if (!config.gtid_encoded.empty()) {
    flags |= kBinlogThroughGtid;
  }

  payload.push_back(kComBinlogDumpGtid);
  WriteFixedInt(&payload, flags, 2);
  WriteFixedInt(&payload, config.server_id, 4);
  WriteFixedInt(&payload, config.binlog_filename.size(), 4);
  payload.insert(payload.end(), config.binlog_filename.begin(),
                 config.binlog_filename.end());
  WriteFixedInt(&payload, config.binlog_position, 8);
  WriteFixedInt(&payload, config.gtid_encoded.size(), 4);
  payload.insert(payload.end(), config.gtid_encoded.begin(),
                 config.gtid_encoded.end());

  // Send as a single command packet with sequence_id = 0
  PacketBuffer pkt_buf;
  uint8_t seq_id = 0;
  pkt_buf.WritePacket(payload.data(), payload.size(), &seq_id);

  return sock->WriteAll(pkt_buf.Data(), pkt_buf.Size());
}

mes_error_t BinlogStream::FetchEvent(SocketHandle* sock,
                                     BinlogEventPacket* result) {
  uint8_t seq_id = 0;
  mes_error_t rc = ReadPacket(sock, &packet_buf_, &seq_id);
  if (rc != MES_OK) {
    return rc;
  }

  // Empty packet is treated as a heartbeat
  if (packet_buf_.empty()) {
    result->data = nullptr;
    result->size = 0;
    result->is_heartbeat = true;
    return MES_OK;
  }

  uint8_t status_byte = packet_buf_[0];

  // ERR packet
  if (status_byte == 0xFF) {
    return MES_ERR_STREAM;
  }

  // EOF packet - stream ended
  if (status_byte == 0xFE) {
    return MES_ERR_STREAM;
  }

  // OK packet - binlog event follows after the status byte
  if (status_byte == 0x00) {
    // Just the OK byte with no event data: heartbeat
    if (packet_buf_.size() <= 1) {
      result->data = nullptr;
      result->size = 0;
      result->is_heartbeat = true;
      return MES_OK;
    }

    // Check event type at offset 4 within the binlog event header.
    // The binlog event header starts right after the OK byte (index 1).
    // Event type is at byte 4 of the event header, so index 1+4=5.
    if (packet_buf_.size() > 5 &&
        packet_buf_[5] == kHeartbeatLogEvent) {
      result->data = packet_buf_.data() + 1;
      result->size = packet_buf_.size() - 1;
      result->is_heartbeat = true;
      return MES_OK;
    }

    // Real binlog event
    result->data = packet_buf_.data() + 1;
    result->size = packet_buf_.size() - 1;
    result->is_heartbeat = false;
    return MES_OK;
  }

  // Unexpected status byte - treat as error
  return MES_ERR_STREAM;
}

}  // namespace mes::protocol
