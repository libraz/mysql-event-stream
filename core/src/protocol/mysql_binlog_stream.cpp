// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#include "protocol/mysql_binlog_stream.h"

#include <cstdint>
#include <string>
#include <vector>

#include "logger.h"
#include "protocol/mysql_packet.h"
#include "protocol/mysql_socket.h"

namespace mes::protocol {

/// COM_BINLOG_DUMP command byte
constexpr uint8_t kComBinlogDump = 0x12;

namespace {

/// HEARTBEAT_LOG_EVENT type ID in the binlog event header
constexpr uint8_t kHeartbeatLogEvent = 27;

/// COM_BINLOG_DUMP_GTID command byte
constexpr uint8_t kComBinlogDumpGtid = 0x1E;

/// Flag to indicate GTID-based positioning
constexpr uint16_t kBinlogThroughGtid = 0x04;

/// ER_SOURCE_FATAL_ERROR_READING_BINLOG: the server gave up on the dump.
/// The code is shared between a purged GTID interval and several recoverable
/// file/position conditions, so its meaning depends on how the dump started.
constexpr uint16_t kErrFatalErrorReadingBinlog = 1236;

}  // namespace

std::vector<uint8_t> BuildComBinlogDumpGtidPayload(const BinlogStreamConfig& config) {
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
  payload.insert(payload.end(), config.binlog_filename.begin(), config.binlog_filename.end());
  WriteFixedInt(&payload, config.binlog_position, 8);
  WriteFixedInt(&payload, config.gtid_encoded.size(), 4);
  payload.insert(payload.end(), config.gtid_encoded.begin(), config.gtid_encoded.end());

  return payload;
}

mes_error_t BinlogStream::Start(SocketHandle* sock, const BinlogStreamConfig& config) {
  position_from_gtid_ = config.position_from_gtid;
  std::vector<uint8_t> payload = BuildComBinlogDumpGtidPayload(config);

  // Send as a single command packet with sequence_id = 0
  PacketBuffer pkt_buf;
  uint8_t seq_id = 0;
  pkt_buf.WritePacket(payload.data(), payload.size(), &seq_id);

  return sock->WriteAll(pkt_buf.Data(), pkt_buf.Size());
}

mes_error_t BinlogStream::FetchEvent(SocketHandle* sock, std::vector<uint8_t>* buffer,
                                     BinlogEventPacket* result, uint32_t max_event_size) {
  // Initialize output so callers can rely on consistent defaults on all paths.
  result->data = nullptr;
  result->size = 0;
  result->data_offset = 0;
  result->is_heartbeat = false;
  result->server_error_code = 0;
  result->error_message.clear();

  uint8_t seq_id = 0;
  // The replication packet contains a one-byte OK marker before the event.
  // Include it in the packet cap so an event exactly at max_event_size is
  // accepted while max_event_size + 1 event bytes are rejected.
  const size_t max_packet_payload = BinlogPacketPayloadLimit(max_event_size);
  mes_error_t rc = ReadPacket(sock, buffer, &seq_id, max_packet_payload);
  if (rc != MES_OK) {
    result->error_message = "Failed to read binlog stream packet";
    return rc;
  }

  // Empty packet is treated as a heartbeat
  if (buffer->empty()) {
    result->is_heartbeat = true;
    return MES_OK;
  }

  uint8_t status_byte = (*buffer)[0];

  // ERR packet
  if (status_byte == 0xFF) {
    uint16_t err_code = 0;
    std::string msg;
    ParseErrPacketPayload(buffer->data(), buffer->size(), &err_code, &msg);
    result->server_error_code = err_code;
    result->error_message = "MySQL server error " + std::to_string(err_code);
    if (!msg.empty()) result->error_message += ": " + msg;
    StructuredLog()
        .Event("binlog_stream_server_error")
        .Field("error_code", static_cast<uint64_t>(err_code))
        .Field("message", msg)
        .Error();
    // On a GTID dump this code means the requested interval has been purged,
    // which reconnecting cannot recover. The same code on a file/position dump
    // reports a stale offset or a missing log file instead -- recoverable by
    // restarting from a valid offset -- so only the GTID case may claim the
    // unrecoverable classification.
    if (err_code == kErrFatalErrorReadingBinlog && position_from_gtid_) {
      return MES_ERR_GTID_PURGED;
    }
    return MES_ERR_STREAM;
  }

  // EOF packet - stream ended
  if (status_byte == 0xFE) {
    result->error_message = "Binlog stream ended (EOF packet)";
    StructuredLog().Event("binlog_stream_eof").Warn();
    return MES_ERR_DISCONNECTED;
  }

  // OK packet - binlog event follows after the status byte
  if (status_byte == 0x00) {
    // Just the OK byte with no event data: heartbeat
    if (buffer->size() <= 1) {
      result->is_heartbeat = true;
      return MES_OK;
    }

    // Event bytes start at offset 1 (past the OK byte).
    result->data = buffer->data() + 1;
    result->size = buffer->size() - 1;
    result->data_offset = 1;

    // Check event type at offset 4 within the binlog event header.
    // The binlog event header starts right after the OK byte (index 1).
    // Event type is at byte 4 of the event header, so index 1+4=5.
    if (buffer->size() > 5 && (*buffer)[5] == kHeartbeatLogEvent) {
      result->is_heartbeat = true;
    }
    return MES_OK;
  }

  // Unexpected status byte - treat as error
  result->error_message = "Unexpected binlog stream packet status " +
                          std::to_string(static_cast<unsigned int>(status_byte));
  return MES_ERR_STREAM;
}

mes_error_t BinlogStream::StartComBinlogDump(SocketHandle* sock, const BinlogStreamConfig& config) {
  // MariaDB reaches its GTID dump through this command too, so the caller's
  // flag is the only thing that distinguishes the two start modes here.
  position_from_gtid_ = config.position_from_gtid;

  // Build COM_BINLOG_DUMP payload:
  //   [1] command byte (0x12)
  //   [4] binlog position (LE)
  //   [2] flags (LE)
  //   [4] server_id (LE)
  //   [N] binlog filename
  std::vector<uint8_t> payload;

  payload.push_back(kComBinlogDump);
  // COM_BINLOG_DUMP uses a 4-byte position field. Reject positions
  // that would be silently truncated.
  if (config.binlog_position > 0xFFFFFFFFULL) {
    return MES_ERR_INVALID_ARG;
  }
  WriteFixedInt(&payload, config.binlog_position, 4);
  WriteFixedInt(&payload, config.flags, 2);
  WriteFixedInt(&payload, config.server_id, 4);
  payload.insert(payload.end(), config.binlog_filename.begin(), config.binlog_filename.end());

  PacketBuffer pkt_buf;
  uint8_t seq_id = 0;
  pkt_buf.WritePacket(payload.data(), payload.size(), &seq_id);

  return sock->WriteAll(pkt_buf.Data(), pkt_buf.Size());
}

}  // namespace mes::protocol
