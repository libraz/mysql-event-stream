// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * @file mysql_binlog_stream.h
 * @brief MySQL COM_BINLOG_DUMP_GTID command and binlog event streaming
 *
 * Provides a blocking interface to start a binlog replication stream via
 * COM_BINLOG_DUMP_GTID and read individual binlog event packets.
 */

#ifndef MES_PROTOCOL_MYSQL_BINLOG_STREAM_H_
#define MES_PROTOCOL_MYSQL_BINLOG_STREAM_H_

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "event_header.h"
#include "mes.h"

namespace mes::protocol {

// Forward declaration; defined in protocol/mysql_socket.h
class SocketHandle;

/**
 * @brief MariaDB dump flag requesting ANNOTATE_ROWS events.
 *
 * MariaDB withholds ANNOTATE_ROWS unless the dump request carries this flag,
 * regardless of the negotiated @\@mariadb_slave_capability.
 */
constexpr uint16_t kBinlogSendAnnotateRows = 0x0002;

/** @brief Configuration for starting a binlog replication stream */
struct BinlogStreamConfig {
  uint32_t server_id = 1;
  std::string binlog_filename;  ///< Empty string means no specific file
  uint64_t binlog_position = kBinlogMagicOffset;
  std::vector<uint8_t> gtid_encoded;  ///< Binary-encoded GTID set
  uint16_t flags = 0;
  /**
   * @brief Whether the requested start position came from a GTID set.
   *
   * Set this on every GTID-based dump, including MariaDB's: MariaDB negotiates
   * the position through @\@slave_connect_state and then issues a plain
   * COM_BINLOG_DUMP, so neither the command byte nor the dump flags can express
   * it. FetchEvent() needs it to tell a purged-GTID error apart from the other
   * conditions the server reports under the same error code; leaving it false
   * costs only the more specific diagnosis.
   */
  bool position_from_gtid = false;
};

/** @brief Build the COM_BINLOG_DUMP_GTID command payload for @p config. */
std::vector<uint8_t> BuildComBinlogDumpGtidPayload(const BinlogStreamConfig& config);

/**
 * @brief A single binlog event received from the replication stream
 *
 * The data pointer is valid only as long as the caller-provided buffer
 * passed to FetchEvent() is not modified. It points past the OK byte
 * into the raw binlog event header + body. `data_offset` is the offset
 * of `data` from the start of that buffer, enabling callers to move
 * the buffer into downstream owners without copying; the downstream
 * owner can recover the event bytes via `buffer.data() + data_offset`.
 */
struct BinlogEventPacket {
  const uint8_t* data = nullptr;   ///< Event data (after OK byte)
  size_t size = 0;                 ///< Size of event data in bytes
  size_t data_offset = 0;          ///< Offset of `data` within the caller's buffer
  bool is_heartbeat = false;       ///< True if this is a heartbeat event
  uint16_t server_error_code = 0;  ///< MySQL ERR packet code, if one was received
  std::string error_message;       ///< Detailed stream failure description
};

/** Packet payload cap for an event, including the replication OK prefix. */
constexpr size_t BinlogPacketPayloadLimit(uint32_t max_event_size) {
  return static_cast<size_t>(max_event_size) + 1U;
}

/**
 * @brief Binlog replication stream reader
 *
 * Sends COM_BINLOG_DUMP_GTID to initiate streaming, then provides
 * blocking reads of individual binlog events. FetchEvent() reads into
 * a caller-provided buffer so the caller can either reuse the buffer
 * across calls or move its ownership downstream without copying.
 */
class BinlogStream {
 public:
  /**
   * @brief Send COM_BINLOG_DUMP_GTID to start binlog streaming
   *
   * If gtid_encoded is non-empty, the BINLOG_THROUGH_GTID flag (0x04)
   * is automatically set.
   *
   * @param sock    Connected and authenticated socket handle
   * @param config  Binlog stream configuration
   * @return MES_OK on success, MES_ERR_STREAM on failure
   */
  mes_error_t Start(SocketHandle* sock, const BinlogStreamConfig& config);

  /**
   * @brief Read the next binlog event from the stream (blocking)
   *
   * Blocks until a complete event packet is received. The packet bytes
   * are written into *buffer (which is resized as needed). The returned
   * BinlogEventPacket's `data` points into *buffer and remains valid
   * until the caller mutates *buffer or passes it to another FetchEvent().
   *
   * On heartbeat or error, `data` is nullptr and `size` is 0; *buffer
   * may still have been resized and its contents are implementation-
   * defined (callers should treat it as scratch in that case).
   *
   * MES_ERR_GTID_PURGED is returned only for a server error whose meaning is
   * a purged GTID interval, which requires the dump to have been started from
   * a GTID set (BinlogStreamConfig::position_from_gtid). On a file/position
   * dump the same server error code carries an unrelated, recoverable
   * condition, so it is reported as MES_ERR_STREAM.
   *
   * @param sock    Socket handle used in Start()
   * @param buffer  Caller-owned scratch buffer reused across calls
   * @param result  Output: populated with event data or heartbeat flag
   * @param max_event_size Maximum event bytes, excluding the one-byte MySQL
   *                       OK prefix. Values are normalized by the caller.
   * @return MES_OK on success, or a stream/disconnect/GTID error
   */
  mes_error_t FetchEvent(SocketHandle* sock, std::vector<uint8_t>* buffer,
                         BinlogEventPacket* result, uint32_t max_event_size);

  /**
   * @brief Send COM_BINLOG_DUMP to start binlog streaming (MariaDB)
   *
   * Unlike COM_BINLOG_DUMP_GTID, MariaDB negotiates GTID position via
   * session variables before this command. The command only specifies
   * the binlog position and filename.
   *
   * @param sock    Connected and authenticated socket handle
   * @param config  Binlog stream configuration (server_id, position, filename)
   * @return MES_OK on success, MES_ERR_STREAM on failure
   */
  mes_error_t StartComBinlogDump(SocketHandle* sock, const BinlogStreamConfig& config);

 private:
  /**
   * @brief Whether the dump in progress was started from a GTID set.
   *
   * Recorded by both start commands and read by FetchEvent(), which receives
   * no configuration of its own. Written on the owner thread before the reader
   * thread exists and not touched again while a dump is running.
   */
  bool position_from_gtid_ = false;
};

}  // namespace mes::protocol

#endif  // MES_PROTOCOL_MYSQL_BINLOG_STREAM_H_
