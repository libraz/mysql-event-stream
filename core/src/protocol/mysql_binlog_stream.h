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

#include "mes.h"

namespace mes::protocol {

// Forward declaration; defined in protocol/mysql_socket.h
class SocketHandle;

/** @brief Configuration for starting a binlog replication stream */
struct BinlogStreamConfig {
  uint32_t server_id = 1;
  std::string binlog_filename;  ///< Empty string means no specific file
  uint64_t binlog_position = 4;
  std::vector<uint8_t> gtid_encoded;  ///< Binary-encoded GTID set
  uint16_t flags = 0;
};

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
  const uint8_t* data = nullptr;  ///< Event data (after OK byte)
  size_t size = 0;                ///< Size of event data in bytes
  size_t data_offset = 0;         ///< Offset of `data` within the caller's buffer
  bool is_heartbeat = false;      ///< True if this is a heartbeat event
};

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
   * @param sock    Socket handle used in Start()
   * @param buffer  Caller-owned scratch buffer reused across calls
   * @param result  Output: populated with event data or heartbeat flag
   * @return MES_OK on success, MES_ERR_STREAM on error or stream end
   */
  mes_error_t FetchEvent(SocketHandle* sock, std::vector<uint8_t>* buffer,
                         BinlogEventPacket* result);

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
};

}  // namespace mes::protocol

#endif  // MES_PROTOCOL_MYSQL_BINLOG_STREAM_H_
