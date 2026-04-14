// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * @file mysql_packet.h
 * @brief MySQL wire protocol packet I/O and length-encoded helpers
 *
 * Provides packet framing (read/write) with multi-packet support for payloads
 * exceeding 16 MB-1, plus MySQL length-encoded integer/string helpers.
 */

#ifndef MES_PROTOCOL_MYSQL_PACKET_H_
#define MES_PROTOCOL_MYSQL_PACKET_H_

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "mes.h"

namespace mes::protocol {

/// MySQL packet marker bytes
constexpr uint8_t kPacketOk = 0x00;
constexpr uint8_t kPacketErr = 0xFF;
constexpr uint8_t kPacketEOF = 0xFE;
constexpr uint8_t kPacketLocalInfile = 0xFB;
constexpr uint8_t kNullColumnMarker = 0xFB;  ///< NULL value marker in text result rows
constexpr uint8_t kComQuery = 0x03;

// Forward declaration; defined in protocol/mysql_socket.h
class SocketHandle;

/**
 * @brief Outgoing MySQL packet builder
 *
 * Accumulates one or more wire-format packets into an internal buffer.
 * Handles multi-packet splitting for payloads larger than 0xFFFFFF bytes.
 */
class PacketBuffer {
 public:
  /** @brief Clear the internal buffer */
  void Clear();

  /**
   * @brief Build wire packet(s) from a payload
   *
   * Writes 3-byte LE payload_length + 1-byte sequence_id + payload.
   * For payloads > 0xFFFFFF (16 MB - 1), splits into multiple packets
   * with incrementing sequence IDs. Exact multiples get a trailing
   * zero-length packet.
   *
   * @param payload  Pointer to the payload data
   * @param len      Length of the payload in bytes
   * @param sequence_id  Pointer to the current sequence ID; updated to the
   *                     next expected value on return
   */
  void WritePacket(const uint8_t* payload, size_t len, uint8_t* sequence_id);

  /** @brief Pointer to the accumulated wire data */
  const uint8_t* Data() const;

  /** @brief Total size of accumulated wire data in bytes */
  size_t Size() const;

 private:
  std::vector<uint8_t> buf_;
};

/**
 * @brief Read one complete MySQL packet from a socket
 *
 * Reads the 4-byte header, then the payload. Handles multi-packet reassembly:
 * if the payload length is exactly 0xFFFFFF, reads successive packets and
 * appends their payloads until a packet shorter than 0xFFFFFF is received.
 *
 * @param sock        Socket to read from
 * @param payload     Output: reassembled payload bytes
 * @param sequence_id Output: sequence ID of the last packet header read
 * @return MES_OK on success, MES_ERR_STREAM on socket errors
 */
mes_error_t ReadPacket(SocketHandle* sock, std::vector<uint8_t>* payload,
                       uint8_t* sequence_id);

/**
 * @brief Read a MySQL length-encoded integer from a buffer
 *
 * Encoding:
 * - < 0xFB (251): value is the byte itself (1 byte)
 * - 0xFB: NULL marker (returns 0)
 * - 0xFC: next 2 bytes LE
 * - 0xFD: next 3 bytes LE
 * - 0xFE: next 8 bytes LE
 * - 0xFF: error/undefined (returns 0)
 *
 * @param data  Pointer to the buffer
 * @param len   Available bytes in the buffer
 * @param pos   Current position; advanced past the consumed bytes
 * @return Decoded value, or 0 on error/NULL
 */
uint64_t ReadLenEncInt(const uint8_t* data, size_t len, size_t* pos);

/**
 * @brief Write a MySQL length-encoded integer to a buffer
 *
 * @param buf  Output buffer to append to
 * @param val  Value to encode
 */
void WriteLenEncInt(std::vector<uint8_t>* buf, uint64_t val);

/**
 * @brief Write a MySQL length-encoded string (len-enc-int + string data)
 *
 * @param buf  Output buffer to append to
 * @param s    String to encode
 */
void WriteLenEncString(std::vector<uint8_t>* buf, const std::string& s);

/**
 * @brief Write a fixed-width little-endian integer
 *
 * @param buf    Output buffer to append to
 * @param val    Value to write
 * @param width  Number of bytes (1, 2, 3, 4, or 8)
 */
void WriteFixedInt(std::vector<uint8_t>* buf, uint64_t val, size_t width);

/**
 * @brief Read a fixed-width little-endian integer
 *
 * @param data   Pointer to the data
 * @param width  Number of bytes (1, 2, 3, 4, or 8)
 * @return Decoded value
 */
uint64_t ReadFixedInt(const uint8_t* data, size_t width);

/**
 * @brief Parse MySQL ERR packet into error code and message string.
 * @param data Packet payload (starting with 0xFF marker)
 * @param len Length of the payload
 * @param[out] error_code Extracted MySQL error code
 * @param[out] message Extracted error message
 */
void ParseErrPacketPayload(const uint8_t* data, size_t len,
                           uint16_t* error_code, std::string* message);

}  // namespace mes::protocol

#endif  // MES_PROTOCOL_MYSQL_PACKET_H_
