// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * @file event_header.h
 * @brief Binlog event header parsing and event type classification
 *
 * Provides types and functions for parsing MySQL binlog v4 event
 * headers and classifying event types relevant to CDC processing.
 */

#ifndef MES_EVENT_HEADER_H_
#define MES_EVENT_HEADER_H_

#include <cstddef>
#include <cstdint>

namespace mes {

/// Binlog event types relevant to CDC processing.
/// Values match MySQL's Log_event_type enum.
enum class BinlogEventType : uint8_t {
  kUnknown = 0,
  kRotateEvent = 4,
  kFormatDescriptionEvent = 15,
  kXidEvent = 16,
  kTableMapEvent = 19,
  kWriteRowsEventV1 = 23,
  kUpdateRowsEventV1 = 24,
  kDeleteRowsEventV1 = 25,
  kWriteRowsEvent = 30,
  kUpdateRowsEvent = 31,
  kDeleteRowsEvent = 32,
  kGtidLogEvent = 33,
  kAnonymousGtidLogEvent = 34,
};

/// MySQL binlog v4 event header (19 bytes).
struct EventHeader {
  uint32_t timestamp = 0;
  uint8_t type_code = 0;
  uint32_t server_id = 0;
  uint32_t event_length = 0;
  uint32_t next_position = 0;
  uint16_t flags = 0;
};

/// Constants for binlog event header parsing.
inline constexpr size_t kEventHeaderSize = 19;
inline constexpr size_t kChecksumSize = 4;

/**
 * @brief Parse a 19-byte binlog event header.
 * @param data Pointer to at least 19 bytes of header data.
 * @param len Length of available data.
 * @param[out] header Parsed header output.
 * @return true if parsing succeeded (len >= 19), false otherwise.
 */
bool ParseEventHeader(const uint8_t* data, size_t len, EventHeader* header);

/**
 * @brief Calculate the body size of an event (excludes header and checksum).
 * @param header Parsed event header.
 * @return Body size in bytes, or 0 if event_length is too small.
 */
size_t EventBodySize(const EventHeader& header);

/**
 * @brief Check if a binlog event type is a row event (WRITE/UPDATE/DELETE, V1 or V2).
 */
bool IsRowEvent(BinlogEventType type);

/**
 * @brief Check if a binlog event type code is a row event (WRITE/UPDATE/DELETE, V1 or V2).
 */
bool IsRowEvent(uint8_t type_code);

/**
 * @brief Get human-readable name of a binlog event type.
 * @param type_code Raw event type code.
 * @return Static string with the event type name, or "UNKNOWN" for unrecognized types.
 */
const char* BinlogEventTypeName(uint8_t type_code);

}  // namespace mes

#endif  // MES_EVENT_HEADER_H_
