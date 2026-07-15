// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#include "event_header.h"

#include "binary_util.h"

namespace mes {

bool ParseEventHeader(const uint8_t* data, size_t len, EventHeader* header) {
  if (len < kEventHeaderSize || data == nullptr || header == nullptr) {
    return false;
  }

  header->timestamp = binary::ReadU32Le(data);
  header->type_code = binary::ReadU8(data + 4);
  header->server_id = binary::ReadU32Le(data + 5);
  header->event_length = binary::ReadU32Le(data + 9);
  header->next_position = binary::ReadU32Le(data + 13);
  header->flags = binary::ReadU16Le(data + 17);

  return true;
}

size_t EventBodySize(const EventHeader& header, bool has_checksum) {
  size_t min_size = kEventHeaderSize + (has_checksum ? kChecksumSize : 0);
  if (header.event_length < min_size) {
    return 0;
  }
  return header.event_length - min_size;
}

size_t EventBodySize(const EventHeader& header) { return EventBodySize(header, true); }

bool IsRowEvent(BinlogEventType type) { return IsRowEvent(static_cast<uint8_t>(type)); }

bool IsRowEvent(uint8_t type_code) {
  switch (type_code) {
    case static_cast<uint8_t>(BinlogEventType::kWriteRowsEventV1):
    case static_cast<uint8_t>(BinlogEventType::kUpdateRowsEventV1):
    case static_cast<uint8_t>(BinlogEventType::kDeleteRowsEventV1):
    case static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent):
    case static_cast<uint8_t>(BinlogEventType::kUpdateRowsEvent):
    case static_cast<uint8_t>(BinlogEventType::kDeleteRowsEvent):
      return true;
    default:
      return false;
  }
}

const char* BinlogEventTypeName(uint8_t type_code) {
  switch (type_code) {
    case static_cast<uint8_t>(BinlogEventType::kRotateEvent):
      return "ROTATE_EVENT";
    case static_cast<uint8_t>(BinlogEventType::kFormatDescriptionEvent):
      return "FORMAT_DESCRIPTION_EVENT";
    case static_cast<uint8_t>(BinlogEventType::kXidEvent):
      return "XID_EVENT";
    case static_cast<uint8_t>(BinlogEventType::kTableMapEvent):
      return "TABLE_MAP_EVENT";
    case static_cast<uint8_t>(BinlogEventType::kWriteRowsEventV1):
      return "WRITE_ROWS_EVENT_V1";
    case static_cast<uint8_t>(BinlogEventType::kUpdateRowsEventV1):
      return "UPDATE_ROWS_EVENT_V1";
    case static_cast<uint8_t>(BinlogEventType::kDeleteRowsEventV1):
      return "DELETE_ROWS_EVENT_V1";
    case static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent):
      return "WRITE_ROWS_EVENT";
    case static_cast<uint8_t>(BinlogEventType::kUpdateRowsEvent):
      return "UPDATE_ROWS_EVENT";
    case static_cast<uint8_t>(BinlogEventType::kDeleteRowsEvent):
      return "DELETE_ROWS_EVENT";
    case static_cast<uint8_t>(BinlogEventType::kGtidLogEvent):
      return "GTID_LOG_EVENT";
    case static_cast<uint8_t>(BinlogEventType::kAnonymousGtidLogEvent):
      return "ANONYMOUS_GTID_LOG_EVENT";
    case static_cast<uint8_t>(BinlogEventType::kPreviousGtidsEvent):
      return "PREVIOUS_GTIDS_EVENT";
    case static_cast<uint8_t>(BinlogEventType::kMariaDBAnnotateRowsEvent):
      return "MARIADB_ANNOTATE_ROWS_EVENT";
    case static_cast<uint8_t>(BinlogEventType::kMariaDBBinlogCheckpointEvent):
      return "MARIADB_BINLOG_CHECKPOINT_EVENT";
    case static_cast<uint8_t>(BinlogEventType::kMariaDBGtidEvent):
      return "MARIADB_GTID_EVENT";
    case static_cast<uint8_t>(BinlogEventType::kMariaDBGtidListEvent):
      return "MARIADB_GTID_LIST_EVENT";
    case static_cast<uint8_t>(BinlogEventType::kMariaDBStartEncryptionEvent):
      return "MARIADB_START_ENCRYPTION_EVENT";
    default:
      return "UNKNOWN";
  }
}

BinlogChecksumAlgorithm DetectFormatDescriptionChecksum(const uint8_t* data, size_t len) {
  // Fixed FDE prefix after the common header: binlog_version(2) +
  // server_version(50) + create_timestamp(4) + header_length(1) = 57.
  constexpr size_t kFdePrefix = 57;
  if (data == nullptr || len < kEventHeaderSize + kFdePrefix + 1 ||
      data[4] != static_cast<uint8_t>(BinlogEventType::kFormatDescriptionEvent)) {
    return BinlogChecksumAlgorithm::kUnknown;
  }
  if (len >= kEventHeaderSize + kFdePrefix + 1 + kChecksumSize &&
      data[len - kChecksumSize - 1] == kBinlogChecksumAlgCrc32) {
    return BinlogChecksumAlgorithm::kCrc32;
  }
  if (data[len - 1] == kBinlogChecksumAlgOff) {
    return BinlogChecksumAlgorithm::kOff;
  }
  return BinlogChecksumAlgorithm::kUnknown;
}

}  // namespace mes
