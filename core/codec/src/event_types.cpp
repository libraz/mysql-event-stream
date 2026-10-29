#include "mes/codec/event_types.h"

namespace mes::codec {

const char* EventTypeName(EventType type) noexcept {
  // No default label: a new enumerator must be named here before it builds.
  switch (type) {
    case EventType::kUnknown:
      return "UNKNOWN_EVENT";
    case EventType::kStartEventV3:
      return "START_EVENT_V3";
    case EventType::kQueryEvent:
      return "QUERY_EVENT";
    case EventType::kStopEvent:
      return "STOP_EVENT";
    case EventType::kRotateEvent:
      return "ROTATE_EVENT";
    case EventType::kIntvarEvent:
      return "INTVAR_EVENT";
    case EventType::kSlaveEvent:
      return "SLAVE_EVENT";
    case EventType::kAppendBlockEvent:
      return "APPEND_BLOCK_EVENT";
    case EventType::kDeleteFileEvent:
      return "DELETE_FILE_EVENT";
    case EventType::kRandEvent:
      return "RAND_EVENT";
    case EventType::kUserVarEvent:
      return "USER_VAR_EVENT";
    case EventType::kFormatDescriptionEvent:
      return "FORMAT_DESCRIPTION_EVENT";
    case EventType::kXidEvent:
      return "XID_EVENT";
    case EventType::kBeginLoadQueryEvent:
      return "BEGIN_LOAD_QUERY_EVENT";
    case EventType::kExecuteLoadQueryEvent:
      return "EXECUTE_LOAD_QUERY_EVENT";
    case EventType::kTableMapEvent:
      return "TABLE_MAP_EVENT";
    case EventType::kWriteRowsEventV1:
      return "WRITE_ROWS_EVENT_V1";
    case EventType::kUpdateRowsEventV1:
      return "UPDATE_ROWS_EVENT_V1";
    case EventType::kDeleteRowsEventV1:
      return "DELETE_ROWS_EVENT_V1";
    case EventType::kIncidentEvent:
      return "INCIDENT_EVENT";
    case EventType::kHeartbeatLogEvent:
      return "HEARTBEAT_LOG_EVENT";
    case EventType::kIgnorableLogEvent:
      return "IGNORABLE_LOG_EVENT";
    case EventType::kRowsQueryLogEvent:
      return "ROWS_QUERY_LOG_EVENT";
    case EventType::kWriteRowsEvent:
      return "WRITE_ROWS_EVENT";
    case EventType::kUpdateRowsEvent:
      return "UPDATE_ROWS_EVENT";
    case EventType::kDeleteRowsEvent:
      return "DELETE_ROWS_EVENT";
    case EventType::kGtidLogEvent:
      return "GTID_LOG_EVENT";
    case EventType::kAnonymousGtidLogEvent:
      return "ANONYMOUS_GTID_LOG_EVENT";
    case EventType::kPreviousGtidsEvent:
      return "PREVIOUS_GTIDS_LOG_EVENT";
    case EventType::kTransactionContextEvent:
      return "TRANSACTION_CONTEXT_EVENT";
    case EventType::kViewChangeEvent:
      return "VIEW_CHANGE_EVENT";
    case EventType::kXaPrepareLogEvent:
      return "XA_PREPARE_LOG_EVENT";
    case EventType::kPartialUpdateRowsEvent:
      return "PARTIAL_UPDATE_ROWS_EVENT";
    case EventType::kTransactionPayloadEvent:
      return "TRANSACTION_PAYLOAD_EVENT";
    case EventType::kHeartbeatLogEventV2:
      return "HEARTBEAT_LOG_EVENT_V2";
    case EventType::kGtidTaggedLogEvent:
      return "GTID_TAGGED_LOG_EVENT";
    case EventType::kMariaDBAnnotateRowsEvent:
      return "MARIADB_ANNOTATE_ROWS_EVENT";
    case EventType::kMariaDBBinlogCheckpointEvent:
      return "MARIADB_BINLOG_CHECKPOINT_EVENT";
    case EventType::kMariaDBGtidEvent:
      return "MARIADB_GTID_EVENT";
    case EventType::kMariaDBGtidListEvent:
      return "MARIADB_GTID_LIST_EVENT";
    case EventType::kMariaDBStartEncryptionEvent:
      return "MARIADB_START_ENCRYPTION_EVENT";
    case EventType::kMariaDBQueryCompressedEvent:
      return "MARIADB_QUERY_COMPRESSED_EVENT";
    case EventType::kMariaDBWriteRowsCompressedEventV1:
      return "MARIADB_WRITE_ROWS_COMPRESSED_EVENT_V1";
    case EventType::kMariaDBUpdateRowsCompressedEventV1:
      return "MARIADB_UPDATE_ROWS_COMPRESSED_EVENT_V1";
    case EventType::kMariaDBDeleteRowsCompressedEventV1:
      return "MARIADB_DELETE_ROWS_COMPRESSED_EVENT_V1";
    case EventType::kMariaDBWriteRowsCompressedEvent:
      return "MARIADB_WRITE_ROWS_COMPRESSED_EVENT";
    case EventType::kMariaDBUpdateRowsCompressedEvent:
      return "MARIADB_UPDATE_ROWS_COMPRESSED_EVENT";
    case EventType::kMariaDBDeleteRowsCompressedEvent:
      return "MARIADB_DELETE_ROWS_COMPRESSED_EVENT";
    case EventType::kMariaDBPartialRowDataEvent:
      return "MARIADB_PARTIAL_ROW_DATA_EVENT";
    case EventType::kEnumEndEvent:
      return "ENUM_END_EVENT";
  }
  return "UNKNOWN";
}

const char* EventTypeName(uint8_t type_code) noexcept {
  return EventTypeName(static_cast<EventType>(type_code));
}

bool IsRowsEvent(EventType type) noexcept {
  switch (type) {
    case EventType::kWriteRowsEventV1:
    case EventType::kUpdateRowsEventV1:
    case EventType::kDeleteRowsEventV1:
    case EventType::kWriteRowsEvent:
    case EventType::kUpdateRowsEvent:
    case EventType::kDeleteRowsEvent:
      return true;
    default:
      return false;
  }
}

bool IsRowsEvent(uint8_t type_code) noexcept {
  return IsRowsEvent(static_cast<EventType>(type_code));
}

}  // namespace mes::codec
