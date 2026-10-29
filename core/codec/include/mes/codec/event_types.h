/**
 * @file event_types.h
 * @brief Binlog event type codes, common-header flags and column type codes.
 *
 * MySQL types occupy 0-42 and MariaDB's own types start at 160; the two
 * numbering spaces do not overlap. `ENUM_END_EVENT` is pinned at 255 as the
 * end marker. How each type is handled is not decided here.
 */

#ifndef MES_CODEC_EVENT_TYPES_H_
#define MES_CODEC_EVENT_TYPES_H_

#include <cstdint>

namespace mes::codec {

/** @brief Binlog event type code (header byte 4). */
enum class EventType : uint8_t {
  kUnknown = 0,
  kStartEventV3 = 1,
  kQueryEvent = 2,
  kStopEvent = 3,
  kRotateEvent = 4,
  kIntvarEvent = 5,
  kSlaveEvent = 7,
  kAppendBlockEvent = 9,
  kDeleteFileEvent = 11,
  kRandEvent = 13,
  kUserVarEvent = 14,
  kFormatDescriptionEvent = 15,
  kXidEvent = 16,
  kBeginLoadQueryEvent = 17,
  kExecuteLoadQueryEvent = 18,
  kTableMapEvent = 19,
  kWriteRowsEventV1 = 23,
  kUpdateRowsEventV1 = 24,
  kDeleteRowsEventV1 = 25,
  kIncidentEvent = 26,
  kHeartbeatLogEvent = 27,
  kIgnorableLogEvent = 28,
  kRowsQueryLogEvent = 29,
  kWriteRowsEvent = 30,
  kUpdateRowsEvent = 31,
  kDeleteRowsEvent = 32,
  kGtidLogEvent = 33,
  kAnonymousGtidLogEvent = 34,
  kPreviousGtidsEvent = 35,
  kTransactionContextEvent = 36,
  kViewChangeEvent = 37,
  kXaPrepareLogEvent = 38,
  kPartialUpdateRowsEvent = 39,
  kTransactionPayloadEvent = 40,
  kHeartbeatLogEventV2 = 41,
  kGtidTaggedLogEvent = 42,

  kMariaDBAnnotateRowsEvent = 160,
  kMariaDBBinlogCheckpointEvent = 161,
  kMariaDBGtidEvent = 162,
  kMariaDBGtidListEvent = 163,
  kMariaDBStartEncryptionEvent = 164,
  kMariaDBQueryCompressedEvent = 165,
  kMariaDBWriteRowsCompressedEventV1 = 166,
  kMariaDBUpdateRowsCompressedEventV1 = 167,
  kMariaDBDeleteRowsCompressedEventV1 = 168,
  kMariaDBWriteRowsCompressedEvent = 169,
  kMariaDBUpdateRowsCompressedEvent = 170,
  kMariaDBDeleteRowsCompressedEvent = 171,
  kMariaDBPartialRowDataEvent = 172,

  kEnumEndEvent = 255,
};

/**
 * @brief Upstream name of an event type code (e.g. "ROTATE_EVENT").
 * @return The name, or "UNKNOWN" for a code outside EventType.
 */
const char* EventTypeName(uint8_t type_code) noexcept;

/** @brief EventTypeName() for an enumerator. */
const char* EventTypeName(EventType type) noexcept;

/** @brief Whether @p type_code is an uncompressed MySQL rows event (v1 23-25 or v2 30-32). */
bool IsRowsEvent(uint8_t type_code) noexcept;

/** @brief IsRowsEvent() for an enumerator. */
bool IsRowsEvent(EventType type) noexcept;

/// @name Common-header flags (header bytes 17-18)
/// @{
inline constexpr uint16_t kLogEventBinlogInUseFlag = 0x0001;     ///< LOG_EVENT_BINLOG_IN_USE_F
inline constexpr uint16_t kLogEventThreadSpecificFlag = 0x0004;  ///< LOG_EVENT_THREAD_SPECIFIC_F
inline constexpr uint16_t kLogEventSuppressUseFlag = 0x0008;     ///< LOG_EVENT_SUPPRESS_USE_F
inline constexpr uint16_t kLogEventArtificialFlag = 0x0020;  ///< LOG_EVENT_ARTIFICIAL_F (dump-made)
inline constexpr uint16_t kLogEventRelayLogFlag = 0x0040;    ///< LOG_EVENT_RELAY_LOG_F
inline constexpr uint16_t kLogEventIgnorableFlag = 0x0080;   ///< LOG_EVENT_IGNORABLE_F
inline constexpr uint16_t kLogEventNoFilterFlag = 0x0100;    ///< LOG_EVENT_NO_FILTER_F
inline constexpr uint16_t kLogEventMtsIsolateFlag = 0x0200;  ///< LOG_EVENT_MTS_ISOLATE_F
/// @}

/** @brief Column type code as carried in TABLE_MAP (MySQL `enum_field_types`). */
enum class ColumnType : uint8_t {
  kDecimal = 0x00,            ///< MYSQL_TYPE_DECIMAL (legacy)
  kTiny = 0x01,               ///< MYSQL_TYPE_TINY (TINYINT)
  kShort = 0x02,              ///< MYSQL_TYPE_SHORT (SMALLINT)
  kLong = 0x03,               ///< MYSQL_TYPE_LONG (INT)
  kFloat = 0x04,              ///< MYSQL_TYPE_FLOAT
  kDouble = 0x05,             ///< MYSQL_TYPE_DOUBLE
  kTimestamp = 0x07,          ///< MYSQL_TYPE_TIMESTAMP
  kLongLong = 0x08,           ///< MYSQL_TYPE_LONGLONG (BIGINT)
  kInt24 = 0x09,              ///< MYSQL_TYPE_INT24 (MEDIUMINT)
  kDate = 0x0A,               ///< MYSQL_TYPE_DATE
  kTime = 0x0B,               ///< MYSQL_TYPE_TIME
  kDatetime = 0x0C,           ///< MYSQL_TYPE_DATETIME
  kYear = 0x0D,               ///< MYSQL_TYPE_YEAR
  kNewDate = 0x0E,            ///< MYSQL_TYPE_NEWDATE (typed-array element marker)
  kVarchar = 0x0F,            ///< MYSQL_TYPE_VARCHAR
  kBit = 0x10,                ///< MYSQL_TYPE_BIT
  kTimestamp2 = 0x11,         ///< MYSQL_TYPE_TIMESTAMP2
  kDatetime2 = 0x12,          ///< MYSQL_TYPE_DATETIME2
  kTime2 = 0x13,              ///< MYSQL_TYPE_TIME2
  kTypedArray = 0x14,         ///< MYSQL_TYPE_TYPED_ARRAY (replication only)
  kBlobCompressed = 0x8C,     ///< MariaDB MYSQL_TYPE_BLOB_COMPRESSED
  kVarcharCompressed = 0x8D,  ///< MariaDB MYSQL_TYPE_VARCHAR_COMPRESSED
  kVector = 0xF2,             ///< MYSQL_TYPE_VECTOR (MySQL 9.0+)
  kJson = 0xF5,               ///< MYSQL_TYPE_JSON
  kNewDecimal = 0xF6,         ///< MYSQL_TYPE_NEWDECIMAL
  kEnum = 0xF7,               ///< MYSQL_TYPE_ENUM
  kSet = 0xF8,                ///< MYSQL_TYPE_SET
  kTinyBlob = 0xF9,           ///< MYSQL_TYPE_TINY_BLOB
  kMediumBlob = 0xFA,         ///< MYSQL_TYPE_MEDIUM_BLOB
  kLongBlob = 0xFB,           ///< MYSQL_TYPE_LONG_BLOB
  kBlob = 0xFC,               ///< MYSQL_TYPE_BLOB
  kVarString = 0xFD,          ///< MYSQL_TYPE_VAR_STRING
  kString = 0xFE,             ///< MYSQL_TYPE_STRING
  kGeometry = 0xFF,           ///< MYSQL_TYPE_GEOMETRY
};

}  // namespace mes::codec

#endif  // MES_CODEC_EVENT_TYPES_H_
