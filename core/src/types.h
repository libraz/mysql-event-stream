// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * @file types.h
 * @brief Core data types for the mysql-event-stream CDC engine
 *
 * Defines column types, change event types, and data structures
 * used throughout the mysql-event-stream library.
 */

#ifndef MES_CORE_SRC_TYPES_H_
#define MES_CORE_SRC_TYPES_H_

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

namespace mes {

/**
 * @brief MySQL column type enumeration
 *
 * Maps to MySQL MYSQL_TYPE_* values from include/field_types.h.
 * Only types relevant to MySQL 8.4 binlog row events are included.
 */
enum class ColumnType : uint8_t {
  kTiny = 0x01,        ///< MYSQL_TYPE_TINY (TINYINT)
  kShort = 0x02,       ///< MYSQL_TYPE_SHORT (SMALLINT)
  kLong = 0x03,        ///< MYSQL_TYPE_LONG (INT)
  kFloat = 0x04,       ///< MYSQL_TYPE_FLOAT
  kDouble = 0x05,      ///< MYSQL_TYPE_DOUBLE
  kTimestamp = 0x07,   ///< MYSQL_TYPE_TIMESTAMP
  kLongLong = 0x08,    ///< MYSQL_TYPE_LONGLONG (BIGINT)
  kInt24 = 0x09,       ///< MYSQL_TYPE_INT24 (MEDIUMINT)
  kDate = 0x0A,        ///< MYSQL_TYPE_DATE
  kTime = 0x0B,        ///< MYSQL_TYPE_TIME
  kDatetime = 0x0C,    ///< MYSQL_TYPE_DATETIME
  kYear = 0x0D,        ///< MYSQL_TYPE_YEAR
  kVarchar = 0x0F,     ///< MYSQL_TYPE_VARCHAR
  kBit = 0x10,         ///< MYSQL_TYPE_BIT
  kTimestamp2 = 0x11,  ///< MYSQL_TYPE_TIMESTAMP2 (with fractional seconds)
  kDatetime2 = 0x12,   ///< MYSQL_TYPE_DATETIME2 (with fractional seconds)
  kTime2 = 0x13,       ///< MYSQL_TYPE_TIME2 (with fractional seconds)
  kVector = 0xF2,      ///< MYSQL_TYPE_VECTOR (MySQL 9.0+)
  kJson = 0xF5,        ///< MYSQL_TYPE_JSON
  kNewDecimal = 0xF6,  ///< MYSQL_TYPE_NEWDECIMAL
  kEnum = 0xF7,        ///< MYSQL_TYPE_ENUM
  kSet = 0xF8,         ///< MYSQL_TYPE_SET
  kTinyBlob = 0xF9,    ///< MYSQL_TYPE_TINY_BLOB
  kMediumBlob = 0xFA,  ///< MYSQL_TYPE_MEDIUM_BLOB
  kLongBlob = 0xFB,    ///< MYSQL_TYPE_LONG_BLOB
  kBlob = 0xFC,        ///< MYSQL_TYPE_BLOB
  kVarString = 0xFD,   ///< MYSQL_TYPE_VAR_STRING
  kString = 0xFE,      ///< MYSQL_TYPE_STRING
  kGeometry = 0xFF,    ///< MYSQL_TYPE_GEOMETRY
};

/**
 * @brief CDC change event type
 */
enum class EventType : uint8_t {
  kInsert,  ///< Row was inserted
  kUpdate,  ///< Row was updated
  kDelete,  ///< Row was deleted
};

/**
 * @brief Type-safe column value storage
 *
 * Uses a tagged union approach with explicit type and null tracking,
 * compatible with -fno-exceptions builds. Different value fields are
 * used depending on the column type.
 */
struct ColumnValue {
  ColumnType type = ColumnType::kLong;
  bool is_null = true;
  std::string name;  ///< Column name (empty = unknown)

  int64_t int_val = 0;             ///< kTiny, kShort, kLong, kLongLong, kInt24, kYear
  double real_val = 0.0;           ///< kFloat, kDouble
  std::string string_val;          ///< STRING, BLOB, JSON, DECIMAL, DATETIME, etc.
  std::vector<uint8_t> bytes_val;  ///< Binary data

  /** @brief Create a NULL value of the given type */
  static ColumnValue Null(ColumnType t) {
    ColumnValue v;
    v.type = t;
    v.is_null = true;
    return v;
  }

  /** @brief Create an integer value */
  static ColumnValue Int(ColumnType t, int64_t val) {
    ColumnValue v;
    v.type = t;
    v.is_null = false;
    v.int_val = val;
    return v;
  }

  /** @brief Create a float value */
  static ColumnValue Float(double val) {
    ColumnValue v;
    v.type = ColumnType::kFloat;
    v.is_null = false;
    v.real_val = val;
    return v;
  }

  /** @brief Create a double value */
  static ColumnValue Double(double val) {
    ColumnValue v;
    v.type = ColumnType::kDouble;
    v.is_null = false;
    v.real_val = val;
    return v;
  }

  /** @brief Create a string value */
  static ColumnValue String(ColumnType t, std::string val) {
    ColumnValue v;
    v.type = t;
    v.is_null = false;
    v.string_val = std::move(val);
    return v;
  }

  /** @brief Create a binary value */
  static ColumnValue Bytes(ColumnType t, std::vector<uint8_t> val) {
    ColumnValue v;
    v.type = t;
    v.is_null = false;
    v.bytes_val = std::move(val);
    return v;
  }
};

/**
 * @brief A row of column values
 */
struct RowData {
  std::vector<ColumnValue> columns;
};

/**
 * @brief Position in the binlog stream
 */
struct BinlogPosition {
  std::string binlog_file;
  uint64_t offset = 0;
};

/**
 * @brief Metadata for a single column from TABLE_MAP event
 */
struct ColumnMetadata {
  ColumnType type = ColumnType::kLong;
  std::string name;
  uint16_t metadata = 0;  ///< Type-specific metadata
  bool is_nullable = true;
  bool is_unsigned = false;
};

/**
 * @brief Metadata for a table from TABLE_MAP event
 */
struct TableMetadata {
  uint64_t table_id = 0;
  std::string database_name;
  std::string table_name;
  std::vector<ColumnMetadata> columns;
};

/**
 * @brief A CDC change event representing a single row change
 */
struct ChangeEvent {
  EventType type = EventType::kInsert;
  std::string database;
  std::string table;
  RowData before;  ///< Populated for UPDATE and DELETE
  RowData after;   ///< Populated for INSERT and UPDATE
  uint32_t timestamp = 0;
  BinlogPosition position;
};

}  // namespace mes

#endif  // MES_CORE_SRC_TYPES_H_
