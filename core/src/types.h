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
#include <memory>
#include <memory_resource>
#include <new>
#include <string>
#include <string_view>
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
  kTimestamp2 = 0x11,         ///< MYSQL_TYPE_TIMESTAMP2 (with fractional seconds)
  kDatetime2 = 0x12,          ///< MYSQL_TYPE_DATETIME2 (with fractional seconds)
  kTime2 = 0x13,              ///< MYSQL_TYPE_TIME2 (with fractional seconds)
  kTypedArray = 0x14,         ///< MYSQL_TYPE_TYPED_ARRAY (binary JSON row payload)
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

/**
 * @brief CDC change event type
 */
enum class EventType : uint8_t {
  kInsert,  ///< Row was inserted
  kUpdate,  ///< Row was updated
  kDelete,  ///< Row was deleted
};

/**
 * @brief Type-safe column value storage.
 *
 * Uses a tagged union approach with explicit type and null tracking,
 * compatible with -fno-exceptions builds. Different value fields are
 * used depending on the column type.
 *
 * @note `string_val` holds both textual (e.g. VARCHAR, DECIMAL) and
 *       binary payloads. `is_binary` records which interpretation applies;
 *       TABLE_MAP charset metadata distinguishes text from binary for every
 *       character-family and BLOB-family column alike, and a column whose
 *       charset is unknown is treated as binary. std::string is used
 *       uniformly as the backing container because its storage layout
 *       is byte-addressable and permits embedded NULs, so it is a
 *       strict superset of what a std::vector<uint8_t> would offer.
 *       Keeping a single buffer instead of separate string/bytes
 *       members saves ~24 bytes per ColumnValue without any loss of
 *       fidelity. Callers that need byte
 *       access can use `bytes_data()` / `bytes_size()` below; the
 *       returned pointer is unmodified from `string_val.data()`.
 */
struct ColumnValue {
  ColumnType type = ColumnType::kLong;
  bool is_null = true;
  /// True when string_val is an opaque byte sequence rather than text.
  bool is_binary = false;
  std::string_view name;  ///< Column name (empty = unknown; owned by ChangeEvent metadata)

  int64_t int_val = 0;     ///< kTiny, kShort, kLong, kLongLong, kInt24, kYear
  double real_val = 0.0;   ///< kFloat, kDouble
  std::string string_val;  ///< STRING, BLOB, JSON, DECIMAL, DATETIME, etc. (binary-safe)

  /** @brief Raw byte pointer for binary payloads. */
  const uint8_t* bytes_data() const { return reinterpret_cast<const uint8_t*>(string_val.data()); }

  /** @brief Size of the byte payload (same as `string_val.size()`). */
  size_t bytes_size() const { return string_val.size(); }

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

  /** @brief Create a binary value from a contiguous byte range. */
  static ColumnValue Bytes(ColumnType t, const uint8_t* data, size_t len) {
    ColumnValue v;
    v.type = t;
    v.is_null = false;
    v.is_binary = true;
    v.string_val.assign(reinterpret_cast<const char*>(data), len);
    return v;
  }

  /** @brief Create a binary value from a std::vector<uint8_t>. */
  static ColumnValue Bytes(ColumnType t, const std::vector<uint8_t>& val) {
    return Bytes(t, val.data(), val.size());
  }
};

/**
 * @brief Process-lifetime allocator for decoded row column arrays.
 *
 * A row is moved into a ChangeEvent and can outlive the decoder that produced
 * it, so a per-decode arena would either dangle or require copying.  A
 * synchronized pool keeps that ownership model intact while reusing the
 * small, same-shaped allocations made by RowData::columns across events and
 * consumer threads.  Individual string/blob payloads retain their normal
 * ownership and are bounded by the parser and column decoders.
 *
 * The pool is constructed on first use and never destroyed. A consumer may hold
 * an engine with static storage duration whose queued ChangeEvents still own
 * rows allocated here; an ordinary function-local static is initialized on the
 * first row decode, hence after such an engine, and would therefore be
 * destroyed before it, leaving those rows to deallocate into a pool whose
 * lifetime has already ended. Constructing into static storage that is never
 * reclaimed keeps the resource valid for the whole process lifetime whatever
 * order the consumer's own statics were initialized in, and unlike a leaked
 * heap allocation it gives the leak sanitizers nothing to report.
 */
inline std::pmr::memory_resource* RowColumnMemoryResource() {
  using PoolResource = std::pmr::synchronized_pool_resource;
  alignas(PoolResource) static unsigned char storage[sizeof(PoolResource)];
  static PoolResource* resource = new (storage) PoolResource();
  return resource;
}

/**
 * @brief A row of column values
 */
struct RowData {
  std::pmr::vector<ColumnValue> columns{RowColumnMemoryResource()};
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
  uint32_t metadata = 0;  ///< Type-specific metadata
  /// MySQL encodes multi-valued-index backing fields as TYPED_ARRAY plus
  /// the logical element type/metadata. Their row payload is still the
  /// Field_json representation (4-byte length + binary JSON).
  bool is_array = false;
  ColumnType array_element_type = ColumnType::kLong;
  bool is_nullable = true;
  bool is_unsigned = false;
  /// TABLE_MAP collation ID, when optional charset metadata was present.
  uint32_t charset_id = 0;
  bool charset_known = false;
};

/**
 * @brief Metadata for a table from TABLE_MAP event
 */
struct TableMetadata {
  uint64_t table_id = 0;
  std::string database_name;
  std::string table_name;
  std::vector<ColumnMetadata> columns;
  /// True when column signedness came from the binlog TABLE_MAP optional
  /// metadata (authoritative). When false, signedness may be filled from a
  /// metadata side-connection as a fallback.
  bool signedness_from_binlog = false;
  /// True only when every column name is known (either carried in the binlog
  /// or resolved via the metadata side-connection). Otherwise emitted events
  /// carry empty names for the unresolved columns.
  bool names_resolved = false;
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
  /// Original MariaDB SQL from the preceding ANNOTATE_ROWS event; null when the
  /// event carried none. Held by shared pointer because one ANNOTATE_ROWS
  /// annotates every row of the ROWS event that follows it: giving each row its
  /// own copy charges the statement length per row, which for an 8 KB statement
  /// dominates the queued event. All ChangeEvents from one ROWS event therefore
  /// share a single copy. Use SourceSql() to read it.
  std::shared_ptr<const std::string> source_sql;

  /**
   * @brief The annotating statement, or an empty string when there was none.
   *
   * The returned reference is valid for as long as this ChangeEvent (or any
   * other event sharing the same statement) is alive.
   */
  const std::string& SourceSql() const {
    static const std::string kNoSourceSql;
    return source_sql ? *source_sql : kNoSourceSql;
  }
  /// False when column names could not be resolved for this row's table, so
  /// column names in @ref before / @ref after are empty. See
  /// TableMetadata::names_resolved.
  bool names_resolved = false;
  /// Keeps the TABLE_MAP names alive for the string_view fields in rows.
  std::shared_ptr<const TableMetadata> table_metadata;
};

}  // namespace mes

#endif  // MES_CORE_SRC_TYPES_H_
