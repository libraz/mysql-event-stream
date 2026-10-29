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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <memory_resource>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "mybinlog/event_types.h"
#include "mybinlog/table_map.h"

namespace mes {

/**
 * @brief MySQL column type code, as carried in TABLE_MAP.
 *
 * The codec's enumeration under the engine's historical name.
 */
using ColumnType = mybinlog::ColumnType;

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
 * @brief Memory resource backing the column array of every decoded row.
 *
 * Every RowData in the process shares this one stateless resource, and that
 * is a correctness constraint rather than a convenience.
 * std::pmr::polymorphic_allocator has
 * propagate_on_container_move_assignment == false, so a move between two
 * pmr containers is the O(1) pointer steal it looks like only while their
 * allocators compare equal; between unequal resources the same move silently
 * degrades into an element-wise copy, with no diagnostic. A row is moved from
 * the decoder into the engine's queue and out again through NextEvent(), so a
 * per-engine, per-thread or per-decode resource would turn each of those hops
 * into a deep copy of the row. A single stateless resource keeps every
 * allocator equal and every hop O(1).
 *
 * std::pmr::new_delete_resource() also returns a pointer valid for the whole
 * program, which rows outliving their engine need: a consumer may hold an
 * engine with static storage duration whose queued events still own rows when
 * its destructor runs during process teardown.
 *
 * Individual string/blob payloads retain their normal ownership and are
 * bounded by the parser and column decoders.
 */
inline std::pmr::memory_resource* RowColumnMemoryResource() {
  return std::pmr::new_delete_resource();
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
 * @brief Resume coordinates carried by an emitted ChangeEvent.
 *
 * The filename is held by shared pointer because every event decoded from one
 * binlog file resumes from the same filename: giving each row its own copy
 * charges the filename length per row. One copy is made per rotation and shared
 * by every event that follows it. Use BinlogFile() to read it.
 */
struct EventPosition {
  std::shared_ptr<const std::string> binlog_file;
  /// Offset to resume from after consuming the event this position describes.
  uint64_t offset = 0;

  /**
   * @brief The binlog filename, or an empty string when none is known yet.
   *
   * The returned reference is valid for as long as this position (or any other
   * position sharing the same filename) is alive.
   */
  const std::string& BinlogFile() const {
    static const std::string kNoBinlogFile;
    return binlog_file ? *binlog_file : kNoBinlogFile;
  }
};

/**
 * @brief A registered TABLE_MAP and what the engine resolved for it.
 *
 * The parsed TABLE_MAP is the base. Column names and signedness that a metadata
 * side-connection supplies are written into a copy that replaces the registered
 * object (see TableMapRegistry::ReplaceMetadata()), never into one already
 * handed out.
 */
struct TableMetadata : mybinlog::TableMap {
  /// True only when every column name is known (either carried in the binlog
  /// or resolved via the metadata side-connection). Otherwise emitted events
  /// carry empty names for the unresolved columns.
  bool names_resolved = false;
  /// Set once it has been reported that signedness is available from neither
  /// source, so the report is made once per table instead of once per event.
  bool unknown_signedness_reported = false;
};

/**
 * @brief Bytes of variable-length content @p metadata retains.
 *
 * Its names, its column array and every column name and ENUM/SET label in it.
 * The fixed TableMetadata structure is not counted; callers that charge it add
 * it themselves.
 */
inline size_t TableMetadataContentBytes(const TableMetadata& metadata) {
  size_t bytes = metadata.schema.size() + metadata.table.size() +
                 metadata.columns.size() * sizeof(mybinlog::TableMapColumn);
  for (const mybinlog::TableMapColumn& column : metadata.columns) {
    bytes += column.name.size() + column.labels.size() * sizeof(std::string);
    for (const std::string& label : column.labels) bytes += label.size();
  }
  return bytes;
}

/**
 * @brief A CDC change event representing a single row change
 */
struct ChangeEvent {
  EventType type = EventType::kInsert;
  /// Database name, borrowed from @ref table_metadata like the column names in
  /// @ref before / @ref after are. The view always spans a whole owning
  /// std::string, so its data() is NUL-terminated; a subrange view would break
  /// the C ABI's NUL-terminated `const char*`.
  std::string_view database;
  /// Table name, borrowed from @ref table_metadata; see @ref database.
  std::string_view table;
  RowData before;  ///< Populated for UPDATE and DELETE
  RowData after;   ///< Populated for INSERT and UPDATE
  uint32_t timestamp = 0;
  EventPosition position;
  /// Original MariaDB SQL from the preceding ANNOTATE_ROWS event; null when the
  /// event carried none. The server emits one ANNOTATE_ROWS per statement
  /// rather than per ROWS event: a statement whose row data exceeds the
  /// server's per-event size limit is split across several consecutive ROWS
  /// events that share that one annotation. The statement therefore stands
  /// until the next annotation or the next control event, and covers every row
  /// the statement produced. Held by shared pointer because giving each row its
  /// own copy charges the statement length per row, which for an 8 KB statement
  /// dominates the queued event. Use SourceSql() to read it.
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

/**
 * @brief Bytes one queued entry charges to a queue's byte budget.
 *
 * The single definition of what a `max_queue_bytes` limit counts, shared by the
 * client's wire-event queue (see client/event_queue.h) and the engine's decoded
 * event queue (see cdc_engine.h): an entry is charged for every byte it keeps
 * resident, both the payload it carries and the bookkeeping that travels with
 * it. Charging both is what makes a queue's resident bytes bounded by its own
 * configured budget rather than by a quantity the caller cannot size -- the
 * number of distinct GTID source UUIDs in the replication history, or a
 * compressed column's decoded-to-wire expansion ratio.
 *
 * Fixed per-entry structure -- the queue node and the entry struct itself -- is
 * not charged here. It is proportional to the entry count, which every queue
 * bounds separately; the engine's decoder charges it to the per-event decode
 * budget instead, where the entry count of one event is otherwise unbounded.
 */
constexpr size_t QueuedEventCharge(size_t payload_bytes, size_t bookkeeping_bytes) {
  return payload_bytes + bookkeeping_bytes;
}

/** @brief Bytes @p row keeps resident: its column array plus every payload in it. */
inline size_t RowDataCharge(const RowData& row) {
  size_t bytes = row.columns.size() * sizeof(ColumnValue);
  for (const ColumnValue& column : row.columns) {
    bytes += column.string_val.size();
  }
  return bytes;
}

/**
 * @brief Bytes a queued ChangeEvent charges to the engine's queue byte budget.
 *
 * The decoded column payloads are the term that matters: a compressed column's
 * on-wire length says nothing about its decoded size, so no entry count can
 * bound them.
 *
 * Two deliberate approximations, both on the safe side of a memory bound:
 *   - A payload shared between entries -- the ANNOTATE_ROWS statement, the
 *     database and table names, the binlog filename -- is charged to every
 *     entry that references it, so the total charge is an upper bound on
 *     resident bytes rather than an exact count.
 *   - The TableMetadata behind @ref ChangeEvent::table_metadata is charged to
 *     every entry that pins it for the same reason. Charging it is what keeps
 *     the bound true after the table-map registry evicts or replaces the
 *     instance while queued events still hold it.
 */
inline size_t ChangeEventCharge(const ChangeEvent& event) {
  size_t metadata_bytes = 0;
  if (event.table_metadata) {
    metadata_bytes = sizeof(TableMetadata) + TableMetadataContentBytes(*event.table_metadata);
  }
  return QueuedEventCharge(RowDataCharge(event.before) + RowDataCharge(event.after),
                           event.database.size() + event.table.size() +
                               event.position.BinlogFile().size() + event.SourceSql().size() +
                               metadata_bytes);
}

}  // namespace mes

#endif  // MES_CORE_SRC_TYPES_H_
