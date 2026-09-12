// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * @file table_map.h
 * @brief TABLE_MAP_EVENT parsing and table metadata registry
 *
 * Parses MySQL TABLE_MAP binlog events and maintains a registry of
 * table metadata keyed by table_id for use when decoding row events.
 */

#ifndef MES_TABLE_MAP_H_
#define MES_TABLE_MAP_H_

#include <cstddef>
#include <cstdint>
#include <functional>
#include <list>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "types.h"

namespace mes {

/**
 * @brief Whether a column receives a bit in the TABLE_MAP SIGNEDNESS bitmap.
 *
 * Mirrors MySQL's Field::has_signedness_information_type(), which decides this
 * from the binlog column type: TYPED_ARRAY itself has no bit, even when its
 * element type is numeric.
 *
 * @param column Column metadata parsed from a TABLE_MAP event.
 * @return true when the column's declared type carries signedness.
 */
bool IsNumericColumnType(const ColumnMetadata& column);

/**
 * @brief Parse a TABLE_MAP_EVENT body into a TableMetadata structure.
 * @param data Pointer to the event body (after the 19-byte header).
 * @param len Length of the event body (excluding checksum).
 * @param[out] metadata Parsed table metadata output.
 * @return true if parsing succeeded, false on malformed data.
 */
bool ParseTableMapEvent(const uint8_t* data, size_t len, TableMetadata* metadata);

/**
 * @brief Registry that caches TABLE_MAP_EVENT metadata by table_id.
 *
 * TABLE_MAP events precede row events and define the schema for decoding.
 */
class TableMapRegistry {
 public:
  /**
   * @brief Number of TABLE_MAP entries retained before eviction begins.
   *
   * Long-running replication cycles through many table_ids (heavy DDL gives
   * each new table definition a fresh one), so the registry is bounded. Once
   * it is full, registering an unseen table_id evicts the least recently used
   * entry -- only that one entry, not the whole cache. Eviction is safe
   * because the server re-emits a TABLE_MAP event before every ROWS event, so
   * an evicted table_id is repopulated the next time its rows arrive.
   */
  static constexpr size_t kMaxEntries = 8192;

  /**
   * @brief Retained metadata bytes held before eviction begins.
   *
   * The entry count alone cannot bound this registry's memory. Per-entry cost
   * is set by schema content the client does not choose -- identifier lengths,
   * column count, and whether the server sends column names at all -- so a
   * source of wide tables under binlog_row_metadata=FULL retains orders of
   * magnitude more per entry than a narrow one. Registration therefore evicts
   * on retained bytes as well, in the same LRU order.
   *
   * Sized as a third of MES_DEFAULT_QUEUE_BYTES, the decoded event queue's
   * default budget: schema metadata is what the decoded payload is read
   * against, so it should stay well below the payload's own allowance. Spread
   * across kMaxEntries that is 2 KiB per entry, more than an ordinary schema
   * retains, which leaves the entry count the binding bound in ordinary use
   * and this one engaging only on the wide-table case it exists for.
   */
  static constexpr size_t kMaxRetainedBytes = 16u * 1024 * 1024;

  /**
   * @brief Process a TABLE_MAP_EVENT body and register the table.
   *
   * Registering may evict more than one entry, because a single wide table can
   * retain what several narrow ones did.
   *
   * @param data Pointer to the event body (after header).
   * @param len Length of the event body (excluding checksum).
   * @param[out] evicted_table_ids Appended with the table_id of every entry
   *        eviction removed, in the order they were removed.
   * @param[out] unchanged Set when the body was byte-identical to the one the
   *        table_id is already registered from.
   * @return true if successfully parsed and registered.
   */
  bool ProcessTableMapEvent(const uint8_t* data, size_t len,
                            std::vector<uint64_t>* evicted_table_ids = nullptr,
                            bool* unchanged = nullptr);

  /**
   * @brief Look up table metadata by table_id.
   * @return Pointer to metadata, or nullptr if not found.
   */
  const TableMetadata* Lookup(uint64_t table_id);

  /** @brief Look up metadata with lifetime retained for queued ChangeEvents. */
  std::shared_ptr<const TableMetadata> SharedLookup(uint64_t table_id);

  /**
   * @brief Install replacement metadata for an already-registered table.
   *
   * A queued ChangeEvent reads its column names out of the TableMetadata it was
   * decoded against, holding it alive through the shared_ptr SharedLookup()
   * returned. Metadata a registration has already handed out must therefore
   * never be written in place: a resolution that only becomes available later
   * installs a new object here instead, so what earlier events report -- and
   * the views they hold into it -- stay as they were decoded.
   *
   * The entry keeps the raw TABLE_MAP body it was registered from, because the
   * replacement describes that same body; only the metadata derived from it is
   * exchanged. Like every other path that reaches an entry, this marks it as
   * most recently used.
   *
   * @param table_id table_id of the registered table.
   * @param metadata Metadata to install in place of the entry's current object.
   * @param[out] evicted_table_ids Appended with the table_id of every entry
   *        eviction removed: resolved column names grow what the entry
   *        retains, which can put the registry over its byte budget.
   * @return true when the table was registered and the metadata was installed.
   */
  bool ReplaceMetadata(uint64_t table_id, TableMetadata metadata,
                       std::vector<uint64_t>* evicted_table_ids = nullptr);

  /** @brief Clear all registered tables. */
  void Clear();

  /** @brief Get number of registered tables. */
  size_t Size() const;

  /**
   * @brief Bytes of variable-length metadata the registered entries retain.
   *
   * Counts what a schema's own content sizes: each entry's raw TABLE_MAP body,
   * its database and table names, its column array and every column name in
   * it. Fixed per-entry structure is not counted, following the same split as
   * QueuedEventCharge(): it is proportional to the entry count, which
   * kMaxEntries bounds on its own.
   */
  size_t RetainedBytes() const;

  /** @brief Visit every registered table without exposing the registry container. */
  void ForEach(const std::function<void(uint64_t, const TableMetadata&)>& visitor) const;

 private:
  struct Entry {
    std::shared_ptr<TableMetadata> metadata;
    std::vector<uint8_t> raw_body;
    std::list<uint64_t>::iterator lru_position;
  };

  static size_t EntryCharge(const Entry& entry);

  void Touch(std::unordered_map<uint64_t, Entry>::iterator it);

  /**
   * @brief Drop least recently used entries until both bounds are satisfied.
   *
   * The entry a registration just installed is the one the ROWS event that
   * follows will decode against, and it is the most recently used, so it is
   * kept even when it alone exceeds the byte budget: the registry never
   * shrinks below one entry.
   */
  void EvictUntilWithinBounds(std::vector<uint64_t>* evicted_table_ids);

  std::unordered_map<uint64_t, Entry> entries_;
  std::list<uint64_t> lru_;  // Most recently used at the front.
  size_t retained_bytes_ = 0;
};

}  // namespace mes

#endif  // MES_TABLE_MAP_H_
