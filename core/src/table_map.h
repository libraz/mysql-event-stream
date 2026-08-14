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
   * @brief Process a TABLE_MAP_EVENT body and register the table.
   * @param data Pointer to the event body (after header).
   * @param len Length of the event body (excluding checksum).
   * @return true if successfully parsed and registered.
   */
  bool ProcessTableMapEvent(const uint8_t* data, size_t len, uint64_t* evicted_table_id = nullptr,
                            bool* unchanged = nullptr);

  /**
   * @brief Look up table metadata by table_id.
   * @return Pointer to metadata, or nullptr if not found.
   */
  const TableMetadata* Lookup(uint64_t table_id);

  /** @brief Look up metadata with lifetime retained for queued ChangeEvents. */
  std::shared_ptr<const TableMetadata> SharedLookup(uint64_t table_id);

  /**
   * @brief Look up mutable table metadata by table_id.
   * @return Pointer to metadata, or nullptr if not found.
   */
  TableMetadata* MutableLookup(uint64_t table_id);

  /** @brief Clear all registered tables. */
  void Clear();

  /** @brief Get number of registered tables. */
  size_t Size() const;

  /** @brief Visit every registered table without exposing the registry container. */
  void ForEach(const std::function<void(uint64_t, const TableMetadata&)>& visitor) const;

 private:
  struct Entry {
    std::shared_ptr<TableMetadata> metadata;
    std::vector<uint8_t> raw_body;
    std::list<uint64_t>::iterator lru_position;
  };

  void Touch(std::unordered_map<uint64_t, Entry>::iterator it);

  std::unordered_map<uint64_t, Entry> entries_;
  std::list<uint64_t> lru_;  // Most recently used at the front.
};

}  // namespace mes

#endif  // MES_TABLE_MAP_H_
