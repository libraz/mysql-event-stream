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
   * @brief Process a TABLE_MAP_EVENT body and register the table.
   * @param data Pointer to the event body (after header).
   * @param len Length of the event body (excluding checksum).
   * @return true if successfully parsed and registered.
   */
  bool ProcessTableMapEvent(const uint8_t* data, size_t len);

  /**
   * @brief Look up table metadata by table_id.
   * @return Pointer to metadata, or nullptr if not found.
   */
  const TableMetadata* Lookup(uint64_t table_id) const;

  /**
   * @brief Look up mutable table metadata by table_id.
   * @return Pointer to metadata, or nullptr if not found.
   */
  TableMetadata* MutableLookup(uint64_t table_id);

  /** @brief Clear all registered tables. */
  void Clear();

  /** @brief Get number of registered tables. */
  size_t Size() const;

 private:
  std::unordered_map<uint64_t, TableMetadata> entries_;
};

}  // namespace mes

#endif  // MES_TABLE_MAP_H_
