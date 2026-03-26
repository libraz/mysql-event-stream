// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * @file cdc_engine.h
 * @brief Main CDC engine that processes binlog streams into ChangeEvents
 *
 * Combines event stream parsing, table map tracking, and row decoding
 * into a single unified interface for binlog CDC processing.
 */

#ifndef MES_CDC_ENGINE_H_
#define MES_CDC_ENGINE_H_

#include <cstddef>
#include <cstdint>
#include <queue>
#include <set>
#include <string>
#include <vector>

#include "event_header.h"
#include "row_decoder.h"
#include "state_machine.h"
#include "table_map.h"
#include "types.h"

namespace mes {

class MetadataFetcher;

/**
 * @brief Main CDC engine that processes a binlog byte stream and produces
 *        ChangeEvents.
 *
 * Usage:
 *   CdcEngine engine;
 *   engine.Feed(data, len);
 *   ChangeEvent event;
 *   while (engine.NextEvent(&event)) {
 *     // process event
 *   }
 */
class CdcEngine {
 public:
  CdcEngine() = default;

  /**
   * @brief Feed raw binlog bytes into the engine.
   * @param data Pointer to input data.
   * @param len Number of bytes.
   * @return Number of bytes consumed.
   */
  size_t Feed(const uint8_t* data, size_t len);

  /**
   * @brief Get the next available ChangeEvent.
   * @param[out] event Output event.
   * @return true if an event was available, false if queue is empty.
   */
  bool NextEvent(ChangeEvent* event);

  /** @brief Check if there are pending ChangeEvents. */
  bool HasEvents() const;

  /** @brief Get current binlog position. */
  const BinlogPosition& CurrentPosition() const;

  /** @brief Reset engine state, clearing all buffers and registries. */
  void Reset();

  /** @brief Get number of pending events in queue. */
  size_t PendingEventCount() const;

  /** @brief Set maximum event queue size. 0 means unlimited. */
  void SetMaxQueueSize(size_t max_size);

  /** @brief Check if the engine is in an error state (e.g., parse error). */
  bool IsError() const;

  /** @brief Set database filter. Only events from these databases are processed. Empty = all. */
  void SetIncludeDatabases(const std::vector<std::string>& databases);

  /** @brief Set table include filter. Only events from these tables are processed. Empty = all.
   *  Format: "database.table" or just "table" (matches any database). */
  void SetIncludeTables(const std::vector<std::string>& tables);

  /** @brief Set table exclude filter. Events from these tables are skipped.
   *  Format: "database.table" or just "table" (matches any database). */
  void SetExcludeTables(const std::vector<std::string>& tables);

  /** @brief Set metadata fetcher for column name resolution */
  void SetMetadataFetcher(MetadataFetcher* fetcher);

 private:
  void ProcessEvent(const EventHeader& header, const uint8_t* body, size_t body_len);
  void ProcessRowEvent(const EventHeader& header, const uint8_t* body, size_t body_len);

  EventStreamParser stream_parser_;
  TableMapRegistry table_registry_;
  BinlogPosition position_;
  std::queue<ChangeEvent> event_queue_;
  size_t max_queue_size_ = 0;

  bool IsTableAllowed(const std::string& database, const std::string& table) const;

  std::set<std::string> include_databases_;
  std::set<std::string> include_tables_;
  std::set<std::string> exclude_tables_;
  std::set<uint64_t> blocked_table_ids_;

  MetadataFetcher* metadata_fetcher_ = nullptr;
};

}  // namespace mes

#endif  // MES_CDC_ENGINE_H_
