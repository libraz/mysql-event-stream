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
#include <memory>
#include <queue>
#include <string>
#include <unordered_set>
#include <vector>

#include "event_header.h"
#include "mes.h"
#include "row_decoder.h"
#include "state_machine.h"
#include "table_map.h"
#include "types.h"

namespace mes {

class MetadataFetcher;

/**
 * @brief Determine whether a QUERY_EVENT body carries a DDL statement.
 *
 * Parses the QUERY_EVENT body to locate the SQL statement and checks whether it
 * begins with a schema-altering verb (ALTER/RENAME/DROP/CREATE/TRUNCATE),
 * after MySQL whitespace, line comments, block comments, or a version-comment
 * prefix.
 * Exposed for testing. Returns false for non-DDL statements (e.g. BEGIN) and
 * for malformed/too-short bodies.
 *
 * @param body QUERY_EVENT body (header already stripped).
 * @param body_len Length of the body.
 * @return true if the statement is DDL.
 */
bool IsDdlQueryEvent(const uint8_t* body, size_t body_len);

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
  ~CdcEngine();

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

  /**
   * @brief Set maximum event queue size. 0 restores the bounded default.
   *
   * The limit is enforced per binlog event, not per row. A single multi-row
   * WRITE_ROWS/UPDATE_ROWS/DELETE_ROWS event is pushed atomically, so the
   * queue may temporarily exceed @p max_size by up to (rows_per_event - 1)
   * items before backpressure is re-evaluated on the next Feed() iteration.
   *
   * Nothing set here bounds rows_per_event: it is bounded by how many rows fit
   * in one event body, which makes SetMaxEventSize() the lever over the entry
   * overshoot rather than this setting. The bytes that overshoot can occupy are
   * bounded on their own terms by SetMaxQueueBytes().
   */
  void SetMaxQueueSize(size_t max_size);

  /** @brief Get the effective event queue limit. */
  size_t MaxQueueSize() const;

  /**
   * @brief Set the byte budget for the decoded event queue. 0 restores the
   *        MES_DEFAULT_QUEUE_BYTES default.
   *
   * An entry count alone cannot bound this queue's memory: a compressed column
   * decodes to a size its on-wire length does not predict, so one event of a few
   * kilobytes can materialize megabytes of column payload. The budget bounds the
   * sum instead. Every queued event is charged ChangeEventCharge(), Feed() stops
   * consuming input once the charge reaches the budget, and the per-event decode
   * budget is capped by it as well, so:
   *
   *   resident queue bytes < max_queue_bytes + max(max_event_size, max_queue_bytes)
   *
   * The second term is the one event Feed() pushes atomically after the last
   * capacity check: its decoded column payloads cannot exceed
   * DecodeBudget::ForEventBody(body_bytes, max_queue_bytes), and its column
   * array follows the wire body, which the parser caps at max_event_size.
   * Nothing in that bound depends on a compressed column's expansion ratio.
   */
  void SetMaxQueueBytes(size_t max_queue_bytes);

  /** @brief Get the effective event queue byte budget. */
  size_t MaxQueueBytes() const;

  /** @brief Bytes currently charged to the pending event queue. */
  size_t QueuedBytes() const;

  /** @brief Check if the engine is in an error state (e.g., parse/decode error). */
  bool IsError() const;

  /**
   * @brief Return a specific error code describing the current error state.
   *
   * Returns MES_OK when the engine is not in an error state. Otherwise maps
   * internal parser/decoder failures to the canonical mes_error_t value so
   * the C ABI layer can surface the precise cause rather than collapsing
   * everything into MES_ERR_PARSE.
   */
  mes_error_t ErrorCode() const;

  /** @brief Set database filter. Only events from these databases are processed. Empty = all. */
  void SetIncludeDatabases(const std::vector<std::string>& databases);

  /** @brief Set table include filter. Only events from these tables are processed. Empty = all.
   *  Format: "database.table" or just "table" (matches any database).
   *  A trailing '*' performs a case-sensitive prefix match. */
  void SetIncludeTables(const std::vector<std::string>& tables);

  /** @brief Set table exclude filter. Events from these tables are skipped.
   *  Format: "database.table" or just "table" (matches any database).
   *  A trailing '*' performs a case-sensitive prefix match. */
  void SetExcludeTables(const std::vector<std::string>& tables);

  /** @brief Set metadata fetcher for column name resolution */
  void SetMetadataFetcher(MetadataFetcher* fetcher);

  /**
   * @brief Override the maximum per-event size accepted by the parser.
   *
   * See EventStreamParser::SetMaxEventSize() for semantics. Intended for
   * workloads that produce binlog events larger than the 64 MiB default
   * (e.g. very large BLOB/JSON columns with max_allowed_packet raised).
   * Out-of-range values are clamped.
   */
  void SetMaxEventSize(uint32_t max_event_size);

  /** @brief Get the currently configured maximum event size (bytes). */
  uint32_t MaxEventSize() const;

  /**
   * @brief Set whether fed events carry a trailing 4-byte CRC32 checksum.
   *
   * Defaults to true. Set to false when feeding raw bytes from a stream
   * produced with binlog_checksum=NONE and no FORMAT_DESCRIPTION_EVENT.
   * If the stream contains an FDE, the checksum algorithm is auto-detected
   * and this setting is overridden.
   */
  void SetChecksumEnabled(bool enabled);

 private:
  void ProcessEvent(const EventHeader& header, const uint8_t* body, size_t body_len);
  void ProcessRowEvent(const EventHeader& header, const uint8_t* body, size_t body_len);
  /** @brief Queue one decoded event, charging what it keeps resident. */
  void EnqueueEvent(ChangeEvent&& event);
  /** @brief Whether the queue has reached either configured limit. */
  bool QueueAtCapacity() const;
  void LogRowDecodeFailure(const char* kind, const TableMetadata& meta);
  /**
   * @brief Set the resume filename, refreshing the copy emitted events share.
   *
   * The sole writer of the current position's filename, so the copy shared with
   * emitted events cannot name a different file than CurrentPosition() reports.
   */
  void SetResumeBinlogFile(std::string binlog_file);
  /** @brief Resume coordinates to stamp on the events decoded from an event. */
  EventPosition CurrentEventPosition() const;
  bool HasIncludeFilters() const;
  bool MatchesTableFilter(const std::unordered_set<std::string>& filters,
                          const std::string& database, const std::string& table) const;
  bool MatchesIncludeFilters(const std::string& database, const std::string& table) const;
  void NoteTableMapForIncludeFilters(const TableMetadata& metadata);
  void WarnIfIncludeFiltersMatchedNothing();
  void ResetIncludeFilterMatchState();

  EventStreamParser stream_parser_;
  TableMapRegistry table_registry_;
  BinlogPosition position_;
  // The resume filename of position_, shared by every ChangeEvent decoded while
  // it applies, so the filename is copied once per rotation rather than once per
  // row. Null until a ROTATE event supplies a filename.
  std::shared_ptr<const std::string> position_binlog_file_;
  // Shared with every ChangeEvent decoded from the ROWS event this annotates,
  // so an ANNOTATE_ROWS statement is stored once per event rather than once
  // per row. Null when no annotation is in effect.
  std::shared_ptr<const std::string> pending_source_sql_;
  std::queue<ChangeEvent> event_queue_;
  // Raw-feed users, including CdcStream, must not accumulate an unbounded
  // ChangeEvent queue when a producer temporarily outpaces the consumer.
  // This matches the asynchronous client's event-count default.
  size_t max_queue_size_ = MES_DEFAULT_QUEUE_SIZE;
  // The count above bounds entries; this bounds the bytes behind them, which an
  // entry count cannot (see SetMaxQueueBytes()). Same default as the client's
  // wire-event queue, so the two stages of one pipeline agree.
  size_t max_queue_bytes_ = MES_DEFAULT_QUEUE_BYTES;
  size_t queued_bytes_ = 0;

  // Scratch buffers reused across ProcessRowEvent calls to avoid a
  // per-event heap allocation in the hot decode path. clear() preserves
  // the allocated capacity, so after a warm-up the row decoder runs
  // allocation-free (apart from the per-row ColumnValue payloads
  // themselves). Not thread-safe; reuse matches the engine's single-
  // owner-thread contract.
  std::vector<RowData> row_buf_;
  std::vector<UpdatePair> update_buf_;

  // Last specific error code set by engine-level processing, such as row
  // decode failures. Parser-only failures are represented by stream_parser_.
  mes_error_t last_error_ = MES_OK;

  bool IsTableAllowed(const std::string& database, const std::string& table) const;
  void RebuildBlockedTableIds();

  std::unordered_set<std::string> include_databases_;
  std::unordered_set<std::string> include_tables_;
  std::unordered_set<std::string> exclude_tables_;
  std::unordered_set<uint64_t> blocked_table_ids_;
  bool include_filter_saw_table_map_ = false;
  bool include_filter_matched_ = false;

  // Non-owning pointer. Caller must ensure the MetadataFetcher outlives this CdcEngine.
  // Set via SetMetadataFetcher(). May be null if metadata resolution is not configured.
  MetadataFetcher* metadata_fetcher_ = nullptr;
};

}  // namespace mes

#endif  // MES_CDC_ENGINE_H_
