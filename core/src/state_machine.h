// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * @file state_machine.h
 * @brief Streaming binlog event parser state machine
 *
 * Accepts arbitrary-sized chunks of binary data and reassembles
 * complete binlog events for processing.
 */

#ifndef MES_STATE_MACHINE_H_
#define MES_STATE_MACHINE_H_

#include <cstddef>
#include <cstdint>
#include <vector>

#include "event_header.h"

namespace mes {

/// Default upper bound on a single binlog event size (64 MiB). Matches
/// MySQL's default max_allowed_packet for binlog events. Callers with
/// very large BLOB/JSON columns can raise the ceiling at runtime via
/// EventStreamParser::SetMaxEventSize() (or mes_set_max_event_size()
/// at the C ABI).
constexpr uint32_t kDefaultMaxEventSize = 64u * 1024u * 1024u;

/// Hard upper bound on the configurable event-size ceiling (1 GiB).
/// Matches MySQL's maximum max_allowed_packet. Attempts to set a
/// larger value are rejected to avoid unbounded per-event allocation.
constexpr uint32_t kAbsoluteMaxEventSize = 1024u * 1024u * 1024u;

/// Parser state for streaming binlog data.
enum class ParserState : uint8_t {
  kWaitingHeader,  ///< Accumulating 19-byte header
  kWaitingBody,    ///< Accumulating event body
  kEventReady,     ///< Complete event available
  kError,          ///< Unrecoverable error
};

/**
 * @brief Streaming binlog event parser.
 *
 * Accepts arbitrary-sized chunks of data and reassembles complete events.
 */
class EventStreamParser {
 public:
  /**
   * @brief Feed raw bytes into the parser.
   * @param data Pointer to input data.
   * @param len Number of bytes available.
   * @return Number of bytes consumed.
   */
  size_t Feed(const uint8_t* data, size_t len);

  /** @brief Check if a complete event is available. */
  bool HasEvent() const;

  /**
   * @brief Get the current event header (valid only when HasEvent() is true).
   */
  const EventHeader& CurrentHeader() const;

  /**
   * @brief Get the current event body data (after header, before checksum).
   *
   * Valid only when HasEvent() is true.
   * @param[out] body_data Pointer to body data.
   * @param[out] body_len Length of body data.
   */
  void CurrentBody(const uint8_t** body_data, size_t* body_len) const;

  /**
   * @brief Get the full raw event data (header + body + checksum).
   *
   * Valid only when HasEvent() is true.
   */
  const uint8_t* RawData() const;

  /** @brief Get the full raw event size. Valid only when HasEvent() is true. */
  size_t RawSize() const;

  /** @brief Advance past the current event and prepare for the next one. */
  void Advance();

  /** @brief Reset the parser to initial state, discarding any buffered data. */
  void Reset();

  /** @brief Get the current parser state. */
  ParserState GetState() const;

  /**
   * @brief Override the maximum per-event size accepted by the parser.
   *
   * Events larger than this value cause Feed() to enter kError with a
   * "event_too_large" log. Callable at any time; takes effect on the
   * next event whose header is parsed. Values outside the range
   * [kEventHeaderSize + kChecksumSize, kAbsoluteMaxEventSize] are
   * clamped to the nearest valid bound so the parser cannot be
   * configured into an unusable state.
   */
  void SetMaxEventSize(uint32_t max_event_size);

  /** @brief Get the current maximum event size (in bytes). */
  uint32_t MaxEventSize() const;

 private:
  ParserState state_ = ParserState::kWaitingHeader;
  std::vector<uint8_t> buffer_;
  EventHeader current_header_{};
  size_t bytes_needed_ = kEventHeaderSize;
  uint32_t max_event_size_ = kDefaultMaxEventSize;
};

}  // namespace mes

#endif  // MES_STATE_MACHINE_H_
