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

 private:
  ParserState state_ = ParserState::kWaitingHeader;
  std::vector<uint8_t> buffer_;
  EventHeader current_header_{};
  size_t bytes_needed_ = kEventHeaderSize;
};

}  // namespace mes

#endif  // MES_STATE_MACHINE_H_
