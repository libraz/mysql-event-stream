// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * @file rotate_event.h
 * @brief ROTATE_EVENT parsing for binlog file rotation tracking
 */

#ifndef MES_ROTATE_EVENT_H_
#define MES_ROTATE_EVENT_H_

#include <cstddef>
#include <cstdint>
#include <string>

namespace mes {

/// Parsed data from a ROTATE_EVENT.
struct RotateEventData {
  uint64_t position = 0;
  std::string new_log_file;
};

/**
 * @brief Parse a ROTATE_EVENT body.
 * @param data Pointer to event body (after 19-byte header).
 * @param len Length of event body (excluding checksum).
 * @param[out] result Parsed rotate event data.
 * @return true if parsing succeeded.
 */
bool ParseRotateEvent(const uint8_t* data, size_t len, RotateEventData* result);

}  // namespace mes

#endif  // MES_ROTATE_EVENT_H_
