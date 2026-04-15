// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * @file crc32.h
 * @brief CRC32 checksum computation for binlog event verification
 */

#ifndef MES_CRC32_H_
#define MES_CRC32_H_

#include <zlib.h>

#include <cstddef>
#include <cstdint>
#include <limits>

namespace mes {

/// @brief Compute CRC32 checksum using zlib.
/// @note Callers must ensure length <= the configured max event size
///       (see EventStreamParser::SetMaxEventSize, capped at
///       kAbsoluteMaxEventSize = 1 GiB). The zlib overflow guard below
///       is a defence-in-depth measure; it is unreachable under normal
///       operation because the parser rejects over-sized events before
///       the CRC step runs.
inline uint32_t ComputeCRC32(const void* data, size_t length) {
  static_assert(sizeof(uInt) >= 4, "zlib uInt must be at least 32 bits");
  // Return UINT32_MAX as a "not computed" sentinel.
  // NOTE(review): Sentinel value collision with a real CRC is astronomically
  // unlikely (1 in 2^32) and this path is already unreachable under the
  // event-size limit enforced in state_machine.cpp. Callers treat
  // this as "not yet computed" rather than as a CRC comparison target, so
  // even a theoretical collision would be surfaced as a checksum mismatch
  // on the next real event rather than silent data corruption.
  if (length > static_cast<size_t>(std::numeric_limits<uInt>::max())) {
    return UINT32_MAX;
  }
  return static_cast<uint32_t>(
      crc32(0L, reinterpret_cast<const Bytef*>(data), static_cast<uInt>(length)));
}

}  // namespace mes

#endif  // MES_CRC32_H_
