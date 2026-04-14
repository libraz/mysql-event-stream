// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * @file crc32.h
 * @brief CRC32 checksum computation for binlog event verification
 */

#ifndef MES_CRC32_H_
#define MES_CRC32_H_

#include <cstddef>
#include <cstdint>
#include <limits>

#include <zlib.h>

namespace mes {

/// @brief Compute CRC32 checksum using zlib.
/// @note Callers must ensure length <= kMaxEventSize (64 MB). The zlib
///       overflow guard below is a defence-in-depth measure; it is
///       unreachable under normal operation.
inline uint32_t ComputeCRC32(const void* data, size_t length) {
  static_assert(sizeof(uInt) >= 4, "zlib uInt must be at least 32 bits");
  // Unreachable: binlog events are limited to kMaxEventSize (64 MB).
  // Return UINT32_MAX as a sentinel that won't collide with a stored
  // checksum (MySQL CRC32 never produces UINT32_MAX for valid data in
  // practice, though it is technically a valid CRC32 value).
  if (length > static_cast<size_t>(std::numeric_limits<uInt>::max())) {
    return UINT32_MAX;
  }
  return static_cast<uint32_t>(
      crc32(0L, reinterpret_cast<const Bytef*>(data), static_cast<uInt>(length)));
}

}  // namespace mes

#endif  // MES_CRC32_H_
