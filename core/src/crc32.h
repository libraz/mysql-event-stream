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

#include <zlib.h>

namespace mes {

/// @brief Compute CRC32 checksum using zlib.
inline uint32_t ComputeCRC32(const void* data, size_t length) {
  return static_cast<uint32_t>(
      crc32(0L, reinterpret_cast<const Bytef*>(data), static_cast<uInt>(length)));
}

}  // namespace mes

#endif  // MES_CRC32_H_
