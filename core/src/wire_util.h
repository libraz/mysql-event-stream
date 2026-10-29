// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * @file wire_util.h
 * @brief Fixed-width integer and length-encoded integer reads for the MySQL wire protocol
 *
 * Protocol-level helpers only: binlog event bodies are interpreted by
 * libmybinlog. Callers bounds-check before reading; none of the fixed-width
 * readers checks the buffer length.
 */

#ifndef MES_CORE_SRC_WIRE_UTIL_H_
#define MES_CORE_SRC_WIRE_UTIL_H_

#include <cstddef>
#include <cstdint>
#include <string_view>

namespace mes::wire {

// --- Little-endian integer reads ---

/** @brief Read 1 byte unsigned integer */
inline uint8_t ReadU8(const uint8_t* data) { return data[0]; }

/** @brief Read 2 bytes in little-endian format */
inline uint16_t ReadU16Le(const uint8_t* data) {
  return static_cast<uint16_t>(data[0]) | (static_cast<uint16_t>(data[1]) << 8);
}

/** @brief Read 3 bytes in little-endian format */
inline uint32_t ReadU24Le(const uint8_t* data) {
  return static_cast<uint32_t>(data[0]) | (static_cast<uint32_t>(data[1]) << 8) |
         (static_cast<uint32_t>(data[2]) << 16);
}

/** @brief Read 4 bytes in little-endian format */
inline uint32_t ReadU32Le(const uint8_t* data) {
  return static_cast<uint32_t>(data[0]) | (static_cast<uint32_t>(data[1]) << 8) |
         (static_cast<uint32_t>(data[2]) << 16) | (static_cast<uint32_t>(data[3]) << 24);
}

/** @brief Read 6 bytes in little-endian format */
inline uint64_t ReadU48Le(const uint8_t* data) {
  return static_cast<uint64_t>(ReadU32Le(data)) |
         (static_cast<uint64_t>(ReadU16Le(data + 4)) << 32);
}

/** @brief Read 8 bytes in little-endian format */
inline uint64_t ReadU64Le(const uint8_t* data) {
  return static_cast<uint64_t>(ReadU32Le(data)) |
         (static_cast<uint64_t>(ReadU32Le(data + 4)) << 32);
}

// --- Big-endian integer reads ---

/** @brief Read 2 bytes in big-endian format */
inline uint16_t ReadU16Be(const uint8_t* data) {
  return static_cast<uint16_t>((static_cast<uint16_t>(data[0]) << 8) |
                               static_cast<uint16_t>(data[1]));
}

/** @brief Read 3 bytes in big-endian format */
inline uint32_t ReadU24Be(const uint8_t* data) {
  return (static_cast<uint32_t>(data[0]) << 16) | (static_cast<uint32_t>(data[1]) << 8) |
         static_cast<uint32_t>(data[2]);
}

/** @brief Read 4 bytes in big-endian format */
inline uint32_t ReadU32Be(const uint8_t* data) {
  return (static_cast<uint32_t>(data[0]) << 24) | (static_cast<uint32_t>(data[1]) << 16) |
         (static_cast<uint32_t>(data[2]) << 8) | static_cast<uint32_t>(data[3]);
}

// --- MySQL length-encoded integer ---

/**
 * @brief Read a MySQL length-encoded integer
 *
 * - < 251: 1 byte value
 * - 252: next 2 bytes (little-endian)
 * - 253: next 3 bytes (little-endian)
 * - 254: next 8 bytes (little-endian)
 *
 * An empty buffer, a truncated field, the NULL marker 0xFB and the undefined
 * prefix 0xFF are all failures. A caller that accepts NULL (a result-set row
 * value) inspects the raw 0xFB byte before calling.
 *
 * @param data Pointer to the encoded integer
 * @param len Available bytes in data buffer
 * @param value Receives the decoded integer on success; untouched on failure
 * @param bytes_consumed Receives 1-9 on success; 0 on failure
 * @return true when a value was decoded
 */
[[nodiscard]] inline bool ReadLenEncInt(const uint8_t* data, size_t len, uint64_t* value,
                                        size_t* bytes_consumed) {
  *bytes_consumed = 0;
  if (len < 1) return false;

  const uint8_t first = data[0];
  size_t width = 0;
  uint64_t decoded = 0;
  if (first < 0xFB) {
    width = 1;
    decoded = first;
  } else if (first == 0xFC) {
    if (len < 3) return false;
    width = 3;
    decoded = ReadU16Le(data + 1);
  } else if (first == 0xFD) {
    if (len < 4) return false;
    width = 4;
    decoded = ReadU24Le(data + 1);
  } else if (first == 0xFE) {
    if (len < 9) return false;
    width = 9;
    decoded = ReadU64Le(data + 1);
  } else {
    // 0xFB (NULL) and 0xFF (undefined)
    return false;
  }

  *value = decoded;
  *bytes_consumed = width;
  return true;
}

// --- String reads ---

/** @brief Create a string_view from raw data and length */
inline std::string_view ReadStringView(const uint8_t* data, size_t len) {
  return {reinterpret_cast<const char*>(data), len};
}

}  // namespace mes::wire

#endif  // MES_CORE_SRC_WIRE_UTIL_H_
