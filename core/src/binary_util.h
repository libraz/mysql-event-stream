// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * @file binary_util.h
 * @brief Utilities for parsing MySQL binlog binary format
 *
 * Low-level binary protocol parsing functions that must match MySQL's wire
 * format exactly. Pointer arithmetic and C-style casts are intentional for
 * protocol compatibility.
 */

#ifndef MES_CORE_SRC_BINARY_UTIL_H_
#define MES_CORE_SRC_BINARY_UTIL_H_

#include <cstddef>
#include <cstdint>
#include <limits>
#include <string>
#include <string_view>

namespace mes::binary {

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

// --- Big-endian integer reads (for DECIMAL and packed formats) ---

/** @brief Read 2 bytes in big-endian format */
inline uint16_t ReadU16Be(const uint8_t* data) {
  return (static_cast<uint16_t>(data[0]) << 8) | static_cast<uint16_t>(data[1]);
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

// --- Fixed-width decimal formatting ---

/** @brief Number of decimal digits in @p value (1 for zero) */
inline int DecimalDigitCount(unsigned value) {
  if (value < 10U) return 1;
  if (value < 100U) return 2;
  if (value < 1000U) return 3;
  if (value < 10000U) return 4;
  if (value < 100000U) return 5;
  if (value < 1000000U) return 6;
  if (value < 10000000U) return 7;
  if (value < 100000000U) return 8;
  if (value < 1000000000U) return 9;
  return 10;
}

/**
 * @brief Write @p value in decimal at @p out, zero-padded to @p width characters
 *
 * Reproduces printf's "%0*d" for int: the pad width counts a leading '-', and a
 * value needing more digits than @p width is written in full rather than
 * truncated. Nothing is null-terminated.
 *
 * Temporal and DECIMAL columns are formatted several times per row, so this
 * deliberately avoids the varargs formatter: printf resolves the decimal point
 * through localeconv_l on every call, which takes a process-wide lock and
 * serializes concurrent decoders.
 *
 * @param out Destination; must have room for max(@p width, 11) characters.
 * @return Pointer one past the last character written.
 */
inline char* WritePaddedInt(char* out, int value, int width) {
  unsigned magnitude;
  if (value < 0) {
    *out++ = '-';
    --width;
    magnitude = 0U - static_cast<unsigned>(value);
  } else {
    magnitude = static_cast<unsigned>(value);
  }

  const int digits = DecimalDigitCount(magnitude);
  const int len = width > digits ? width : digits;
  char* const end = out + len;
  char* cursor = end;
  do {
    *--cursor = static_cast<char>('0' + magnitude % 10);
    magnitude /= 10;
  } while (magnitude != 0);
  while (cursor > out) {
    *--cursor = '0';
  }
  return end;
}

/**
 * @brief Append @p value zero-padded to @p width characters to @p out
 *
 * Formats exactly as @ref WritePaddedInt. @p width must not exceed 20.
 */
inline void AppendPaddedInt(std::string& out, int value, int width) {
  char buf[24];
  out.append(buf, static_cast<size_t>(WritePaddedInt(buf, value, width) - buf));
}

// --- MySQL packed integer (length-encoded integer) ---

/**
 * @brief Read a MySQL packed integer (length-encoded integer)
 *
 * Format:
 * - < 251: 1 byte value
 * - 251: NULL marker (returns 0)
 * - 252: next 2 bytes (little-endian)
 * - 253: next 3 bytes (little-endian)
 * - 254: next 8 bytes (little-endian)
 *
 * @param data Pointer to the packed integer data
 * @param len Available bytes in data buffer
 * @param bytes_consumed Set to the number of bytes consumed:
 *   - 0: error (insufficient data)
 *   - 1: NULL marker (value 0xFB, returned value is 0)
 *   - 1-9: valid integer decoded
 *   Callers MUST check bytes_consumed to distinguish error (0) from NULL (1).
 * @return The decoded integer value (0 on error or NULL, check bytes_consumed)
 */
[[nodiscard]] uint64_t ReadPackedInt(const uint8_t* data, size_t len, size_t& bytes_consumed);

// --- Column count limit ---

/**
 * @brief Upper bound on the number of columns a binlog table may declare.
 *
 * Matches MySQL's and MariaDB's hard limit of 4096 columns per table. Wire
 * formats carry the column count as a packed integer, so every parser must
 * clamp it to this bound before the value reaches allocation or loop
 * arithmetic.
 */
constexpr uint64_t kMaxTableColumns = 4096;

// --- Bitmap utilities ---

/**
 * @brief Calculate number of bytes needed for a bitmap of bit_count bits
 *
 * Saturates instead of wrapping. Column counts reach this function straight
 * from the wire, and a value within 7 of SIZE_MAX would otherwise roll
 * `bit_count + 7` over to a tiny (often zero) byte count, turning a caller's
 * "is the bitmap inside the buffer?" check into a no-op.
 */
inline size_t BitmapBytes(size_t bit_count) {
  constexpr size_t kMax = std::numeric_limits<size_t>::max();
  if (bit_count > kMax - 7) return kMax / 8 + 1;
  return (bit_count + 7) / 8;
}

/** @brief Check if a specific bit is set in a bitmap */
inline bool BitmapIsSet(const uint8_t* bitmap, size_t bit_index) {
  return (bitmap[bit_index / 8] & (1 << (bit_index % 8))) != 0;
}

// --- String reads ---

/** @brief Create a string_view from raw data and length */
inline std::string_view ReadStringView(const uint8_t* data, size_t len) {
  return {reinterpret_cast<const char*>(data), len};
}

// --- Variable-length prefix reads ---

/**
 * @brief Read a variable-length prefix (1-4 bytes) based on pack_length
 *
 * Used by BLOB, JSON, and GEOMETRY types where the length of the data
 * is stored in a prefix of 1-4 bytes depending on the column metadata.
 *
 * @param pack_length Number of prefix bytes (1-4)
 * @param data Pointer to the prefix data
 * @param len Available bytes in data buffer
 * @param bytes_consumed Set to the number of bytes read (0 on error)
 * @return The decoded length value, or 0 if data is too short
 */
uint32_t ReadVarLenPrefix(uint8_t pack_length, const uint8_t* data, size_t len,
                          size_t* bytes_consumed);

// --- Complex decode functions ---

/**
 * @brief Decode MySQL DECIMAL (NEWDECIMAL) binary format to string
 *
 * Based on MySQL's bin2decimal() function from strings/decimal.c.
 *
 * MySQL DECIMAL binary format:
 * - Sign bit is stored in MSB of first byte (0x80)
 * - Positive values: MSB set; to decode, XOR first byte with 0x80
 * - Negative values: MSB clear; to decode, XOR all bytes with 0xFF
 *
 * Digits are carried in nine-digit groups, and each group is validated against
 * the digit count it declares (a full group below 1e9, a partial group below
 * 10^digits) exactly as bin2decimal() does. A group outside its range cannot
 * come from a well-formed encoding, so the value is reported as malformed
 * instead of decoded into silently wrong numeric text.
 *
 * @param data Pointer to binary decimal data
 * @param available Bytes readable at @p data
 * @param precision Total number of digits
 * @param scale Number of digits after decimal point
 * @param bytes_consumed Set to the number of bytes consumed, or 0 if the input
 *   is truncated or malformed
 * @return String representation of the decimal value, or an empty string when
 *   @p bytes_consumed is 0
 */
std::string DecodeDecimal(const uint8_t* data, size_t available, uint8_t precision, uint8_t scale,
                          size_t& bytes_consumed);

/**
 * @brief Calculate the size (in bytes) of a field value in binlog row data
 *
 * Based on calc_field_size() from MySQL source:
 * libs/mysql/binlog/event/binary_log_funcs.cpp
 *
 * @param col_type MySQL column type (raw uint8_t value)
 * @param data Pointer to the field data
 * @param metadata Type-specific metadata from TABLE_MAP event
 * @return Size of the field in bytes, or 0 for unsupported types
 */
uint32_t CalcFieldSize(uint8_t col_type, const uint8_t* data, size_t buf_len, uint32_t metadata);

}  // namespace mes::binary

#endif  // MES_CORE_SRC_BINARY_UTIL_H_
