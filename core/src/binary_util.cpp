// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#include "binary_util.h"

#include <cstdio>
#include <vector>

namespace mes::binary {

// Bytes needed for 1-9 remaining digits (used for NEWDECIMAL)
static const int kDig2Bytes[10] = {0, 1, 1, 2, 2, 3, 3, 4, 4, 4};

/** @brief Calculate the binary size in bytes for a DECIMAL(precision, scale). */
inline size_t DecimalBinarySize(uint8_t precision, uint8_t scale) {
  if (scale > precision) return 0;
  int intg = precision - scale;
  int frac = scale;
  int intg0 = intg / 9, intg_rem = intg % 9;
  int frac0 = frac / 9, frac_rem = frac % 9;
  return static_cast<size_t>(intg0 * 4 + kDig2Bytes[intg_rem] + frac0 * 4 +
                             kDig2Bytes[frac_rem]);
}

uint64_t ReadPackedInt(const uint8_t* data, size_t len, size_t& bytes_consumed) {
  bytes_consumed = 0;
  if (len < 1) return 0;

  uint8_t first = data[0];

  if (first < 251) {
    bytes_consumed = 1;
    return static_cast<uint64_t>(first);
  }

  if (first == 251) {
    // NULL marker
    bytes_consumed = 1;
    return 0;
  }

  if (first == 252) {
    if (len < 3) return 0;
    bytes_consumed = 3;
    return static_cast<uint64_t>(ReadU16Le(data + 1));
  }

  if (first == 253) {
    if (len < 4) return 0;
    bytes_consumed = 4;
    return static_cast<uint64_t>(ReadU24Le(data + 1));
  }

  // first == 254
  if (len < 9) return 0;
  bytes_consumed = 9;
  return ReadU64Le(data + 1);
}

std::string DecodeDecimal(const uint8_t* data, size_t available, uint8_t precision,
                          uint8_t scale, size_t& bytes_consumed) {
  if (precision == 0) {
    bytes_consumed = 0;
    return "0";
  }

  if (scale > precision) {
    bytes_consumed = 0;
    return "";
  }

  int intg = precision - scale;
  int intg0 = intg / 9;        // Full 4-byte groups in integer part
  int intg_rem = intg % 9;     // Remaining digits in integer part
  int frac0 = scale / 9;       // Full 4-byte groups in fractional part
  int frac_rem = scale % 9;    // Remaining digits in fractional part

  int total_size = static_cast<int>(DecimalBinarySize(precision, scale));
  bytes_consumed = static_cast<size_t>(total_size);

  if (total_size == 0) {
    return "0";
  }

  if (static_cast<size_t>(total_size) > available) {
    bytes_consumed = 0;
    return "";
  }

  // Make a mutable copy for sign-based transformation
  std::vector<uint8_t> buf(data, data + total_size);

  // MSB of first byte: set (>= 0x80) = positive, clear (< 0x80) = negative
  bool is_negative = (buf[0] & 0x80) == 0;

  // Toggle the sign bit first, then complement all bytes for negative values.
  // This matches MySQL's bin2decimal(): *from ^= 0x80, then apply mask per group.
  // For negative, buf[0] is XORed with both 0x80 and 0xFF (net 0x7F), which
  // correctly reverses the encoding of sign-bit-set + full complement.
  buf[0] ^= 0x80;
  if (is_negative) {
    for (auto& byte : buf) {
      byte ^= 0xFF;
    }
  }

  const uint8_t* ptr = buf.data();
  std::string result;

  // Process integer remainder (leading partial group)
  if (intg_rem > 0) {
    int bytes = kDig2Bytes[intg_rem];
    int32_t val = 0;
    for (int i = 0; i < bytes; i++) {
      val = (val << 8) | *ptr++;
    }
    result += std::to_string(val);
  }

  // Process full 4-byte groups in integer part
  for (int i = 0; i < intg0; i++) {
    int32_t val = 0;
    for (int j = 0; j < 4; j++) {
      val = (val << 8) | *ptr++;
    }
    if (result.empty()) {
      result += std::to_string(val);
    } else {
      char fmt_buf[16];
      std::snprintf(fmt_buf, sizeof(fmt_buf), "%09d", val);
      result += fmt_buf;
    }
  }

  if (result.empty()) {
    result = "0";
  }

  // Fractional part
  if (scale > 0) {
    result += ".";

    for (int i = 0; i < frac0; i++) {
      int32_t val = 0;
      for (int j = 0; j < 4; j++) {
        val = (val << 8) | *ptr++;
      }
      char fmt_buf[16];
      std::snprintf(fmt_buf, sizeof(fmt_buf), "%09d", val);
      result += fmt_buf;
    }

    if (frac_rem > 0) {
      int bytes = kDig2Bytes[frac_rem];
      int32_t val = 0;
      for (int i = 0; i < bytes; i++) {
        val = (val << 8) | *ptr++;
      }
      char fmt_buf[16];
      std::snprintf(fmt_buf, sizeof(fmt_buf), "%0*d", frac_rem, val);
      result += fmt_buf;
    }
  }

  if (is_negative) {
    result = "-" + result;
  }

  return result;
}

uint32_t ReadVarLenPrefix(uint8_t pack_length, const uint8_t* data, size_t len,
                          size_t* bytes_consumed) {
  if (len < pack_length) {
    *bytes_consumed = 0;
    return 0;
  }
  *bytes_consumed = pack_length;
  switch (pack_length) {
    case 1:
      return ReadU8(data);
    case 2:
      return ReadU16Le(data);
    case 3:
      return ReadU24Le(data);
    case 4:
      return ReadU32Le(data);
    default:
      *bytes_consumed = 0;
      return 0;
  }
}

uint32_t CalcFieldSize(uint8_t col_type, const uint8_t* data, size_t buf_len,
                       uint16_t metadata) {
  switch (col_type) {
    // Fixed-size integer types
    case 0x01:  // MYSQL_TYPE_TINY
      return 1;
    case 0x02:  // MYSQL_TYPE_SHORT
      return 2;
    case 0x03:  // MYSQL_TYPE_LONG
      return 4;
    case 0x04:  // MYSQL_TYPE_FLOAT
      return 4;
    case 0x05:  // MYSQL_TYPE_DOUBLE
      return 8;
    case 0x08:  // MYSQL_TYPE_LONGLONG
      return 8;
    case 0x09:  // MYSQL_TYPE_INT24
      return 3;
    case 0x0D:  // MYSQL_TYPE_YEAR
      return 1;

    // Date/Time types (fixed size)
    case 0x0A:  // MYSQL_TYPE_DATE
      return 3;
    case 0x0B:  // MYSQL_TYPE_TIME
      return 3;
    case 0x07:  // MYSQL_TYPE_TIMESTAMP
      return 4;
    case 0x0C:  // MYSQL_TYPE_DATETIME
      return 8;

    // Date/Time types with fractional seconds
    case 0x11:  // MYSQL_TYPE_TIMESTAMP2
      return 4 + (metadata + 1) / 2;
    case 0x12:  // MYSQL_TYPE_DATETIME2
      return 5 + (metadata + 1) / 2;
    case 0x13:  // MYSQL_TYPE_TIME2
      return 3 + (metadata + 1) / 2;

    // VARCHAR / VAR_STRING
    case 0xFD:  // MYSQL_TYPE_VAR_STRING
    case 0x0F: {  // MYSQL_TYPE_VARCHAR
      if (metadata > 255) {
        if (buf_len < 2) return 0;
        return 2 + ReadU16Le(data);
      }
      if (buf_len < 1) return 0;
      return 1 + ReadU8(data);
    }

    // BIT
    case 0x10: {  // MYSQL_TYPE_BIT
      uint32_t bytes = (metadata >> 8) & 0xFF;
      uint32_t bits = metadata & 0xFF;
      return bytes + (bits > 0 ? 1 : 0);
    }

    // NEWDECIMAL
    case 0xF6: {  // MYSQL_TYPE_NEWDECIMAL
      uint8_t precision = static_cast<uint8_t>(metadata >> 8);
      uint8_t scale = static_cast<uint8_t>(metadata & 0xFF);
      return static_cast<uint32_t>(DecimalBinarySize(precision, scale));
    }

    // JSON (stored like BLOB)
    case 0xF5: {  // MYSQL_TYPE_JSON
      uint8_t pack_len = static_cast<uint8_t>(metadata);
      if (pack_len == 0 || pack_len > 4) pack_len = 4;
      size_t consumed = 0;
      uint32_t json_len = ReadVarLenPrefix(pack_len, data, buf_len, &consumed);
      if (consumed == 0) return 0;
      return static_cast<uint32_t>(consumed) + json_len;
    }

    // VECTOR (same encoding as BLOB, MySQL 9.0+)
    case 0xF2: {  // MYSQL_TYPE_VECTOR
      uint8_t pack_len = static_cast<uint8_t>(metadata);
      if (pack_len == 0 || pack_len > 4) return 0;
      size_t consumed = 0;
      uint32_t vec_len = ReadVarLenPrefix(pack_len, data, buf_len, &consumed);
      if (consumed == 0) return 0;
      return static_cast<uint32_t>(consumed) + vec_len;
    }

    // BLOB (includes TEXT, TINY_BLOB, MEDIUM_BLOB, LONG_BLOB)
    case 0xF9:  // MYSQL_TYPE_TINY_BLOB
    case 0xFA:  // MYSQL_TYPE_MEDIUM_BLOB
    case 0xFB:  // MYSQL_TYPE_LONG_BLOB
    case 0xFC: {  // MYSQL_TYPE_BLOB
      uint8_t pack_len = static_cast<uint8_t>(metadata);
      if (pack_len == 0 || pack_len > 4) return 0;
      size_t consumed = 0;
      uint32_t blob_len = ReadVarLenPrefix(pack_len, data, buf_len, &consumed);
      if (consumed == 0) return 0;
      return static_cast<uint32_t>(consumed) + blob_len;
    }

    // STRING (CHAR) - also handles ENUM and SET via metadata encoding
    case 0xFE: {  // MYSQL_TYPE_STRING
      uint8_t type_byte = static_cast<uint8_t>(metadata >> 8);
      if (type_byte == 0xF7 || type_byte == 0xF8) {
        // ENUM or SET: metadata low byte is the size
        return metadata & 0xFF;
      }
      // Fixed or variable length string
      uint32_t max_len =
          (((metadata >> 4) & 0x300) ^ 0x300) + (metadata & 0xFF);
      if (max_len > 255) {
        if (buf_len < 2) return 0;
        return 2 + ReadU16Le(data);
      }
      if (buf_len < 1) return 0;
      return 1 + ReadU8(data);
    }

    // GEOMETRY (stored like BLOB)
    case 0xFF: {  // MYSQL_TYPE_GEOMETRY
      uint8_t pack_len = static_cast<uint8_t>(metadata);
      if (pack_len == 0 || pack_len > 4) return 0;
      size_t consumed = 0;
      uint32_t geo_len = ReadVarLenPrefix(pack_len, data, buf_len, &consumed);
      if (consumed == 0) return 0;
      return static_cast<uint32_t>(consumed) + geo_len;
    }

    default:
      return 0;
  }
}

}  // namespace mes::binary
