// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#include "row_decoder.h"

#include <cstdio>
#include <cstring>

#include "binary_util.h"

namespace mes {

namespace {

// Read big-endian integer of arbitrary byte count (1-8).
uint64_t ReadBigEndian(const uint8_t* data, size_t bytes) {
  uint64_t val = 0;
  for (size_t i = 0; i < bytes; i++) {
    val = (val << 8) | data[i];
  }
  return val;
}

// Format an integer with zero-padding to the given width.
void AppendZeroPadded(std::string& out, int val, int width) {
  char buf[16];
  std::snprintf(buf, sizeof(buf), "%0*d", width, val);
  out += buf;
}

// Parse the common ROWS_EVENT post-header and return a pointer to row data.
// Sets column_count, columns_present, and optionally columns_present_update.
// Returns nullptr on error.
const uint8_t* ParseRowsPostHeader(const uint8_t* data, size_t len, bool is_v2, bool is_update,
                                   size_t* column_count, const uint8_t** columns_present,
                                   const uint8_t** columns_present_update, size_t* remaining) {
  // Minimum: 6 (table_id) + 2 (flags) = 8
  if (len < 8) return nullptr;

  const uint8_t* ptr = data + 8;  // skip table_id (6) + flags (2)
  size_t left = len - 8;

  if (is_v2) {
    if (left < 2) return nullptr;
    uint16_t var_header_len = binary::ReadU16Le(ptr);
    if (var_header_len < 2 || left < var_header_len) return nullptr;
    ptr += var_header_len;
    left -= var_header_len;
  }

  // column_count (packed int)
  if (left < 1) return nullptr;
  size_t consumed = 0;
  uint64_t col_count = binary::ReadPackedInt(ptr, left, consumed);
  if (consumed == 0) return nullptr;
  ptr += consumed;
  left -= consumed;

  // columns_present bitmap
  size_t bitmap_bytes = binary::BitmapBytes(col_count);
  if (left < bitmap_bytes) return nullptr;
  *columns_present = ptr;
  ptr += bitmap_bytes;
  left -= bitmap_bytes;

  // For UPDATE: columns_present_update bitmap
  if (is_update) {
    if (left < bitmap_bytes) return nullptr;
    *columns_present_update = ptr;
    ptr += bitmap_bytes;
    left -= bitmap_bytes;
  }

  *column_count = static_cast<size_t>(col_count);
  *remaining = left;
  return ptr;
}

// Count how many bits are set in a columns_present bitmap.
size_t CountPresentColumns(const uint8_t* bitmap, size_t column_count) {
  size_t count = 0;
  for (size_t i = 0; i < column_count; i++) {
    if (binary::BitmapIsSet(bitmap, i)) count++;
  }
  return count;
}

// Decode a single row from the data pointer. Advances ptr and remaining.
bool DecodeOneRow(const uint8_t*& ptr, size_t& remaining, const TableMetadata& metadata,
                  size_t column_count, const uint8_t* columns_present, RowData* row) {
  size_t present_count = CountPresentColumns(columns_present, column_count);

  // Null bitmap
  size_t null_bitmap_bytes = binary::BitmapBytes(present_count);
  if (remaining < null_bitmap_bytes) return false;
  const uint8_t* null_bitmap = ptr;
  ptr += null_bitmap_bytes;
  remaining -= null_bitmap_bytes;

  row->columns.resize(column_count);
  size_t null_bit_index = 0;
  const size_t meta_col_count = metadata.columns.size();

  for (size_t i = 0; i < column_count; i++) {
    // Cache the column info pointer once per iteration; this avoids three
    // bounds checks and three indexed loads into metadata.columns[i].
    const ColumnMetadata* col_info = (i < meta_col_count) ? &metadata.columns[i] : nullptr;
    const ColumnType col_type = col_info ? col_info->type : ColumnType::kLong;

    if (!binary::BitmapIsSet(columns_present, i)) {
      // Column not present in this event
      row->columns[i] = ColumnValue::Null(col_type);
      continue;
    }

    if (binary::BitmapIsSet(null_bitmap, null_bit_index)) {
      row->columns[i] = ColumnValue::Null(col_type);
      null_bit_index++;
      continue;
    }
    null_bit_index++;

    const uint16_t meta = col_info ? col_info->metadata : 0;
    const bool is_unsigned = col_info ? col_info->is_unsigned : false;

    size_t consumed = 0;
    row->columns[i] = DecodeColumnValue(col_type, meta, is_unsigned, ptr, remaining, &consumed);
    if (consumed == 0) {
      // DESIGN(review): consumed=0 serves a dual purpose — it signals
      // either a decode error OR a legitimate zero-size DECIMAL(0,0).
      // This ambiguity is intentional: DecodeDecimal returns consumed=0
      // with string_val="0" for precision==0, which is the only valid
      // zero-consumed case. All other column types or non-zero precisions
      // with consumed==0 are errors. The checks below disambiguate.
      // For any other type, consumed==0 must fail to prevent infinite loops.
      if (col_type != ColumnType::kNewDecimal) {
        return false;
      }
      const uint8_t precision = static_cast<uint8_t>(meta >> 8);
      if (precision != 0) {
        return false;
      }
      if (row->columns[i].is_null || row->columns[i].string_val.empty()) {
        return false;
      }
      // Legitimate DECIMAL(0, 0): fall through (no pointer advance needed).
    }
    if (consumed > remaining) return false;
    ptr += consumed;
    remaining -= consumed;
  }
  return true;
}

// Compute fractional seconds microseconds from stored frac value.
int FracToMicroseconds(int frac, uint16_t meta) {
  // MySQL packs fractional seconds in pairs that share the same byte width:
  //   fsp 1,2 -> 1 byte, value = microseconds / 10000  (range 0..99)
  //   fsp 3,4 -> 2 bytes, value = microseconds / 100   (range 0..9999)
  //   fsp 5,6 -> 3 bytes, value = microseconds         (range 0..999999)
  // Within each pair the encoder (my_datetime_packed_to_binary in MySQL)
  // stores the same scaled value regardless of fsp; the lower precisions
  // simply produce values whose trailing low digit(s) are always zero.
  // Decoding multipliers must therefore be identical within each pair.
  switch (meta) {
    case 1:
    case 2:
      return frac * 10000;
    case 3:
    case 4:
      return frac * 100;
    case 5:
    case 6:
      return frac;
    default:
      return 0;
  }
}

// Format fractional part as ".FFFFFF" string, trimming nothing (always 6 digits).
void AppendFractional(std::string& out, int usec) {
  char buf[16];
  std::snprintf(buf, sizeof(buf), ".%06d", usec);
  out += buf;
}

struct RowsContext {
  const uint8_t* ptr;
  size_t remaining;
  size_t column_count;
  const uint8_t* columns_present;
  const uint8_t* columns_present_update;
};

bool ParseRowsContext(const uint8_t* data, size_t len, const TableMetadata& metadata, bool is_v2,
                      bool is_update, RowsContext* ctx) {
  if (!data || !ctx) return false;
  ctx->columns_present_update = nullptr;
  ctx->ptr =
      ParseRowsPostHeader(data, len, is_v2, is_update, &ctx->column_count, &ctx->columns_present,
                          &ctx->columns_present_update, &ctx->remaining);
  return ctx->ptr != nullptr;
}

}  // namespace

ColumnValue DecodeColumnValue(ColumnType type, uint16_t meta, bool is_unsigned, const uint8_t* data,
                              size_t len, size_t* bytes_consumed) {
  *bytes_consumed = 0;

  switch (type) {
    case ColumnType::kTiny: {
      if (len < 1) return ColumnValue::Null(type);
      *bytes_consumed = 1;
      if (is_unsigned) {
        return ColumnValue::Int(type, static_cast<int64_t>(binary::ReadU8(data)));
      }
      return ColumnValue::Int(type, static_cast<int64_t>(static_cast<int8_t>(data[0])));
    }

    case ColumnType::kShort: {
      if (len < 2) return ColumnValue::Null(type);
      *bytes_consumed = 2;
      uint16_t raw = binary::ReadU16Le(data);
      if (is_unsigned) {
        return ColumnValue::Int(type, static_cast<int64_t>(raw));
      }
      return ColumnValue::Int(type, static_cast<int64_t>(static_cast<int16_t>(raw)));
    }

    case ColumnType::kLong: {
      if (len < 4) return ColumnValue::Null(type);
      *bytes_consumed = 4;
      uint32_t raw = binary::ReadU32Le(data);
      if (is_unsigned) {
        return ColumnValue::Int(type, static_cast<int64_t>(raw));
      }
      return ColumnValue::Int(type, static_cast<int64_t>(static_cast<int32_t>(raw)));
    }

    case ColumnType::kLongLong: {
      if (len < 8) return ColumnValue::Null(type);
      *bytes_consumed = 8;
      uint64_t raw = binary::ReadU64Le(data);
      if (is_unsigned) {
        if (raw > static_cast<uint64_t>(INT64_MAX)) {
          return ColumnValue::String(type, std::to_string(raw));
        }
        return ColumnValue::Int(type, static_cast<int64_t>(raw));
      }
      return ColumnValue::Int(type, static_cast<int64_t>(raw));
    }

    case ColumnType::kInt24: {
      if (len < 3) return ColumnValue::Null(type);
      *bytes_consumed = 3;
      uint32_t raw = binary::ReadU24Le(data);
      if (is_unsigned) {
        return ColumnValue::Int(type, static_cast<int64_t>(raw));
      }
      // Sign extension for 24-bit value
      static constexpr uint32_t kInt24SignBit = 0x800000;
      if (raw & kInt24SignBit) {
        raw |= 0xFF000000;
      }
      return ColumnValue::Int(type, static_cast<int64_t>(static_cast<int32_t>(raw)));
    }

    case ColumnType::kYear: {
      if (len < 1) return ColumnValue::Null(type);
      *bytes_consumed = 1;
      uint8_t val = binary::ReadU8(data);
      if (val == 0) {
        return ColumnValue::Int(type, 0);
      }
      return ColumnValue::Int(type, static_cast<int64_t>(val) + 1900);
    }

    case ColumnType::kFloat: {
      if (len < 4) return ColumnValue::Null(type);
      *bytes_consumed = 4;
      float fval = 0.0f;
      std::memcpy(&fval, data, 4);
      return ColumnValue::Float(static_cast<double>(fval));
    }

    case ColumnType::kDouble: {
      if (len < 8) return ColumnValue::Null(type);
      *bytes_consumed = 8;
      double dval = 0.0;
      std::memcpy(&dval, data, 8);
      return ColumnValue::Double(dval);
    }

    case ColumnType::kVarchar:
    case ColumnType::kVarString: {
      size_t prefix_size;
      size_t str_len;
      if (meta > 255) {
        if (len < 2) return ColumnValue::Null(type);
        str_len = binary::ReadU16Le(data);
        prefix_size = 2;
      } else {
        if (len < 1) return ColumnValue::Null(type);
        str_len = binary::ReadU8(data);
        prefix_size = 1;
      }
      if (len < prefix_size + str_len) return ColumnValue::Null(type);
      *bytes_consumed = prefix_size + str_len;
      return ColumnValue::String(
          type, std::string(reinterpret_cast<const char*>(data + prefix_size), str_len));
    }

    case ColumnType::kBlob:
    case ColumnType::kTinyBlob:
    case ColumnType::kMediumBlob:
    case ColumnType::kLongBlob:
    case ColumnType::kVector: {
      // NOTE(review): MySQL TABLE_MAP should always record a valid
      // pack_length in the metadata low byte (1 for TINY_BLOB,
      // 2 for BLOB/TEXT, 3 for MEDIUM_BLOB, 4 for LONG_BLOB). A value
      // of 0 is not expected from a correct server; this legacy
      // fallback to 1 is preserved for defensive compatibility. The
      // prefix_consumed/blob_len bounds checks below reject the row
      // before a mismatched fallback can desynchronize later columns.
      uint8_t pack_length = static_cast<uint8_t>(meta);
      if (pack_length == 0) pack_length = 1;
      if (pack_length > 4) return ColumnValue::Null(type);
      size_t prefix_consumed = 0;
      uint32_t blob_len = binary::ReadVarLenPrefix(pack_length, data, len, &prefix_consumed);
      if (prefix_consumed == 0) return ColumnValue::Null(type);
      if (len < prefix_consumed + blob_len) return ColumnValue::Null(type);
      *bytes_consumed = prefix_consumed + blob_len;
      return ColumnValue::Bytes(
          type, std::vector<uint8_t>(data + prefix_consumed, data + prefix_consumed + blob_len));
    }

    case ColumnType::kJson: {
      uint8_t pack_length = static_cast<uint8_t>(meta);
      if (pack_length == 0) pack_length = 4;
      if (pack_length > 4) return ColumnValue::Null(type);
      size_t prefix_consumed = 0;
      uint32_t json_len = binary::ReadVarLenPrefix(pack_length, data, len, &prefix_consumed);
      if (prefix_consumed == 0) return ColumnValue::Null(type);
      if (len < prefix_consumed + json_len) return ColumnValue::Null(type);
      *bytes_consumed = prefix_consumed + json_len;
      // Store JSON as bytes (binary JSON format, not text)
      return ColumnValue::Bytes(
          type, std::vector<uint8_t>(data + prefix_consumed, data + prefix_consumed + json_len));
    }

    case ColumnType::kString: {
      uint8_t real_type = static_cast<uint8_t>(meta >> 8);

      if (real_type == 0xF7) {
        // ENUM
        uint32_t size = meta & 0xFF;
        if (size == 1) {
          if (len < 1) return ColumnValue::Null(ColumnType::kEnum);
          *bytes_consumed = 1;
          return ColumnValue::Int(ColumnType::kEnum, static_cast<int64_t>(binary::ReadU8(data)));
        }
        if (size == 2) {
          if (len < 2) return ColumnValue::Null(ColumnType::kEnum);
          *bytes_consumed = 2;
          return ColumnValue::Int(ColumnType::kEnum, static_cast<int64_t>(binary::ReadU16Le(data)));
        }
        return ColumnValue::Null(ColumnType::kEnum);
      }

      if (real_type == 0xF8) {
        // SET
        uint32_t size = meta & 0xFF;
        if (size < 1 || size > 8) return ColumnValue::Null(ColumnType::kSet);
        if (len < size) return ColumnValue::Null(ColumnType::kSet);
        *bytes_consumed = size;
        uint64_t val = 0;
        for (uint32_t i = 0; i < size; i++) {
          val |= static_cast<uint64_t>(data[i]) << (i * 8);
        }
        return ColumnValue::Int(ColumnType::kSet, static_cast<int64_t>(val));
      }

      // CHAR type
      // NOTE(review): Formula matches MySQL server source (log_event.cc
      // Rows_log_event::print_verbose_one_row). The `^ 0x300` is
      // intentional: it decodes the field length when real_type was
      // upgraded from CHAR to STRING, where the upper 2 bits of meta[0]
      // carry the high bits of max_len XORed with 0x3. Do not replace
      // with `| 0x300`.
      uint32_t max_len = (((meta >> 4) & 0x300) ^ 0x300) + (meta & 0xFF);
      size_t prefix_size;
      size_t str_len;
      if (max_len > 255) {
        if (len < 2) return ColumnValue::Null(type);
        str_len = binary::ReadU16Le(data);
        prefix_size = 2;
      } else {
        if (len < 1) return ColumnValue::Null(type);
        str_len = binary::ReadU8(data);
        prefix_size = 1;
      }
      if (len < prefix_size + str_len) return ColumnValue::Null(type);
      *bytes_consumed = prefix_size + str_len;
      return ColumnValue::String(
          type, std::string(reinterpret_cast<const char*>(data + prefix_size), str_len));
    }

    case ColumnType::kDate: {
      if (len < 3) return ColumnValue::Null(type);
      *bytes_consumed = 3;
      uint32_t val = binary::ReadU24Le(data);
      int day = val & 0x1F;
      int month = (val >> 5) & 0x0F;
      int year = val >> 9;
      char buf[16];
      std::snprintf(buf, sizeof(buf), "%04d-%02d-%02d", year, month, day);
      return ColumnValue::String(type, std::string(buf));
    }

    case ColumnType::kTime: {
      if (len < 3) return ColumnValue::Null(type);
      *bytes_consumed = 3;
      uint32_t raw = binary::ReadU24Le(data);
      // Sign-extend 24-bit to 32-bit
      int32_t val =
          (raw & 0x800000) ? static_cast<int32_t>(raw | 0xFF000000) : static_cast<int32_t>(raw);
      bool negative = val < 0;
      // Guard: raw == 0x800000 produces val == INT32_MIN (-2147483648),
      // whose negation is undefined behavior. This value is outside the
      // valid MySQL TIME range (±838:59:59), so treat it as invalid.
      if (val == INT32_MIN) return ColumnValue::Null(type);
      if (negative) val = -val;
      int sec = val % 100;
      int min = (val / 100) % 100;
      int hour = val / 10000;
      char buf[32];
      if (negative) {
        std::snprintf(buf, sizeof(buf), "-%02d:%02d:%02d", hour, min, sec);
      } else {
        std::snprintf(buf, sizeof(buf), "%02d:%02d:%02d", hour, min, sec);
      }
      return ColumnValue::String(type, std::string(buf));
    }

    case ColumnType::kTimestamp: {
      if (len < 4) return ColumnValue::Null(type);
      *bytes_consumed = 4;
      uint32_t val = binary::ReadU32Le(data);
      return ColumnValue::Int(type, static_cast<int64_t>(val));
    }

    case ColumnType::kDatetime: {
      if (len < 8) return ColumnValue::Null(type);
      *bytes_consumed = 8;
      uint64_t val = binary::ReadU64Le(data);
      int64_t sval = static_cast<int64_t>(val);
      int sec = sval % 100;
      sval /= 100;
      int min = sval % 100;
      sval /= 100;
      int hour = sval % 100;
      sval /= 100;
      int day = sval % 100;
      sval /= 100;
      int month = sval % 100;
      sval /= 100;
      int year = static_cast<int>(sval);
      char buf[32];
      std::snprintf(buf, sizeof(buf), "%04d-%02d-%02d %02d:%02d:%02d", year, month, day, hour, min,
                    sec);
      return ColumnValue::String(type, std::string(buf));
    }

    case ColumnType::kDatetime2: {
      size_t frac_bytes = (meta + 1) / 2;
      size_t total = 5 + frac_bytes;
      if (len < total) return ColumnValue::Null(type);
      *bytes_consumed = total;

      // 5 bytes big-endian
      int64_t packed = static_cast<int64_t>(ReadBigEndian(data, 5));
      static constexpr int64_t kDatetime2IntOfs = 0x8000000000LL;
      int64_t intpart = packed - kDatetime2IntOfs;
      bool negative = intpart < 0;
      if (negative) intpart = -intpart;

      int64_t ymd = intpart >> 17;
      int64_t hms = intpart & 0x1FFFF;
      int64_t ym = ymd >> 5;
      int day = static_cast<int>(ymd & 0x1F);
      int month = static_cast<int>(ym % 13);
      int year = static_cast<int>(ym / 13);
      int second = static_cast<int>(hms & 0x3F);
      int minute = static_cast<int>((hms >> 6) & 0x3F);
      int hour = static_cast<int>(hms >> 12);

      std::string result;
      if (negative) result += "-";
      char buf[32];
      std::snprintf(buf, sizeof(buf), "%04d-%02d-%02d %02d:%02d:%02d", year, month, day, hour,
                    minute, second);
      result += buf;

      if (meta > 0 && frac_bytes > 0) {
        int frac = static_cast<int>(ReadBigEndian(data + 5, frac_bytes));
        int usec = FracToMicroseconds(frac, meta);
        AppendFractional(result, usec);
      }

      return ColumnValue::String(type, result);
    }

    case ColumnType::kTimestamp2: {
      size_t frac_bytes = (meta + 1) / 2;
      size_t total = 4 + frac_bytes;
      if (len < total) return ColumnValue::Null(type);
      *bytes_consumed = total;

      uint32_t ts = binary::ReadU32Be(data);
      std::string result = std::to_string(ts);

      if (meta > 0 && frac_bytes > 0) {
        int frac = static_cast<int>(ReadBigEndian(data + 4, frac_bytes));
        int usec = FracToMicroseconds(frac, meta);
        char frac_buf[16];
        std::snprintf(frac_buf, sizeof(frac_buf), ".%06d", usec);
        result += frac_buf;
      }

      return ColumnValue::String(type, result);
    }

    case ColumnType::kTime2: {
      size_t frac_bytes = (meta + 1) / 2;
      size_t total = 3 + frac_bytes;
      if (len < total) return ColumnValue::Null(type);
      *bytes_consumed = total;

      uint32_t packed = binary::ReadU24Be(data);
      static constexpr int32_t kTimefIntOfs = 0x800000;
      int32_t intpart = static_cast<int32_t>(packed) - kTimefIntOfs;
      bool negative = intpart < 0;
      if (negative) intpart = -intpart;

      int hour = (intpart >> 12) & 0x3FF;
      int minute = (intpart >> 6) & 0x3F;
      int second = intpart & 0x3F;

      std::string result;
      if (negative) result += "-";
      char buf[16];
      std::snprintf(buf, sizeof(buf), "%02d:%02d:%02d", hour, minute, second);
      result += buf;

      if (meta > 0 && frac_bytes > 0) {
        int frac = static_cast<int>(ReadBigEndian(data + 3, frac_bytes));
        int usec = FracToMicroseconds(frac, meta);
        AppendFractional(result, usec);
      }

      return ColumnValue::String(type, result);
    }

    case ColumnType::kNewDecimal: {
      uint8_t precision = static_cast<uint8_t>(meta >> 8);
      uint8_t scale = static_cast<uint8_t>(meta & 0xFF);
      size_t consumed = 0;
      std::string val = binary::DecodeDecimal(data, len, precision, scale, consumed);
      if (consumed == 0 && !val.empty()) {
        // precision==0 returns "0" with consumed==0, which is valid
      } else if (consumed == 0 && val.empty()) {
        return ColumnValue::Null(type);
      }
      *bytes_consumed = consumed;
      return ColumnValue::String(type, val);
    }

    case ColumnType::kBit: {
      uint32_t full_bytes = (meta >> 8) & 0xFF;
      uint32_t extra_bits = meta & 0xFF;
      uint32_t total_bytes = full_bytes + (extra_bits > 0 ? 1 : 0);
      if (total_bytes == 0) total_bytes = 1;
      if (len < total_bytes) return ColumnValue::Null(type);
      *bytes_consumed = total_bytes;
      uint64_t val = ReadBigEndian(data, total_bytes);
      return ColumnValue::Int(type, static_cast<int64_t>(val));
    }

    case ColumnType::kGeometry: {
      uint8_t pack_length = static_cast<uint8_t>(meta);
      if (pack_length == 0) pack_length = 4;
      if (pack_length > 4) return ColumnValue::Null(type);
      size_t prefix_consumed = 0;
      uint32_t geo_len = binary::ReadVarLenPrefix(pack_length, data, len, &prefix_consumed);
      if (prefix_consumed == 0) return ColumnValue::Null(type);
      if (len < prefix_consumed + geo_len) return ColumnValue::Null(type);
      *bytes_consumed = prefix_consumed + geo_len;
      return ColumnValue::Bytes(
          type, std::vector<uint8_t>(data + prefix_consumed, data + prefix_consumed + geo_len));
    }

    // Note: kEnum (0xF7) and kSet (0xF8) are handled within the kString case
    // above, as MySQL binlog always transmits them as MYSQL_TYPE_STRING.
    default: {
      uint32_t field_size = binary::CalcFieldSize(static_cast<uint8_t>(type), data, len, meta);
      *bytes_consumed = field_size;
      return ColumnValue::Null(type);
    }
  }
}

static bool DecodeSimpleRows(const uint8_t* data, size_t len, const TableMetadata& metadata,
                             bool is_v2, std::vector<RowData>* rows) {
  if (!rows) return false;
  RowsContext ctx{};
  if (!ParseRowsContext(data, len, metadata, is_v2, false, &ctx)) return false;

  rows->clear();
  rows->reserve(8);  // typical rows per event; avoids reallocation in common case
  while (ctx.remaining > 0) {
    RowData row;
    if (!DecodeOneRow(ctx.ptr, ctx.remaining, metadata, ctx.column_count, ctx.columns_present,
                      &row)) {
      return false;
    }
    rows->push_back(std::move(row));
  }
  return true;
}

bool DecodeWriteRows(const uint8_t* data, size_t len, const TableMetadata& metadata, bool is_v2,
                     std::vector<RowData>* rows) {
  return DecodeSimpleRows(data, len, metadata, is_v2, rows);
}

bool DecodeUpdateRows(const uint8_t* data, size_t len, const TableMetadata& metadata, bool is_v2,
                      std::vector<UpdatePair>* pairs) {
  if (!pairs) return false;
  RowsContext ctx{};
  if (!ParseRowsContext(data, len, metadata, is_v2, true, &ctx)) return false;

  pairs->clear();
  pairs->reserve(8);
  while (ctx.remaining > 0) {
    UpdatePair pair;
    if (!DecodeOneRow(ctx.ptr, ctx.remaining, metadata, ctx.column_count, ctx.columns_present,
                      &pair.before)) {
      return false;
    }
    if (!DecodeOneRow(ctx.ptr, ctx.remaining, metadata, ctx.column_count,
                      ctx.columns_present_update, &pair.after)) {
      return false;
    }
    pairs->push_back(std::move(pair));
  }
  return true;
}

bool DecodeDeleteRows(const uint8_t* data, size_t len, const TableMetadata& metadata, bool is_v2,
                      std::vector<RowData>* rows) {
  return DecodeSimpleRows(data, len, metadata, is_v2, rows);
}

}  // namespace mes
