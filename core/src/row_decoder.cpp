// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#include "row_decoder.h"

#include <zlib.h>

#include <algorithm>
#include <cstring>
#include <limits>

#include "binary_util.h"

namespace mes {

namespace {

// MySQL collation id of the `binary` collation, the one that marks a
// character-family column as holding opaque bytes rather than text.
constexpr uint32_t kBinaryCollationId = 63;

/**
 * @brief The single decision: does a character-family column hold bytes?
 *
 * Every character-family and BLOB-family decode arm resolves text vs. bytes
 * here and nowhere else, so a column's classification is a function of its
 * resolved collation alone and never of which arm produced the value.
 *
 * The two members of each on-wire pair — VARCHAR/VARBINARY, CHAR/BINARY,
 * TEXT/BLOB — share a binlog type byte, so the declared collation is the only
 * discriminator. When TABLE_MAP carries no collation for the column (MariaDB
 * omits the field entirely under its default binlog_row_metadata=NO_LOG) the
 * pair cannot be told apart, and the value is surfaced as bytes: bytes
 * reproduce the payload exactly and leave decoding to the consumer, whereas
 * text would push a binary key or hash through a UTF-8 decoder and lose it
 * irrecoverably.
 *
 * @param charset_known Whether TABLE_MAP carried a collation for the column.
 * @param binary_charset Whether that collation is the binary one.
 * @return true when the payload must be surfaced as an opaque byte sequence.
 */
bool PayloadIsBinary(bool charset_known, bool binary_charset) {
  return !charset_known || binary_charset;
}

/// Build a character-family value from a contiguous range of wire bytes.
ColumnValue MakeCharacterValue(ColumnType type, const uint8_t* data, size_t len, bool charset_known,
                               bool binary_charset) {
  if (PayloadIsBinary(charset_known, binary_charset)) {
    return ColumnValue::Bytes(type, data, len);
  }
  return ColumnValue::String(type, std::string(reinterpret_cast<const char*>(data), len));
}

// Build a character-family value from an already-materialized buffer, moving it
// in rather than copying: a second copy would double the peak for exactly the
// field types most able to expand.
ColumnValue MakeCharacterValue(ColumnType type, std::string value, bool charset_known,
                               bool binary_charset) {
  ColumnValue result = ColumnValue::String(type, std::move(value));
  result.is_binary = PayloadIsBinary(charset_known, binary_charset);
  return result;
}

// Read big-endian integer of arbitrary byte count (1-8).
uint64_t ReadBigEndian(const uint8_t* data, size_t bytes) {
  uint64_t val = 0;
  for (size_t i = 0; i < bytes; i++) {
    val = (val << 8) | data[i];
  }
  return val;
}

// Parse the common ROWS_EVENT post-header and return a pointer to row data.
// Sets column_count, columns_present, and optionally columns_present_update.
// expected_column_count is the count declared by the TABLE_MAP event.
// Returns nullptr on error.
const uint8_t* ParseRowsPostHeader(const uint8_t* data, size_t len, bool is_v2, bool is_update,
                                   size_t expected_column_count, size_t* column_count,
                                   const uint8_t** columns_present,
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
  uint64_t col_count = binary::ReadBinlogLength(ptr, left, consumed);
  if (consumed == 0) return nullptr;
  // Range-check before the count reaches any arithmetic. The packed-int
  // nine-byte form accepts values up to UINT64_MAX, and both the bitmap byte
  // count below and the scan loops derive from this value; a count within 7 of
  // the maximum would collapse the byte count and let the scans walk past the
  // buffer. Cross-checking against the TABLE_MAP declaration here (rather than
  // after the scans) also keeps a mismatched count from being scanned at all.
  if (col_count == 0 || col_count > binary::kMaxTableColumns) return nullptr;
  if (col_count != expected_column_count) return nullptr;
  ptr += consumed;
  left -= consumed;

  // columns_present bitmap
  size_t bitmap_bytes = binary::BitmapBytes(col_count);
  if (left < bitmap_bytes) return nullptr;
  *columns_present = ptr;
  for (uint64_t i = 0; i < col_count; ++i) {
    if (!binary::BitmapIsSet(*columns_present, static_cast<size_t>(i))) return nullptr;
  }
  ptr += bitmap_bytes;
  left -= bitmap_bytes;

  // For UPDATE: columns_present_update bitmap
  if (is_update) {
    if (left < bitmap_bytes) return nullptr;
    *columns_present_update = ptr;
    for (uint64_t i = 0; i < col_count; ++i) {
      if (!binary::BitmapIsSet(*columns_present_update, static_cast<size_t>(i))) return nullptr;
    }
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

// Decode a single row from the data pointer. Advances ptr and remaining, and
// charges everything the row materializes against budget.
bool DecodeOneRow(const uint8_t*& ptr, size_t& remaining, const TableMetadata& metadata,
                  size_t column_count, size_t present_count, const uint8_t* columns_present,
                  DecodeBudget& budget, RowData* row) {
  if (present_count == 0) return false;

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
      // ParseRowsPostHeader rejects partial row images because the public C
      // ABI has no "absent" state distinct from SQL NULL.
      return false;
    }

    if (binary::BitmapIsSet(null_bitmap, null_bit_index)) {
      row->columns[i] = ColumnValue::Null(col_type);
      null_bit_index++;
      continue;
    }
    null_bit_index++;

    const uint32_t meta = col_info ? col_info->metadata : 0;
    const bool is_unsigned = col_info ? col_info->is_unsigned : false;
    const bool charset_known = col_info ? col_info->charset_known : false;
    const bool binary_charset = charset_known && col_info->charset_id == kBinaryCollationId;

    size_t consumed = 0;
    row->columns[i] = DecodeColumnValue(col_type, meta, is_unsigned, ptr, remaining, &consumed,
                                        charset_known, binary_charset, budget.Remaining());
    if (!budget.Charge(row->columns[i].string_val.size())) return false;
    if (consumed == 0) {
      // consumed=0 serves a dual purpose — it signals
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

// Room for any temporal rendering below: an optional sign, six integer fields
// at the widest rendering binary::WritePaddedInt can produce (11 characters),
// five separators and a fractional part.
constexpr size_t kTemporalBufferSize = 96;

// Write the fractional part at the column's declared precision. fsp must not
// exceed 6.
char* WriteFractional(char* out, int usec, uint16_t fsp) {
  if (fsp == 0) return out;
  static constexpr int kDivisor[] = {1, 100000, 10000, 1000, 100, 10, 1};
  *out++ = '.';
  return binary::WritePaddedInt(out, usec / kDivisor[fsp], static_cast<int>(fsp));
}

// Append the fractional part at the column's declared precision.
void AppendFractional(std::string& out, int usec, uint16_t fsp) {
  char buf[24];
  out.append(buf, static_cast<size_t>(WriteFractional(buf, usec, fsp) - buf));
}

// Write "YYYY-MM-DD" with MySQL's zero padding.
char* WriteCalendarDate(char* out, int year, int month, int day) {
  out = binary::WritePaddedInt(out, year, 4);
  *out++ = '-';
  out = binary::WritePaddedInt(out, month, 2);
  *out++ = '-';
  return binary::WritePaddedInt(out, day, 2);
}

// Write "hh:mm:ss" with MySQL's zero padding.
char* WriteClockTime(char* out, int hour, int minute, int second) {
  out = binary::WritePaddedInt(out, hour, 2);
  *out++ = ':';
  out = binary::WritePaddedInt(out, minute, 2);
  *out++ = ':';
  return binary::WritePaddedInt(out, second, 2);
}

// field_limit is the column type's own ceiling; value_limit is what the
// event's decode budget still allows. Expansion is clamped to both before a
// single byte is allocated, so a compressed field can never overshoot the
// budget even transiently.
bool DecodeMariaCompressedPayload(const uint8_t* data, size_t len, size_t field_limit,
                                  size_t value_limit, std::string* output) {
  output->clear();
  if (len == 0) return true;  // MariaDB stores an empty value with no header.

  const uint8_t header = data[0];
  const uint8_t method = static_cast<uint8_t>(header >> 4);
  if (method == 0) {
    if (header != 0) return false;
    if (len - 1 > value_limit) return false;
    output->assign(reinterpret_cast<const char*>(data + 1), len - 1);
    return true;
  }
  if (method != 8) return false;

  const size_t original_length_bytes = header & 0x07;
  if (original_length_bytes == 0 || original_length_bytes > 4 || len < 1 + original_length_bytes) {
    return false;
  }
  const uint64_t original_length = ReadBigEndian(data + 1, original_length_bytes);
  const size_t safe_limit = std::min({field_limit, kMaxDecompressedColumnBytes, value_limit});
  if (original_length == 0 || original_length > safe_limit ||
      original_length > std::numeric_limits<uInt>::max()) {
    return false;
  }

  const size_t compressed_offset = 1 + original_length_bytes;
  const size_t compressed_length = len - compressed_offset;
  if (compressed_length == 0 || compressed_length > std::numeric_limits<uInt>::max()) {
    return false;
  }

  output->resize(static_cast<size_t>(original_length));
  z_stream stream{};
  stream.next_in = const_cast<Bytef*>(reinterpret_cast<const Bytef*>(data + compressed_offset));
  stream.avail_in = static_cast<uInt>(compressed_length);
  stream.next_out = reinterpret_cast<Bytef*>(&(*output)[0]);
  stream.avail_out = static_cast<uInt>(original_length);

  const int window_bits = (header & 0x08) != 0 ? -MAX_WBITS : MAX_WBITS;
  if (inflateInit2(&stream, window_bits) != Z_OK) {
    output->clear();
    return false;
  }
  const int inflate_result = inflate(&stream, Z_FINISH);
  const bool ok =
      inflate_result == Z_STREAM_END && stream.total_out == original_length && stream.avail_in == 0;
  const int end_result = inflateEnd(&stream);
  if (!ok || end_result != Z_OK) {
    output->clear();
    return false;
  }
  return true;
}

struct RowsContext {
  const uint8_t* ptr;
  size_t remaining;
  size_t column_count;
  size_t present_count;
  size_t present_count_update;
  const uint8_t* columns_present;
  const uint8_t* columns_present_update;
};

bool ParseRowsContext(const uint8_t* data, size_t len, const TableMetadata& metadata, bool is_v2,
                      bool is_update, RowsContext* ctx) {
  if (!data || !ctx) return false;
  ctx->columns_present_update = nullptr;
  ctx->ptr =
      ParseRowsPostHeader(data, len, is_v2, is_update, metadata.columns.size(), &ctx->column_count,
                          &ctx->columns_present, &ctx->columns_present_update, &ctx->remaining);
  if (ctx->ptr == nullptr) return false;
  ctx->present_count = CountPresentColumns(ctx->columns_present, ctx->column_count);
  ctx->present_count_update =
      is_update ? CountPresentColumns(ctx->columns_present_update, ctx->column_count) : 0;
  return ctx->present_count > 0 && (!is_update || ctx->present_count_update > 0);
}

}  // namespace

ColumnValue DecodeColumnValue(ColumnType type, uint32_t meta, bool is_unsigned, const uint8_t* data,
                              size_t len, size_t* bytes_consumed, bool charset_known,
                              bool binary_charset, size_t max_value_bytes) {
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
      // MySQL stores FLOAT little-endian. Assemble the bit pattern via a
      // byte-order-independent read, then reinterpret it as a host float so
      // decoding is correct on big-endian builds as well.
      uint32_t bits = binary::ReadU32Le(data);
      float fval = 0.0f;
      std::memcpy(&fval, &bits, 4);
      return ColumnValue::Float(static_cast<double>(fval));
    }

    case ColumnType::kDouble: {
      if (len < 8) return ColumnValue::Null(type);
      *bytes_consumed = 8;
      // MySQL stores DOUBLE little-endian; see kFloat for the rationale.
      uint64_t bits = binary::ReadU64Le(data);
      double dval = 0.0;
      std::memcpy(&dval, &bits, 8);
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
      return MakeCharacterValue(type, data + prefix_size, str_len, charset_known, binary_charset);
    }

    case ColumnType::kVarcharCompressed: {
      const size_t prefix_size = meta > 255 ? 2 : 1;
      if (len < prefix_size) return ColumnValue::Null(type);
      const size_t payload_length =
          prefix_size == 2 ? binary::ReadU16Le(data) : binary::ReadU8(data);
      if (payload_length > len - prefix_size) return ColumnValue::Null(type);

      std::string value;
      if (!DecodeMariaCompressedPayload(data + prefix_size, payload_length, meta, max_value_bytes,
                                        &value)) {
        return ColumnValue::Null(type);
      }
      *bytes_consumed = prefix_size + payload_length;
      return MakeCharacterValue(type, std::move(value), charset_known, binary_charset);
    }

    case ColumnType::kBlob:
    case ColumnType::kTinyBlob:
    case ColumnType::kMediumBlob:
    case ColumnType::kLongBlob:
    case ColumnType::kVector: {
      // Note: MySQL TABLE_MAP should always record a valid
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
      if (blob_len > len - prefix_consumed) return ColumnValue::Null(type);
      *bytes_consumed = prefix_consumed + blob_len;
      return MakeCharacterValue(type, data + prefix_consumed, blob_len, charset_known,
                                binary_charset);
    }

    case ColumnType::kBlobCompressed: {
      const uint8_t pack_length = static_cast<uint8_t>(meta);
      if (pack_length == 0 || pack_length > 4) return ColumnValue::Null(type);
      size_t prefix_consumed = 0;
      const uint32_t payload_length =
          binary::ReadVarLenPrefix(pack_length, data, len, &prefix_consumed);
      if (prefix_consumed == 0 || payload_length > len - prefix_consumed) {
        return ColumnValue::Null(type);
      }

      const size_t field_limit = pack_length == 4 ? std::numeric_limits<uint32_t>::max()
                                                  : (size_t{1} << (pack_length * 8)) - 1;
      std::string value;
      if (!DecodeMariaCompressedPayload(data + prefix_consumed, payload_length, field_limit,
                                        max_value_bytes, &value)) {
        return ColumnValue::Null(type);
      }
      *bytes_consumed = prefix_consumed + payload_length;
      return MakeCharacterValue(type, std::move(value), charset_known, binary_charset);
    }

    case ColumnType::kJson: {
      uint8_t pack_length = static_cast<uint8_t>(meta);
      if (pack_length == 0) pack_length = 4;
      if (pack_length > 4) return ColumnValue::Null(type);
      size_t prefix_consumed = 0;
      uint32_t json_len = binary::ReadVarLenPrefix(pack_length, data, len, &prefix_consumed);
      if (prefix_consumed == 0) return ColumnValue::Null(type);
      if (json_len > len - prefix_consumed) return ColumnValue::Null(type);
      *bytes_consumed = prefix_consumed + json_len;
      // Store JSON as bytes (binary JSON format, not text)
      return ColumnValue::Bytes(type, data + prefix_consumed, json_len);
    }

    case ColumnType::kTypedArray: {
      // Field_typed_array derives from Field_json. Its TABLE_MAP metadata
      // describes the logical array element, but its row bytes use JSON's
      // fixed four-byte BLOB length followed by MySQL binary JSON.
      size_t prefix_consumed = 0;
      const uint32_t json_len = binary::ReadVarLenPrefix(4, data, len, &prefix_consumed);
      if (prefix_consumed == 0 || json_len > len - prefix_consumed) {
        return ColumnValue::Null(type);
      }
      *bytes_consumed = prefix_consumed + json_len;
      return ColumnValue::Bytes(type, data + prefix_consumed, json_len);
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
        if (val > static_cast<uint64_t>(INT64_MAX)) {
          return ColumnValue::String(ColumnType::kSet, std::to_string(val));
        }
        return ColumnValue::Int(ColumnType::kSet, static_cast<int64_t>(val));
      }

      // CHAR type
      // Note: Formula matches MySQL server source (log_event.cc
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
      return MakeCharacterValue(type, data + prefix_size, str_len, charset_known, binary_charset);
    }

    case ColumnType::kDate: {
      if (len < 3) return ColumnValue::Null(type);
      *bytes_consumed = 3;
      uint32_t val = binary::ReadU24Le(data);
      int day = val & 0x1F;
      int month = (val >> 5) & 0x0F;
      int year = val >> 9;
      char buf[kTemporalBufferSize];
      char* end = WriteCalendarDate(buf, year, month, day);
      return ColumnValue::String(type, std::string(buf, static_cast<size_t>(end - buf)));
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
      char buf[kTemporalBufferSize];
      char* end = buf;
      if (negative) *end++ = '-';
      end = WriteClockTime(end, hour, min, sec);
      return ColumnValue::String(type, std::string(buf, static_cast<size_t>(end - buf)));
    }

    case ColumnType::kTimestamp: {
      if (len < 4) return ColumnValue::Null(type);
      *bytes_consumed = 4;
      uint32_t val = binary::ReadU32Le(data);
      return ColumnValue::String(type, std::to_string(val));
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
      char buf[kTemporalBufferSize];
      char* end = WriteCalendarDate(buf, year, month, day);
      *end++ = ' ';
      end = WriteClockTime(end, hour, min, sec);
      return ColumnValue::String(type, std::string(buf, static_cast<size_t>(end - buf)));
    }

    case ColumnType::kDatetime2: {
      if (meta > 6) return ColumnValue::Null(type);
      size_t frac_bytes = (meta + 1) / 2;
      size_t total = 5 + frac_bytes;
      if (len < total) return ColumnValue::Null(type);
      *bytes_consumed = total;

      // MySQL stores DATETIME2 as a 5-byte integer part (offset by
      // 0x8000000000) plus a fractional part. Like TIME2, negative values are
      // stored in complement form, so the fraction cannot be decoded
      // independently of the sign — reconstruct the combined packed value
      // exactly as my_datetime_packed_from_binary does, then split sign and
      // magnitude. Legitimate MySQL datetimes (year >= 1) are always positive;
      // this matters for the sub-epoch values a malicious server could emit.
      static constexpr int64_t kDatetime2IntOfs = 0x8000000000LL;
      int64_t intpart = static_cast<int64_t>(ReadBigEndian(data, 5)) - kDatetime2IntOfs;

      int64_t usec = 0;
      if (frac_bytes > 0) {
        int64_t frac = static_cast<int64_t>(ReadBigEndian(data + 5, frac_bytes));
        int64_t complement = 0;
        int64_t multiplier = 1;
        switch (frac_bytes) {
          case 1:  // fsp 1,2: stored in 1/100s
            complement = 0x100;
            multiplier = 10000;
            break;
          case 2:  // fsp 3,4: stored in 1/10000s
            complement = 0x10000;
            multiplier = 100;
            break;
          default:  // fsp 5,6: stored in microseconds (3 bytes)
            complement = 0x1000000;
            multiplier = 1;
            break;
        }
        if (intpart < 0 && frac > 0) {
          // Reverse the complement encoding used for negative fractions.
          ++intpart;
          frac -= complement;
        }
        usec = frac * multiplier;
      }

      // Multiplication is defined for negative operands; left-shifting a
      // negative signed integer is undefined in C++17. Keep the magnitude
      // unsigned as well so the minimum representable packed value is safe.
      int64_t packed = intpart * (int64_t{1} << 24) + usec;
      bool negative = packed < 0;
      uint64_t magnitude =
          negative ? uint64_t{0} - static_cast<uint64_t>(packed) : static_cast<uint64_t>(packed);

      uint64_t datetime = magnitude >> 24;
      int micros = static_cast<int>(magnitude & 0xFFFFFF);
      uint64_t ymd = datetime >> 17;
      uint64_t hms = datetime & 0x1FFFF;
      uint64_t ym = ymd >> 5;
      int day = static_cast<int>(ymd & 0x1F);
      int month = static_cast<int>(ym % 13);
      int year = static_cast<int>(ym / 13);
      int second = static_cast<int>(hms & 0x3F);
      int minute = static_cast<int>((hms >> 6) & 0x3F);
      int hour = static_cast<int>(hms >> 12);

      char buf[kTemporalBufferSize];
      char* end = buf;
      if (negative) *end++ = '-';
      end = WriteCalendarDate(end, year, month, day);
      *end++ = ' ';
      end = WriteClockTime(end, hour, minute, second);

      if (frac_bytes > 0) {
        end = WriteFractional(end, micros, static_cast<uint16_t>(meta));
      }

      return ColumnValue::String(type, std::string(buf, static_cast<size_t>(end - buf)));
    }

    case ColumnType::kTimestamp2: {
      if (meta > 6) return ColumnValue::Null(type);
      size_t frac_bytes = (meta + 1) / 2;
      size_t total = 4 + frac_bytes;
      if (len < total) return ColumnValue::Null(type);
      *bytes_consumed = total;

      uint32_t ts = binary::ReadU32Be(data);
      std::string result = std::to_string(ts);

      if (meta > 0 && frac_bytes > 0) {
        int frac = static_cast<int>(ReadBigEndian(data + 4, frac_bytes));
        int usec = FracToMicroseconds(frac, meta);
        AppendFractional(result, usec, static_cast<uint16_t>(meta));
      }

      return ColumnValue::String(type, result);
    }

    case ColumnType::kTime2: {
      if (meta > 6) return ColumnValue::Null(type);
      size_t frac_bytes = (meta + 1) / 2;
      size_t total = 3 + frac_bytes;
      if (len < total) return ColumnValue::Null(type);
      *bytes_consumed = total;

      // MySQL stores TIME2 as a single signed packed value (integer part plus
      // fractional part) offset by 0x800000. Negative values are stored in
      // complement form, so the fraction cannot be decoded independently of the
      // sign — we must reconstruct the combined packed value exactly as
      // my_time_packed_from_binary does, then split sign and magnitude.
      static constexpr int32_t kTimeIntOfs = 0x800000;
      int64_t intpart = static_cast<int64_t>(binary::ReadU24Be(data)) - kTimeIntOfs;

      int64_t usec = 0;
      if (frac_bytes > 0) {
        int64_t frac = static_cast<int64_t>(ReadBigEndian(data + 3, frac_bytes));
        int64_t complement = 0;
        int64_t multiplier = 1;
        switch (frac_bytes) {
          case 1:  // fsp 1,2: stored in 1/100s
            complement = 0x100;
            multiplier = 10000;
            break;
          case 2:  // fsp 3,4: stored in 1/10000s
            complement = 0x10000;
            multiplier = 100;
            break;
          default:  // fsp 5,6: stored in microseconds (3 bytes)
            complement = 0x1000000;
            multiplier = 1;
            break;
        }
        if (intpart < 0 && frac > 0) {
          // Reverse the complement encoding used for negative fractions.
          ++intpart;
          frac -= complement;
        }
        usec = frac * multiplier;
      }

      // Left-shifting negative signed values is undefined in C++17.
      int64_t packed_time = intpart * (int64_t{1} << 24) + usec;
      bool negative = packed_time < 0;
      if (negative) packed_time = -packed_time;

      int64_t hms = packed_time >> 24;
      int micros = static_cast<int>(packed_time & 0xFFFFFF);
      int hour = static_cast<int>(hms >> 12);
      int minute = static_cast<int>((hms >> 6) & 0x3F);
      int second = static_cast<int>(hms & 0x3F);

      char buf[kTemporalBufferSize];
      char* end = buf;
      if (negative) *end++ = '-';
      end = WriteClockTime(end, hour, minute, second);

      if (frac_bytes > 0) {
        end = WriteFractional(end, micros, static_cast<uint16_t>(meta));
      }

      return ColumnValue::String(type, std::string(buf, static_cast<size_t>(end - buf)));
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
      if (val > static_cast<uint64_t>(INT64_MAX)) {
        return ColumnValue::String(type, std::to_string(val));
      }
      return ColumnValue::Int(type, static_cast<int64_t>(val));
    }

    case ColumnType::kGeometry: {
      uint8_t pack_length = static_cast<uint8_t>(meta);
      if (pack_length == 0) pack_length = 4;
      if (pack_length > 4) return ColumnValue::Null(type);
      size_t prefix_consumed = 0;
      uint32_t geo_len = binary::ReadVarLenPrefix(pack_length, data, len, &prefix_consumed);
      if (prefix_consumed == 0) return ColumnValue::Null(type);
      if (geo_len > len - prefix_consumed) return ColumnValue::Null(type);
      *bytes_consumed = prefix_consumed + geo_len;
      return ColumnValue::Bytes(type, data + prefix_consumed, geo_len);
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
                             bool is_v2, std::vector<RowData>* rows, DecodeBudget* budget) {
  if (!rows) return false;
  RowsContext ctx{};
  if (!ParseRowsContext(data, len, metadata, is_v2, false, &ctx)) return false;

  DecodeBudget local_budget = DecodeBudget::ForEventBody(len);
  DecodeBudget& event_budget = budget != nullptr ? *budget : local_budget;

  rows->clear();
  rows->reserve(8);  // typical rows per event; avoids reallocation in common case
  while (ctx.remaining > 0) {
    const size_t remaining_before = ctx.remaining;
    RowData row;
    if (!DecodeOneRow(ctx.ptr, ctx.remaining, metadata, ctx.column_count, ctx.present_count,
                      ctx.columns_present, event_budget, &row)) {
      return false;
    }
    if (ctx.remaining >= remaining_before) return false;
    rows->push_back(std::move(row));
  }
  return true;
}

bool DecodeWriteRows(const uint8_t* data, size_t len, const TableMetadata& metadata, bool is_v2,
                     std::vector<RowData>* rows, DecodeBudget* budget) {
  return DecodeSimpleRows(data, len, metadata, is_v2, rows, budget);
}

bool DecodeUpdateRows(const uint8_t* data, size_t len, const TableMetadata& metadata, bool is_v2,
                      std::vector<UpdatePair>* pairs, DecodeBudget* budget) {
  if (!pairs) return false;
  RowsContext ctx{};
  // Note: When ParseRowsContext returns true with is_update=true,
  // ParseRowsPostHeader guarantees that ctx.columns_present_update is
  // non-null -- the is_update branch inside ParseRowsPostHeader either
  // assigns *columns_present_update and returns a non-null pointer, or
  // returns nullptr (in which case ParseRowsContext returns false and we
  // bail out here). A previous review flagged this as a potential NULL
  // deref; it is not reachable. Keep the control flow intact.
  if (!ParseRowsContext(data, len, metadata, is_v2, true, &ctx)) return false;

  DecodeBudget local_budget = DecodeBudget::ForEventBody(len);
  DecodeBudget& event_budget = budget != nullptr ? *budget : local_budget;

  pairs->clear();
  pairs->reserve(8);
  while (ctx.remaining > 0) {
    const size_t remaining_before = ctx.remaining;
    UpdatePair pair;
    if (!DecodeOneRow(ctx.ptr, ctx.remaining, metadata, ctx.column_count, ctx.present_count,
                      ctx.columns_present, event_budget, &pair.before)) {
      return false;
    }
    if (!DecodeOneRow(ctx.ptr, ctx.remaining, metadata, ctx.column_count, ctx.present_count_update,
                      ctx.columns_present_update, event_budget, &pair.after)) {
      return false;
    }
    if (ctx.remaining >= remaining_before) return false;
    pairs->push_back(std::move(pair));
  }
  return true;
}

bool DecodeDeleteRows(const uint8_t* data, size_t len, const TableMetadata& metadata, bool is_v2,
                      std::vector<RowData>* rows, DecodeBudget* budget) {
  return DecodeSimpleRows(data, len, metadata, is_v2, rows, budget);
}

}  // namespace mes
