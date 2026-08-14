// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * @file workloads.h
 * @brief Synthetic binlog streams shaped like real CDC traffic.
 *
 * The schemas here exist so the decoder is measured on what production
 * actually sends: wide tables, string payloads, temporal and DECIMAL columns,
 * UPDATE before/after images, and MariaDB ANNOTATE_ROWS statements attached to
 * multi-row events. Every event carries a valid CRC32 and full TABLE_MAP
 * optional metadata (SIGNEDNESS / COLUMN_CHARSET / COLUMN_NAME), so the decode
 * path taken is the same one a real server drives.
 */

#ifndef MES_CORE_BENCHMARKS_WORKLOADS_H_
#define MES_CORE_BENCHMARKS_WORKLOADS_H_

#include <algorithm>
#include <cstdint>
#include <string>
#include <vector>

#include "binary_util.h"
#include "event_header.h"
#include "test_helpers.h"

namespace mes {
namespace bench {

/// One column of a benchmark schema: TABLE_MAP description plus the row bytes
/// that column contributes to every row image.
struct ColumnSpec {
  std::string name;
  uint8_t type_code = 0;
  std::vector<uint8_t> table_map_meta;
  std::vector<uint8_t> row_value;
  bool numeric = false;    ///< Consumes a SIGNEDNESS bit.
  bool character = false;  ///< Consumes a COLUMN_CHARSET slot.
  uint8_t collation = 45;  ///< utf8mb4_general_ci unless overridden.
};

// --- Value encoders -------------------------------------------------------

inline std::vector<uint8_t> IntLe(uint64_t value, size_t bytes) {
  std::vector<uint8_t> out(bytes);
  for (size_t i = 0; i < bytes; ++i) out[i] = static_cast<uint8_t>(value >> (i * 8));
  return out;
}

inline std::vector<uint8_t> IntBe(uint64_t value, size_t bytes) {
  std::vector<uint8_t> out(bytes);
  for (size_t i = 0; i < bytes; ++i) out[i] = static_cast<uint8_t>(value >> ((bytes - 1 - i) * 8));
  return out;
}

/// Length-prefixed payload as VARCHAR / CHAR / BLOB store it on the wire.
inline std::vector<uint8_t> LengthPrefixed(const std::string& text, size_t prefix_bytes) {
  std::vector<uint8_t> out = IntLe(text.size(), prefix_bytes);
  out.insert(out.end(), text.begin(), text.end());
  return out;
}

/// DATETIME2: 5-byte big-endian integer part (offset 0x8000000000) plus an
/// fsp-sized fraction.
inline std::vector<uint8_t> Datetime2(int year, int month, int day, int hour, int minute, int sec,
                                      int micros, uint8_t fsp) {
  const int64_t ymd = ((static_cast<int64_t>(year) * 13 + month) << 5) | day;
  const int64_t hms = (static_cast<int64_t>(hour) << 12) | (minute << 6) | sec;
  const int64_t intpart = (ymd << 17) | hms;
  std::vector<uint8_t> out = IntBe(static_cast<uint64_t>(intpart + 0x8000000000LL), 5);
  if (fsp >= 1 && fsp <= 2) {
    const std::vector<uint8_t> frac = IntBe(static_cast<uint64_t>(micros / 10000), 1);
    out.insert(out.end(), frac.begin(), frac.end());
  } else if (fsp >= 3 && fsp <= 4) {
    const std::vector<uint8_t> frac = IntBe(static_cast<uint64_t>(micros / 100), 2);
    out.insert(out.end(), frac.begin(), frac.end());
  } else if (fsp >= 5 && fsp <= 6) {
    const std::vector<uint8_t> frac = IntBe(static_cast<uint64_t>(micros), 3);
    out.insert(out.end(), frac.begin(), frac.end());
  }
  return out;
}

/// TIMESTAMP2: 4-byte big-endian epoch seconds plus an fsp-sized fraction.
inline std::vector<uint8_t> Timestamp2(uint32_t epoch, int micros, uint8_t fsp) {
  std::vector<uint8_t> out = IntBe(epoch, 4);
  if (fsp >= 1 && fsp <= 2) {
    const std::vector<uint8_t> frac = IntBe(static_cast<uint64_t>(micros / 10000), 1);
    out.insert(out.end(), frac.begin(), frac.end());
  } else if (fsp >= 3 && fsp <= 4) {
    const std::vector<uint8_t> frac = IntBe(static_cast<uint64_t>(micros / 100), 2);
    out.insert(out.end(), frac.begin(), frac.end());
  } else if (fsp >= 5 && fsp <= 6) {
    const std::vector<uint8_t> frac = IntBe(static_cast<uint64_t>(micros), 3);
    out.insert(out.end(), frac.begin(), frac.end());
  }
  return out;
}

/// TIME2 at fsp 0: 3-byte big-endian packed value offset by 0x800000.
inline std::vector<uint8_t> Time2(int hour, int minute, int sec) {
  const int64_t hms = (static_cast<int64_t>(hour) << 12) | (minute << 6) | sec;
  return IntBe(static_cast<uint64_t>(hms + 0x800000), 3);
}

/// DATE: 3-byte little-endian (year << 9) | (month << 5) | day.
inline std::vector<uint8_t> Date(int year, int month, int day) {
  return IntLe(static_cast<uint64_t>((year << 9) | (month << 5) | day), 3);
}

/// NEWDECIMAL as MySQL's bin2decimal reads it: leading partial group, then
/// 9-digit groups of 4 bytes each, same again for the fraction, with the sign
/// bit of the first byte set for a positive value. `digits` holds exactly
/// `precision` decimal digits with the point implied before the last `scale`.
inline std::vector<uint8_t> Decimal(uint8_t precision, uint8_t scale, const std::string& digits) {
  static const int kDig2Bytes[10] = {0, 1, 1, 2, 2, 3, 3, 4, 4, 4};
  const int intg = precision - scale;
  const int intg0 = intg / 9;
  const int intg_rem = intg % 9;
  const int frac0 = scale / 9;
  const int frac_rem = scale % 9;

  std::vector<uint8_t> out;
  size_t pos = 0;
  auto group = [&](int digit_count, int byte_count) {
    uint32_t value = 0;
    for (int i = 0; i < digit_count; ++i) {
      value = value * 10 + static_cast<uint32_t>(digits[pos++] - '0');
    }
    const std::vector<uint8_t> encoded = IntBe(value, static_cast<size_t>(byte_count));
    out.insert(out.end(), encoded.begin(), encoded.end());
  };

  if (intg_rem > 0) group(intg_rem, kDig2Bytes[intg_rem]);
  for (int i = 0; i < intg0; ++i) group(9, 4);
  for (int i = 0; i < frac0; ++i) group(9, 4);
  if (frac_rem > 0) group(frac_rem, kDig2Bytes[frac_rem]);

  if (!out.empty()) out[0] ^= 0x80;
  return out;
}

// --- Column constructors --------------------------------------------------

inline ColumnSpec Fixed(const std::string& name, uint8_t type_code, uint64_t value, size_t bytes) {
  ColumnSpec c;
  c.name = name;
  c.type_code = type_code;
  c.row_value = IntLe(value, bytes);
  c.numeric = true;
  return c;
}

/// VARCHAR: TABLE_MAP metadata is the byte length, which also selects the
/// one- or two-byte row prefix.
inline ColumnSpec Varchar(const std::string& name, uint16_t max_bytes, const std::string& text) {
  ColumnSpec c;
  c.name = name;
  c.type_code = static_cast<uint8_t>(ColumnType::kVarchar);
  c.table_map_meta = IntLe(max_bytes, 2);
  c.row_value = LengthPrefixed(text, max_bytes > 255 ? 2 : 1);
  c.character = true;
  return c;
}

/// CHAR(n) with n <= 255: metadata byte 0 is MYSQL_TYPE_STRING, byte 1 the
/// length; the row image still carries a one-byte length prefix.
inline ColumnSpec Char(const std::string& name, uint8_t max_bytes, const std::string& text) {
  ColumnSpec c;
  c.name = name;
  c.type_code = static_cast<uint8_t>(ColumnType::kString);
  c.table_map_meta = {static_cast<uint8_t>(ColumnType::kString), max_bytes};
  c.row_value = LengthPrefixed(text, 1);
  c.character = true;
  return c;
}

/// TEXT / BLOB. `pack_length` is the row prefix width (2 = TEXT/BLOB,
/// 4 = LONGTEXT/LONGBLOB). Collation 63 marks the column binary.
inline ColumnSpec Blob(const std::string& name, uint8_t pack_length, const std::string& payload,
                       uint8_t collation) {
  ColumnSpec c;
  c.name = name;
  c.type_code = static_cast<uint8_t>(ColumnType::kBlob);
  c.table_map_meta = {pack_length};
  c.row_value = LengthPrefixed(payload, pack_length);
  c.character = true;
  c.collation = collation;
  return c;
}

// --- Event assembly -------------------------------------------------------

/// TABLE_MAP body carrying SIGNEDNESS, COLUMN_CHARSET and COLUMN_NAME, the
/// same optional metadata a server sends with binlog_row_metadata=FULL.
inline std::vector<uint8_t> BuildTableMap(uint64_t table_id, const std::string& db,
                                          const std::string& table,
                                          const std::vector<ColumnSpec>& columns) {
  test::EventBuilder b;
  b.WriteU48Le(table_id);
  b.WriteU16Le(0);
  b.WriteU8(static_cast<uint8_t>(db.size()));
  b.WriteString(db);
  b.WriteU8(0);
  b.WriteU8(static_cast<uint8_t>(table.size()));
  b.WriteString(table);
  b.WriteU8(0);
  b.WriteU8(static_cast<uint8_t>(columns.size()));
  for (const auto& c : columns) b.WriteU8(c.type_code);

  std::vector<uint8_t> meta;
  for (const auto& c : columns) {
    meta.insert(meta.end(), c.table_map_meta.begin(), c.table_map_meta.end());
  }
  b.WriteU8(static_cast<uint8_t>(meta.size()));
  b.WriteBytes(meta);

  const size_t bitmap_bytes = binary::BitmapBytes(columns.size());
  for (size_t i = 0; i < bitmap_bytes; ++i) {
    const size_t bits = std::min<size_t>(8, columns.size() - i * 8);
    b.WriteU8(static_cast<uint8_t>((1u << bits) - 1));  // every column nullable
  }

  // SIGNEDNESS: one MSB-first bit per numeric column, all signed.
  size_t numeric_count = 0;
  for (const auto& c : columns) {
    if (c.numeric) ++numeric_count;
  }
  if (numeric_count > 0) {
    const size_t signedness_bytes = (numeric_count + 7) / 8;
    b.WriteU8(1);
    b.WriteU8(static_cast<uint8_t>(signedness_bytes));
    for (size_t i = 0; i < signedness_bytes; ++i) b.WriteU8(0x00);
  }

  // COLUMN_CHARSET: one collation id per character column, in TABLE_MAP order.
  std::vector<uint8_t> charsets;
  for (const auto& c : columns) {
    if (c.character) charsets.push_back(c.collation);
  }
  if (!charsets.empty()) {
    b.WriteU8(3);
    b.WriteU8(static_cast<uint8_t>(charsets.size()));
    b.WriteBytes(charsets);
  }

  // COLUMN_NAME: one length-encoded name per column.
  std::vector<uint8_t> names;
  for (const auto& c : columns) {
    names.push_back(static_cast<uint8_t>(c.name.size()));
    names.insert(names.end(), c.name.begin(), c.name.end());
  }
  b.WriteU8(4);
  b.WriteU8(static_cast<uint8_t>(names.size()));
  b.WriteBytes(names);

  return b.Data();
}

/// Concatenated row payload for one full row image.
inline std::vector<uint8_t> BuildRowImage(const std::vector<ColumnSpec>& columns) {
  std::vector<uint8_t> out;
  for (const auto& c : columns) out.insert(out.end(), c.row_value.begin(), c.row_value.end());
  return out;
}

/// WRITE_ROWS / DELETE_ROWS v2 body carrying `rows` identical row images.
inline std::vector<uint8_t> BuildRowsBody(uint64_t table_id, const std::vector<ColumnSpec>& columns,
                                          size_t rows) {
  test::EventBuilder b;
  b.WriteU48Le(table_id);
  b.WriteU16Le(0);
  b.WriteU16Le(2);  // v2 extra header: length only
  b.WriteU8(static_cast<uint8_t>(columns.size()));
  const size_t bitmap_bytes = binary::BitmapBytes(columns.size());
  for (size_t i = 0; i < bitmap_bytes; ++i) {
    const size_t bits = std::min<size_t>(8, columns.size() - i * 8);
    b.WriteU8(static_cast<uint8_t>((1u << bits) - 1));
  }
  const std::vector<uint8_t> image = BuildRowImage(columns);
  for (size_t r = 0; r < rows; ++r) {
    for (size_t i = 0; i < bitmap_bytes; ++i) b.WriteU8(0x00);  // no NULLs
    b.WriteBytes(image);
  }
  return b.Data();
}

/// UPDATE_ROWS v2 body: `rows` before/after pairs. The after image differs
/// from the before image so the decoder cannot share any work between them.
inline std::vector<uint8_t> BuildUpdateRowsBody(uint64_t table_id,
                                                const std::vector<ColumnSpec>& before,
                                                const std::vector<ColumnSpec>& after, size_t rows) {
  test::EventBuilder b;
  b.WriteU48Le(table_id);
  b.WriteU16Le(0);
  b.WriteU16Le(2);
  b.WriteU8(static_cast<uint8_t>(before.size()));
  const size_t bitmap_bytes = binary::BitmapBytes(before.size());
  for (int pass = 0; pass < 2; ++pass) {
    for (size_t i = 0; i < bitmap_bytes; ++i) {
      const size_t bits = std::min<size_t>(8, before.size() - i * 8);
      b.WriteU8(static_cast<uint8_t>((1u << bits) - 1));
    }
  }
  const std::vector<uint8_t> before_image = BuildRowImage(before);
  const std::vector<uint8_t> after_image = BuildRowImage(after);
  for (size_t r = 0; r < rows; ++r) {
    for (size_t i = 0; i < bitmap_bytes; ++i) b.WriteU8(0x00);
    b.WriteBytes(before_image);
    for (size_t i = 0; i < bitmap_bytes; ++i) b.WriteU8(0x00);
    b.WriteBytes(after_image);
  }
  return b.Data();
}

// --- Schemas --------------------------------------------------------------

/// Single nullable INT: the shape the original feed benchmark measures.
inline std::vector<ColumnSpec> SchemaInt1() {
  return {Fixed("id", static_cast<uint8_t>(ColumnType::kLong), 42, 4)};
}

/// Seven columns dominated by the temporal / DECIMAL formatting path.
inline std::vector<ColumnSpec> SchemaTemporal7() {
  std::vector<ColumnSpec> cols;
  cols.push_back(Fixed("id", static_cast<uint8_t>(ColumnType::kLongLong), 1234567890123ULL, 8));

  ColumnSpec created;
  created.name = "created_at";
  created.type_code = static_cast<uint8_t>(ColumnType::kDatetime2);
  created.table_map_meta = {6};
  created.row_value = Datetime2(2026, 8, 15, 13, 45, 7, 123456, 6);
  cols.push_back(created);

  ColumnSpec updated;
  updated.name = "updated_at";
  updated.type_code = static_cast<uint8_t>(ColumnType::kDatetime2);
  updated.table_map_meta = {0};
  updated.row_value = Datetime2(2026, 8, 15, 13, 45, 8, 0, 0);
  cols.push_back(updated);

  ColumnSpec ts;
  ts.name = "event_ts";
  ts.type_code = static_cast<uint8_t>(ColumnType::kTimestamp2);
  ts.table_map_meta = {3};
  ts.row_value = Timestamp2(1786000000u, 456000, 3);
  cols.push_back(ts);

  ColumnSpec order_date;
  order_date.name = "order_date";
  order_date.type_code = static_cast<uint8_t>(ColumnType::kDate);
  order_date.row_value = Date(2026, 8, 15);
  cols.push_back(order_date);

  ColumnSpec window_start;
  window_start.name = "window_start";
  window_start.type_code = static_cast<uint8_t>(ColumnType::kTime2);
  window_start.table_map_meta = {0};
  window_start.row_value = Time2(9, 30, 0);
  cols.push_back(window_start);

  ColumnSpec amount;
  amount.name = "amount";
  amount.type_code = static_cast<uint8_t>(ColumnType::kNewDecimal);
  amount.table_map_meta = {18, 4};
  amount.row_value = Decimal(18, 4, "123456789012345678");
  amount.numeric = true;
  cols.push_back(amount);

  return cols;
}

/// Seven string columns plus a key: the same column count as SchemaTemporal7
/// with no formatting work, so the two isolate the printf cost.
inline std::vector<ColumnSpec> SchemaStrings7() {
  std::vector<ColumnSpec> cols;
  cols.push_back(Fixed("id", static_cast<uint8_t>(ColumnType::kLongLong), 1234567890123ULL, 8));
  cols.push_back(Varchar("sku", 255, "SKU-000123456"));
  cols.push_back(Varchar("customer_name", 1020, "Yamada Taro"));
  cols.push_back(Varchar("email", 1020, "customer000123@example.com"));
  cols.push_back(Varchar("address", 2040, std::string(64, 'a')));
  cols.push_back(Char("region", 32, "ap-northeast-1"));
  cols.push_back(Blob("notes", 2, std::string(180, 'n'), 45));
  return cols;
}

/// Twenty-eight columns spanning every decode family a wide OLTP table hits.
inline std::vector<ColumnSpec> SchemaWide28() {
  std::vector<ColumnSpec> cols;
  cols.push_back(Fixed("id", static_cast<uint8_t>(ColumnType::kLongLong), 1234567890123ULL, 8));
  cols.push_back(Fixed("user_id", static_cast<uint8_t>(ColumnType::kLong), 987654, 4));
  cols.push_back(Fixed("tenant_id", static_cast<uint8_t>(ColumnType::kLong), 17, 4));
  cols.push_back(Fixed("status", static_cast<uint8_t>(ColumnType::kTiny), 3, 1));
  cols.push_back(Fixed("priority", static_cast<uint8_t>(ColumnType::kTiny), 1, 1));
  cols.push_back(Fixed("qty", static_cast<uint8_t>(ColumnType::kShort), 42, 2));
  cols.push_back(Fixed("region_code", static_cast<uint8_t>(ColumnType::kInt24), 8421, 3));
  cols.push_back(Fixed("fiscal_year", static_cast<uint8_t>(ColumnType::kYear), 126, 1));

  ColumnSpec weight;
  weight.name = "weight";
  weight.type_code = static_cast<uint8_t>(ColumnType::kFloat);
  weight.table_map_meta = {4};
  weight.row_value = IntLe(0x42C80000u, 4);  // 100.0f
  weight.numeric = true;
  cols.push_back(weight);

  ColumnSpec score;
  score.name = "score";
  score.type_code = static_cast<uint8_t>(ColumnType::kDouble);
  score.table_map_meta = {8};
  score.row_value = IntLe(0x4059000000000000ULL, 8);  // 100.0
  score.numeric = true;
  cols.push_back(score);

  ColumnSpec amount;
  amount.name = "amount";
  amount.type_code = static_cast<uint8_t>(ColumnType::kNewDecimal);
  amount.table_map_meta = {18, 4};
  amount.row_value = Decimal(18, 4, "123456789012345678");
  amount.numeric = true;
  cols.push_back(amount);

  ColumnSpec tax;
  tax.name = "tax";
  tax.type_code = static_cast<uint8_t>(ColumnType::kNewDecimal);
  tax.table_map_meta = {10, 2};
  tax.row_value = Decimal(10, 2, "1234567890");
  tax.numeric = true;
  cols.push_back(tax);

  ColumnSpec discount;
  discount.name = "discount";
  discount.type_code = static_cast<uint8_t>(ColumnType::kNewDecimal);
  discount.table_map_meta = {6, 2};
  discount.row_value = Decimal(6, 2, "123456");
  discount.numeric = true;
  cols.push_back(discount);

  ColumnSpec created;
  created.name = "created_at";
  created.type_code = static_cast<uint8_t>(ColumnType::kDatetime2);
  created.table_map_meta = {6};
  created.row_value = Datetime2(2026, 8, 15, 13, 45, 7, 123456, 6);
  cols.push_back(created);

  ColumnSpec updated;
  updated.name = "updated_at";
  updated.type_code = static_cast<uint8_t>(ColumnType::kDatetime2);
  updated.table_map_meta = {0};
  updated.row_value = Datetime2(2026, 8, 15, 13, 45, 8, 0, 0);
  cols.push_back(updated);

  ColumnSpec shipped;
  shipped.name = "shipped_at";
  shipped.type_code = static_cast<uint8_t>(ColumnType::kDatetime2);
  shipped.table_map_meta = {3};
  shipped.row_value = Datetime2(2026, 8, 16, 9, 0, 0, 250000, 3);
  cols.push_back(shipped);

  ColumnSpec event_ts;
  event_ts.name = "event_ts";
  event_ts.type_code = static_cast<uint8_t>(ColumnType::kTimestamp2);
  event_ts.table_map_meta = {0};
  event_ts.row_value = Timestamp2(1786000000u, 0, 0);
  cols.push_back(event_ts);

  ColumnSpec logged_ts;
  logged_ts.name = "logged_ts";
  logged_ts.type_code = static_cast<uint8_t>(ColumnType::kTimestamp2);
  logged_ts.table_map_meta = {6};
  logged_ts.row_value = Timestamp2(1786000001u, 654321, 6);
  cols.push_back(logged_ts);

  ColumnSpec order_date;
  order_date.name = "order_date";
  order_date.type_code = static_cast<uint8_t>(ColumnType::kDate);
  order_date.row_value = Date(2026, 8, 15);
  cols.push_back(order_date);

  ColumnSpec window_start;
  window_start.name = "window_start";
  window_start.type_code = static_cast<uint8_t>(ColumnType::kTime2);
  window_start.table_map_meta = {0};
  window_start.row_value = Time2(9, 30, 0);
  cols.push_back(window_start);

  ColumnSpec flags;
  flags.name = "flags";
  flags.type_code = static_cast<uint8_t>(ColumnType::kBit);
  flags.table_map_meta = {0x00, 0x02};  // BIT(16): 2 full bytes, 0 extra bits
  flags.row_value = IntBe(0xA55Au, 2);
  cols.push_back(flags);

  cols.push_back(Varchar("sku", 255, "SKU-000123456"));
  cols.push_back(Varchar("customer_name", 1020, "Yamada Taro"));
  cols.push_back(Varchar("email", 1020, "customer000123@example.com"));
  cols.push_back(Varchar("address", 2040, std::string(64, 'a')));
  cols.push_back(Char("region", 32, "ap-northeast-1"));
  cols.push_back(Blob("notes", 2, std::string(180, 'n'), 45));
  cols.push_back(Blob("payload", 4, std::string(96, '\x01'), 63));

  return cols;
}

/// The same 28 columns with different values, used as the UPDATE after image.
inline std::vector<ColumnSpec> SchemaWide28After() {
  std::vector<ColumnSpec> cols = SchemaWide28();
  cols[3].row_value = IntLe(4, 1);                                  // status
  cols[5].row_value = IntLe(43, 2);                                 // qty
  cols[10].row_value = Decimal(18, 4, "987654321098765432");        // amount
  cols[14].row_value = Datetime2(2026, 8, 15, 14, 0, 0, 0, 0);      // updated_at
  cols[21].row_value = LengthPrefixed("SKU-000999888", 1);          // sku
  cols[27].row_value = LengthPrefixed(std::string(96, '\x02'), 4);  // payload
  return cols;
}

}  // namespace bench
}  // namespace mes

#endif  // MES_CORE_BENCHMARKS_WORKLOADS_H_
