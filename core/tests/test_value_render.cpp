// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * @file test_value_render.cpp
 * @brief The ColumnValue strings mes.h defines, from wire bytes through libmybinlog
 *
 * Each case encodes a value the way a server writes it, decodes it with
 * libmybinlog and renders it; the expectations are the strings, integers and
 * text/bytes classification the C ABI hands to callers. What the decoder
 * accepts is libmybinlog's to test; what a value renders to is this file's.
 */

#include <gtest/gtest.h>
#include <zlib.h>

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <limits>
#include <string>
#include <utility>
#include <vector>

#include "mybinlog/event_header.h"
#include "mybinlog/event_types.h"
#include "mybinlog/row_decoder.h"
#include "mybinlog/server_flavor.h"
#include "mybinlog/status.h"
#include "mybinlog/table_map.h"
#include "mybinlog/value.h"
#include "test_helpers.h"
#include "types.h"
#include "value_render.h"

namespace mes {
namespace {

// 2024-03-15 10:30:45 UTC
constexpr uint32_t kTimestamp20240315_103045 = 1710502245;

// utf8mb4_general_ci stands in for any text collation.
constexpr uint32_t kTextCollationId = 45;
constexpr uint32_t kBinaryCollationId = 63;

constexpr uint64_t kTableId = 10;

using Writer = test::EventBuilder;

/** @brief A column of the table a case decodes against. */
struct Column {
  ColumnType type = ColumnType::kLong;
  uint32_t meta = 0;
  bool is_unsigned = false;
  bool has_charset = false;
  uint32_t charset = 0;
};

mybinlog::TableMap TableOf(const std::vector<Column>& columns) {
  mybinlog::TableMap table;
  table.table_id = kTableId;
  table.schema = "db";
  table.table = "t";
  for (const Column& c : columns) {
    mybinlog::TableMapColumn column;
    column.type = c.type;
    column.meta = c.meta;
    column.nullable = true;
    column.is_unsigned = c.is_unsigned;
    column.has_charset = c.has_charset;
    column.charset = c.charset;
    table.columns.push_back(column);
  }
  return table;
}

/** @brief The post-header of a v2 rows event over @p columns, every column present. */
void WriteRowsPostHeader(Writer* w, size_t columns, bool update) {
  w->WriteU48Le(kTableId);
  w->WriteU16Le(0);  // flags
  w->WriteU16Le(2);  // v2 extra-data length
  w->WriteU8(static_cast<uint8_t>(columns));
  for (int image = 0; image < (update ? 2 : 1); ++image) {
    std::vector<uint8_t> present((columns + 7) / 8, 0);
    for (size_t i = 0; i < columns; ++i) present[i / 8] |= static_cast<uint8_t>(1u << (i % 8));
    w->WriteBytes(present);
  }
}

/**
 * @brief Decode a rows event body and render every image it carries.
 *
 * Each WRITE/DELETE row is one entry of @p images; each UPDATE pair is two,
 * before then after.
 *
 * @return false when the decode fails or a value cannot be rendered.
 */
bool DecodeAndRender(const mybinlog::TableMap& table, mybinlog::EventType type,
                     const std::vector<uint8_t>& body, std::vector<RowData>* images,
                     mybinlog::DecodeBudget* budget = nullptr) {
  mybinlog::EventHeader header;
  header.type_code = static_cast<uint8_t>(type);
  mybinlog::DecodeOptions options;
  if (budget != nullptr) {
    options.budget = budget;
    options.per_value_overhead = sizeof(ColumnValue);
    options.per_row_overhead = sizeof(ChangeEvent);
  }
  mybinlog::RowsEvent decoded;
  if (mybinlog::DecodeRowsEvent(header, body.data(), body.size(), table, options, &decoded) !=
      mybinlog::Status::kOk) {
    return false;
  }
  images->clear();
  for (auto& row : decoded.rows) {
    images->emplace_back();
    if (!RenderRow(std::move(row), &images->back(), budget)) return false;
  }
  for (auto& pair : decoded.updates) {
    images->emplace_back();
    if (!RenderRow(std::move(pair.first), &images->back(), budget)) return false;
    images->emplace_back();
    if (!RenderRow(std::move(pair.second), &images->back(), budget)) return false;
  }
  return true;
}

/** @brief Outcome of rendering one value of a single-column table. */
struct Rendered {
  bool ok = false;
  ColumnValue value;
};

/** @brief Decode @p wire as the only value of a one-column WRITE_ROWS row and render it. */
Rendered RenderOne(const Column& column, const std::vector<uint8_t>& wire) {
  Writer w;
  WriteRowsPostHeader(&w, 1, false);
  w.WriteU8(0x00);  // null bitmap
  w.WriteBytes(wire);
  std::vector<RowData> images;
  Rendered result;
  result.ok =
      DecodeAndRender(TableOf({column}), mybinlog::EventType::kWriteRowsEvent, w.Data(), &images);
  if (result.ok) {
    EXPECT_EQ(images.size(), 1u);
    EXPECT_EQ(images[0].columns.size(), 1u);
    result.value = std::move(images[0].columns[0]);
  }
  return result;
}

Rendered RenderOne(ColumnType type, uint32_t meta, const std::vector<uint8_t>& wire,
                   bool is_unsigned = false) {
  return RenderOne(Column{type, meta, is_unsigned, false, 0}, wire);
}

/** @brief Render @p wire and return its text, failing the test when it does not render. */
std::string Text(ColumnType type, uint32_t meta, const std::vector<uint8_t>& wire) {
  Rendered r = RenderOne(type, meta, wire);
  EXPECT_TRUE(r.ok);
  EXPECT_FALSE(r.value.is_null);
  return r.value.string_val;
}

std::vector<uint8_t> Datetime2(int year, int month, int day, int hour, int min, int sec) {
  Writer w;
  test::WriteDatetime2(w, year, month, day, hour, min, sec);
  return w.Data();
}

std::vector<uint8_t> BuildMariaZlibPayload(const std::string& input, bool raw_stream) {
  size_t length_bytes = 1;
  while (length_bytes < 4 && input.size() >= (size_t{1} << (length_bytes * 8))) {
    ++length_bytes;
  }

  std::vector<uint8_t> result;
  result.push_back(static_cast<uint8_t>(0x80 | length_bytes | (raw_stream ? 0x08 : 0)));
  for (size_t i = 0; i < length_bytes; ++i) {
    const size_t shift = (length_bytes - i - 1) * 8;
    result.push_back(static_cast<uint8_t>(input.size() >> shift));
  }

  z_stream stream{};
  const int window_bits = raw_stream ? -MAX_WBITS : MAX_WBITS;
  if (deflateInit2(&stream, Z_DEFAULT_COMPRESSION, Z_DEFLATED, window_bits, 8,
                   Z_DEFAULT_STRATEGY) != Z_OK) {
    return {};
  }
  const size_t payload_offset = result.size();
  result.resize(payload_offset + compressBound(static_cast<uLong>(input.size())));
  stream.next_in = reinterpret_cast<Bytef*>(const_cast<char*>(input.data()));
  stream.avail_in = static_cast<uInt>(input.size());
  stream.next_out = result.data() + payload_offset;
  stream.avail_out = static_cast<uInt>(result.size() - payload_offset);
  const int deflate_result = deflate(&stream, Z_FINISH);
  const size_t compressed_size = stream.total_out;
  const int end_result = deflateEnd(&stream);
  if (deflate_result != Z_STREAM_END || end_result != Z_OK) return {};
  result.resize(payload_offset + compressed_size);
  return result;
}

// --- Integers ---

TEST(ValueRender, IntegersFollowTheColumnsSignedness) {
  struct Case {
    const char* label;
    ColumnType type;
    bool is_unsigned;
    std::vector<uint8_t> wire;
    int64_t expected;
  };
  const Case cases[] = {
      {"TINYINT 127", ColumnType::kTiny, false, {127}, 127},
      {"TINYINT -1", ColumnType::kTiny, false, {0xFF}, -1},
      {"TINYINT UNSIGNED 255", ColumnType::kTiny, true, {255}, 255},
      {"TINYINT UNSIGNED 0", ColumnType::kTiny, true, {0}, 0},
      {"SMALLINT -32768", ColumnType::kShort, false, {0x00, 0x80}, -32768},
      {"SMALLINT UNSIGNED 65535", ColumnType::kShort, true, {0xFF, 0xFF}, 65535},
      {"INT -100", ColumnType::kLong, false, {0x9C, 0xFF, 0xFF, 0xFF}, -100},
      {"INT UNSIGNED 4000000000", ColumnType::kLong, true, {0x00, 0x28, 0x6B, 0xEE}, 4000000000},
      {"BIGINT -1000",
       ColumnType::kLongLong,
       false,
       {0x18, 0xFC, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF},
       -1000},
      {"BIGINT UNSIGNED 9e18",
       ColumnType::kLongLong,
       true,
       {0x00, 0x00, 0x84, 0xE2, 0x50, 0x6C, 0xE6, 0x7C},
       9000000000000000000LL},
      {"MEDIUMINT -1", ColumnType::kInt24, false, {0xFF, 0xFF, 0xFF}, -1},
      {"MEDIUMINT 100", ColumnType::kInt24, false, {100, 0, 0}, 100},
      {"MEDIUMINT UNSIGNED 0xFFFFFF", ColumnType::kInt24, true, {0xFF, 0xFF, 0xFF}, 0xFFFFFF},
  };
  for (const Case& c : cases) {
    SCOPED_TRACE(c.label);
    const Rendered r = RenderOne(c.type, 0, c.wire, c.is_unsigned);
    ASSERT_TRUE(r.ok);
    EXPECT_FALSE(r.value.is_null);
    EXPECT_EQ(r.value.type, c.type);
    EXPECT_EQ(r.value.int_val, c.expected);
    EXPECT_TRUE(r.value.string_val.empty());
  }
}

// An unsigned BIGINT above INT64_MAX cannot be held by int_val, so its exact
// decimal value is carried as text instead (MES_COL_STRING).
TEST(ValueRender, UnsignedBigintAboveInt64MaxIsDecimalText) {
  auto le64 = [](uint64_t v) {
    Writer w;
    w.WriteU64Le(v);
    return w.Data();
  };
  const Rendered max = RenderOne(ColumnType::kLongLong, 0, le64(UINT64_MAX), true);
  ASSERT_TRUE(max.ok);
  EXPECT_EQ(max.value.string_val, "18446744073709551615");

  const Rendered just_over =
      RenderOne(ColumnType::kLongLong, 0, le64(static_cast<uint64_t>(INT64_MAX) + 1), true);
  ASSERT_TRUE(just_over.ok);
  EXPECT_EQ(just_over.value.string_val, "9223372036854775808");

  const Rendered at_max =
      RenderOne(ColumnType::kLongLong, 0, le64(static_cast<uint64_t>(INT64_MAX)), true);
  ASSERT_TRUE(at_max.ok);
  EXPECT_EQ(at_max.value.int_val, INT64_MAX);
  EXPECT_TRUE(at_max.value.string_val.empty());
}

// --- Floating point ---

TEST(ValueRender, FloatAndDoubleKeepTheirColumnType) {
  auto f32 = [](float v) {
    Writer w;
    w.WriteFloat(v);
    return w.Data();
  };
  auto f64 = [](double v) {
    Writer w;
    w.WriteDouble(v);
    return w.Data();
  };
  const Rendered pi = RenderOne(ColumnType::kFloat, 4, f32(3.14f));
  ASSERT_TRUE(pi.ok);
  EXPECT_EQ(pi.value.type, ColumnType::kFloat);
  EXPECT_NEAR(pi.value.real_val, 3.14, 0.01);
  EXPECT_DOUBLE_EQ(RenderOne(ColumnType::kFloat, 4, f32(0.0f)).value.real_val, 0.0);
  EXPECT_NEAR(RenderOne(ColumnType::kFloat, 4, f32(-1.5f)).value.real_val, -1.5, 0.01);

  const Rendered e = RenderOne(ColumnType::kDouble, 8, f64(2.718281828));
  ASSERT_TRUE(e.ok);
  EXPECT_EQ(e.value.type, ColumnType::kDouble);
  EXPECT_NEAR(e.value.real_val, 2.718281828, 0.000001);
  EXPECT_DOUBLE_EQ(RenderOne(ColumnType::kDouble, 8, f64(0.0)).value.real_val, 0.0);
  EXPECT_NEAR(RenderOne(ColumnType::kDouble, 8, f64(-999.999)).value.real_val, -999.999, 0.001);
}

// --- YEAR, BIT, ENUM, SET ---

TEST(ValueRender, YearIsTheCalendarYear) {
  EXPECT_EQ(RenderOne(ColumnType::kYear, 0, {0}).value.int_val, 0);
  EXPECT_EQ(RenderOne(ColumnType::kYear, 0, {100}).value.int_val, 2000);
  EXPECT_EQ(RenderOne(ColumnType::kYear, 0, {155}).value.int_val, 2055);
}

TEST(ValueRender, BitIsItsIntegerValue) {
  EXPECT_EQ(RenderOne(ColumnType::kBit, (0 << 8) | 1, {1}).value.int_val, 1);
  EXPECT_EQ(RenderOne(ColumnType::kBit, (2 << 8) | 0, {0x01, 0x02}).value.int_val, 0x0102);
  EXPECT_EQ(RenderOne(ColumnType::kBit, 0x0100, {0xAB}).value.int_val, 0xAB);
}

// BIT(64) can set the sign bit; it must not reach a caller as a negative int64.
TEST(ValueRender, Bit64AboveSignedRangeIsDecimalText) {
  const Rendered r =
      RenderOne(ColumnType::kBit, (8 << 8) | 0, {0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00});
  ASSERT_TRUE(r.ok);
  EXPECT_EQ(r.value.type, ColumnType::kBit);
  EXPECT_EQ(r.value.string_val, "9223372036854775808");
}

TEST(ValueRender, EnumAndSetCarryTheirOrdinalUnderTheirRealType) {
  const Rendered enum1 = RenderOne(ColumnType::kString, (0xF7 << 8) | 1, {3});
  ASSERT_TRUE(enum1.ok);
  EXPECT_EQ(enum1.value.type, ColumnType::kEnum);
  EXPECT_EQ(enum1.value.int_val, 3);

  const Rendered enum2 = RenderOne(ColumnType::kString, (0xF7 << 8) | 2, {0x2C, 0x01});
  ASSERT_TRUE(enum2.ok);
  EXPECT_EQ(enum2.value.type, ColumnType::kEnum);
  EXPECT_EQ(enum2.value.int_val, 300);

  const Rendered set = RenderOne(ColumnType::kString, (0xF8 << 8) | 2, {0x05, 0x00});
  ASSERT_TRUE(set.ok);
  EXPECT_EQ(set.value.type, ColumnType::kSet);
  EXPECT_EQ(set.value.int_val, 5);
}

TEST(ValueRender, Set64AboveSignedRangeIsDecimalText) {
  const Rendered r = RenderOne(ColumnType::kString, (0xF8 << 8) | 8,
                               {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80});
  ASSERT_TRUE(r.ok);
  EXPECT_EQ(r.value.type, ColumnType::kSet);
  EXPECT_EQ(r.value.string_val, "9223372036854775808");
}

// --- Temporal ---

TEST(ValueRender, DateIsZeroPaddedCalendarText) {
  const uint32_t packed = (2024 << 9) | (3 << 5) | 15;
  Writer w;
  w.WriteU24Le(packed);
  EXPECT_EQ(Text(ColumnType::kDate, 0, w.Data()), "2024-03-15");
}

TEST(ValueRender, Datetime2RendersAtTheDeclaredPrecision) {
  struct Case {
    uint32_t fsp;
    std::vector<uint8_t> fraction;
    const char* expected;
  };
  const Case cases[] = {
      {0, {}, "2024-03-15 10:30:45"},
      {1, {50}, "2024-03-15 10:30:45.5"},
      {2, {12}, "2024-03-15 10:30:45.12"},
      {3, {0x04, 0xCE}, "2024-03-15 10:30:45.123"},
      {6, {0x01, 0xE2, 0x40}, "2024-03-15 10:30:45.123456"},
  };
  for (const Case& c : cases) {
    SCOPED_TRACE(c.expected);
    std::vector<uint8_t> wire = Datetime2(2024, 3, 15, 10, 30, 45);
    wire.insert(wire.end(), c.fraction.begin(), c.fraction.end());
    EXPECT_EQ(Text(ColumnType::kDatetime2, c.fsp, wire), c.expected);
  }
  EXPECT_EQ(Text(ColumnType::kDatetime2, 0, Datetime2(2024, 1, 15, 10, 30, 45)),
            "2024-01-15 10:30:45");
}

// Every fractional precision shares a byte width with its neighbour, and the
// stored value is scaled the same way within each pair.
TEST(ValueRender, Datetime2FractionAtEveryPrecision) {
  struct Case {
    uint32_t fsp;
    std::vector<uint8_t> fraction;
    const char* expected;
  };
  const Case cases[] = {
      {1, {50}, "2024-06-01 00:00:00.5"},
      {2, {99}, "2024-06-01 00:00:00.99"},
      {3, {0x04, 0xCE}, "2024-06-01 00:00:00.123"},
      {4, {0x16, 0x2E}, "2024-06-01 00:00:00.5678"},
      {5, {0x01, 0xE2, 0x3A}, "2024-06-01 00:00:00.12345"},
      {6, {0x0F, 0x42, 0x3F}, "2024-06-01 00:00:00.999999"},
  };
  for (const Case& c : cases) {
    SCOPED_TRACE(c.expected);
    std::vector<uint8_t> wire = Datetime2(2024, 6, 1, 0, 0, 0);
    wire.insert(wire.end(), c.fraction.begin(), c.fraction.end());
    EXPECT_EQ(Text(ColumnType::kDatetime2, c.fsp, wire), c.expected);
  }
}

TEST(ValueRender, ZeroDatetime2IsZeroText) {
  const std::vector<uint8_t> wire = {0x80, 0x00, 0x00, 0x00, 0x00};
  EXPECT_EQ(Text(ColumnType::kDatetime2, 0, wire), "0000-00-00 00:00:00");
}

// No server writes a negative DATETIME; it used to render with a leading '-'
// and is now a decode error (MES-5).
TEST(ValueRender, NegativeDatetime2IsRejected) {
  const int64_t ym = 2024 * 13 + 3;
  const int64_t ymd = (ym << 5) | 15;
  const int64_t hms = (static_cast<int64_t>(10) << 12) | (30 << 6) | 45;
  const int64_t magnitude_packed = (((ymd << 17) | hms) << 24) + 500000;
  Writer w;
  test::WriteDatetime2Packed(w, -magnitude_packed, 6);
  EXPECT_FALSE(RenderOne(ColumnType::kDatetime2, 6, w.Data()).ok);
}

TEST(ValueRender, Timestamp2IsEpochSecondsWithTheDeclaredFraction) {
  struct Case {
    uint32_t fsp;
    std::vector<uint8_t> fraction;
    const char* expected;
  };
  const Case cases[] = {
      {0, {}, "1710502245"},
      {1, {50}, "1710502245.5"},
      {4, {0x04, 0xD2}, "1710502245.1234"},
      {6, {0x01, 0xE2, 0x40}, "1710502245.123456"},
  };
  for (const Case& c : cases) {
    SCOPED_TRACE(c.expected);
    Writer w;
    w.WriteU32Be(kTimestamp20240315_103045);
    w.WriteBytes(c.fraction);
    const Rendered r = RenderOne(ColumnType::kTimestamp2, c.fsp, w.Data());
    ASSERT_TRUE(r.ok);
    EXPECT_EQ(r.value.type, ColumnType::kTimestamp2);
    EXPECT_EQ(r.value.string_val, c.expected);
  }
}

// The pre-5.6 TIMESTAMP is epoch seconds too, carried as text so the C ABI
// reports it as a string rather than an integer.
TEST(ValueRender, TimestampIsEpochSecondsText) {
  Writer w;
  w.WriteU32Le(kTimestamp20240315_103045);
  const Rendered r = RenderOne(ColumnType::kTimestamp, 0, w.Data());
  ASSERT_TRUE(r.ok);
  EXPECT_EQ(r.value.type, ColumnType::kTimestamp);
  EXPECT_EQ(r.value.string_val, std::to_string(kTimestamp20240315_103045));
}

TEST(ValueRender, Time2RendersSignAndFraction) {
  const uint32_t intpart = (10 << 12) | (30 << 6) | 45;
  Writer positive;
  positive.WriteU24Be(intpart + 0x800000);
  EXPECT_EQ(Text(ColumnType::kTime2, 0, positive.Data()), "10:30:45");

  Writer with_frac;
  with_frac.WriteU24Be(intpart + 0x800000);
  with_frac.WriteU16Be(5000);
  EXPECT_EQ(Text(ColumnType::kTime2, 3, with_frac.Data()), "10:30:45.500");

  const int32_t negative_intpart = -((5 << 12) | (15 << 6) | 30);
  Writer negative;
  negative.WriteU24Be(static_cast<uint32_t>(negative_intpart + 0x800000));
  EXPECT_EQ(Text(ColumnType::kTime2, 0, negative.Data()), "-05:15:30");
}

// Negative TIME with fractional seconds is stored in complement form; these
// disk encodings are the ones MySQL's my_time_packed_from_binary documents.
TEST(ValueRender, NegativeTime2FractionsInComplementForm) {
  struct Case {
    uint32_t intpart;
    uint8_t frac;
    const char* expected;
  };
  const Case cases[] = {
      {0x7FFFFF, 0x9D, "-00:00:00.99"},
      {0x7FFFFF, 0xFF, "-00:00:00.01"},
      {0x7FFFFE, 0xFF, "-00:00:01.01"},
      {0x7FFFFF, 0x00, "-00:00:01.00"},
  };
  for (const Case& c : cases) {
    SCOPED_TRACE(c.expected);
    Writer w;
    w.WriteU24Be(c.intpart);
    w.WriteU8(c.frac);
    EXPECT_EQ(Text(ColumnType::kTime2, 2, w.Data()), c.expected);
  }
}

TEST(ValueRender, NegativeTime2AtEveryPrecision) {
  struct Case {
    uint16_t fsp;
    int micros;
    const char* expected;
  };
  const Case cases[] = {
      {1, 100000, "-05:15:30.1"},     {2, 120000, "-05:15:30.12"},
      {3, 123000, "-05:15:30.123"},   {4, 123400, "-05:15:30.1234"},
      {5, 123450, "-05:15:30.12345"}, {6, 123456, "-05:15:30.123456"},
  };

  const int64_t hms = (int64_t{5} << 12) | (15 << 6) | 30;
  for (const auto& test_case : cases) {
    SCOPED_TRACE(test_case.expected);
    const size_t frac_bytes = (test_case.fsp + 1) / 2;
    const int64_t complement = int64_t{1} << (frac_bytes * 8);
    const int64_t multiplier = frac_bytes == 1 ? 10000 : (frac_bytes == 2 ? 100 : 1);
    const uint32_t stored_intpart = static_cast<uint32_t>(-hms - 1 + 0x800000);
    const uint32_t stored_frac = static_cast<uint32_t>(complement - test_case.micros / multiplier);

    Writer w;
    w.WriteU24Be(stored_intpart);
    if (frac_bytes == 1) {
      w.WriteU8(static_cast<uint8_t>(stored_frac));
    } else if (frac_bytes == 2) {
      w.WriteU16Be(static_cast<uint16_t>(stored_frac));
    } else {
      w.WriteU24Be(stored_frac);
    }
    EXPECT_EQ(Text(ColumnType::kTime2, test_case.fsp, w.Data()), test_case.expected);
  }
}

TEST(ValueRender, LegacyTimeAndDatetime) {
  auto time24 = [](int32_t v) {
    Writer w;
    w.WriteU24Le(static_cast<uint32_t>(v) & 0xFFFFFF);
    return w.Data();
  };
  EXPECT_EQ(Text(ColumnType::kTime, 0, time24(103045)), "10:30:45");
  EXPECT_EQ(Text(ColumnType::kTime, 0, time24(-13000)), "-01:30:00");
  EXPECT_EQ(Text(ColumnType::kTime, 0, time24(-530)), "-00:05:30");

  Writer datetime;
  datetime.WriteU64Le(20240315103045ULL);
  EXPECT_EQ(Text(ColumnType::kDatetime, 0, datetime.Data()), "2024-03-15 10:30:45");
}

// --- DECIMAL ---

TEST(ValueRender, NewDecimalIsExactDecimalText) {
  // DECIMAL(10,2) 12345678.90
  std::vector<uint8_t> positive = {static_cast<uint8_t>(((12345678 >> 24) & 0xFF) ^ 0x80),
                                   static_cast<uint8_t>((12345678 >> 16) & 0xFF),
                                   static_cast<uint8_t>((12345678 >> 8) & 0xFF),
                                   static_cast<uint8_t>(12345678 & 0xFF), 90};
  EXPECT_EQ(Text(ColumnType::kNewDecimal, (10 << 8) | 2, positive), "12345678.90");

  // DECIMAL(5,2) -123.45: sign bit, then every byte inverted.
  std::vector<uint8_t> negative = {static_cast<uint8_t>(((123 >> 8) & 0xFF) ^ 0x80 ^ 0xFF),
                                   static_cast<uint8_t>((123 & 0xFF) ^ 0xFF),
                                   static_cast<uint8_t>(45 ^ 0xFF)};
  EXPECT_EQ(Text(ColumnType::kNewDecimal, (5 << 8) | 2, negative), "-123.45");

  // DECIMAL(5,0) 12345
  std::vector<uint8_t> integral = {static_cast<uint8_t>(((12345 >> 16) & 0xFF) ^ 0x80),
                                   static_cast<uint8_t>((12345 >> 8) & 0xFF),
                                   static_cast<uint8_t>(12345 & 0xFF)};
  EXPECT_EQ(Text(ColumnType::kNewDecimal, (5 << 8) | 0, integral), "12345");

  // DECIMAL(5,2) -1.50
  EXPECT_EQ(Text(ColumnType::kNewDecimal, 0x0502, {0x7F, 0xFE, 0xCD}), "-1.50");
}

// A precision of 0 is metadata no server writes; it used to render "0" without
// consuming a byte and is now a decode error (MES-5).
TEST(ValueRender, NewDecimalPrecisionZeroIsRejected) {
  EXPECT_FALSE(RenderOne(ColumnType::kNewDecimal, 0, {0}).ok);

  Writer w;
  WriteRowsPostHeader(&w, 2, false);
  w.WriteU8(0x00);
  w.WriteU32Le(42);
  std::vector<RowData> images;
  EXPECT_FALSE(DecodeAndRender(TableOf({{ColumnType::kNewDecimal, 0}, {ColumnType::kLong, 0}}),
                               mybinlog::EventType::kWriteRowsEvent, w.Data(), &images));
}

// --- Character data ---

TEST(ValueRender, CharacterColumnsCarryTheirPayload) {
  EXPECT_EQ(Text(ColumnType::kVarchar, 100, {5, 'h', 'e', 'l', 'l', 'o'}), "hello");
  EXPECT_EQ(Text(ColumnType::kVarchar, 500, {3, 0, 'a', 'b', 'c'}), "abc");
  // VAR_STRING metadata carries the real type in its high byte, like CHAR.
  EXPECT_EQ(Text(ColumnType::kVarString, 0xFD64, {3, 'a', 'b', 'c'}), "abc");
  // CHAR(10): real type 0xFE in the metadata high byte, 1-byte prefix.
  EXPECT_EQ(Text(ColumnType::kString, 0xFE0A, {5, 'h', 'e', 'l', 'l', 'o'}), "hello");
  // CHAR with a maximum length of 63.
  EXPECT_EQ(Text(ColumnType::kString, 0xFE3F, {5, 'A', 'B', 'C', 'D', 'E'}), "ABCDE");
  // CHAR(100) utf8mb4: a 400-byte maximum moves to a 2-byte prefix.
  EXPECT_EQ(Text(ColumnType::kString, 0xEE90, {7, 0, 't', 'e', 's', 't', 'i', 'n', 'g'}),
            "testing");
}

TEST(ValueRender, CharsetMetadataSelectsTextOrBytes) {
  const Rendered binary_varchar = RenderOne(
      Column{ColumnType::kVarchar, 100, false, true, kBinaryCollationId}, {2, 0xFF, 0xFE});
  ASSERT_TRUE(binary_varchar.ok);
  EXPECT_TRUE(binary_varchar.value.is_binary);
  EXPECT_EQ(binary_varchar.value.string_val, std::string("\xFF\xFE", 2));

  const Rendered text_blob =
      RenderOne(Column{ColumnType::kBlob, 1, false, true, kTextCollationId}, {2, 'o', 'k'});
  ASSERT_TRUE(text_blob.ok);
  EXPECT_FALSE(text_blob.value.is_binary);
  EXPECT_EQ(text_blob.value.string_val, "ok");
}

TEST(ValueRender, BlobFamilyIsBytesAtEveryPrefixWidth) {
  struct Case {
    ColumnType type;
    uint32_t meta;
    std::vector<uint8_t> wire;
    size_t size;
  };
  const Case cases[] = {
      {ColumnType::kBlob, 1, {3, 'a', 'b', 'c'}, 3},
      {ColumnType::kBlob, 2, {5, 0, 'h', 'e', 'l', 'l', 'o'}, 5},
      {ColumnType::kBlob, 3, {3, 0, 0, 'a', 'b', 'c'}, 3},
      {ColumnType::kBlob, 4, {4, 0, 0, 0, 't', 'e', 's', 't'}, 4},
      {ColumnType::kTinyBlob, 1, {2, 'a', 'b'}, 2},
      {ColumnType::kMediumBlob, 3, {3, 0, 0, 'a', 'b', 'c'}, 3},
      {ColumnType::kLongBlob, 4, {4, 0, 0, 0, 'a', 'b', 'c', 'd'}, 4},
  };
  for (const Case& c : cases) {
    SCOPED_TRACE(static_cast<int>(c.type) * 10 + static_cast<int>(c.meta));
    const Rendered r = RenderOne(c.type, c.meta, c.wire);
    ASSERT_TRUE(r.ok);
    EXPECT_TRUE(r.value.is_binary);
    EXPECT_EQ(r.value.bytes_size(), c.size);
    EXPECT_EQ(r.value.bytes_data()[0], c.wire[c.meta]);
  }
}

// JSON stays the server's binary JSON, as bytes, whatever the prefix width.
TEST(ValueRender, JsonIsBinaryJsonBytes) {
  const std::vector<std::vector<uint8_t>> wires = {
      {2, '{', '}'}, {2, 0, '{', '}'}, {2, 0, 0, '{', '}'}, {2, 0, 0, 0, '{', '}'}};
  for (uint32_t width = 1; width <= 4; ++width) {
    SCOPED_TRACE(width);
    const Rendered r = RenderOne(ColumnType::kJson, width, wires[width - 1]);
    ASSERT_TRUE(r.ok);
    EXPECT_EQ(r.value.type, ColumnType::kJson);
    EXPECT_TRUE(r.value.is_binary);
    EXPECT_EQ(r.value.string_val, "{}");
  }
}

TEST(ValueRender, GeometryIsBytesAtEveryPrefixWidth) {
  const std::vector<std::vector<uint8_t>> wires = {
      {3, 1, 2, 3}, {3, 0, 1, 2, 3}, {3, 0, 0, 1, 2, 3}, {3, 0, 0, 0, 1, 2, 3}};
  for (uint32_t width = 1; width <= 4; ++width) {
    SCOPED_TRACE(width);
    const Rendered r = RenderOne(ColumnType::kGeometry, width, wires[width - 1]);
    ASSERT_TRUE(r.ok);
    EXPECT_TRUE(r.value.is_binary);
    EXPECT_EQ(r.value.string_val, std::string("\x01\x02\x03", 3));
  }
}

// A length-prefix width of 0 was read as 1 (BLOB) or 4 (JSON, GEOMETRY); it is
// metadata no server writes and is now a decode error (MES-5).
TEST(ValueRender, ZeroWidthLengthPrefixIsRejected) {
  EXPECT_FALSE(RenderOne(ColumnType::kBlob, 0, {2, 'a', 'b'}).ok);
  EXPECT_FALSE(RenderOne(ColumnType::kJson, 0, {2, 0, 0, 0, '{', '}'}).ok);
  EXPECT_FALSE(RenderOne(ColumnType::kGeometry, 0, {3, 0, 0, 0, 1, 2, 3}).ok);
}

// MySQL 9 stores VECTOR like a BLOB: a length prefix and the raw little-endian
// float32 elements. Its collation is always binary.
TEST(ValueRender, VectorIsBytes) {
  const float elements[] = {1.0f, -2.5f, 3.25f};
  Writer w;
  w.WriteU32Le(static_cast<uint32_t>(sizeof(elements)));
  w.WriteBytes(reinterpret_cast<const uint8_t*>(elements), sizeof(elements));
  for (const Column& column : {Column{ColumnType::kVector, 4},
                               Column{ColumnType::kVector, 4, false, true, kBinaryCollationId}}) {
    const Rendered r = RenderOne(column, w.Data());
    ASSERT_TRUE(r.ok);
    EXPECT_EQ(r.value.type, ColumnType::kVector);
    EXPECT_TRUE(r.value.is_binary);
    ASSERT_EQ(r.value.bytes_size(), sizeof(elements));
    EXPECT_EQ(std::memcmp(r.value.bytes_data(), elements, sizeof(elements)), 0);
  }
}

TEST(ValueRender, TypedArrayIsItsBinaryJsonStorage) {
  const std::vector<uint8_t> binary_json = {0x02, 0x01, 0x00, 0x05, 0x2A};
  Writer w;
  w.WriteU32Le(static_cast<uint32_t>(binary_json.size()));
  w.WriteBytes(binary_json);
  const Rendered r = RenderOne(ColumnType::kTypedArray, 0, w.Data());
  ASSERT_TRUE(r.ok);
  EXPECT_EQ(r.value.type, ColumnType::kTypedArray);
  EXPECT_TRUE(r.value.is_binary);
  EXPECT_EQ(r.value.string_val, std::string(binary_json.begin(), binary_json.end()));
}

TEST(ValueRender, LargeBinaryPayloadsPreserveExactBytes) {
  constexpr size_t kPayloadSize = 4 * 1024 * 1024;
  std::vector<uint8_t> encoded(sizeof(uint32_t) + kPayloadSize);
  const uint32_t payload_size = static_cast<uint32_t>(kPayloadSize);
  for (size_t i = 0; i < 4; ++i) encoded[i] = static_cast<uint8_t>(payload_size >> (8 * i));
  for (size_t i = 0; i < kPayloadSize; ++i) {
    encoded[sizeof(uint32_t) + i] = static_cast<uint8_t>((i * 31u) & 0xFFu);
  }
  for (ColumnType type : {ColumnType::kBlob, ColumnType::kJson, ColumnType::kGeometry}) {
    const Rendered r = RenderOne(type, 4, encoded);
    ASSERT_TRUE(r.ok);
    ASSERT_EQ(r.value.bytes_size(), kPayloadSize);
    EXPECT_EQ(std::memcmp(r.value.bytes_data(), encoded.data() + sizeof(uint32_t), kPayloadSize),
              0);
  }
}

// --- MariaDB COMPRESSED columns ---

TEST(ValueRender, MariaDbCompressedColumnsRenderTheirExpansion) {
  EXPECT_EQ(Text(ColumnType::kVarcharCompressed, 100, {6, 0, 'h', 'e', 'l', 'l', 'o'}), "hello");

  const Rendered empty = RenderOne(ColumnType::kBlobCompressed, 1, {0});
  ASSERT_TRUE(empty.ok);
  EXPECT_FALSE(empty.value.is_null);
  EXPECT_EQ(empty.value.bytes_size(), 0u);

  const std::string wrapped_expected(1000, 'a');
  const auto wrapped = BuildMariaZlibPayload(wrapped_expected, false);
  ASSERT_FALSE(wrapped.empty());
  Writer wrapped_wire;
  wrapped_wire.WriteU16Le(static_cast<uint16_t>(wrapped.size()));
  wrapped_wire.WriteBytes(wrapped);
  EXPECT_EQ(Text(ColumnType::kVarcharCompressed, 1001, wrapped_wire.Data()), wrapped_expected);

  const std::string raw_expected(4096, '\x7F');
  const auto raw = BuildMariaZlibPayload(raw_expected, true);
  ASSERT_FALSE(raw.empty());
  Writer raw_wire;
  raw_wire.WriteU16Le(static_cast<uint16_t>(raw.size()));
  raw_wire.WriteBytes(raw);
  const Rendered blob = RenderOne(ColumnType::kBlobCompressed, 2, raw_wire.Data());
  ASSERT_TRUE(blob.ok);
  EXPECT_TRUE(blob.value.is_binary);
  EXPECT_EQ(blob.value.string_val, raw_expected);
}

// --- Charset classification matrix ---

// Every on-wire encoding that occupies a slot in the TABLE_MAP charset index
// space, crossed with every state that slot can be in. VAR_STRING and CHAR have
// no COMPRESSED counterpart: MariaDB's column compression covers VARCHAR and
// the BLOB family only.
enum class CharsetMatrixFraming {
  kOneBytePrefix,            ///< VARCHAR/VAR_STRING/CHAR with a length below 256
  kTwoBytePrefix,            ///< BLOB-family pack_length 2
  kCompressedOneBytePrefix,  ///< MariaDB compressed field behind a 1-byte length
  kCompressedTwoBytePrefix,  ///< MariaDB compressed field behind a 2-byte length
};

struct CharsetMatrixEncoding {
  const char* label;
  ColumnType type;
  uint32_t metadata;
  CharsetMatrixFraming framing;
};

struct CharsetMatrixState {
  const char* label;
  bool charset_known;
  bool binary_charset;
  bool expect_binary;
};

// A payload carrying an embedded NUL and a byte no UTF-8 decoder can represent,
// so a misclassified cell shows up in the value and not only in the flag.
std::string CharsetMatrixPayload() {
  static constexpr char kBytes[] = {'a', 'b', '\0', '\xFF', 'c', 'd'};
  return std::string(kBytes, sizeof(kBytes));
}

std::vector<uint8_t> FrameCharsetMatrixValue(CharsetMatrixFraming framing,
                                             const std::string& payload) {
  Writer w;
  switch (framing) {
    case CharsetMatrixFraming::kOneBytePrefix:
      w.WriteU8(static_cast<uint8_t>(payload.size()));
      w.WriteString(payload);
      break;
    case CharsetMatrixFraming::kTwoBytePrefix:
      w.WriteU16Le(static_cast<uint16_t>(payload.size()));
      w.WriteString(payload);
      break;
    case CharsetMatrixFraming::kCompressedOneBytePrefix:
    case CharsetMatrixFraming::kCompressedTwoBytePrefix: {
      const auto compressed = BuildMariaZlibPayload(payload, true);
      if (compressed.empty()) return {};
      if (framing == CharsetMatrixFraming::kCompressedOneBytePrefix) {
        w.WriteU8(static_cast<uint8_t>(compressed.size()));
      } else {
        w.WriteU16Le(static_cast<uint16_t>(compressed.size()));
      }
      w.WriteBytes(compressed);
      break;
    }
  }
  return w.Data();
}

const CharsetMatrixEncoding kCharsetMatrixEncodings[] = {
    {"VARCHAR", ColumnType::kVarchar, 100, CharsetMatrixFraming::kOneBytePrefix},
    {"VAR_STRING", ColumnType::kVarString, 0xFE64, CharsetMatrixFraming::kOneBytePrefix},
    {"CHAR", ColumnType::kString, 0xFE0A, CharsetMatrixFraming::kOneBytePrefix},
    {"BLOB", ColumnType::kBlob, 2, CharsetMatrixFraming::kTwoBytePrefix},
    {"VARCHAR COMPRESSED", ColumnType::kVarcharCompressed, 100,
     CharsetMatrixFraming::kCompressedOneBytePrefix},
    {"BLOB COMPRESSED", ColumnType::kBlobCompressed, 2,
     CharsetMatrixFraming::kCompressedTwoBytePrefix},
};

// Absent metadata resolves to bytes for every encoding: without a collation
// VARBINARY and VARCHAR cannot be told apart, and bytes preserve the value.
const CharsetMatrixState kCharsetMatrixStates[] = {
    {"absent", false, false, true},
    {"text collation", true, false, false},
    {"binary collation", true, true, true},
};

Column MatrixColumn(const CharsetMatrixEncoding& encoding, const CharsetMatrixState& state) {
  return Column{encoding.type, encoding.metadata, false, state.charset_known,
                state.binary_charset ? kBinaryCollationId : kTextCollationId};
}

TEST(ValueRender, CharsetMatrixClassifiesEveryEncodingByCollation) {
  const std::string payload = CharsetMatrixPayload();
  size_t cells = 0;
  for (const auto& encoding : kCharsetMatrixEncodings) {
    for (const auto& state : kCharsetMatrixStates) {
      SCOPED_TRACE(std::string(encoding.label) + " / charset " + state.label);
      const std::vector<uint8_t> wire = FrameCharsetMatrixValue(encoding.framing, payload);
      ASSERT_FALSE(wire.empty());
      const Rendered r = RenderOne(MatrixColumn(encoding, state), wire);
      ASSERT_TRUE(r.ok);
      ASSERT_FALSE(r.value.is_null);
      EXPECT_EQ(r.value.is_binary, state.expect_binary);
      EXPECT_EQ(r.value.string_val, payload);
      ++cells;
    }
  }
  EXPECT_EQ(cells, 18u);
}

// is_binary is a function of the collation, never of which decode path
// produced the value, so each COMPRESSED encoding agrees with its plain one.
TEST(ValueRender, CompressedEncodingsAgreeWithTheirUncompressedCounterparts) {
  struct Pair {
    const char* label;
    CharsetMatrixEncoding plain;
    CharsetMatrixEncoding compressed;
  };
  const Pair kPairs[] = {
      {"VARCHAR", kCharsetMatrixEncodings[0], kCharsetMatrixEncodings[4]},
      {"BLOB", kCharsetMatrixEncodings[3], kCharsetMatrixEncodings[5]},
  };
  const std::string payload = CharsetMatrixPayload();
  for (const auto& pair : kPairs) {
    for (const auto& state : kCharsetMatrixStates) {
      SCOPED_TRACE(std::string(pair.label) + " / charset " + state.label);
      const Rendered plain = RenderOne(MatrixColumn(pair.plain, state),
                                       FrameCharsetMatrixValue(pair.plain.framing, payload));
      const Rendered compressed =
          RenderOne(MatrixColumn(pair.compressed, state),
                    FrameCharsetMatrixValue(pair.compressed.framing, payload));
      ASSERT_TRUE(plain.ok);
      ASSERT_TRUE(compressed.ok);
      EXPECT_EQ(compressed.value.is_binary, plain.value.is_binary);
      EXPECT_EQ(compressed.value.string_val, plain.value.string_val);
    }
  }
}

// The same matrix across the columns of one row, so the classification reaches
// every column through the row path.
TEST(ValueRenderRows, CharsetMatrixClassifiesEveryEncodingByCollation) {
  const CharsetMatrixEncoding kRowEncodings[] = {
      kCharsetMatrixEncodings[0],  // VARCHAR
      kCharsetMatrixEncodings[3],  // BLOB
      kCharsetMatrixEncodings[4],  // VARCHAR COMPRESSED
      kCharsetMatrixEncodings[5],  // BLOB COMPRESSED
  };
  const std::string payload = CharsetMatrixPayload();
  for (const auto& state : kCharsetMatrixStates) {
    SCOPED_TRACE(std::string("charset ") + state.label);
    std::vector<Column> columns;
    Writer w;
    WriteRowsPostHeader(&w, 4, false);
    w.WriteU8(0x00);  // null bitmap
    for (const auto& encoding : kRowEncodings) {
      columns.push_back(MatrixColumn(encoding, state));
      const std::vector<uint8_t> wire = FrameCharsetMatrixValue(encoding.framing, payload);
      ASSERT_FALSE(wire.empty());
      w.WriteBytes(wire);
    }
    std::vector<RowData> images;
    ASSERT_TRUE(
        DecodeAndRender(TableOf(columns), mybinlog::EventType::kWriteRowsEvent, w.Data(), &images));
    ASSERT_EQ(images.size(), 1u);
    ASSERT_EQ(images[0].columns.size(), 4u);
    for (size_t i = 0; i < 4; ++i) {
      SCOPED_TRACE(kRowEncodings[i].label);
      EXPECT_EQ(images[0].columns[i].is_binary, state.expect_binary);
      EXPECT_EQ(images[0].columns[i].string_val, payload);
    }
  }
}

// --- Kinds mes.h cannot represent ---

// The C ABI has no state for a JSON diff or for a column a row image left out,
// so neither renders.
TEST(ValueRender, JsonDiffAndAbsentColumnsAreNotRendered) {
  ColumnValue out;
  mybinlog::Value diff;
  diff.kind = mybinlog::Value::Kind::kJsonDiff;
  diff.type = ColumnType::kJson;
  diff.bytes = std::string("\x00\x00\x00\x00", 4);
  EXPECT_FALSE(RenderValue(std::move(diff), &out));

  mybinlog::Value absent;
  absent.kind = mybinlog::Value::Kind::kAbsent;
  EXPECT_FALSE(RenderValue(std::move(absent), &out));
}

TEST(ValueRender, NullIsNullOfTheColumnType) {
  mybinlog::Value value;
  value.kind = mybinlog::Value::Kind::kNull;
  value.type = ColumnType::kVarchar;
  ColumnValue out = ColumnValue::Int(ColumnType::kLong, 7);
  ASSERT_TRUE(RenderValue(std::move(value), &out));
  EXPECT_TRUE(out.is_null);
  EXPECT_EQ(out.type, ColumnType::kVarchar);
}

// --- Rows ---

TEST(ValueRenderRows, SingleRowIntVarchar) {
  Writer w;
  WriteRowsPostHeader(&w, 2, false);
  w.WriteU8(0x00);
  w.WriteU32Le(42);
  w.WriteU8(5);
  w.WriteString("Alice");
  std::vector<RowData> images;
  ASSERT_TRUE(DecodeAndRender(TableOf({{ColumnType::kLong, 0}, {ColumnType::kVarchar, 100}}),
                              mybinlog::EventType::kWriteRowsEvent, w.Data(), &images));
  ASSERT_EQ(images.size(), 1u);
  ASSERT_EQ(images[0].columns.size(), 2u);
  EXPECT_EQ(images[0].columns.get_allocator().resource(), RowColumnMemoryResource());
  EXPECT_EQ(images[0].columns[0].int_val, 42);
  EXPECT_EQ(images[0].columns[1].string_val, "Alice");
}

TEST(ValueRenderRows, EveryRowOfAnEventRenders) {
  Writer w;
  WriteRowsPostHeader(&w, 1, false);
  w.WriteU8(0x00);
  w.WriteU32Le(100);
  w.WriteU8(0x00);
  w.WriteU32Le(200);
  std::vector<RowData> images;
  ASSERT_TRUE(DecodeAndRender(TableOf({{ColumnType::kLong, 0}}),
                              mybinlog::EventType::kWriteRowsEvent, w.Data(), &images));
  ASSERT_EQ(images.size(), 2u);
  EXPECT_EQ(images[0].columns[0].int_val, 100);
  EXPECT_EQ(images[1].columns[0].int_val, 200);
}

TEST(ValueRenderRows, UpdateRendersBothImagesAndDeleteItsOne) {
  Writer update;
  WriteRowsPostHeader(&update, 1, true);
  update.WriteU8(0x00);
  update.WriteU32Le(100);
  update.WriteU8(0x00);
  update.WriteU32Le(200);
  std::vector<RowData> images;
  ASSERT_TRUE(DecodeAndRender(TableOf({{ColumnType::kLong, 0}}),
                              mybinlog::EventType::kUpdateRowsEvent, update.Data(), &images));
  ASSERT_EQ(images.size(), 2u);
  EXPECT_EQ(images[0].columns[0].int_val, 100);
  EXPECT_EQ(images[1].columns[0].int_val, 200);

  Writer del;
  WriteRowsPostHeader(&del, 1, false);
  del.WriteU8(0x00);
  del.WriteU32Le(999);
  ASSERT_TRUE(DecodeAndRender(TableOf({{ColumnType::kLong, 0}}),
                              mybinlog::EventType::kDeleteRowsEvent, del.Data(), &images));
  ASSERT_EQ(images.size(), 1u);
  EXPECT_EQ(images[0].columns[0].int_val, 999);
}

TEST(ValueRenderRows, V1RowsEventRenders) {
  Writer w;
  w.WriteU48Le(kTableId);
  w.WriteU16Le(0);
  w.WriteU8(1);
  w.WriteU8(0x01);
  w.WriteU8(0x00);
  w.WriteU32Le(42);
  std::vector<RowData> images;
  ASSERT_TRUE(DecodeAndRender(TableOf({{ColumnType::kLong, 0}}),
                              mybinlog::EventType::kWriteRowsEventV1, w.Data(), &images));
  ASSERT_EQ(images.size(), 1u);
  EXPECT_EQ(images[0].columns[0].int_val, 42);
}

TEST(ValueRenderRows, NullColumnIsNull) {
  Writer w;
  WriteRowsPostHeader(&w, 2, false);
  w.WriteU8(0x02);  // column 1 is NULL
  w.WriteU32Le(42);
  std::vector<RowData> images;
  ASSERT_TRUE(DecodeAndRender(TableOf({{ColumnType::kLong, 0}, {ColumnType::kVarchar, 100}}),
                              mybinlog::EventType::kWriteRowsEvent, w.Data(), &images));
  ASSERT_EQ(images.size(), 1u);
  ASSERT_EQ(images[0].columns.size(), 2u);
  EXPECT_FALSE(images[0].columns[0].is_null);
  EXPECT_EQ(images[0].columns[0].int_val, 42);
  EXPECT_TRUE(images[0].columns[1].is_null);
}

/** @brief Value a non-NULL column @p column of row image @p image carries. */
int32_t CellValue(size_t image, size_t column) {
  return static_cast<int32_t>(image * 100 + column + 1);
}

void WriteBitmap(Writer* w, size_t columns, const std::vector<size_t>& set) {
  std::vector<uint8_t> bytes((columns + 7) / 8, 0);
  for (size_t bit : set) bytes[bit / 8] |= static_cast<uint8_t>(1u << (bit % 8));
  w->WriteBytes(bytes);
}

void WriteRowImage(Writer* w, size_t columns, size_t image, const std::vector<size_t>& nulls) {
  WriteBitmap(w, columns, nulls);
  for (size_t i = 0; i < columns; ++i) {
    if (std::find(nulls.begin(), nulls.end(), i) == nulls.end()) {
      w->WriteU32Le(static_cast<uint32_t>(CellValue(image, i)));
    }
  }
}

void ExpectRowImage(const RowData& row, size_t columns, size_t image,
                    const std::vector<size_t>& nulls) {
  ASSERT_EQ(row.columns.size(), columns);
  for (size_t i = 0; i < columns; ++i) {
    SCOPED_TRACE("column " + std::to_string(i));
    const bool is_null = std::find(nulls.begin(), nulls.end(), i) != nulls.end();
    EXPECT_EQ(row.columns[i].is_null, is_null);
    if (!is_null) EXPECT_EQ(row.columns[i].int_val, CellValue(image, i));
  }
}

std::vector<Column> IntColumns(size_t columns) {
  return std::vector<Column>(columns, Column{ColumnType::kLong, 0});
}

std::vector<size_t> AllColumns(size_t columns) {
  std::vector<size_t> all(columns);
  for (size_t i = 0; i < columns; ++i) all[i] = i;
  return all;
}

TEST(ValueRenderRows, NullBitmapsAcrossByteBoundariesAndRows) {
  struct Case {
    const char* description;
    size_t columns;
    std::vector<std::vector<size_t>> rows;  // NULL columns per row image
  };
  const Case cases[] = {
      {"8 columns, last NULL", 8, {{7}}},
      {"9 columns, NULL only in the second byte", 9, {{8}}},
      {"9 columns, NULL on both sides of the boundary", 9, {{7, 8}}},
      {"16 columns, first and last of each byte", 16, {{0, 7, 8, 15}}},
      {"16 columns, every column NULL", 16, {AllColumns(16)}},
      {"two rows, alternating bitmaps", 9, {{0, 2, 4, 6, 8}, {1, 3, 5, 7}}},
  };
  for (const Case& c : cases) {
    SCOPED_TRACE(c.description);
    Writer w;
    WriteRowsPostHeader(&w, c.columns, false);
    for (size_t image = 0; image < c.rows.size(); ++image) {
      WriteRowImage(&w, c.columns, image, c.rows[image]);
    }
    std::vector<RowData> images;
    ASSERT_TRUE(DecodeAndRender(TableOf(IntColumns(c.columns)),
                                mybinlog::EventType::kWriteRowsEvent, w.Data(), &images));
    ASSERT_EQ(images.size(), c.rows.size());
    for (size_t image = 0; image < c.rows.size(); ++image) {
      SCOPED_TRACE("row " + std::to_string(image));
      ExpectRowImage(images[image], c.columns, image, c.rows[image]);
    }
  }
}

TEST(ValueRenderRows, BeforeAndAfterImagesKeepTheirOwnNullBitmaps) {
  constexpr size_t kColumns = 9;
  const std::vector<size_t> before_nulls = {1};
  const std::vector<size_t> after_nulls = {8};
  Writer w;
  WriteRowsPostHeader(&w, kColumns, true);
  WriteRowImage(&w, kColumns, 0, before_nulls);
  WriteRowImage(&w, kColumns, 1, after_nulls);
  std::vector<RowData> images;
  ASSERT_TRUE(DecodeAndRender(TableOf(IntColumns(kColumns)), mybinlog::EventType::kUpdateRowsEvent,
                              w.Data(), &images));
  ASSERT_EQ(images.size(), 2u);
  {
    SCOPED_TRACE("before");
    ExpectRowImage(images[0], kColumns, 0, before_nulls);
  }
  {
    SCOPED_TRACE("after");
    ExpectRowImage(images[1], kColumns, 1, after_nulls);
  }
}

// The C ABI has no "absent" value distinct from SQL NULL, so a partial row
// image is refused rather than letting a downstream upsert clear the column it
// left out.
TEST(ValueRenderRows, PartialRowImagesAreNotRendered) {
  Writer write;
  write.WriteU48Le(kTableId);
  write.WriteU16Le(0);
  write.WriteU16Le(2);
  write.WriteU8(3);
  write.WriteU8(0x03);  // only the first two of three columns
  write.WriteU8(0x00);
  write.WriteU32Le(42);
  write.WriteU8(2);
  write.WriteString("hi");
  std::vector<RowData> images;
  EXPECT_FALSE(DecodeAndRender(
      TableOf({{ColumnType::kLong, 0}, {ColumnType::kVarchar, 100}, {ColumnType::kLong, 0}}),
      mybinlog::EventType::kWriteRowsEvent, write.Data(), &images));

  Writer update;
  update.WriteU48Le(kTableId);
  update.WriteU16Le(0);
  update.WriteU16Le(2);
  update.WriteU8(2);
  update.WriteU8(0x03);  // before image has both columns
  update.WriteU8(0x01);  // after image carries only the first
  update.WriteU8(0x00);
  update.WriteU32Le(10);
  update.WriteU32Le(20);
  update.WriteU8(0x00);
  update.WriteU32Le(30);
  EXPECT_FALSE(DecodeAndRender(TableOf(IntColumns(2)), mybinlog::EventType::kUpdateRowsEvent,
                               update.Data(), &images));
}

// --- TABLE_MAP metadata that decides the rendering ---

/**
 * @brief A TABLE_MAP with no optional metadata resolves every character-family
 *        and BLOB-family column to bytes.
 *
 * VARCHAR/VARBINARY and TEXT/BLOB each share a binlog type byte, so the declared
 * collation is the only discriminator. With none carried, bytes is the only
 * lossless reading, so the columns declared as text come back as bytes too.
 */
TEST(ValueRenderRows, AbsentCharsetMetadataResolvesCharacterColumnsToBytes) {
  Writer table_map;
  table_map.WriteU48Le(kTableId);
  table_map.WriteU16Le(0);
  table_map.WriteU8(2);
  table_map.WriteString("db");
  table_map.WriteU8(0);
  table_map.WriteU8(1);
  table_map.WriteString("t");
  table_map.WriteU8(0);
  table_map.WriteU8(4);
  table_map.WriteBytes(
      {static_cast<uint8_t>(ColumnType::kVarchar), static_cast<uint8_t>(ColumnType::kVarchar),
       static_cast<uint8_t>(ColumnType::kBlob), static_cast<uint8_t>(ColumnType::kBlob)});
  table_map.WriteU8(6);
  table_map.WriteBytes({0x64, 0x00, 0x64, 0x00, 0x02, 0x02});
  table_map.WriteU8(0x0F);  // null bitmap; nothing follows (binlog_row_metadata=NO_LOG)

  mybinlog::TableMap table;
  ASSERT_EQ(mybinlog::ParseTableMapEvent(mybinlog::EventHeader{}, table_map.DataPtr(),
                                         table_map.Size(), mybinlog::ServerFlavor::kMySQL, &table),
            mybinlog::Status::kOk);

  const std::string kBinaryPayload("\x00\xFF\x01", 3);
  Writer w;
  WriteRowsPostHeader(&w, 4, false);
  w.WriteU8(0x00);
  w.WriteU8(static_cast<uint8_t>(kBinaryPayload.size()));
  w.WriteString(kBinaryPayload);
  w.WriteU8(2);
  w.WriteString("hi");
  w.WriteU16Le(2);
  w.WriteBytes({0xFE, 0x00});
  w.WriteU16Le(4);
  w.WriteString("text");

  std::vector<RowData> images;
  ASSERT_TRUE(DecodeAndRender(table, mybinlog::EventType::kWriteRowsEvent, w.Data(), &images));
  ASSERT_EQ(images.size(), 1u);
  ASSERT_EQ(images[0].columns.size(), 4u);
  const std::string kExpected[] = {kBinaryPayload, "hi", std::string("\xFE\x00", 2), "text"};
  const ColumnType kExpectedTypes[] = {ColumnType::kVarchar, ColumnType::kVarchar,
                                       ColumnType::kBlob, ColumnType::kBlob};
  for (size_t i = 0; i < 4; ++i) {
    SCOPED_TRACE(i);
    const ColumnValue& column = images[0].columns[i];
    ASSERT_FALSE(column.is_null);
    EXPECT_EQ(column.type, kExpectedTypes[i]);
    EXPECT_TRUE(column.is_binary);  // MES_COL_BYTES
    EXPECT_EQ(column.string_val, kExpected[i]);
  }
}

// (TINYINT, SMALLINT, MEDIUMINT, INT, BIGINT), each holding a value above the
// signed range of its width so that reading it as signed is observable.
std::vector<uint8_t> UnsignedNumericTableMap(bool with_signedness) {
  Writer w;
  w.WriteU48Le(kTableId);
  w.WriteU16Le(0);
  w.WriteU8(2);
  w.WriteString("db");
  w.WriteU8(0);
  w.WriteU8(1);
  w.WriteString("t");
  w.WriteU8(0);
  w.WriteU8(5);
  w.WriteBytes({static_cast<uint8_t>(ColumnType::kTiny), static_cast<uint8_t>(ColumnType::kShort),
                static_cast<uint8_t>(ColumnType::kInt24), static_cast<uint8_t>(ColumnType::kLong),
                static_cast<uint8_t>(ColumnType::kLongLong)});
  w.WriteU8(0);     // metadata block: fixed-size types only
  w.WriteU8(0x1F);  // null bitmap
  // SIGNEDNESS, length 1, MSB-first over the five numeric columns: all unsigned.
  if (with_signedness) w.WriteBytes({0x01, 0x01, 0xF8});
  return w.Data();
}

std::vector<RowData> RenderUnsignedNumericRow(bool with_signedness) {
  const std::vector<uint8_t> table_map = UnsignedNumericTableMap(with_signedness);
  mybinlog::TableMap table;
  EXPECT_EQ(mybinlog::ParseTableMapEvent(mybinlog::EventHeader{}, table_map.data(),
                                         table_map.size(), mybinlog::ServerFlavor::kMySQL, &table),
            mybinlog::Status::kOk);
  EXPECT_EQ(table.has_signedness, with_signedness);
  Writer w;
  WriteRowsPostHeader(&w, 5, false);
  w.WriteU8(0x00);
  w.WriteU8(0x80);
  w.WriteU16Le(0xFF00);
  w.WriteU24Le(0x800001);
  w.WriteU32Le(0xFFFFFF00);
  w.WriteU64Le((uint64_t{1} << 63) | 1);
  std::vector<RowData> images;
  EXPECT_TRUE(DecodeAndRender(table, mybinlog::EventType::kWriteRowsEvent, w.Data(), &images));
  return images;
}

/**
 * @brief Without SIGNEDNESS every numeric column decodes as signed.
 *
 * The exact-decimal string reserved for an unsigned value above INT64_MAX is
 * only reachable when the column is known to be unsigned, so here the BIGINT
 * stays an integer and wraps negative.
 */
TEST(ValueRenderRows, AbsentSignednessMetadataResolvesNumericColumnsToSigned) {
  const std::vector<RowData> images = RenderUnsignedNumericRow(false);
  ASSERT_EQ(images.size(), 1u);
  ASSERT_EQ(images[0].columns.size(), 5u);
  const int64_t kExpected[] = {-128, -256, -8388607, -256, -9223372036854775807LL};
  for (size_t i = 0; i < 5; ++i) {
    SCOPED_TRACE(i);
    const ColumnValue& column = images[0].columns[i];
    ASSERT_FALSE(column.is_null);
    EXPECT_EQ(column.int_val, kExpected[i]);  // MES_COL_INT
    EXPECT_TRUE(column.string_val.empty()) << column.string_val;
  }
}

TEST(ValueRenderRows, PresentSignednessMetadataResolvesNumericColumnsToUnsigned) {
  const std::vector<RowData> images = RenderUnsignedNumericRow(true);
  ASSERT_EQ(images.size(), 1u);
  ASSERT_EQ(images[0].columns.size(), 5u);
  EXPECT_EQ(images[0].columns[0].int_val, 0x80);
  EXPECT_EQ(images[0].columns[1].int_val, 0xFF00);
  EXPECT_EQ(images[0].columns[2].int_val, 0x800001);
  EXPECT_EQ(images[0].columns[3].int_val, 0xFFFFFF00);
  EXPECT_EQ(images[0].columns[4].string_val, "9223372036854775809");  // MES_COL_STRING
  EXPECT_FALSE(images[0].columns[4].is_binary);
}

// --- Decode budget ---

TEST(ValueRenderBudget, ChargesEveryRowAndColumn) {
  Writer w;
  WriteRowsPostHeader(&w, 2, false);
  w.WriteU8(0x00);
  w.WriteU32Le(1);
  w.WriteU8(5);
  w.WriteString("Alice");
  w.WriteU8(0x00);
  w.WriteU32Le(2);
  w.WriteU8(3);
  w.WriteString("Bob");

  mybinlog::DecodeBudget budget;
  std::vector<RowData> images;
  ASSERT_TRUE(DecodeAndRender(TableOf({{ColumnType::kLong, 0}, {ColumnType::kVarchar, 100}}),
                              mybinlog::EventType::kWriteRowsEvent, w.Data(), &images, &budget));
  ASSERT_EQ(images.size(), 2u);
  // The "Alice" and "Bob" payloads, the two column slots of each row and one
  // queue entry per row.
  EXPECT_EQ(budget.used, 5u + 3u + 2u * 2u * sizeof(ColumnValue) + 2u * sizeof(ChangeEvent));
  EXPECT_LE(budget.used, budget.limit);
}

TEST(ValueRenderBudget, ChargesBothImagesOfAnUpdate) {
  Writer w;
  WriteRowsPostHeader(&w, 1, true);
  w.WriteU8(0x00);
  w.WriteU8(4);
  w.WriteString("aaaa");
  w.WriteU8(0x00);
  w.WriteU8(2);
  w.WriteString("bb");

  mybinlog::DecodeBudget budget;
  std::vector<RowData> images;
  ASSERT_TRUE(DecodeAndRender(TableOf({{ColumnType::kVarchar, 100}}),
                              mybinlog::EventType::kUpdateRowsEvent, w.Data(), &images, &budget));
  ASSERT_EQ(images.size(), 2u);
  EXPECT_EQ(budget.used, 4u + 2u + 2u * sizeof(ColumnValue) + sizeof(ChangeEvent));
}

// Text the renderer builds from fields was never a decoded payload, so it is
// charged when it is rendered: the event pays for every byte its values hold.
TEST(ValueRenderBudget, ChargesTheTextItRenders) {
  const uint32_t packed = (2024 << 9) | (3 << 5) | 15;
  Writer w;
  WriteRowsPostHeader(&w, 1, false);
  w.WriteU8(0x00);
  w.WriteU24Le(packed);

  mybinlog::DecodeBudget budget;
  std::vector<RowData> images;
  ASSERT_TRUE(DecodeAndRender(TableOf({{ColumnType::kDate, 0}}),
                              mybinlog::EventType::kWriteRowsEvent, w.Data(), &images, &budget));
  ASSERT_EQ(images.size(), 1u);
  EXPECT_EQ(images[0].columns[0].string_val, "2024-03-15");
  EXPECT_EQ(budget.used,
            std::string("2024-03-15").size() + sizeof(ColumnValue) + sizeof(ChangeEvent));

  // A budget with room for the decode but not for the rendered text refuses it.
  mybinlog::DecodeBudget tight;
  tight.limit = sizeof(ColumnValue) + sizeof(ChangeEvent) + 9;
  EXPECT_FALSE(DecodeAndRender(TableOf({{ColumnType::kDate, 0}}),
                               mybinlog::EventType::kWriteRowsEvent, w.Data(), &images, &tight));
  EXPECT_LE(tight.used, tight.limit);
}

// A deflate bomb spread across columns: the per-field cap says nothing about
// how many fields a row carries, so the per-event budget is what bounds it.
TEST(ValueRenderBudget, CompressedColumnBombStaysWithinTheBudget) {
  constexpr size_t kColumns = 8;
  constexpr size_t kExpandedBytes = 4U * 1024U * 1024U;
  constexpr size_t kBudgetBytes = 6U * 1024U * 1024U;

  const auto payload = BuildMariaZlibPayload(std::string(kExpandedBytes, 'a'), true);
  ASSERT_FALSE(payload.empty());

  Writer w;
  WriteRowsPostHeader(&w, kColumns, false);
  w.WriteU8(0x00);
  for (size_t i = 0; i < kColumns; ++i) {
    w.WriteU32Le(static_cast<uint32_t>(payload.size()));
    w.WriteBytes(payload);
  }

  mybinlog::DecodeBudget budget;
  budget.limit = kBudgetBytes;
  std::vector<RowData> images;
  EXPECT_FALSE(DecodeAndRender(
      TableOf(std::vector<Column>(kColumns, Column{ColumnType::kBlobCompressed, 4})),
      mybinlog::EventType::kWriteRowsEvent, w.Data(), &images, &budget));
  EXPECT_LE(budget.used, budget.limit);
  EXPECT_LT(budget.used, kColumns * kExpandedBytes);
  EXPECT_LT(w.Size(), 64u * 1024u);
}

// --- Fixed-width decimal formatting ---

// Every expectation below is what printf("%0*d") produces for the same
// arguments; the temporal formatters depend on that equivalence.
std::string Padded(int value, int width) {
  char buf[32];
  return std::string(buf, static_cast<size_t>(WritePaddedInt(buf, value, width) - buf));
}

TEST(ValueRenderPaddedInt, PadsToWidth) {
  EXPECT_EQ(Padded(7, 2), "07");
  EXPECT_EQ(Padded(0, 2), "00");
  EXPECT_EQ(Padded(59, 2), "59");
  EXPECT_EQ(Padded(2026, 4), "2026");
  EXPECT_EQ(Padded(1, 4), "0001");
  EXPECT_EQ(Padded(123, 9), "000000123");
}

TEST(ValueRenderPaddedInt, WidthOneOrZeroWritesEveryDigit) {
  EXPECT_EQ(Padded(0, 0), "0");
  EXPECT_EQ(Padded(0, 1), "0");
  EXPECT_EQ(Padded(4095, 1), "4095");
}

TEST(ValueRenderPaddedInt, ValueWiderThanWidthIsNotTruncated) {
  EXPECT_EQ(Padded(32767, 4), "32767");
  EXPECT_EQ(Padded(1234567890, 2), "1234567890");
}

TEST(ValueRenderPaddedInt, SignCountsTowardTheWidth) {
  EXPECT_EQ(Padded(-5, 2), "-5");
  EXPECT_EQ(Padded(-5, 4), "-005");
  EXPECT_EQ(Padded(-123, 2), "-123");
  EXPECT_EQ(Padded(-1, 9), "-00000001");
}

TEST(ValueRenderPaddedInt, ExtremeValues) {
  EXPECT_EQ(Padded(std::numeric_limits<int>::max(), 2), "2147483647");
  EXPECT_EQ(Padded(std::numeric_limits<int>::min(), 2), "-2147483648");
  EXPECT_EQ(Padded(std::numeric_limits<int>::min(), 15), "-00002147483648");
}

TEST(ValueRenderPaddedInt, AppendLeavesExistingContent) {
  std::string out = "12:";
  AppendPaddedInt(out, 5, 2);
  AppendPaddedInt(out, -7, 3);
  EXPECT_EQ(out, "12:05-07");
}

}  // namespace
}  // namespace mes
