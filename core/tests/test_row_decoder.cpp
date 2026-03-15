// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#include "row_decoder.h"

#include <cmath>
#include <cstring>
#include <vector>

#include <gtest/gtest.h>

#include "binary_util.h"

namespace mes {
namespace {

// Helper to write little-endian values into a buffer.
class BinaryWriter {
 public:
  void WriteU8(uint8_t v) { buf_.push_back(v); }
  void WriteU16Le(uint16_t v) {
    buf_.push_back(static_cast<uint8_t>(v));
    buf_.push_back(static_cast<uint8_t>(v >> 8));
  }
  void WriteU24Le(uint32_t v) {
    buf_.push_back(static_cast<uint8_t>(v));
    buf_.push_back(static_cast<uint8_t>(v >> 8));
    buf_.push_back(static_cast<uint8_t>(v >> 16));
  }
  void WriteU32Le(uint32_t v) {
    buf_.push_back(static_cast<uint8_t>(v));
    buf_.push_back(static_cast<uint8_t>(v >> 8));
    buf_.push_back(static_cast<uint8_t>(v >> 16));
    buf_.push_back(static_cast<uint8_t>(v >> 24));
  }
  void WriteU48Le(uint64_t v) {
    WriteU32Le(static_cast<uint32_t>(v));
    WriteU16Le(static_cast<uint16_t>(v >> 32));
  }
  void WriteU64Le(uint64_t v) {
    WriteU32Le(static_cast<uint32_t>(v));
    WriteU32Le(static_cast<uint32_t>(v >> 32));
  }
  void WriteU16Be(uint16_t v) {
    buf_.push_back(static_cast<uint8_t>(v >> 8));
    buf_.push_back(static_cast<uint8_t>(v));
  }
  void WriteU24Be(uint32_t v) {
    buf_.push_back(static_cast<uint8_t>(v >> 16));
    buf_.push_back(static_cast<uint8_t>(v >> 8));
    buf_.push_back(static_cast<uint8_t>(v));
  }
  void WriteU32Be(uint32_t v) {
    buf_.push_back(static_cast<uint8_t>(v >> 24));
    buf_.push_back(static_cast<uint8_t>(v >> 16));
    buf_.push_back(static_cast<uint8_t>(v >> 8));
    buf_.push_back(static_cast<uint8_t>(v));
  }
  void WriteFloat(float v) {
    uint8_t tmp[4];
    std::memcpy(tmp, &v, 4);
    buf_.insert(buf_.end(), tmp, tmp + 4);
  }
  void WriteDouble(double v) {
    uint8_t tmp[8];
    std::memcpy(tmp, &v, 8);
    buf_.insert(buf_.end(), tmp, tmp + 8);
  }
  void WriteBytes(const uint8_t* d, size_t n) {
    buf_.insert(buf_.end(), d, d + n);
  }
  void WriteString(const std::string& s) {
    buf_.insert(buf_.end(), s.begin(), s.end());
  }

  const uint8_t* Data() const { return buf_.data(); }
  size_t Size() const { return buf_.size(); }
  std::vector<uint8_t>& Buffer() { return buf_; }

 private:
  std::vector<uint8_t> buf_;
};

// --- DecodeColumnValue tests ---

TEST(DecodeColumnValueTest, TinySignedPositive) {
  uint8_t data[] = {127};
  size_t consumed = 0;
  auto val = DecodeColumnValue(ColumnType::kTiny, 0, false, data, 1, &consumed);
  EXPECT_EQ(consumed, 1u);
  EXPECT_FALSE(val.is_null);
  EXPECT_EQ(val.int_val, 127);
}

TEST(DecodeColumnValueTest, TinySignedNegative) {
  uint8_t data[] = {0xFF};  // -1 as int8_t
  size_t consumed = 0;
  auto val = DecodeColumnValue(ColumnType::kTiny, 0, false, data, 1, &consumed);
  EXPECT_EQ(consumed, 1u);
  EXPECT_FALSE(val.is_null);
  EXPECT_EQ(val.int_val, -1);
}

TEST(DecodeColumnValueTest, TinyUnsigned) {
  uint8_t data[] = {255};
  size_t consumed = 0;
  auto val = DecodeColumnValue(ColumnType::kTiny, 0, true, data, 1, &consumed);
  EXPECT_EQ(consumed, 1u);
  EXPECT_EQ(val.int_val, 255);
}

TEST(DecodeColumnValueTest, TinyUnsignedZero) {
  uint8_t data[] = {0};
  size_t consumed = 0;
  auto val = DecodeColumnValue(ColumnType::kTiny, 0, true, data, 1, &consumed);
  EXPECT_EQ(consumed, 1u);
  EXPECT_EQ(val.int_val, 0);
}

TEST(DecodeColumnValueTest, ShortSigned) {
  BinaryWriter w;
  w.WriteU16Le(static_cast<uint16_t>(-32768));
  size_t consumed = 0;
  auto val =
      DecodeColumnValue(ColumnType::kShort, 0, false, w.Data(), w.Size(), &consumed);
  EXPECT_EQ(consumed, 2u);
  EXPECT_EQ(val.int_val, -32768);
}

TEST(DecodeColumnValueTest, ShortUnsigned) {
  BinaryWriter w;
  w.WriteU16Le(65535);
  size_t consumed = 0;
  auto val =
      DecodeColumnValue(ColumnType::kShort, 0, true, w.Data(), w.Size(), &consumed);
  EXPECT_EQ(consumed, 2u);
  EXPECT_EQ(val.int_val, 65535);
}

TEST(DecodeColumnValueTest, LongSigned) {
  BinaryWriter w;
  w.WriteU32Le(static_cast<uint32_t>(-100));
  size_t consumed = 0;
  auto val =
      DecodeColumnValue(ColumnType::kLong, 0, false, w.Data(), w.Size(), &consumed);
  EXPECT_EQ(consumed, 4u);
  EXPECT_EQ(val.int_val, -100);
}

TEST(DecodeColumnValueTest, LongUnsigned) {
  BinaryWriter w;
  w.WriteU32Le(4000000000u);
  size_t consumed = 0;
  auto val =
      DecodeColumnValue(ColumnType::kLong, 0, true, w.Data(), w.Size(), &consumed);
  EXPECT_EQ(consumed, 4u);
  EXPECT_EQ(val.int_val, 4000000000);
}

TEST(DecodeColumnValueTest, LongLongSigned) {
  BinaryWriter w;
  w.WriteU64Le(static_cast<uint64_t>(-1000LL));
  size_t consumed = 0;
  auto val = DecodeColumnValue(ColumnType::kLongLong, 0, false, w.Data(),
                               w.Size(), &consumed);
  EXPECT_EQ(consumed, 8u);
  EXPECT_EQ(val.int_val, -1000);
}

TEST(DecodeColumnValueTest, LongLongUnsigned) {
  BinaryWriter w;
  w.WriteU64Le(9000000000000000000ULL);
  size_t consumed = 0;
  auto val = DecodeColumnValue(ColumnType::kLongLong, 0, true, w.Data(),
                               w.Size(), &consumed);
  EXPECT_EQ(consumed, 8u);
  EXPECT_EQ(static_cast<uint64_t>(val.int_val), 9000000000000000000ULL);
}

TEST(DecodeColumnValueTest, Int24SignedNegative) {
  BinaryWriter w;
  // -1 in 24-bit: 0xFFFFFF
  w.WriteU24Le(0xFFFFFF);
  size_t consumed = 0;
  auto val =
      DecodeColumnValue(ColumnType::kInt24, 0, false, w.Data(), w.Size(), &consumed);
  EXPECT_EQ(consumed, 3u);
  EXPECT_EQ(val.int_val, -1);
}

TEST(DecodeColumnValueTest, Int24SignedPositive) {
  BinaryWriter w;
  w.WriteU24Le(100);
  size_t consumed = 0;
  auto val =
      DecodeColumnValue(ColumnType::kInt24, 0, false, w.Data(), w.Size(), &consumed);
  EXPECT_EQ(consumed, 3u);
  EXPECT_EQ(val.int_val, 100);
}

TEST(DecodeColumnValueTest, Int24Unsigned) {
  BinaryWriter w;
  w.WriteU24Le(0xFFFFFF);
  size_t consumed = 0;
  auto val =
      DecodeColumnValue(ColumnType::kInt24, 0, true, w.Data(), w.Size(), &consumed);
  EXPECT_EQ(consumed, 3u);
  EXPECT_EQ(val.int_val, 0xFFFFFF);
}

TEST(DecodeColumnValueTest, FloatNormal) {
  BinaryWriter w;
  w.WriteFloat(3.14f);
  size_t consumed = 0;
  auto val =
      DecodeColumnValue(ColumnType::kFloat, 0, false, w.Data(), w.Size(), &consumed);
  EXPECT_EQ(consumed, 4u);
  EXPECT_FALSE(val.is_null);
  EXPECT_NEAR(val.float_val, 3.14, 0.01);
}

TEST(DecodeColumnValueTest, FloatZero) {
  BinaryWriter w;
  w.WriteFloat(0.0f);
  size_t consumed = 0;
  auto val =
      DecodeColumnValue(ColumnType::kFloat, 0, false, w.Data(), w.Size(), &consumed);
  EXPECT_EQ(consumed, 4u);
  EXPECT_DOUBLE_EQ(val.float_val, 0.0);
}

TEST(DecodeColumnValueTest, FloatNegative) {
  BinaryWriter w;
  w.WriteFloat(-1.5f);
  size_t consumed = 0;
  auto val =
      DecodeColumnValue(ColumnType::kFloat, 0, false, w.Data(), w.Size(), &consumed);
  EXPECT_EQ(consumed, 4u);
  EXPECT_NEAR(val.float_val, -1.5, 0.01);
}

TEST(DecodeColumnValueTest, DoubleNormal) {
  BinaryWriter w;
  w.WriteDouble(2.718281828);
  size_t consumed = 0;
  auto val = DecodeColumnValue(ColumnType::kDouble, 0, false, w.Data(),
                               w.Size(), &consumed);
  EXPECT_EQ(consumed, 8u);
  EXPECT_NEAR(val.float_val, 2.718281828, 0.000001);
}

TEST(DecodeColumnValueTest, DoubleZero) {
  BinaryWriter w;
  w.WriteDouble(0.0);
  size_t consumed = 0;
  auto val = DecodeColumnValue(ColumnType::kDouble, 0, false, w.Data(),
                               w.Size(), &consumed);
  EXPECT_EQ(consumed, 8u);
  EXPECT_DOUBLE_EQ(val.float_val, 0.0);
}

TEST(DecodeColumnValueTest, DoubleNegative) {
  BinaryWriter w;
  w.WriteDouble(-999.999);
  size_t consumed = 0;
  auto val = DecodeColumnValue(ColumnType::kDouble, 0, false, w.Data(),
                               w.Size(), &consumed);
  EXPECT_EQ(consumed, 8u);
  EXPECT_NEAR(val.float_val, -999.999, 0.001);
}

TEST(DecodeColumnValueTest, YearZero) {
  uint8_t data[] = {0};
  size_t consumed = 0;
  auto val = DecodeColumnValue(ColumnType::kYear, 0, false, data, 1, &consumed);
  EXPECT_EQ(consumed, 1u);
  EXPECT_EQ(val.int_val, 0);
}

TEST(DecodeColumnValueTest, Year2000) {
  uint8_t data[] = {100};  // 100 + 1900 = 2000
  size_t consumed = 0;
  auto val = DecodeColumnValue(ColumnType::kYear, 0, false, data, 1, &consumed);
  EXPECT_EQ(consumed, 1u);
  EXPECT_EQ(val.int_val, 2000);
}

TEST(DecodeColumnValueTest, Year2055) {
  uint8_t data[] = {155};  // 155 + 1900 = 2055
  size_t consumed = 0;
  auto val = DecodeColumnValue(ColumnType::kYear, 0, false, data, 1, &consumed);
  EXPECT_EQ(consumed, 1u);
  EXPECT_EQ(val.int_val, 2055);
}

TEST(DecodeColumnValueTest, Date) {
  // Encode 2024-03-15
  // day=15, month=3, year=2024
  // val = (2024 << 9) | (3 << 5) | 15 = 1036288 + 96 + 15 = 1036399
  uint32_t val = (2024 << 9) | (3 << 5) | 15;
  BinaryWriter w;
  w.WriteU24Le(val);
  size_t consumed = 0;
  auto result =
      DecodeColumnValue(ColumnType::kDate, 0, false, w.Data(), w.Size(), &consumed);
  EXPECT_EQ(consumed, 3u);
  EXPECT_EQ(result.string_val, "2024-03-15");
}

TEST(DecodeColumnValueTest, Datetime2NoFrac) {
  // Encode 2024-03-15 10:30:45 with fsp=0
  // ym = year*13 + month = 2024*13 + 3 = 26315
  // ymd = (ym << 5) | day = (26315 << 5) | 15 = 842095
  // hms = (hour << 12) | (minute << 6) | second = (10<<12)|(30<<6)|45
  //     = 40960 + 1920 + 45 = 42925
  // intpart = (ymd << 17) | hms = (842095 << 17) | 42925
  int64_t ym = 2024 * 13 + 3;
  int64_t ymd = (ym << 5) | 15;
  int64_t hms = (10LL << 12) | (30 << 6) | 45;
  int64_t intpart = (ymd << 17) | hms;
  int64_t packed = intpart + 0x8000000000LL;

  BinaryWriter w;
  // Write 5 bytes big-endian
  w.WriteU8(static_cast<uint8_t>(packed >> 32));
  w.WriteU8(static_cast<uint8_t>(packed >> 24));
  w.WriteU8(static_cast<uint8_t>(packed >> 16));
  w.WriteU8(static_cast<uint8_t>(packed >> 8));
  w.WriteU8(static_cast<uint8_t>(packed));

  size_t consumed = 0;
  auto result = DecodeColumnValue(ColumnType::kDatetime2, 0, false, w.Data(),
                                  w.Size(), &consumed);
  EXPECT_EQ(consumed, 5u);
  EXPECT_EQ(result.string_val, "2024-03-15 10:30:45");
}

TEST(DecodeColumnValueTest, Datetime2WithFrac3) {
  // 2024-03-15 10:30:45.123 with fsp=3
  int64_t ym = 2024 * 13 + 3;
  int64_t ymd = (ym << 5) | 15;
  int64_t hms = (10LL << 12) | (30 << 6) | 45;
  int64_t intpart = (ymd << 17) | hms;
  int64_t packed = intpart + 0x8000000000LL;

  BinaryWriter w;
  w.WriteU8(static_cast<uint8_t>(packed >> 32));
  w.WriteU8(static_cast<uint8_t>(packed >> 24));
  w.WriteU8(static_cast<uint8_t>(packed >> 16));
  w.WriteU8(static_cast<uint8_t>(packed >> 8));
  w.WriteU8(static_cast<uint8_t>(packed));

  // frac for fsp=3: frac_bytes = (3+1)/2 = 2 bytes
  // For odd precision (3), stored value = actual * 10 = 123 * 10 = 1230
  // Written as 2 bytes big-endian
  uint16_t frac = 1230;
  w.WriteU16Be(frac);

  size_t consumed = 0;
  auto result = DecodeColumnValue(ColumnType::kDatetime2, 3, false, w.Data(),
                                  w.Size(), &consumed);
  EXPECT_EQ(consumed, 7u);
  EXPECT_EQ(result.string_val, "2024-03-15 10:30:45.123000");
}

TEST(DecodeColumnValueTest, Datetime2WithFrac6) {
  // 2024-03-15 10:30:45.123456 with fsp=6
  int64_t ym = 2024 * 13 + 3;
  int64_t ymd = (ym << 5) | 15;
  int64_t hms = (10LL << 12) | (30 << 6) | 45;
  int64_t intpart = (ymd << 17) | hms;
  int64_t packed = intpart + 0x8000000000LL;

  BinaryWriter w;
  w.WriteU8(static_cast<uint8_t>(packed >> 32));
  w.WriteU8(static_cast<uint8_t>(packed >> 24));
  w.WriteU8(static_cast<uint8_t>(packed >> 16));
  w.WriteU8(static_cast<uint8_t>(packed >> 8));
  w.WriteU8(static_cast<uint8_t>(packed));

  // frac for fsp=6: frac_bytes = 3 bytes big-endian
  // For even precision (6): stored = 123456
  uint32_t frac = 123456;
  w.WriteU24Be(frac);

  size_t consumed = 0;
  auto result = DecodeColumnValue(ColumnType::kDatetime2, 6, false, w.Data(),
                                  w.Size(), &consumed);
  EXPECT_EQ(consumed, 8u);
  EXPECT_EQ(result.string_val, "2024-03-15 10:30:45.123456");
}

TEST(DecodeColumnValueTest, Timestamp2NoFrac) {
  // Unix timestamp 1710502245 (2024-03-15 10:30:45 UTC)
  BinaryWriter w;
  w.WriteU32Be(1710502245);
  size_t consumed = 0;
  auto result = DecodeColumnValue(ColumnType::kTimestamp2, 0, false, w.Data(),
                                  w.Size(), &consumed);
  EXPECT_EQ(consumed, 4u);
  EXPECT_EQ(result.string_val, "1710502245");
}

TEST(DecodeColumnValueTest, Timestamp2WithFrac6) {
  BinaryWriter w;
  w.WriteU32Be(1710502245);
  // fsp=6, frac_bytes=3, stored as 123456
  w.WriteU24Be(123456);
  size_t consumed = 0;
  auto result = DecodeColumnValue(ColumnType::kTimestamp2, 6, false, w.Data(),
                                  w.Size(), &consumed);
  EXPECT_EQ(consumed, 7u);
  EXPECT_EQ(result.string_val, "1710502245.123456");
}

TEST(DecodeColumnValueTest, Time2NoFrac) {
  // Encode 10:30:45 with fsp=0
  // intpart = (hour << 12) | (minute << 6) | second
  //         = (10 << 12) | (30 << 6) | 45 = 42925
  // packed = intpart + 0x800000
  uint32_t intpart = (10 << 12) | (30 << 6) | 45;
  uint32_t packed = intpart + 0x800000;
  BinaryWriter w;
  w.WriteU24Be(packed);
  size_t consumed = 0;
  auto result = DecodeColumnValue(ColumnType::kTime2, 0, false, w.Data(),
                                  w.Size(), &consumed);
  EXPECT_EQ(consumed, 3u);
  EXPECT_EQ(result.string_val, "10:30:45");
}

TEST(DecodeColumnValueTest, Time2Negative) {
  // Encode -05:15:30 with fsp=0
  int32_t intpart = (5 << 12) | (15 << 6) | 30;
  int32_t neg = -intpart;
  uint32_t packed = static_cast<uint32_t>(neg + 0x800000);
  BinaryWriter w;
  w.WriteU24Be(packed);
  size_t consumed = 0;
  auto result = DecodeColumnValue(ColumnType::kTime2, 0, false, w.Data(),
                                  w.Size(), &consumed);
  EXPECT_EQ(consumed, 3u);
  EXPECT_EQ(result.string_val, "-05:15:30");
}

TEST(DecodeColumnValueTest, Time2WithFrac3) {
  uint32_t intpart = (10 << 12) | (30 << 6) | 45;
  uint32_t packed = intpart + 0x800000;
  BinaryWriter w;
  w.WriteU24Be(packed);
  // fsp=3: frac_bytes = 2, odd so stored = 500 * 10 = 5000?
  // Actually for fsp=3, frac 500ms = 500000 usec
  // stored frac for odd (3): val = 500 * 10 = 5000
  // usec = (5000 / 10) * 1000 = 500000
  uint16_t frac = 5000;
  w.WriteU16Be(frac);
  size_t consumed = 0;
  auto result = DecodeColumnValue(ColumnType::kTime2, 3, false, w.Data(),
                                  w.Size(), &consumed);
  EXPECT_EQ(consumed, 5u);
  EXPECT_EQ(result.string_val, "10:30:45.500000");
}

TEST(DecodeColumnValueTest, VarcharShort) {
  // meta <= 255: 1-byte length prefix
  BinaryWriter w;
  w.WriteU8(5);
  w.WriteString("hello");
  size_t consumed = 0;
  auto result = DecodeColumnValue(ColumnType::kVarchar, 100, false, w.Data(),
                                  w.Size(), &consumed);
  EXPECT_EQ(consumed, 6u);
  EXPECT_EQ(result.string_val, "hello");
}

TEST(DecodeColumnValueTest, VarcharLong) {
  // meta > 255: 2-byte length prefix
  BinaryWriter w;
  w.WriteU16Le(3);
  w.WriteString("abc");
  size_t consumed = 0;
  auto result = DecodeColumnValue(ColumnType::kVarchar, 500, false, w.Data(),
                                  w.Size(), &consumed);
  EXPECT_EQ(consumed, 5u);
  EXPECT_EQ(result.string_val, "abc");
}

TEST(DecodeColumnValueTest, BlobPack1) {
  BinaryWriter w;
  w.WriteU8(3);
  w.WriteString("abc");
  size_t consumed = 0;
  auto result = DecodeColumnValue(ColumnType::kBlob, 1, false, w.Data(),
                                  w.Size(), &consumed);
  EXPECT_EQ(consumed, 4u);
  EXPECT_EQ(result.bytes_val.size(), 3u);
  EXPECT_EQ(result.bytes_val[0], 'a');
}

TEST(DecodeColumnValueTest, BlobPack2) {
  BinaryWriter w;
  w.WriteU16Le(5);
  w.WriteString("hello");
  size_t consumed = 0;
  auto result = DecodeColumnValue(ColumnType::kBlob, 2, false, w.Data(),
                                  w.Size(), &consumed);
  EXPECT_EQ(consumed, 7u);
  EXPECT_EQ(result.bytes_val.size(), 5u);
}

TEST(DecodeColumnValueTest, BlobPack4) {
  BinaryWriter w;
  w.WriteU32Le(4);
  w.WriteString("test");
  size_t consumed = 0;
  auto result = DecodeColumnValue(ColumnType::kBlob, 4, false, w.Data(),
                                  w.Size(), &consumed);
  EXPECT_EQ(consumed, 8u);
  EXPECT_EQ(result.bytes_val.size(), 4u);
}

TEST(DecodeColumnValueTest, JsonAsBlob) {
  BinaryWriter w;
  w.WriteU32Le(2);
  w.WriteString("{}");
  size_t consumed = 0;
  auto result = DecodeColumnValue(ColumnType::kJson, 4, false, w.Data(),
                                  w.Size(), &consumed);
  EXPECT_EQ(consumed, 6u);
  EXPECT_EQ(result.bytes_val.size(), 2u);
}

TEST(DecodeColumnValueTest, NewDecimal) {
  // DECIMAL(10,2): precision=10, scale=2, meta = (10 << 8) | 2
  // Encode "12345678.90"
  // intg = 10 - 2 = 8, intg0 = 0, intg_rem = 8 -> 4 bytes
  // frac0 = 0, frac_rem = 2 -> 1 byte
  // Total = 4 + 1 = 5 bytes
  // Integer part: 12345678 in 4 bytes big-endian
  // Frac part: 90 in 1 byte
  // Positive: XOR first byte with 0x80

  uint8_t data[5];
  // integer part: 12345678
  data[0] = static_cast<uint8_t>((12345678 >> 24) & 0xFF);
  data[1] = static_cast<uint8_t>((12345678 >> 16) & 0xFF);
  data[2] = static_cast<uint8_t>((12345678 >> 8) & 0xFF);
  data[3] = static_cast<uint8_t>(12345678 & 0xFF);
  // frac part: 90
  data[4] = 90;
  // Positive: XOR first byte with 0x80
  data[0] ^= 0x80;

  uint16_t meta = (10 << 8) | 2;
  size_t consumed = 0;
  auto result =
      DecodeColumnValue(ColumnType::kNewDecimal, meta, false, data, 5, &consumed);
  EXPECT_EQ(consumed, 5u);
  EXPECT_EQ(result.string_val, "12345678.90");
}

TEST(DecodeColumnValueTest, BitSingle) {
  // BIT(1): extra_bits=1, full_bytes=0, meta = (0 << 8) | 1
  // total_bytes = 0 + 1 = 1
  uint8_t data[] = {1};
  uint16_t meta = (0 << 8) | 1;
  size_t consumed = 0;
  auto result =
      DecodeColumnValue(ColumnType::kBit, meta, false, data, 1, &consumed);
  EXPECT_EQ(consumed, 1u);
  EXPECT_EQ(result.int_val, 1);
}

TEST(DecodeColumnValueTest, BitMultipleBytes) {
  // BIT(16): extra_bits=0, full_bytes=2, meta = (2 << 8) | 0
  uint8_t data[] = {0x01, 0x02};
  uint16_t meta = (2 << 8) | 0;
  size_t consumed = 0;
  auto result =
      DecodeColumnValue(ColumnType::kBit, meta, false, data, 2, &consumed);
  EXPECT_EQ(consumed, 2u);
  EXPECT_EQ(result.int_val, 0x0102);
}

TEST(DecodeColumnValueTest, StringEnum1Byte) {
  // ENUM encoded in STRING: real_type = 0xF7, size = 1
  // meta = (0xF7 << 8) | 1
  uint8_t data[] = {3};
  uint16_t meta = (0xF7 << 8) | 1;
  size_t consumed = 0;
  auto result =
      DecodeColumnValue(ColumnType::kString, meta, false, data, 1, &consumed);
  EXPECT_EQ(consumed, 1u);
  EXPECT_EQ(result.type, ColumnType::kEnum);
  EXPECT_EQ(result.int_val, 3);
}

TEST(DecodeColumnValueTest, StringEnum2Byte) {
  // ENUM with 2-byte: meta = (0xF7 << 8) | 2
  BinaryWriter w;
  w.WriteU16Le(300);
  uint16_t meta = (0xF7 << 8) | 2;
  size_t consumed = 0;
  auto result = DecodeColumnValue(ColumnType::kString, meta, false, w.Data(),
                                  w.Size(), &consumed);
  EXPECT_EQ(consumed, 2u);
  EXPECT_EQ(result.type, ColumnType::kEnum);
  EXPECT_EQ(result.int_val, 300);
}

TEST(DecodeColumnValueTest, StringSet) {
  // SET encoded in STRING: real_type = 0xF8, size = 2
  // meta = (0xF8 << 8) | 2
  BinaryWriter w;
  w.WriteU16Le(0x0005);
  uint16_t meta = (0xF8 << 8) | 2;
  size_t consumed = 0;
  auto result = DecodeColumnValue(ColumnType::kString, meta, false, w.Data(),
                                  w.Size(), &consumed);
  EXPECT_EQ(consumed, 2u);
  EXPECT_EQ(result.type, ColumnType::kSet);
  EXPECT_EQ(result.int_val, 5);
}

TEST(DecodeColumnValueTest, StringChar) {
  // CHAR(10) with type_byte=0xFE: meta = (0xFE << 8) | 10 = 0xFE0A
  // max_len = (((0xFE0A >> 4) & 0x300) ^ 0x300) + (0xFE0A & 0xFF)
  //         = (0x300 ^ 0x300) + 10 = 10 (1-byte prefix)
  BinaryWriter w;
  w.WriteU8(5);  // 1-byte length prefix
  w.WriteString("hello");
  uint16_t meta = 0xFE0A;
  size_t consumed = 0;
  auto result = DecodeColumnValue(ColumnType::kString, meta, false, w.Data(),
                                  w.Size(), &consumed);
  EXPECT_EQ(consumed, 6u);
  EXPECT_EQ(result.string_val, "hello");
}

// --- DecodeWriteRows test ---

TEST(DecodeWriteRowsTest, SingleRowIntVarchar) {
  // Build a WRITE_ROWS_EVENT V2 body:
  // Schema: INT (signed) + VARCHAR(100)
  TableMetadata metadata;
  metadata.table_id = 42;
  metadata.database_name = "testdb";
  metadata.table_name = "users";
  metadata.columns.resize(2);
  metadata.columns[0].type = ColumnType::kLong;
  metadata.columns[0].metadata = 0;
  metadata.columns[0].is_unsigned = false;
  metadata.columns[1].type = ColumnType::kVarchar;
  metadata.columns[1].metadata = 100;
  metadata.columns[1].is_unsigned = false;

  BinaryWriter w;
  // table_id: 6 bytes
  w.WriteU48Le(42);
  // flags: 2 bytes
  w.WriteU16Le(0);
  // V2: var_header_len = 2 (just the length field itself)
  w.WriteU16Le(2);
  // column_count = 2 (packed int)
  w.WriteU8(2);
  // columns_present bitmap: both present = 0x03
  w.WriteU8(0x03);
  // Row 1:
  // null_bitmap for 2 present columns: 1 byte, no nulls = 0x00
  w.WriteU8(0x00);
  // INT value: 42
  w.WriteU32Le(42);
  // VARCHAR value: "Alice" (meta=100 <= 255, so 1-byte prefix)
  w.WriteU8(5);
  w.WriteString("Alice");

  std::vector<RowData> rows;
  ASSERT_TRUE(
      DecodeWriteRows(w.Data(), w.Size(), metadata, true, &rows));
  ASSERT_EQ(rows.size(), 1u);
  ASSERT_EQ(rows[0].columns.size(), 2u);
  EXPECT_EQ(rows[0].columns[0].int_val, 42);
  EXPECT_EQ(rows[0].columns[1].string_val, "Alice");
}

TEST(DecodeWriteRowsTest, TwoRows) {
  TableMetadata metadata;
  metadata.table_id = 10;
  metadata.database_name = "db";
  metadata.table_name = "t";
  metadata.columns.resize(1);
  metadata.columns[0].type = ColumnType::kLong;
  metadata.columns[0].metadata = 0;
  metadata.columns[0].is_unsigned = false;

  BinaryWriter w;
  w.WriteU48Le(10);
  w.WriteU16Le(0);
  w.WriteU16Le(2);  // V2 var_header_len
  w.WriteU8(1);     // column_count
  w.WriteU8(0x01);  // columns_present
  // Row 1
  w.WriteU8(0x00);  // null bitmap
  w.WriteU32Le(100);
  // Row 2
  w.WriteU8(0x00);
  w.WriteU32Le(200);

  std::vector<RowData> rows;
  ASSERT_TRUE(DecodeWriteRows(w.Data(), w.Size(), metadata, true, &rows));
  ASSERT_EQ(rows.size(), 2u);
  EXPECT_EQ(rows[0].columns[0].int_val, 100);
  EXPECT_EQ(rows[1].columns[0].int_val, 200);
}

TEST(DecodeUpdateRowsTest, SinglePair) {
  TableMetadata metadata;
  metadata.table_id = 10;
  metadata.database_name = "db";
  metadata.table_name = "t";
  metadata.columns.resize(1);
  metadata.columns[0].type = ColumnType::kLong;
  metadata.columns[0].metadata = 0;
  metadata.columns[0].is_unsigned = false;

  BinaryWriter w;
  w.WriteU48Le(10);
  w.WriteU16Le(0);
  w.WriteU16Le(2);  // V2 var_header_len
  w.WriteU8(1);     // column_count
  w.WriteU8(0x01);  // columns_present (before)
  w.WriteU8(0x01);  // columns_present_update (after)
  // Before row
  w.WriteU8(0x00);
  w.WriteU32Le(100);
  // After row
  w.WriteU8(0x00);
  w.WriteU32Le(200);

  std::vector<UpdatePair> pairs;
  ASSERT_TRUE(DecodeUpdateRows(w.Data(), w.Size(), metadata, true, &pairs));
  ASSERT_EQ(pairs.size(), 1u);
  EXPECT_EQ(pairs[0].before.columns[0].int_val, 100);
  EXPECT_EQ(pairs[0].after.columns[0].int_val, 200);
}

TEST(DecodeDeleteRowsTest, SingleRow) {
  TableMetadata metadata;
  metadata.table_id = 10;
  metadata.database_name = "db";
  metadata.table_name = "t";
  metadata.columns.resize(1);
  metadata.columns[0].type = ColumnType::kLong;
  metadata.columns[0].metadata = 0;
  metadata.columns[0].is_unsigned = false;

  BinaryWriter w;
  w.WriteU48Le(10);
  w.WriteU16Le(0);
  w.WriteU16Le(2);
  w.WriteU8(1);
  w.WriteU8(0x01);
  // Row
  w.WriteU8(0x00);
  w.WriteU32Le(999);

  std::vector<RowData> rows;
  ASSERT_TRUE(DecodeDeleteRows(w.Data(), w.Size(), metadata, true, &rows));
  ASSERT_EQ(rows.size(), 1u);
  EXPECT_EQ(rows[0].columns[0].int_val, 999);
}

TEST(DecodeWriteRowsTest, NullColumn) {
  TableMetadata metadata;
  metadata.table_id = 10;
  metadata.database_name = "db";
  metadata.table_name = "t";
  metadata.columns.resize(2);
  metadata.columns[0].type = ColumnType::kLong;
  metadata.columns[0].metadata = 0;
  metadata.columns[1].type = ColumnType::kVarchar;
  metadata.columns[1].metadata = 100;

  BinaryWriter w;
  w.WriteU48Le(10);
  w.WriteU16Le(0);
  w.WriteU16Le(2);
  w.WriteU8(2);     // column_count
  w.WriteU8(0x03);  // columns_present: both
  // null_bitmap: column 1 (VARCHAR) is null -> bit 1 set = 0x02
  w.WriteU8(0x02);
  // INT value
  w.WriteU32Le(42);
  // VARCHAR is NULL, no data

  std::vector<RowData> rows;
  ASSERT_TRUE(DecodeWriteRows(w.Data(), w.Size(), metadata, true, &rows));
  ASSERT_EQ(rows.size(), 1u);
  ASSERT_EQ(rows[0].columns.size(), 2u);
  EXPECT_FALSE(rows[0].columns[0].is_null);
  EXPECT_EQ(rows[0].columns[0].int_val, 42);
  EXPECT_TRUE(rows[0].columns[1].is_null);
}

TEST(DecodeWriteRowsTest, NullPointers) {
  TableMetadata metadata;
  std::vector<RowData> rows;
  EXPECT_FALSE(DecodeWriteRows(nullptr, 10, metadata, true, &rows));
  uint8_t data[10] = {};
  EXPECT_FALSE(DecodeWriteRows(data, 10, metadata, true, nullptr));
}

}  // namespace
}  // namespace mes
