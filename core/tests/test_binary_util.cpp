// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#include "binary_util.h"

#include <gtest/gtest.h>

namespace mes::binary {
namespace {

// --- Little-endian reads ---

TEST(BinaryUtilTest, ReadU8) {
  uint8_t data[] = {0x42};
  EXPECT_EQ(ReadU8(data), 0x42);
}

TEST(BinaryUtilTest, ReadU16Le) {
  uint8_t data[] = {0x34, 0x12};
  EXPECT_EQ(ReadU16Le(data), 0x1234);
}

TEST(BinaryUtilTest, ReadU16LeZero) {
  uint8_t data[] = {0x00, 0x00};
  EXPECT_EQ(ReadU16Le(data), 0x0000);
}

TEST(BinaryUtilTest, ReadU16LeMax) {
  uint8_t data[] = {0xFF, 0xFF};
  EXPECT_EQ(ReadU16Le(data), 0xFFFF);
}

TEST(BinaryUtilTest, ReadU24Le) {
  uint8_t data[] = {0x56, 0x34, 0x12};
  EXPECT_EQ(ReadU24Le(data), 0x123456u);
}

TEST(BinaryUtilTest, ReadU32Le) {
  uint8_t data[] = {0x78, 0x56, 0x34, 0x12};
  EXPECT_EQ(ReadU32Le(data), 0x12345678u);
}

TEST(BinaryUtilTest, ReadU48Le) {
  uint8_t data[] = {0xBC, 0x9A, 0x78, 0x56, 0x34, 0x12};
  EXPECT_EQ(ReadU48Le(data), 0x123456789ABCuLL);
}

TEST(BinaryUtilTest, ReadU64Le) {
  uint8_t data[] = {0xEF, 0xCD, 0xAB, 0x90, 0x78, 0x56, 0x34, 0x12};
  EXPECT_EQ(ReadU64Le(data), 0x1234567890ABCDEFuLL);
}

TEST(BinaryUtilTest, ReadU64LeZero) {
  uint8_t data[] = {0, 0, 0, 0, 0, 0, 0, 0};
  EXPECT_EQ(ReadU64Le(data), 0u);
}

// --- Big-endian reads ---

TEST(BinaryUtilTest, ReadU16Be) {
  uint8_t data[] = {0x12, 0x34};
  EXPECT_EQ(ReadU16Be(data), 0x1234);
}

TEST(BinaryUtilTest, ReadU24Be) {
  uint8_t data[] = {0x12, 0x34, 0x56};
  EXPECT_EQ(ReadU24Be(data), 0x123456u);
}

TEST(BinaryUtilTest, ReadU32Be) {
  uint8_t data[] = {0x12, 0x34, 0x56, 0x78};
  EXPECT_EQ(ReadU32Be(data), 0x12345678u);
}

// --- ReadPackedInt ---

TEST(BinaryUtilTest, ReadPackedInt1Byte) {
  uint8_t data[] = {42};
  size_t consumed = 0;
  EXPECT_EQ(ReadPackedInt(data, sizeof(data), consumed), 42u);
  EXPECT_EQ(consumed, 1u);
}

TEST(BinaryUtilTest, ReadPackedInt1ByteZero) {
  uint8_t data[] = {0};
  size_t consumed = 0;
  EXPECT_EQ(ReadPackedInt(data, sizeof(data), consumed), 0u);
  EXPECT_EQ(consumed, 1u);
}

TEST(BinaryUtilTest, ReadPackedInt1ByteMax) {
  uint8_t data[] = {250};
  size_t consumed = 0;
  EXPECT_EQ(ReadPackedInt(data, sizeof(data), consumed), 250u);
  EXPECT_EQ(consumed, 1u);
}

TEST(BinaryUtilTest, ReadPackedIntNull) {
  uint8_t data[] = {251};
  size_t consumed = 0;
  EXPECT_EQ(ReadPackedInt(data, sizeof(data), consumed), 0u);
  EXPECT_EQ(consumed, 1u);
}

TEST(BinaryUtilTest, ReadPackedInt2Bytes) {
  // 252 marker + 2 bytes little-endian
  uint8_t data[] = {252, 0x00, 0x01};  // 256
  size_t consumed = 0;
  EXPECT_EQ(ReadPackedInt(data, sizeof(data), consumed), 256u);
  EXPECT_EQ(consumed, 3u);
}

TEST(BinaryUtilTest, ReadPackedInt3Bytes) {
  // 253 marker + 3 bytes little-endian
  uint8_t data[] = {253, 0x01, 0x00, 0x01};  // 65537
  size_t consumed = 0;
  EXPECT_EQ(ReadPackedInt(data, sizeof(data), consumed), 65537u);
  EXPECT_EQ(consumed, 4u);
}

TEST(BinaryUtilTest, ReadPackedInt8Bytes) {
  // 254 marker + 8 bytes little-endian
  uint8_t data[] = {254, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};
  size_t consumed = 0;
  EXPECT_EQ(ReadPackedInt(data, sizeof(data), consumed), 1u);
  EXPECT_EQ(consumed, 9u);
}

TEST(BinaryUtilTest, ReadPackedInt8BytesLarge) {
  uint8_t data[] = {254, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F};
  size_t consumed = 0;
  EXPECT_EQ(ReadPackedInt(data, sizeof(data), consumed), 0x7FFFFFFFFFFFFFFFuLL);
  EXPECT_EQ(consumed, 9u);
}

// --- ReadPackedInt bounds checking ---

TEST(BinaryUtilTest, ReadPackedIntEmptyBuffer) {
  uint8_t data[] = {0};
  size_t consumed = 99;
  EXPECT_EQ(ReadPackedInt(data, 0, consumed), 0u);
  EXPECT_EQ(consumed, 0u);
}

TEST(BinaryUtilTest, ReadPackedInt252InsufficientLen) {
  // 252 marker requires 3 bytes total, but only 2 available
  uint8_t data[] = {252, 0x00};
  size_t consumed = 99;
  EXPECT_EQ(ReadPackedInt(data, 2, consumed), 0u);
  EXPECT_EQ(consumed, 0u);
}

TEST(BinaryUtilTest, ReadPackedInt253InsufficientLen) {
  // 253 marker requires 4 bytes total, but only 3 available
  uint8_t data[] = {253, 0x01, 0x00};
  size_t consumed = 99;
  EXPECT_EQ(ReadPackedInt(data, 3, consumed), 0u);
  EXPECT_EQ(consumed, 0u);
}

TEST(BinaryUtilTest, ReadPackedInt254InsufficientLen) {
  // 254 marker requires 9 bytes total, but only 5 available
  uint8_t data[] = {254, 0x01, 0x00, 0x00, 0x00};
  size_t consumed = 99;
  EXPECT_EQ(ReadPackedInt(data, 5, consumed), 0u);
  EXPECT_EQ(consumed, 0u);
}

// --- Bitmap utilities ---

TEST(BinaryUtilTest, BitmapBytes) {
  EXPECT_EQ(BitmapBytes(0), 0u);
  EXPECT_EQ(BitmapBytes(1), 1u);
  EXPECT_EQ(BitmapBytes(7), 1u);
  EXPECT_EQ(BitmapBytes(8), 1u);
  EXPECT_EQ(BitmapBytes(9), 2u);
  EXPECT_EQ(BitmapBytes(16), 2u);
  EXPECT_EQ(BitmapBytes(17), 3u);
}

TEST(BinaryUtilTest, BitmapIsSet) {
  uint8_t bitmap[] = {0b10100101, 0b00001111};

  // First byte: 10100101
  EXPECT_TRUE(BitmapIsSet(bitmap, 0));   // bit 0
  EXPECT_FALSE(BitmapIsSet(bitmap, 1));  // bit 1
  EXPECT_TRUE(BitmapIsSet(bitmap, 2));   // bit 2
  EXPECT_FALSE(BitmapIsSet(bitmap, 3));  // bit 3
  EXPECT_FALSE(BitmapIsSet(bitmap, 4));  // bit 4
  EXPECT_TRUE(BitmapIsSet(bitmap, 5));   // bit 5
  EXPECT_FALSE(BitmapIsSet(bitmap, 6));  // bit 6
  EXPECT_TRUE(BitmapIsSet(bitmap, 7));   // bit 7

  // Second byte: 00001111
  EXPECT_TRUE(BitmapIsSet(bitmap, 8));    // bit 0 of second byte
  EXPECT_TRUE(BitmapIsSet(bitmap, 9));    // bit 1
  EXPECT_TRUE(BitmapIsSet(bitmap, 10));   // bit 2
  EXPECT_TRUE(BitmapIsSet(bitmap, 11));   // bit 3
  EXPECT_FALSE(BitmapIsSet(bitmap, 12));  // bit 4
  EXPECT_FALSE(BitmapIsSet(bitmap, 15));  // bit 7
}

// --- ReadStringView ---

TEST(BinaryUtilTest, ReadStringView) {
  uint8_t data[] = {'h', 'e', 'l', 'l', 'o'};
  auto sv = ReadStringView(data, 5);
  EXPECT_EQ(sv, "hello");
  EXPECT_EQ(sv.size(), 5u);
}

TEST(BinaryUtilTest, ReadStringViewEmpty) {
  uint8_t data[] = {0};
  auto sv = ReadStringView(data, 0);
  EXPECT_EQ(sv.size(), 0u);
}

// --- DecodeDecimal ---

TEST(DecodeDecimalTest, PositiveInteger) {
  // DECIMAL(5,0) = "12345"
  // intg=5, intg_rem=5, intg0=0, frac0=0, frac_rem=0
  // dig2bytes[5] = 3 => total 3 bytes
  // Positive encoding: value 12345 in 3 bytes big-endian = 0x003039
  // Then XOR first byte with 0x80 => 0x80, 0x30, 0x39
  uint8_t data[] = {0x80, 0x30, 0x39};
  size_t consumed = 0;
  auto result = DecodeDecimal(data, sizeof(data), 5, 0, consumed);
  EXPECT_EQ(result, "12345");
  EXPECT_EQ(consumed, 3u);
}

TEST(DecodeDecimalTest, NegativeInteger) {
  // DECIMAL(5,0) = "-12345"
  // Positive raw: 0x80, 0x30, 0x39
  // Negative: XOR all with 0xFF => 0x7F, 0xCF, 0xC6
  uint8_t data[] = {0x7F, 0xCF, 0xC6};
  size_t consumed = 0;
  auto result = DecodeDecimal(data, sizeof(data), 5, 0, consumed);
  EXPECT_EQ(result, "-12345");
  EXPECT_EQ(consumed, 3u);
}

TEST(DecodeDecimalTest, Zero) {
  // DECIMAL(10,2) = "0.00"
  // intg=8, intg_rem=8, intg0=0, frac0=0, frac_rem=2
  // dig2bytes[8]=4 + dig2bytes[2]=1 => total 5 bytes
  // Zero value: all zeros, then XOR first byte with 0x80
  uint8_t data[] = {0x80, 0x00, 0x00, 0x00, 0x00};
  size_t consumed = 0;
  auto result = DecodeDecimal(data, sizeof(data), 10, 2, consumed);
  EXPECT_EQ(result, "0.00");
  EXPECT_EQ(consumed, 5u);
}

TEST(DecodeDecimalTest, WithFraction) {
  // DECIMAL(10,2) = "12345678.12"
  // intg=8, intg_rem=8, intg0=0, frac0=0, frac_rem=2
  // dig2bytes[8]=4, dig2bytes[2]=1 => total 5 bytes
  // Integer part: 12345678 in 4 bytes big-endian = 0x00BC614E
  // Frac part: 12 in 1 byte = 0x0C
  // XOR first byte with 0x80 => 0x80, 0xBC, 0x61, 0x4E, 0x0C
  uint8_t data[] = {0x80, 0xBC, 0x61, 0x4E, 0x0C};
  size_t consumed = 0;
  auto result = DecodeDecimal(data, sizeof(data), 10, 2, consumed);
  EXPECT_EQ(result, "12345678.12");
  EXPECT_EQ(consumed, 5u);
}

TEST(DecodeDecimalTest, ZeroPrecision) {
  size_t consumed = 0;
  auto result = DecodeDecimal(nullptr, 0, 0, 0, consumed);
  EXPECT_EQ(result, "0");
  EXPECT_EQ(consumed, 0u);
}

TEST(DecodeDecimalTest, SmallPositive) {
  // DECIMAL(3,1) = "1.5"
  // intg=2, intg_rem=2, intg0=0, frac0=0, frac_rem=1
  // dig2bytes[2]=1, dig2bytes[1]=1 => total 2 bytes
  // Integer part: 1 in 1 byte = 0x01
  // Frac part: 5 in 1 byte = 0x05
  // XOR first byte with 0x80 => 0x81, 0x05
  uint8_t data[] = {0x81, 0x05};
  size_t consumed = 0;
  auto result = DecodeDecimal(data, sizeof(data), 3, 1, consumed);
  EXPECT_EQ(result, "1.5");
  EXPECT_EQ(consumed, 2u);
}

// --- CalcFieldSize ---

TEST(CalcFieldSizeTest, FixedSizeTypes) {
  uint8_t dummy[8] = {};
  EXPECT_EQ(CalcFieldSize(0x01, dummy, sizeof(dummy), 0), 1u);   // TINY
  EXPECT_EQ(CalcFieldSize(0x02, dummy, sizeof(dummy), 0), 2u);   // SHORT
  EXPECT_EQ(CalcFieldSize(0x03, dummy, sizeof(dummy), 0), 4u);   // LONG
  EXPECT_EQ(CalcFieldSize(0x04, dummy, sizeof(dummy), 0), 4u);   // FLOAT
  EXPECT_EQ(CalcFieldSize(0x05, dummy, sizeof(dummy), 0), 8u);   // DOUBLE
  EXPECT_EQ(CalcFieldSize(0x08, dummy, sizeof(dummy), 0), 8u);   // LONGLONG
  EXPECT_EQ(CalcFieldSize(0x09, dummy, sizeof(dummy), 0), 3u);   // INT24
  EXPECT_EQ(CalcFieldSize(0x0D, dummy, sizeof(dummy), 0), 1u);   // YEAR
}

TEST(CalcFieldSizeTest, DateTimeTypes) {
  uint8_t dummy[8] = {};
  EXPECT_EQ(CalcFieldSize(0x0A, dummy, sizeof(dummy), 0), 3u);   // DATE
  EXPECT_EQ(CalcFieldSize(0x0B, dummy, sizeof(dummy), 0), 3u);   // TIME
  EXPECT_EQ(CalcFieldSize(0x07, dummy, sizeof(dummy), 0), 4u);   // TIMESTAMP
  EXPECT_EQ(CalcFieldSize(0x0C, dummy, sizeof(dummy), 0), 8u);   // DATETIME
}

TEST(CalcFieldSizeTest, DateTimeWithFractionalSeconds) {
  uint8_t dummy[8] = {};
  // TIMESTAMP2: 4 + (metadata+1)/2
  EXPECT_EQ(CalcFieldSize(0x11, dummy, sizeof(dummy), 0), 4u);   // fsp=0
  EXPECT_EQ(CalcFieldSize(0x11, dummy, sizeof(dummy), 3), 6u);   // fsp=3
  EXPECT_EQ(CalcFieldSize(0x11, dummy, sizeof(dummy), 6), 7u);   // fsp=6

  // DATETIME2: 5 + (metadata+1)/2
  EXPECT_EQ(CalcFieldSize(0x12, dummy, sizeof(dummy), 0), 5u);
  EXPECT_EQ(CalcFieldSize(0x12, dummy, sizeof(dummy), 3), 7u);
  EXPECT_EQ(CalcFieldSize(0x12, dummy, sizeof(dummy), 6), 8u);

  // TIME2: 3 + (metadata+1)/2
  EXPECT_EQ(CalcFieldSize(0x13, dummy, sizeof(dummy), 0), 3u);
  EXPECT_EQ(CalcFieldSize(0x13, dummy, sizeof(dummy), 3), 5u);
  EXPECT_EQ(CalcFieldSize(0x13, dummy, sizeof(dummy), 6), 6u);
}

TEST(CalcFieldSizeTest, Varchar) {
  // metadata <= 255: 1 byte length prefix
  uint8_t data_short[] = {5, 'h', 'e', 'l', 'l', 'o'};
  EXPECT_EQ(CalcFieldSize(0x0F, data_short, sizeof(data_short), 100), 6u);  // 1 + 5

  // metadata > 255: 2 byte length prefix
  uint8_t data_long[] = {0x03, 0x00, 'a', 'b', 'c'};  // length = 3
  EXPECT_EQ(CalcFieldSize(0x0F, data_long, sizeof(data_long), 500), 5u);   // 2 + 3
}

TEST(CalcFieldSizeTest, Blob) {
  // metadata=1: 1 byte length
  uint8_t data1[] = {3, 'a', 'b', 'c'};
  EXPECT_EQ(CalcFieldSize(0xFC, data1, sizeof(data1), 1), 4u);  // 1 + 3

  // metadata=2: 2 byte length
  uint8_t data2[] = {0x05, 0x00, 'h', 'e', 'l', 'l', 'o'};
  EXPECT_EQ(CalcFieldSize(0xFC, data2, sizeof(data2), 2), 7u);  // 2 + 5
}

TEST(CalcFieldSizeTest, StringEnum) {
  // ENUM: metadata high byte = 0xF7, low byte = size
  uint8_t dummy[8] = {};
  // ENUM with 1-byte storage
  EXPECT_EQ(CalcFieldSize(0xFE, dummy, sizeof(dummy), 0xF701), 1u);
  // ENUM with 2-byte storage
  EXPECT_EQ(CalcFieldSize(0xFE, dummy, sizeof(dummy), 0xF702), 2u);
}

TEST(CalcFieldSizeTest, StringSet) {
  // SET: metadata high byte = 0xF8, low byte = size
  uint8_t dummy[8] = {};
  EXPECT_EQ(CalcFieldSize(0xFE, dummy, sizeof(dummy), 0xF804), 4u);
}

TEST(CalcFieldSizeTest, StringChar) {
  // STRING/CHAR metadata encoding:
  // max_len = (((metadata >> 4) & 0x300) ^ 0x300) + (metadata & 0xFF)
  // For max_len <= 255 (1-byte length prefix): need (metadata>>4)&0x300 == 0x300
  // metadata = 0x3005: type_byte=0x30, max_len = (0x300^0x300)+5 = 5
  uint8_t data1[] = {3, 'a', 'b', 'c'};
  EXPECT_EQ(CalcFieldSize(0xFE, data1, sizeof(data1), 0x3005), 4u);  // 1 + 3

  // For max_len > 255 (2-byte length prefix): metadata = 0x0105
  // (0x0105 >> 4) = 0x10, 0x10 & 0x300 = 0, 0 ^ 0x300 = 0x300, 0x300 + 5 = 773
  // max_len = 773 > 255, so uses 2-byte length prefix
  uint8_t data2[] = {0x03, 0x00, 'a', 'b', 'c'};  // length = 3 (LE)
  EXPECT_EQ(CalcFieldSize(0xFE, data2, sizeof(data2), 0x0105), 5u);  // 2 + 3
}

TEST(CalcFieldSizeTest, NewDecimal) {
  // DECIMAL(10,2): precision=10, scale=2
  // metadata = (10 << 8) | 2 = 0x0A02
  // intg=8, intg_rem=8, intg0=0
  // dig2bytes[8]=4, frac0=0, frac_rem=2, dig2bytes[2]=1
  // total = 4 + 0 + 0 + 1 = 5
  uint8_t dummy[8] = {};
  EXPECT_EQ(CalcFieldSize(0xF6, dummy, sizeof(dummy), 0x0A02), 5u);
}

TEST(CalcFieldSizeTest, Bit) {
  uint8_t dummy[8] = {};
  // BIT(1): metadata = (0 << 8) | 1 => bytes=0, bits=1 => 0 + 1 = 1
  EXPECT_EQ(CalcFieldSize(0x10, dummy, sizeof(dummy), 0x0001), 1u);
  // BIT(8): metadata = (1 << 8) | 0 => bytes=1, bits=0 => 1 + 0 = 1
  EXPECT_EQ(CalcFieldSize(0x10, dummy, sizeof(dummy), 0x0100), 1u);
  // BIT(9): metadata = (1 << 8) | 1 => bytes=1, bits=1 => 1 + 1 = 2
  EXPECT_EQ(CalcFieldSize(0x10, dummy, sizeof(dummy), 0x0101), 2u);
}

TEST(CalcFieldSizeTest, Json) {
  // JSON with metadata=4: 4 bytes length prefix
  uint8_t data[] = {0x0A, 0x00, 0x00, 0x00, /* 10 bytes of JSON data */
                    'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j'};
  EXPECT_EQ(CalcFieldSize(0xF5, data, sizeof(data), 4), 14u);  // 4 + 10
}

TEST(CalcFieldSizeTest, Geometry) {
  // GEOMETRY with metadata=4: like BLOB
  uint8_t data[] = {0x05, 0x00, 0x00, 0x00, 1, 2, 3, 4, 5};
  EXPECT_EQ(CalcFieldSize(0xFF, data, sizeof(data), 4), 9u);  // 4 + 5
}

TEST(CalcFieldSizeTest, UnsupportedType) {
  uint8_t dummy[8] = {};
  EXPECT_EQ(CalcFieldSize(0x06, dummy, sizeof(dummy), 0), 0u);  // MYSQL_TYPE_NULL
}

// --- CalcFieldSize for JSON with various pack_length values ---

TEST(CalcFieldSizeTest, JsonPack1) {
  uint8_t data[] = {0x05, 'a', 'b', 'c', 'd', 'e'};
  EXPECT_EQ(CalcFieldSize(0xF5, data, sizeof(data), 1), 6u);  // 1 + 5
}

TEST(CalcFieldSizeTest, JsonPack2) {
  uint8_t data[] = {0x05, 0x00, 'a', 'b', 'c', 'd', 'e'};
  EXPECT_EQ(CalcFieldSize(0xF5, data, sizeof(data), 2), 7u);  // 2 + 5
}

TEST(CalcFieldSizeTest, JsonPack3) {
  uint8_t data[] = {0x05, 0x00, 0x00, 'a', 'b', 'c', 'd', 'e'};
  EXPECT_EQ(CalcFieldSize(0xF5, data, sizeof(data), 3), 8u);  // 3 + 5
}

TEST(CalcFieldSizeTest, JsonDefaultPack) {
  // metadata=0 defaults to pack_length=4, uses U32Le
  uint8_t data[] = {0x05, 0x00, 0x00, 0x00, 'a', 'b', 'c', 'd', 'e'};
  EXPECT_EQ(CalcFieldSize(0xF5, data, sizeof(data), 0), 9u);  // 4 + 5
}

// --- CalcFieldSize for BLOB with pack_length 3, 4 ---

TEST(CalcFieldSizeTest, BlobPack3) {
  uint8_t data[] = {0x05, 0x00, 0x00, 'a', 'b', 'c', 'd', 'e'};
  EXPECT_EQ(CalcFieldSize(0xFC, data, sizeof(data), 3), 8u);  // 3 + 5
}

TEST(CalcFieldSizeTest, BlobPack4) {
  uint8_t data[] = {0x05, 0x00, 0x00, 0x00, 'a', 'b', 'c', 'd', 'e'};
  EXPECT_EQ(CalcFieldSize(0xFC, data, sizeof(data), 4), 9u);  // 4 + 5
}

TEST(CalcFieldSizeTest, BlobUnsupportedPack) {
  uint8_t data[8] = {};
  EXPECT_EQ(CalcFieldSize(0xFC, data, sizeof(data), 5), 0u);
}

// --- CalcFieldSize for GEOMETRY with various pack lengths ---

TEST(CalcFieldSizeTest, GeometryPack1) {
  uint8_t data[] = {0x05, 1, 2, 3, 4, 5};
  EXPECT_EQ(CalcFieldSize(0xFF, data, sizeof(data), 1), 6u);  // 1 + 5
}

TEST(CalcFieldSizeTest, GeometryPack2) {
  uint8_t data[] = {0x05, 0x00, 1, 2, 3, 4, 5};
  EXPECT_EQ(CalcFieldSize(0xFF, data, sizeof(data), 2), 7u);  // 2 + 5
}

TEST(CalcFieldSizeTest, GeometryPack3) {
  uint8_t data[] = {0x05, 0x00, 0x00, 1, 2, 3, 4, 5};
  EXPECT_EQ(CalcFieldSize(0xFF, data, sizeof(data), 3), 8u);  // 3 + 5
}

TEST(CalcFieldSizeTest, GeometryUnsupportedPack) {
  uint8_t data[8] = {};
  EXPECT_EQ(CalcFieldSize(0xFF, data, sizeof(data), 5), 0u);
}

// --- DecodeDecimal with full 9-digit groups ---

TEST(DecodeDecimalTest, LargeWithFullGroups) {
  // DECIMAL(18,0): intg=18, intg0=2, intg_rem=0
  // total = 0 + 2*4 = 8 bytes
  uint8_t data[8];
  // First 4-byte group: 123456789
  uint32_t g1 = 123456789;
  data[0] = (g1 >> 24) & 0xFF;
  data[1] = (g1 >> 16) & 0xFF;
  data[2] = (g1 >> 8) & 0xFF;
  data[3] = g1 & 0xFF;
  // Second 4-byte group: 12345678
  uint32_t g2 = 12345678;
  data[4] = (g2 >> 24) & 0xFF;
  data[5] = (g2 >> 16) & 0xFF;
  data[6] = (g2 >> 8) & 0xFF;
  data[7] = g2 & 0xFF;
  // Positive: XOR first byte with 0x80
  data[0] ^= 0x80;
  size_t consumed = 0;
  auto result = DecodeDecimal(data, sizeof(data), 18, 0, consumed);
  EXPECT_EQ(consumed, 8u);
  EXPECT_EQ(result, "123456789012345678");
}

TEST(DecodeDecimalTest, WithFullFracGroup) {
  // DECIMAL(11,9): intg=2, intg_rem=2, intg0=0
  // frac0=1, frac_rem=0
  // total = dig2bytes[2] + 0 + 1*4 + 0 = 1 + 4 = 5
  uint8_t data[5];
  data[0] = 12;  // intg_rem: value 12
  // frac0 group: 123456789
  uint32_t f1 = 123456789;
  data[1] = (f1 >> 24) & 0xFF;
  data[2] = (f1 >> 16) & 0xFF;
  data[3] = (f1 >> 8) & 0xFF;
  data[4] = f1 & 0xFF;
  data[0] ^= 0x80;  // positive
  size_t consumed = 0;
  auto result = DecodeDecimal(data, sizeof(data), 11, 9, consumed);
  EXPECT_EQ(consumed, 5u);
  EXPECT_EQ(result, "12.123456789");
}

TEST(DecodeDecimalTest, NegativeWithFraction) {
  // DECIMAL(5,2) = "-1.50"
  // intg=3, intg_rem=3, intg0=0, frac0=0, frac_rem=2
  // dig2bytes[3]=2, dig2bytes[2]=1 => total 3
  // Positive: XOR first byte with 0x80 => 0x80, 0x01, 0x32
  // Negative: XOR all with 0xFF => 0x7F, 0xFE, 0xCD
  uint8_t data[] = {0x7F, 0xFE, 0xCD};
  size_t consumed = 0;
  auto result = DecodeDecimal(data, sizeof(data), 5, 2, consumed);
  EXPECT_EQ(result, "-1.50");
  EXPECT_EQ(consumed, 3u);
}

TEST(DecodeDecimalTest, ScaleOnly) {
  // DECIMAL(2,2) = "0.99"
  // intg=0, frac_rem=2
  // total = dig2bytes[2] = 1
  uint8_t data[] = {0x80 | 99};  // positive, value 99
  size_t consumed = 0;
  auto result = DecodeDecimal(data, sizeof(data), 2, 2, consumed);
  EXPECT_EQ(consumed, 1u);
  EXPECT_EQ(result, "0.99");
}

}  // namespace
}  // namespace mes::binary
