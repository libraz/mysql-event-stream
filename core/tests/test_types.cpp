// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#include <gtest/gtest.h>

#include "types.h"

namespace mes {
namespace {

// --- ColumnValue factory methods ---

TEST(ColumnValueTest, DefaultName) {
  ColumnValue v;
  EXPECT_TRUE(v.name.empty());
}

TEST(ColumnValueTest, FactoryMethodsHaveEmptyName) {
  auto null_val = ColumnValue::Null(ColumnType::kLong);
  EXPECT_TRUE(null_val.name.empty());

  auto int_val = ColumnValue::Int(ColumnType::kLong, 42);
  EXPECT_TRUE(int_val.name.empty());

  auto float_val = ColumnValue::Float(1.5);
  EXPECT_TRUE(float_val.name.empty());

  auto double_val = ColumnValue::Double(2.5);
  EXPECT_TRUE(double_val.name.empty());

  auto str_val = ColumnValue::String(ColumnType::kVarchar, "test");
  EXPECT_TRUE(str_val.name.empty());

  auto bytes_val = ColumnValue::Bytes(ColumnType::kBlob, {0x01, 0x02});
  EXPECT_TRUE(bytes_val.name.empty());
}

TEST(ColumnValueTest, NullValue) {
  auto v = ColumnValue::Null(ColumnType::kLong);
  EXPECT_EQ(v.type, ColumnType::kLong);
  EXPECT_TRUE(v.is_null);
  EXPECT_EQ(v.int_val, 0);
}

TEST(ColumnValueTest, NullWithDifferentTypes) {
  auto v1 = ColumnValue::Null(ColumnType::kVarchar);
  EXPECT_EQ(v1.type, ColumnType::kVarchar);
  EXPECT_TRUE(v1.is_null);

  auto v2 = ColumnValue::Null(ColumnType::kBlob);
  EXPECT_EQ(v2.type, ColumnType::kBlob);
  EXPECT_TRUE(v2.is_null);
}

TEST(ColumnValueTest, IntValue) {
  auto v = ColumnValue::Int(ColumnType::kLong, 42);
  EXPECT_EQ(v.type, ColumnType::kLong);
  EXPECT_FALSE(v.is_null);
  EXPECT_EQ(v.int_val, 42);
}

TEST(ColumnValueTest, IntNegative) {
  auto v = ColumnValue::Int(ColumnType::kLongLong, -123456789LL);
  EXPECT_EQ(v.type, ColumnType::kLongLong);
  EXPECT_FALSE(v.is_null);
  EXPECT_EQ(v.int_val, -123456789LL);
}

TEST(ColumnValueTest, IntTiny) {
  auto v = ColumnValue::Int(ColumnType::kTiny, 127);
  EXPECT_EQ(v.type, ColumnType::kTiny);
  EXPECT_FALSE(v.is_null);
  EXPECT_EQ(v.int_val, 127);
}

TEST(ColumnValueTest, IntYear) {
  auto v = ColumnValue::Int(ColumnType::kYear, 2024);
  EXPECT_EQ(v.type, ColumnType::kYear);
  EXPECT_FALSE(v.is_null);
  EXPECT_EQ(v.int_val, 2024);
}

TEST(ColumnValueTest, FloatValue) {
  auto v = ColumnValue::Float(3.14);
  EXPECT_EQ(v.type, ColumnType::kFloat);
  EXPECT_FALSE(v.is_null);
  EXPECT_DOUBLE_EQ(v.real_val, 3.14);
}

TEST(ColumnValueTest, DoubleValue) {
  auto v = ColumnValue::Double(2.718281828);
  EXPECT_EQ(v.type, ColumnType::kDouble);
  EXPECT_FALSE(v.is_null);
  EXPECT_DOUBLE_EQ(v.real_val, 2.718281828);
}

TEST(ColumnValueTest, StringValue) {
  auto v = ColumnValue::String(ColumnType::kVarchar, "hello world");
  EXPECT_EQ(v.type, ColumnType::kVarchar);
  EXPECT_FALSE(v.is_null);
  EXPECT_EQ(v.string_val, "hello world");
}

TEST(ColumnValueTest, StringJson) {
  auto v = ColumnValue::String(ColumnType::kJson, R"({"key":"value"})");
  EXPECT_EQ(v.type, ColumnType::kJson);
  EXPECT_FALSE(v.is_null);
  EXPECT_EQ(v.string_val, R"({"key":"value"})");
}

TEST(ColumnValueTest, StringDecimal) {
  auto v = ColumnValue::String(ColumnType::kNewDecimal, "123.456");
  EXPECT_EQ(v.type, ColumnType::kNewDecimal);
  EXPECT_FALSE(v.is_null);
  EXPECT_EQ(v.string_val, "123.456");
}

TEST(ColumnValueTest, BytesValue) {
  std::vector<uint8_t> data = {0x01, 0x02, 0x03, 0xFF};
  auto v = ColumnValue::Bytes(ColumnType::kBlob, data);
  EXPECT_EQ(v.type, ColumnType::kBlob);
  EXPECT_FALSE(v.is_null);
  EXPECT_EQ(v.bytes_val, data);
}

TEST(ColumnValueTest, BytesMoveSemantics) {
  std::vector<uint8_t> data = {0xDE, 0xAD, 0xBE, 0xEF};
  auto v = ColumnValue::Bytes(ColumnType::kGeometry, std::move(data));
  EXPECT_EQ(v.type, ColumnType::kGeometry);
  EXPECT_FALSE(v.is_null);
  EXPECT_EQ(v.bytes_val.size(), 4u);
  EXPECT_EQ(v.bytes_val[0], 0xDE);
}

// --- RowData ---

TEST(RowDataTest, Construction) {
  RowData row;
  EXPECT_TRUE(row.columns.empty());
}

TEST(RowDataTest, WithColumns) {
  RowData row;
  row.columns.push_back(ColumnValue::Int(ColumnType::kLong, 1));
  row.columns.push_back(ColumnValue::String(ColumnType::kVarchar, "test"));
  row.columns.push_back(ColumnValue::Null(ColumnType::kDouble));
  EXPECT_EQ(row.columns.size(), 3u);
  EXPECT_EQ(row.columns[0].int_val, 1);
  EXPECT_EQ(row.columns[1].string_val, "test");
  EXPECT_TRUE(row.columns[2].is_null);
}

// --- BinlogPosition ---

TEST(BinlogPositionTest, DefaultValues) {
  BinlogPosition pos;
  EXPECT_TRUE(pos.binlog_file.empty());
  EXPECT_EQ(pos.offset, 0u);
}

TEST(BinlogPositionTest, SetValues) {
  BinlogPosition pos;
  pos.binlog_file = "binlog.000001";
  pos.offset = 12345;
  EXPECT_EQ(pos.binlog_file, "binlog.000001");
  EXPECT_EQ(pos.offset, 12345u);
}

// --- ColumnMetadata ---

TEST(ColumnMetadataTest, DefaultValues) {
  ColumnMetadata meta;
  EXPECT_EQ(meta.type, ColumnType::kLong);
  EXPECT_TRUE(meta.name.empty());
  EXPECT_EQ(meta.metadata, 0);
  EXPECT_TRUE(meta.is_nullable);
  EXPECT_FALSE(meta.is_unsigned);
}

// --- TableMetadata ---

TEST(TableMetadataTest, Construction) {
  TableMetadata table;
  table.table_id = 42;
  table.database_name = "mydb";
  table.table_name = "users";

  ColumnMetadata col;
  col.type = ColumnType::kLong;
  col.name = "id";
  col.is_unsigned = true;
  col.is_nullable = false;
  table.columns.push_back(col);

  col.type = ColumnType::kVarchar;
  col.name = "name";
  col.is_unsigned = false;
  col.is_nullable = true;
  col.metadata = 255;
  table.columns.push_back(col);

  EXPECT_EQ(table.table_id, 42u);
  EXPECT_EQ(table.database_name, "mydb");
  EXPECT_EQ(table.table_name, "users");
  EXPECT_EQ(table.columns.size(), 2u);
  EXPECT_EQ(table.columns[0].name, "id");
  EXPECT_EQ(table.columns[1].name, "name");
}

// --- ChangeEvent ---

TEST(ChangeEventTest, InsertEvent) {
  ChangeEvent event;
  event.type = EventType::kInsert;
  event.database = "mydb";
  event.table = "users";
  event.timestamp = 1700000000;
  event.position.binlog_file = "binlog.000001";
  event.position.offset = 100;

  event.after.columns.push_back(ColumnValue::Int(ColumnType::kLong, 1));
  event.after.columns.push_back(ColumnValue::String(ColumnType::kVarchar, "alice"));

  EXPECT_EQ(event.type, EventType::kInsert);
  EXPECT_EQ(event.database, "mydb");
  EXPECT_EQ(event.table, "users");
  EXPECT_TRUE(event.before.columns.empty());
  EXPECT_EQ(event.after.columns.size(), 2u);
  EXPECT_EQ(event.timestamp, 1700000000u);
}

TEST(ChangeEventTest, UpdateEvent) {
  ChangeEvent event;
  event.type = EventType::kUpdate;
  event.database = "mydb";
  event.table = "users";

  event.before.columns.push_back(ColumnValue::String(ColumnType::kVarchar, "old_name"));
  event.after.columns.push_back(ColumnValue::String(ColumnType::kVarchar, "new_name"));

  EXPECT_EQ(event.type, EventType::kUpdate);
  EXPECT_EQ(event.before.columns.size(), 1u);
  EXPECT_EQ(event.after.columns.size(), 1u);
  EXPECT_EQ(event.before.columns[0].string_val, "old_name");
  EXPECT_EQ(event.after.columns[0].string_val, "new_name");
}

TEST(ChangeEventTest, DeleteEvent) {
  ChangeEvent event;
  event.type = EventType::kDelete;
  event.database = "mydb";
  event.table = "users";

  event.before.columns.push_back(ColumnValue::Int(ColumnType::kLong, 42));

  EXPECT_EQ(event.type, EventType::kDelete);
  EXPECT_EQ(event.before.columns.size(), 1u);
  EXPECT_TRUE(event.after.columns.empty());
}

// --- ColumnType enum values ---

TEST(ColumnTypeTest, EnumValues) {
  EXPECT_EQ(static_cast<uint8_t>(ColumnType::kTiny), 0x01);
  EXPECT_EQ(static_cast<uint8_t>(ColumnType::kShort), 0x02);
  EXPECT_EQ(static_cast<uint8_t>(ColumnType::kLong), 0x03);
  EXPECT_EQ(static_cast<uint8_t>(ColumnType::kFloat), 0x04);
  EXPECT_EQ(static_cast<uint8_t>(ColumnType::kDouble), 0x05);
  EXPECT_EQ(static_cast<uint8_t>(ColumnType::kTimestamp), 0x07);
  EXPECT_EQ(static_cast<uint8_t>(ColumnType::kLongLong), 0x08);
  EXPECT_EQ(static_cast<uint8_t>(ColumnType::kInt24), 0x09);
  EXPECT_EQ(static_cast<uint8_t>(ColumnType::kJson), 0xF5);
  EXPECT_EQ(static_cast<uint8_t>(ColumnType::kNewDecimal), 0xF6);
  EXPECT_EQ(static_cast<uint8_t>(ColumnType::kBlob), 0xFC);
  EXPECT_EQ(static_cast<uint8_t>(ColumnType::kString), 0xFE);
  EXPECT_EQ(static_cast<uint8_t>(ColumnType::kGeometry), 0xFF);
}

// --- EventType enum values ---

TEST(EventTypeTest, EnumValues) {
  EXPECT_EQ(static_cast<uint8_t>(EventType::kInsert), 0);
  EXPECT_EQ(static_cast<uint8_t>(EventType::kUpdate), 1);
  EXPECT_EQ(static_cast<uint8_t>(EventType::kDelete), 2);
}

}  // namespace
}  // namespace mes
