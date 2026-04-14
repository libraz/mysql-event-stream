// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#include <gtest/gtest.h>

#include <cstring>
#include <vector>

#include "binary_util.h"
#include "table_map.h"
#include "test_helpers.h"

namespace mes {
namespace {

// Helper to build a TABLE_MAP_EVENT body in a buffer.
class TableMapBuilder {
 public:
  // table_id: 6 bytes LE
  void WriteTableId(uint64_t table_id) {
    WriteByte(static_cast<uint8_t>(table_id));
    WriteByte(static_cast<uint8_t>(table_id >> 8));
    WriteByte(static_cast<uint8_t>(table_id >> 16));
    WriteByte(static_cast<uint8_t>(table_id >> 24));
    WriteByte(static_cast<uint8_t>(table_id >> 32));
    WriteByte(static_cast<uint8_t>(table_id >> 40));
  }

  // flags: 2 bytes LE
  void WriteFlags(uint16_t flags) {
    WriteByte(static_cast<uint8_t>(flags));
    WriteByte(static_cast<uint8_t>(flags >> 8));
  }

  // database_name: length byte + string + null terminator
  void WriteDatabaseName(const std::string& name) {
    WriteByte(static_cast<uint8_t>(name.size()));
    WriteString(name);
    WriteByte(0);  // null terminator
  }

  // table_name: length byte + string + null terminator
  void WriteTableName(const std::string& name) {
    WriteByte(static_cast<uint8_t>(name.size()));
    WriteString(name);
    WriteByte(0);  // null terminator
  }

  // column_count as packed integer (< 251 fits in 1 byte)
  void WriteColumnCount(uint64_t count) {
    if (count < 251) {
      WriteByte(static_cast<uint8_t>(count));
    }
  }

  // column types: 1 byte per column
  void WriteColumnTypes(const std::vector<uint8_t>& types) {
    for (auto t : types) {
      WriteByte(t);
    }
  }

  // metadata block: packed length + metadata bytes
  void WriteMetadataBlock(const std::vector<uint8_t>& meta_bytes) {
    // packed integer for length
    if (meta_bytes.size() < 251) {
      WriteByte(static_cast<uint8_t>(meta_bytes.size()));
    }
    for (auto b : meta_bytes) {
      WriteByte(b);
    }
  }

  // null bitmap
  void WriteNullBitmap(const std::vector<uint8_t>& bitmap) {
    for (auto b : bitmap) {
      WriteByte(b);
    }
  }

  const std::vector<uint8_t>& Data() const { return buf_; }
  size_t Size() const { return buf_.size(); }

 private:
  void WriteByte(uint8_t b) { buf_.push_back(b); }
  void WriteString(const std::string& s) { buf_.insert(buf_.end(), s.begin(), s.end()); }

  std::vector<uint8_t> buf_;
};

TEST(TableMapTest, ParseBasicIntColumns) {
  TableMapBuilder builder;
  builder.WriteTableId(42);
  builder.WriteFlags(0);
  builder.WriteDatabaseName("testdb");
  builder.WriteTableName("users");

  // 3 columns: INT, BIGINT, TINYINT
  std::vector<uint8_t> col_types = {
      static_cast<uint8_t>(ColumnType::kLong),      // INT
      static_cast<uint8_t>(ColumnType::kLongLong),  // BIGINT
      static_cast<uint8_t>(ColumnType::kTiny),      // TINYINT
  };
  builder.WriteColumnCount(3);
  builder.WriteColumnTypes(col_types);

  // No metadata for fixed types
  builder.WriteMetadataBlock({});

  // Null bitmap: all nullable (bits 0,1,2 set) = 0b00000111 = 0x07
  builder.WriteNullBitmap({0x07});

  TableMetadata metadata;
  ASSERT_TRUE(ParseTableMapEvent(builder.Data().data(), builder.Size(), &metadata));
  EXPECT_EQ(metadata.table_id, 42u);
  EXPECT_EQ(metadata.database_name, "testdb");
  EXPECT_EQ(metadata.table_name, "users");
  ASSERT_EQ(metadata.columns.size(), 3u);

  EXPECT_EQ(metadata.columns[0].type, ColumnType::kLong);
  EXPECT_EQ(metadata.columns[0].metadata, 0);
  EXPECT_TRUE(metadata.columns[0].is_nullable);

  EXPECT_EQ(metadata.columns[1].type, ColumnType::kLongLong);
  EXPECT_TRUE(metadata.columns[1].is_nullable);

  EXPECT_EQ(metadata.columns[2].type, ColumnType::kTiny);
  EXPECT_TRUE(metadata.columns[2].is_nullable);
}

TEST(TableMapTest, ParseMixedColumnTypes) {
  TableMapBuilder builder;
  builder.WriteTableId(100);
  builder.WriteFlags(0);
  builder.WriteDatabaseName("mydb");
  builder.WriteTableName("orders");

  // 5 columns: INT, VARCHAR(255), BLOB, DATETIME2(3), NEWDECIMAL(10,2)
  std::vector<uint8_t> col_types = {
      static_cast<uint8_t>(ColumnType::kLong),        // INT
      static_cast<uint8_t>(ColumnType::kVarchar),     // VARCHAR
      static_cast<uint8_t>(ColumnType::kBlob),        // BLOB
      static_cast<uint8_t>(ColumnType::kDatetime2),   // DATETIME2
      static_cast<uint8_t>(ColumnType::kNewDecimal),  // NEWDECIMAL
  };
  builder.WriteColumnCount(5);
  builder.WriteColumnTypes(col_types);

  // Metadata block:
  // INT: 0 bytes
  // VARCHAR(255): 2 bytes LE -> 0xFF, 0x00
  // BLOB: 1 byte (pack_length) -> 2 (for regular BLOB)
  // DATETIME2: 1 byte (fsp) -> 3
  // NEWDECIMAL: 2 bytes (precision, scale) -> 10, 2
  std::vector<uint8_t> meta_bytes = {
      0xFF, 0x00,  // VARCHAR max_length=255
      0x02,        // BLOB pack_length=2
      0x03,        // DATETIME2 fsp=3
      0x0A, 0x02,  // NEWDECIMAL precision=10, scale=2
  };
  builder.WriteMetadataBlock(meta_bytes);

  // Null bitmap: column 0 not nullable, columns 1-4 nullable
  // bits: 0=0, 1=1, 2=1, 3=1, 4=1 -> 0b00011110 = 0x1E
  builder.WriteNullBitmap({0x1E});

  TableMetadata metadata;
  ASSERT_TRUE(ParseTableMapEvent(builder.Data().data(), builder.Size(), &metadata));
  EXPECT_EQ(metadata.table_id, 100u);
  EXPECT_EQ(metadata.database_name, "mydb");
  EXPECT_EQ(metadata.table_name, "orders");
  ASSERT_EQ(metadata.columns.size(), 5u);

  // INT: no metadata
  EXPECT_EQ(metadata.columns[0].type, ColumnType::kLong);
  EXPECT_EQ(metadata.columns[0].metadata, 0);
  EXPECT_FALSE(metadata.columns[0].is_nullable);

  // VARCHAR(255): metadata = 255
  EXPECT_EQ(metadata.columns[1].type, ColumnType::kVarchar);
  EXPECT_EQ(metadata.columns[1].metadata, 255);
  EXPECT_TRUE(metadata.columns[1].is_nullable);

  // BLOB: metadata = 2
  EXPECT_EQ(metadata.columns[2].type, ColumnType::kBlob);
  EXPECT_EQ(metadata.columns[2].metadata, 2);
  EXPECT_TRUE(metadata.columns[2].is_nullable);

  // DATETIME2: metadata = 3
  EXPECT_EQ(metadata.columns[3].type, ColumnType::kDatetime2);
  EXPECT_EQ(metadata.columns[3].metadata, 3);
  EXPECT_TRUE(metadata.columns[3].is_nullable);

  // NEWDECIMAL(10,2): metadata = (10 << 8) | 2 = 2562
  EXPECT_EQ(metadata.columns[4].type, ColumnType::kNewDecimal);
  EXPECT_EQ(metadata.columns[4].metadata, (10 << 8) | 2);
  EXPECT_TRUE(metadata.columns[4].is_nullable);
}

TEST(TableMapTest, ParseTruncatedBuffer) {
  // Only 5 bytes - way too short
  uint8_t buf[5] = {0};
  TableMetadata metadata;
  EXPECT_FALSE(ParseTableMapEvent(buf, sizeof(buf), &metadata));
}

TEST(TableMapTest, ParseNullData) {
  TableMetadata metadata;
  EXPECT_FALSE(ParseTableMapEvent(nullptr, 100, &metadata));
}

TEST(TableMapTest, ParseNullOutput) {
  uint8_t buf[32] = {0};
  EXPECT_FALSE(ParseTableMapEvent(buf, sizeof(buf), nullptr));
}

TEST(TableMapRegistryTest, ProcessAndLookup) {
  TableMapBuilder builder;
  builder.WriteTableId(1);
  builder.WriteFlags(0);
  builder.WriteDatabaseName("db1");
  builder.WriteTableName("t1");
  builder.WriteColumnCount(1);
  builder.WriteColumnTypes({static_cast<uint8_t>(ColumnType::kLong)});
  builder.WriteMetadataBlock({});
  builder.WriteNullBitmap({0x01});

  TableMapRegistry registry;
  EXPECT_EQ(registry.Size(), 0u);

  ASSERT_TRUE(registry.ProcessTableMapEvent(builder.Data().data(), builder.Size()));
  EXPECT_EQ(registry.Size(), 1u);

  const TableMetadata* meta = registry.Lookup(1);
  ASSERT_NE(meta, nullptr);
  EXPECT_EQ(meta->table_id, 1u);
  EXPECT_EQ(meta->database_name, "db1");
  EXPECT_EQ(meta->table_name, "t1");
  ASSERT_EQ(meta->columns.size(), 1u);
  EXPECT_EQ(meta->columns[0].type, ColumnType::kLong);
}

TEST(TableMapRegistryTest, LookupMissing) {
  TableMapRegistry registry;
  EXPECT_EQ(registry.Lookup(999), nullptr);
}

TEST(TableMapRegistryTest, MultipleTables) {
  TableMapRegistry registry;

  // Table 1
  {
    TableMapBuilder builder;
    builder.WriteTableId(10);
    builder.WriteFlags(0);
    builder.WriteDatabaseName("db");
    builder.WriteTableName("table_a");
    builder.WriteColumnCount(1);
    builder.WriteColumnTypes({static_cast<uint8_t>(ColumnType::kLong)});
    builder.WriteMetadataBlock({});
    builder.WriteNullBitmap({0x01});
    ASSERT_TRUE(registry.ProcessTableMapEvent(builder.Data().data(), builder.Size()));
  }

  // Table 2
  {
    TableMapBuilder builder;
    builder.WriteTableId(20);
    builder.WriteFlags(0);
    builder.WriteDatabaseName("db");
    builder.WriteTableName("table_b");
    builder.WriteColumnCount(2);
    builder.WriteColumnTypes(
        {static_cast<uint8_t>(ColumnType::kTiny), static_cast<uint8_t>(ColumnType::kShort)});
    builder.WriteMetadataBlock({});
    builder.WriteNullBitmap({0x03});
    ASSERT_TRUE(registry.ProcessTableMapEvent(builder.Data().data(), builder.Size()));
  }

  EXPECT_EQ(registry.Size(), 2u);
  ASSERT_NE(registry.Lookup(10), nullptr);
  EXPECT_EQ(registry.Lookup(10)->table_name, "table_a");
  ASSERT_NE(registry.Lookup(20), nullptr);
  EXPECT_EQ(registry.Lookup(20)->table_name, "table_b");
}

TEST(TableMapRegistryTest, ReplaceExistingTable) {
  TableMapRegistry registry;

  // First registration
  {
    TableMapBuilder builder;
    builder.WriteTableId(5);
    builder.WriteFlags(0);
    builder.WriteDatabaseName("db");
    builder.WriteTableName("old_name");
    builder.WriteColumnCount(1);
    builder.WriteColumnTypes({static_cast<uint8_t>(ColumnType::kLong)});
    builder.WriteMetadataBlock({});
    builder.WriteNullBitmap({0x01});
    ASSERT_TRUE(registry.ProcessTableMapEvent(builder.Data().data(), builder.Size()));
  }

  EXPECT_EQ(registry.Lookup(5)->table_name, "old_name");

  // Replace with new metadata for same table_id
  {
    TableMapBuilder builder;
    builder.WriteTableId(5);
    builder.WriteFlags(0);
    builder.WriteDatabaseName("db");
    builder.WriteTableName("new_name");
    builder.WriteColumnCount(2);
    builder.WriteColumnTypes(
        {static_cast<uint8_t>(ColumnType::kLong), static_cast<uint8_t>(ColumnType::kTiny)});
    builder.WriteMetadataBlock({});
    builder.WriteNullBitmap({0x03});
    ASSERT_TRUE(registry.ProcessTableMapEvent(builder.Data().data(), builder.Size()));
  }

  EXPECT_EQ(registry.Size(), 1u);
  ASSERT_NE(registry.Lookup(5), nullptr);
  EXPECT_EQ(registry.Lookup(5)->table_name, "new_name");
  EXPECT_EQ(registry.Lookup(5)->columns.size(), 2u);
}

TEST(TableMapRegistryTest, Clear) {
  TableMapRegistry registry;

  TableMapBuilder builder;
  builder.WriteTableId(1);
  builder.WriteFlags(0);
  builder.WriteDatabaseName("db");
  builder.WriteTableName("t");
  builder.WriteColumnCount(1);
  builder.WriteColumnTypes({static_cast<uint8_t>(ColumnType::kLong)});
  builder.WriteMetadataBlock({});
  builder.WriteNullBitmap({0x01});
  ASSERT_TRUE(registry.ProcessTableMapEvent(builder.Data().data(), builder.Size()));

  EXPECT_EQ(registry.Size(), 1u);
  registry.Clear();
  EXPECT_EQ(registry.Size(), 0u);
  EXPECT_EQ(registry.Lookup(1), nullptr);
}

TEST(TableMapRegistryTest, ProcessMalformedData) {
  TableMapRegistry registry;
  uint8_t buf[5] = {0};
  EXPECT_FALSE(registry.ProcessTableMapEvent(buf, sizeof(buf)));
  EXPECT_EQ(registry.Size(), 0u);
}

// --- Additional column type parsing tests ---

TEST(ParseTableMapTest, VarcharColumn) {
  test::EventBuilder b;
  b.WriteU48Le(42);  // table_id
  b.WriteU16Le(0);   // flags
  b.WriteU8(4);      // db name len
  b.WriteString("test");
  b.WriteU8(0);  // null term
  b.WriteU8(1);  // table name len
  b.WriteString("t");
  b.WriteU8(0);       // null term
  b.WriteU8(1);       // column count
  b.WriteU8(0x0F);    // VARCHAR
  b.WriteU8(2);       // metadata length
  b.WriteU16Le(200);  // varchar max_length
  b.WriteU8(0x01);    // null bitmap
  TableMetadata metadata;
  ASSERT_TRUE(ParseTableMapEvent(b.Data().data(), b.Size(), &metadata));
  EXPECT_EQ(metadata.columns[0].type, ColumnType::kVarchar);
  EXPECT_EQ(metadata.columns[0].metadata, 200);
}

TEST(ParseTableMapTest, BlobColumn) {
  test::EventBuilder b;
  b.WriteU48Le(42);
  b.WriteU16Le(0);
  b.WriteU8(4);
  b.WriteString("test");
  b.WriteU8(0);
  b.WriteU8(1);
  b.WriteString("t");
  b.WriteU8(0);
  b.WriteU8(1);     // column count
  b.WriteU8(0xFC);  // BLOB
  b.WriteU8(1);     // metadata length
  b.WriteU8(2);     // pack_length = 2
  b.WriteU8(0x01);  // null bitmap
  TableMetadata metadata;
  ASSERT_TRUE(ParseTableMapEvent(b.Data().data(), b.Size(), &metadata));
  EXPECT_EQ(metadata.columns[0].type, ColumnType::kBlob);
  EXPECT_EQ(metadata.columns[0].metadata, 2);
}

TEST(ParseTableMapTest, Datetime2Column) {
  test::EventBuilder b;
  b.WriteU48Le(42);
  b.WriteU16Le(0);
  b.WriteU8(4);
  b.WriteString("test");
  b.WriteU8(0);
  b.WriteU8(1);
  b.WriteString("t");
  b.WriteU8(0);
  b.WriteU8(1);     // column count
  b.WriteU8(0x12);  // DATETIME2
  b.WriteU8(1);     // metadata length
  b.WriteU8(6);     // fsp = 6
  b.WriteU8(0x01);  // null bitmap
  TableMetadata metadata;
  ASSERT_TRUE(ParseTableMapEvent(b.Data().data(), b.Size(), &metadata));
  EXPECT_EQ(metadata.columns[0].type, ColumnType::kDatetime2);
  EXPECT_EQ(metadata.columns[0].metadata, 6);
}

TEST(ParseTableMapTest, NewDecimalColumn) {
  test::EventBuilder b;
  b.WriteU48Le(42);
  b.WriteU16Le(0);
  b.WriteU8(4);
  b.WriteString("test");
  b.WriteU8(0);
  b.WriteU8(1);
  b.WriteString("t");
  b.WriteU8(0);
  b.WriteU8(1);
  b.WriteU8(0xF6);  // NEWDECIMAL
  b.WriteU8(2);     // metadata length
  b.WriteU8(10);    // precision
  b.WriteU8(2);     // scale
  b.WriteU8(0x01);
  TableMetadata metadata;
  ASSERT_TRUE(ParseTableMapEvent(b.Data().data(), b.Size(), &metadata));
  EXPECT_EQ(metadata.columns[0].type, ColumnType::kNewDecimal);
  EXPECT_EQ(metadata.columns[0].metadata, (10 << 8) | 2);
}

TEST(ParseTableMapTest, MultipleColumnTypes) {
  test::EventBuilder b;
  b.WriteU48Le(42);
  b.WriteU16Le(0);
  b.WriteU8(2);
  b.WriteString("db");
  b.WriteU8(0);
  b.WriteU8(1);
  b.WriteString("t");
  b.WriteU8(0);
  b.WriteU8(3);       // 3 columns
  b.WriteU8(0x03);    // INT
  b.WriteU8(0x0F);    // VARCHAR
  b.WriteU8(0x12);    // DATETIME2
  b.WriteU8(3);       // metadata length: 0 + 2 + 1
  b.WriteU16Le(500);  // VARCHAR metadata
  b.WriteU8(3);       // DATETIME2 fsp
  b.WriteU8(0x07);    // null bitmap: all nullable
  TableMetadata metadata;
  ASSERT_TRUE(ParseTableMapEvent(b.Data().data(), b.Size(), &metadata));
  ASSERT_EQ(metadata.columns.size(), 3u);
  EXPECT_EQ(metadata.columns[0].type, ColumnType::kLong);
  EXPECT_EQ(metadata.columns[1].type, ColumnType::kVarchar);
  EXPECT_EQ(metadata.columns[1].metadata, 500);
  EXPECT_EQ(metadata.columns[2].type, ColumnType::kDatetime2);
  EXPECT_EQ(metadata.columns[2].metadata, 3);
}

TEST(TableMapRegistryTest, ReplaceExisting) {
  TableMapRegistry registry;
  auto body1 = test::BuildTableMapBody(42, "db", "table1");
  auto body2 = test::BuildTableMapBody(42, "db", "table2");
  ASSERT_TRUE(registry.ProcessTableMapEvent(body1.data(), body1.size()));
  EXPECT_EQ(registry.Lookup(42)->table_name, "table1");
  ASSERT_TRUE(registry.ProcessTableMapEvent(body2.data(), body2.size()));
  EXPECT_EQ(registry.Lookup(42)->table_name, "table2");
  EXPECT_EQ(registry.Size(), 1u);
}

TEST(TableMapRegistryTest, MutableLookupNotFound) {
  TableMapRegistry registry;
  EXPECT_EQ(registry.MutableLookup(999), nullptr);
}

TEST(TableMapRegistryTest, MutableLookupFound) {
  TableMapRegistry registry;
  auto body = test::BuildTableMapBody(42, "db", "tbl");
  ASSERT_TRUE(registry.ProcessTableMapEvent(body.data(), body.size()));
  auto* meta = registry.MutableLookup(42);
  ASSERT_NE(meta, nullptr);
  EXPECT_EQ(meta->table_name, "tbl");
}

}  // namespace
}  // namespace mes
