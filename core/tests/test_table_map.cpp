// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "mybinlog/server_flavor.h"
#include "table_map.h"
#include "test_helpers.h"

namespace mes {
namespace {

constexpr mybinlog::ServerFlavor kMySQL = mybinlog::ServerFlavor::kMySQL;

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

  // Append raw bytes (e.g. an optional metadata block after the null bitmap).
  void WriteRawBytes(const std::vector<uint8_t>& bytes) {
    for (auto b : bytes) {
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

TEST(TableMapRegistryTest, ReusesUnchangedRawTableMapBody) {
  TableMapBuilder builder;
  builder.WriteTableId(84);
  builder.WriteFlags(0);
  builder.WriteDatabaseName("db");
  builder.WriteTableName("t");
  builder.WriteColumnCount(1);
  builder.WriteColumnTypes({static_cast<uint8_t>(ColumnType::kLong)});
  builder.WriteMetadataBlock({});
  builder.WriteNullBitmap({0x01});

  TableMapRegistry registry;
  bool unchanged = true;
  ASSERT_TRUE(registry.ProcessTableMapEvent(builder.Data().data(), builder.Size(), kMySQL, nullptr,
                                            &unchanged));
  EXPECT_FALSE(unchanged);
  ASSERT_TRUE(registry.ProcessTableMapEvent(builder.Data().data(), builder.Size(), kMySQL, nullptr,
                                            &unchanged));
  EXPECT_TRUE(unchanged);
  ASSERT_NE(registry.Lookup(84), nullptr);
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

  ASSERT_TRUE(registry.ProcessTableMapEvent(builder.Data().data(), builder.Size(), kMySQL));
  EXPECT_EQ(registry.Size(), 1u);

  const TableMetadata* meta = registry.Lookup(1);
  ASSERT_NE(meta, nullptr);
  EXPECT_EQ(meta->table_id, 1u);
  EXPECT_EQ(meta->schema, "db1");
  EXPECT_EQ(meta->table, "t1");
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
    ASSERT_TRUE(registry.ProcessTableMapEvent(builder.Data().data(), builder.Size(), kMySQL));
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
    ASSERT_TRUE(registry.ProcessTableMapEvent(builder.Data().data(), builder.Size(), kMySQL));
  }

  EXPECT_EQ(registry.Size(), 2u);
  ASSERT_NE(registry.Lookup(10), nullptr);
  EXPECT_EQ(registry.Lookup(10)->table, "table_a");
  ASSERT_NE(registry.Lookup(20), nullptr);
  EXPECT_EQ(registry.Lookup(20)->table, "table_b");
}

TEST(TableMapRegistryTest, CapacityEvictsLeastRecentlyUsedEntry) {
  TableMapRegistry registry;
  const auto add_table = [&registry](uint64_t id) {
    TableMapBuilder builder;
    builder.WriteTableId(id);
    builder.WriteFlags(0);
    builder.WriteDatabaseName("db");
    builder.WriteTableName("t");
    builder.WriteColumnCount(1);
    builder.WriteColumnTypes({static_cast<uint8_t>(ColumnType::kLong)});
    builder.WriteMetadataBlock({});
    builder.WriteNullBitmap({0x01});
    return registry.ProcessTableMapEvent(builder.Data().data(), builder.Size(), kMySQL);
  };

  for (uint64_t id = 1; id <= TableMapRegistry::kMaxEntries; ++id) {
    ASSERT_TRUE(add_table(id));
  }
  ASSERT_NE(registry.Lookup(1), nullptr);  // Refresh the oldest entry.

  std::vector<uint64_t> evicted;
  ASSERT_TRUE(add_table(TableMapRegistry::kMaxEntries + 1));
  // The optional eviction out-param is used by CdcEngine to bound its filter cache.
  TableMapBuilder replacement;
  replacement.WriteTableId(TableMapRegistry::kMaxEntries + 2);
  replacement.WriteFlags(0);
  replacement.WriteDatabaseName("db");
  replacement.WriteTableName("t");
  replacement.WriteColumnCount(1);
  replacement.WriteColumnTypes({static_cast<uint8_t>(ColumnType::kLong)});
  replacement.WriteMetadataBlock({});
  replacement.WriteNullBitmap({0x01});
  ASSERT_TRUE(registry.ProcessTableMapEvent(replacement.Data().data(), replacement.Size(), kMySQL,
                                            &evicted));

  EXPECT_EQ(evicted, std::vector<uint64_t>{3u});
  EXPECT_NE(registry.Lookup(1), nullptr);
  EXPECT_EQ(registry.Lookup(2), nullptr);
  EXPECT_EQ(registry.Size(), TableMapRegistry::kMaxEntries);
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
    ASSERT_TRUE(registry.ProcessTableMapEvent(builder.Data().data(), builder.Size(), kMySQL));
  }

  EXPECT_EQ(registry.Lookup(5)->table, "old_name");

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
    ASSERT_TRUE(registry.ProcessTableMapEvent(builder.Data().data(), builder.Size(), kMySQL));
  }

  EXPECT_EQ(registry.Size(), 1u);
  ASSERT_NE(registry.Lookup(5), nullptr);
  EXPECT_EQ(registry.Lookup(5)->table, "new_name");
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
  ASSERT_TRUE(registry.ProcessTableMapEvent(builder.Data().data(), builder.Size(), kMySQL));

  EXPECT_EQ(registry.Size(), 1u);
  registry.Clear();
  EXPECT_EQ(registry.Size(), 0u);
  EXPECT_EQ(registry.Lookup(1), nullptr);
}

TEST(TableMapRegistryTest, ProcessMalformedData) {
  TableMapRegistry registry;
  uint8_t buf[5] = {0};
  EXPECT_FALSE(registry.ProcessTableMapEvent(buf, sizeof(buf), kMySQL));
  EXPECT_EQ(registry.Size(), 0u);
}

TEST(TableMapRegistryTest, ReplaceExisting) {
  TableMapRegistry registry;
  auto body1 = test::BuildTableMapBody(42, "db", "table1");
  auto body2 = test::BuildTableMapBody(42, "db", "table2");
  ASSERT_TRUE(registry.ProcessTableMapEvent(body1.data(), body1.size(), kMySQL));
  EXPECT_EQ(registry.Lookup(42)->table, "table1");
  ASSERT_TRUE(registry.ProcessTableMapEvent(body2.data(), body2.size(), kMySQL));
  EXPECT_EQ(registry.Lookup(42)->table, "table2");
  EXPECT_EQ(registry.Size(), 1u);
}

TEST(TableMapRegistryTest, ReplaceMetadataRejectsAnUnregisteredTableId) {
  TableMapRegistry registry;
  EXPECT_FALSE(registry.ReplaceMetadata(999, TableMetadata{}));
  EXPECT_EQ(registry.Size(), 0u);
}

// A registration handed out through SharedLookup() outlives the entry from a
// queued event's point of view, so installing a resolution that carries longer
// column names must leave the shared object's own bytes alone. Longer names are
// what makes the distinction observable: the replacement name here exceeds the
// inline capacity of std::string while the registered one fits inside it, so an
// in-place write is forced to reallocate and would leave the borrowed view
// pointing at freed storage.
TEST(TableMapRegistryTest, ReplacedMetadataLeavesAnAlreadySharedRegistrationUntouched) {
  TableMapRegistry registry;
  auto body = test::BuildTableMapBody(42, "db", "tbl");
  // COLUMN_NAME optional metadata: one length-encoded name, "id".
  body.insert(body.end(), {0x04, 0x03, 0x02, 'i', 'd'});
  ASSERT_TRUE(registry.ProcessTableMapEvent(body.data(), body.size(), kMySQL));

  const auto shared = registry.SharedLookup(42);
  ASSERT_TRUE(shared);
  ASSERT_EQ(shared->columns.size(), 1u);
  ASSERT_EQ(shared->columns[0].name, "id");
  const std::string_view borrowed(shared->columns[0].name);
  const char* borrowed_data = borrowed.data();

  static constexpr const char* kResolvedName = "a_column_name_longer_than_the_inline_buffer";
  TableMetadata resolved = *shared;
  resolved.columns[0].name = kResolvedName;
  resolved.names_resolved = true;
  ASSERT_TRUE(registry.ReplaceMetadata(42, std::move(resolved)));

  // What the earlier registration handed out is unchanged, bytes and address.
  EXPECT_EQ(shared->columns[0].name, "id");
  EXPECT_EQ(shared->columns[0].name.data(), borrowed_data);
  EXPECT_EQ(borrowed, "id");
  EXPECT_FALSE(shared->names_resolved);
  // Lookups from here on see the resolution, on a different object.
  const TableMetadata* current = registry.Lookup(42);
  ASSERT_NE(current, nullptr);
  EXPECT_NE(current, shared.get());
  EXPECT_EQ(current->columns[0].name, kResolvedName);
  EXPECT_TRUE(current->names_resolved);
}

TEST(TableMapRegistryTest, ReplacedMetadataKeepsTheEntrysRawBody) {
  // The replacement describes the body the entry was registered from, so that
  // body must still be recognized as unchanged afterwards -- otherwise the next
  // occurrence of the same TABLE_MAP would re-parse and discard the resolution.
  TableMapRegistry registry;
  auto body = test::BuildTableMapBody(42, "db", "tbl");
  bool unchanged = true;
  ASSERT_TRUE(registry.ProcessTableMapEvent(body.data(), body.size(), kMySQL, nullptr, &unchanged));
  ASSERT_FALSE(unchanged);

  TableMetadata resolved = *registry.Lookup(42);
  resolved.names_resolved = true;
  ASSERT_TRUE(registry.ReplaceMetadata(42, std::move(resolved)));

  ASSERT_TRUE(registry.ProcessTableMapEvent(body.data(), body.size(), kMySQL, nullptr, &unchanged));
  EXPECT_TRUE(unchanged);
  ASSERT_NE(registry.Lookup(42), nullptr);
  EXPECT_TRUE(registry.Lookup(42)->names_resolved);
  EXPECT_EQ(registry.Size(), 1u);
}

TEST(TableMapRegistryTest, ReplacedMetadataKeepsTheEntrysRecency) {
  // Installing a replacement reaches the entry, so it counts as a use: the
  // entry must not remain the eviction candidate it was before.
  TableMapRegistry registry;
  const auto add_table = [&registry](uint64_t id, std::vector<uint64_t>* evicted) {
    TableMapBuilder builder;
    builder.WriteTableId(id);
    builder.WriteFlags(0);
    builder.WriteDatabaseName("db");
    builder.WriteTableName("t");
    builder.WriteColumnCount(1);
    builder.WriteColumnTypes({static_cast<uint8_t>(ColumnType::kLong)});
    builder.WriteMetadataBlock({});
    builder.WriteNullBitmap({0x01});
    return registry.ProcessTableMapEvent(builder.Data().data(), builder.Size(), kMySQL, evicted);
  };

  for (uint64_t id = 1; id <= TableMapRegistry::kMaxEntries; ++id) {
    ASSERT_TRUE(add_table(id, nullptr));
  }
  // Built from scratch rather than copied out of the registry, so the
  // replacement is the only thing that reaches the entry.
  TableMetadata resolved;
  resolved.table_id = 1;
  resolved.schema = "db";
  resolved.table = "t";
  resolved.columns.resize(1);
  resolved.names_resolved = true;
  ASSERT_TRUE(registry.ReplaceMetadata(1, std::move(resolved)));

  std::vector<uint64_t> evicted;
  ASSERT_TRUE(add_table(TableMapRegistry::kMaxEntries + 1, &evicted));
  EXPECT_EQ(evicted, std::vector<uint64_t>{2u});
  ASSERT_NE(registry.Lookup(1), nullptr);
  EXPECT_TRUE(registry.Lookup(1)->names_resolved);
}

// Build a TABLE_MAP body for a table of @p columns INT columns, each named with
// @p name_length characters. Identifier length on a wide table is what makes
// one registry entry retain orders of magnitude more than another, and column
// names reach the registry only through the COLUMN_NAME optional metadata that
// binlog_row_metadata=FULL emits. @p name_length must stay under 251 so each
// name's length prefix is a single byte.
std::vector<uint8_t> BuildWideTableMapBody(uint64_t table_id, size_t columns, size_t name_length) {
  TableMapBuilder builder;
  builder.WriteTableId(table_id);
  builder.WriteFlags(0);
  builder.WriteDatabaseName("db");
  builder.WriteTableName("t");
  builder.WriteColumnCount(columns);
  builder.WriteColumnTypes(std::vector<uint8_t>(columns, static_cast<uint8_t>(ColumnType::kLong)));
  builder.WriteMetadataBlock({});
  builder.WriteNullBitmap(std::vector<uint8_t>((columns + 7) / 8, 0xFF));

  const std::string name(name_length, 'c');
  std::vector<uint8_t> names;
  for (size_t i = 0; i < columns; i++) {
    names.push_back(static_cast<uint8_t>(name_length));
    names.insert(names.end(), name.begin(), name.end());
  }
  std::vector<uint8_t> field = {0x04};  // COLUMN_NAME
  if (names.size() < 251) {
    field.push_back(static_cast<uint8_t>(names.size()));
  } else {
    field.push_back(0xFC);
    field.push_back(static_cast<uint8_t>(names.size()));
    field.push_back(static_cast<uint8_t>(names.size() >> 8));
  }
  field.insert(field.end(), names.begin(), names.end());
  builder.WriteRawBytes(field);
  return builder.Data();
}

// Registrations of the wide schema above, enough that their retained bytes
// exceed kMaxRetainedBytes several times over while the count stays far below
// kMaxEntries.
constexpr uint64_t kWideRegistrations = 400;
constexpr size_t kWideColumns = 250;
constexpr size_t kWideNameLength = 200;

TEST(TableMapRegistryTest, ByteBudgetEvictsLongBeforeTheEntryCountDoes) {
  TableMapRegistry registry;
  std::vector<uint64_t> evicted;
  for (uint64_t id = 1; id <= kWideRegistrations; ++id) {
    const auto body = BuildWideTableMapBody(id, kWideColumns, kWideNameLength);
    ASSERT_TRUE(registry.ProcessTableMapEvent(body.data(), body.size(), kMySQL, &evicted));
  }

  // Per-entry cost here is set by the server's schema, not by anything the
  // client chose, so the entry count never reached its own bound.
  EXPECT_LT(registry.Size(), kWideRegistrations);
  EXPECT_LT(registry.Size(), TableMapRegistry::kMaxEntries);
  EXPECT_LE(registry.RetainedBytes(), TableMapRegistry::kMaxRetainedBytes);
  // Eviction follows the same LRU order the count bound uses.
  ASSERT_FALSE(evicted.empty());
  EXPECT_EQ(evicted.front(), 1u);
  // The newest registration is what the ROWS event that follows decodes
  // against, so it survives however much the registry had to drop.
  EXPECT_NE(registry.Lookup(kWideRegistrations), nullptr);
}

TEST(TableMapRegistryTest, RetainedBytesCountsColumnNames) {
  TableMapRegistry narrow_registry;
  const auto narrow = BuildWideTableMapBody(1, 4, 4);
  ASSERT_TRUE(narrow_registry.ProcessTableMapEvent(narrow.data(), narrow.size(), kMySQL));
  EXPECT_GT(narrow_registry.RetainedBytes(), 0u);

  TableMapRegistry wide_registry;
  const auto wide = BuildWideTableMapBody(1, 4, kWideNameLength);
  ASSERT_TRUE(wide_registry.ProcessTableMapEvent(wide.data(), wide.size(), kMySQL));

  // The two schemas differ in nothing but identifier length, so the whole
  // difference is the column names: counted once in the parsed metadata and
  // once in the raw body they were parsed from.
  EXPECT_GE(wide_registry.RetainedBytes() - narrow_registry.RetainedBytes(),
            2u * 4u * (kWideNameLength - 4u));

  wide_registry.Clear();
  EXPECT_EQ(wide_registry.RetainedBytes(), 0u);
}

TEST(TableMapRegistryTest, EvictionLeavesHandedOutMetadataIntact) {
  TableMapRegistry registry;
  const auto body = BuildWideTableMapBody(1, 4, kWideNameLength);
  ASSERT_TRUE(registry.ProcessTableMapEvent(body.data(), body.size(), kMySQL));
  // What a queued ChangeEvent holds while it waits to be drained.
  std::shared_ptr<const TableMetadata> shared = registry.SharedLookup(1);
  ASSERT_NE(shared, nullptr);
  ASSERT_EQ(shared->columns.size(), 4u);
  const std::string expected_name = shared->columns[0].name;
  const char* borrowed = shared->columns[0].name.data();

  // Nothing below may touch table_id 1: a lookup would refresh its recency and
  // move it off the eviction end.
  std::vector<uint64_t> evicted;
  for (uint64_t id = 2; id <= kWideRegistrations; ++id) {
    const auto wide = BuildWideTableMapBody(id, kWideColumns, kWideNameLength);
    ASSERT_TRUE(registry.ProcessTableMapEvent(wide.data(), wide.size(), kMySQL, &evicted));
  }
  ASSERT_FALSE(evicted.empty());
  EXPECT_EQ(evicted.front(), 1u);
  EXPECT_EQ(registry.Lookup(1), nullptr);

  // Dropping the registry's own reference left the events reading exactly what
  // they were decoded against, bytes and address.
  EXPECT_EQ(shared->table, "t");
  EXPECT_EQ(shared->columns.size(), 4u);
  EXPECT_EQ(shared->columns[0].name, expected_name);
  EXPECT_EQ(shared->columns[0].name.data(), borrowed);
}

// The same TABLE_MAP bytes index their charset metadata differently on MariaDB
// (GEOMETRY takes a slot), so a body registered under one flavor is parsed again
// under the other instead of being reported unchanged.
TEST(TableMapRegistryTest, IdenticalBodyUnderAnotherFlavorIsParsedAgain) {
  TableMapRegistry registry;
  const auto body = test::BuildTableMapBody(42, "db", "tbl");
  bool unchanged = true;
  ASSERT_TRUE(registry.ProcessTableMapEvent(body.data(), body.size(), kMySQL, nullptr, &unchanged));
  EXPECT_FALSE(unchanged);
  ASSERT_TRUE(registry.ProcessTableMapEvent(body.data(), body.size(),
                                            mybinlog::ServerFlavor::kMariaDB, nullptr, &unchanged));
  EXPECT_FALSE(unchanged);
  ASSERT_TRUE(registry.ProcessTableMapEvent(body.data(), body.size(),
                                            mybinlog::ServerFlavor::kMariaDB, nullptr, &unchanged));
  EXPECT_TRUE(unchanged);
  EXPECT_EQ(registry.Size(), 1u);
}

}  // namespace
}  // namespace mes