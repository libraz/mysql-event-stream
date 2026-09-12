// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#include <gtest/gtest.h>

#include <cstddef>
#include <string>
#include <utility>
#include <vector>

#include "client/column_name_source.h"
#include "client/metadata_fetcher.h"

namespace mes {

/** @brief Stores cache entries through the fetcher's own accounting, which
 *  filling the cache by SHOW COLUMNS would otherwise need a server to reach. */
class MetadataFetcherTestAccess {
 public:
  static void Store(MetadataFetcher* fetcher, const std::string& database, const std::string& table,
                    std::vector<ColumnInfo> columns) {
    fetcher->StoreCacheEntry(database, table, std::move(columns));
  }

  static void StoreUnresolvable(MetadataFetcher* fetcher, const std::string& database,
                                const std::string& table, size_t expected_count) {
    fetcher->StoreNegativeEntry(database, table, expected_count);
  }
};

namespace {

// A table of @p columns columns, each named with @p name_length characters.
// Column count and identifier length are what make one cached table retain
// orders of magnitude more than another, and the client chooses neither.
std::vector<ColumnInfo> WideColumns(size_t columns, size_t name_length) {
  std::vector<ColumnInfo> infos;
  infos.reserve(columns);
  for (size_t i = 0; i < columns; i++) {
    ColumnInfo info;
    info.name.assign(name_length, 'c');
    infos.push_back(std::move(info));
  }
  return infos;
}

// Cached tables of the wide schema above, enough that their retained bytes
// exceed kMaxRetainedBytes several times over while the count stays far below
// kMaxCacheEntries.
constexpr size_t kWideTables = 400;
constexpr size_t kWideColumns = 250;
constexpr size_t kWideNameLength = 200;

TEST(MetadataFetcherCacheTest, ByteBudgetDropsTheCacheLongBeforeTheEntryCountDoes) {
  MetadataFetcher fetcher;
  for (size_t i = 0; i < kWideTables; i++) {
    MetadataFetcherTestAccess::Store(&fetcher, "db", "t" + std::to_string(i),
                                     WideColumns(kWideColumns, kWideNameLength));
  }

  // Per-entry cost here is set by the schema the server reports, not by
  // anything the client chose, so the entry count never came near its bound:
  // it is the byte budget that kept this cache from growing without limit.
  EXPECT_LT(fetcher.CacheEntryCount(), kWideTables);
  EXPECT_LT(fetcher.CacheEntryCount(), MetadataFetcher::kMaxCacheEntries);
  EXPECT_LE(fetcher.RetainedBytes(), MetadataFetcher::kMaxRetainedBytes);
  // Overflow drops the whole cache rather than evicting an entry, leaving only
  // what was stored after the most recent drop.
  EXPECT_GE(fetcher.CacheEntryCount(), 1u);
}

TEST(MetadataFetcherCacheTest, RetainedBytesCountsColumnNames) {
  MetadataFetcher narrow;
  MetadataFetcherTestAccess::Store(&narrow, "db", "t", WideColumns(4, 4));
  EXPECT_GT(narrow.RetainedBytes(), 0u);

  MetadataFetcher wide;
  MetadataFetcherTestAccess::Store(&wide, "db", "t", WideColumns(4, kWideNameLength));

  // The two schemas differ in nothing but identifier length, so the whole
  // difference is the column names.
  EXPECT_EQ(wide.RetainedBytes() - narrow.RetainedBytes(), 4u * (kWideNameLength - 4u));
  EXPECT_EQ(wide.CacheEntryCount(), 1u);
}

TEST(MetadataFetcherCacheTest, RetainedBytesFallsBackToZeroAsEntriesAreRemoved) {
  MetadataFetcher fetcher;
  MetadataFetcherTestAccess::Store(&fetcher, "db", "one", WideColumns(4, 16));
  MetadataFetcherTestAccess::Store(&fetcher, "db", "two", WideColumns(4, 16));
  MetadataFetcherTestAccess::StoreUnresolvable(&fetcher, "other", "three", 4);
  ASSERT_EQ(fetcher.CacheEntryCount(), 3u);
  const size_t all_three = fetcher.RetainedBytes();

  fetcher.InvalidateCache("db", "one");
  EXPECT_LT(fetcher.RetainedBytes(), all_three);
  fetcher.InvalidateCache("db", "two");
  fetcher.InvalidateCache("other", "three");
  // Every charge an entry added is given back when it goes, so the total cannot
  // drift upwards over a long stream of invalidations.
  EXPECT_EQ(fetcher.CacheEntryCount(), 0u);
  EXPECT_EQ(fetcher.RetainedBytes(), 0u);

  MetadataFetcherTestAccess::Store(&fetcher, "db", "one", WideColumns(4, 16));
  fetcher.ClearCache();
  EXPECT_EQ(fetcher.CacheEntryCount(), 0u);
  EXPECT_EQ(fetcher.RetainedBytes(), 0u);
}

TEST(MetadataFetcherCacheTest, ReplacingAnEntryReplacesItsCharge) {
  MetadataFetcher fetcher;
  MetadataFetcherTestAccess::Store(&fetcher, "db", "t", WideColumns(4, 16));
  const size_t narrow_bytes = fetcher.RetainedBytes();

  MetadataFetcherTestAccess::Store(&fetcher, "db", "t", WideColumns(4, kWideNameLength));
  EXPECT_EQ(fetcher.CacheEntryCount(), 1u);
  EXPECT_EQ(fetcher.RetainedBytes(), narrow_bytes + 4u * (kWideNameLength - 16u));

  MetadataFetcherTestAccess::Store(&fetcher, "db", "t", WideColumns(4, 16));
  EXPECT_EQ(fetcher.RetainedBytes(), narrow_bytes);
}

TEST(MetadataFetcherCacheTest, UnresolvableTablesAreChargedForTheirIdentifiers) {
  // A negative entry retains only the two identifiers -- no column data -- so
  // it cannot reach the byte bound on its own. It is charged all the same
  // because it shares the positive entries' bound and their overflow policy.
  MetadataFetcher fetcher;
  MetadataFetcherTestAccess::StoreUnresolvable(&fetcher, "database", "table", 4);
  EXPECT_EQ(fetcher.CacheEntryCount(), 1u);
  EXPECT_EQ(fetcher.RetainedBytes(), std::string("database").size() + std::string("table").size());

  fetcher.InvalidateCache("database", "table");
  EXPECT_EQ(fetcher.RetainedBytes(), 0u);
}

}  // namespace
}  // namespace mes
