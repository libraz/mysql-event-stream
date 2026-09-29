// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#include <gtest/gtest.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#ifndef _WIN32
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

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

#ifndef _WIN32

/**
 * @brief Loopback MySQL server that accepts any login and answers every
 *        COM_QUERY with a fixed one-row SHOW COLUMNS result, counting queries.
 */
class ShowColumnsPeer {
 public:
  ShowColumnsPeer() {
    listener_ = socket(AF_INET, SOCK_STREAM, 0);
    EXPECT_GE(listener_, 0);
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    EXPECT_EQ(bind(listener_, reinterpret_cast<const sockaddr*>(&address), sizeof(address)), 0);
    EXPECT_EQ(listen(listener_, 1), 0);
    socklen_t length = sizeof(address);
    EXPECT_EQ(getsockname(listener_, reinterpret_cast<sockaddr*>(&address), &length), 0);
    port_ = ntohs(address.sin_port);
    thread_ = std::thread([this] { Serve(); });
  }

  ~ShowColumnsPeer() {
    if (thread_.joinable()) thread_.join();
    if (listener_ >= 0) close(listener_);
  }

  ShowColumnsPeer(const ShowColumnsPeer&) = delete;
  ShowColumnsPeer& operator=(const ShowColumnsPeer&) = delete;

  uint16_t port() const { return port_; }
  int queries() const { return queries_.load(std::memory_order_acquire); }

 private:
  static constexpr uint8_t kComQuery = 0x03;

  void Serve() {
    const int peer = accept(listener_, nullptr, nullptr);
    if (peer < 0) return;
    std::vector<uint8_t> command;
    if (SendPacket(peer, 0, Handshake()) && ReadPacket(peer, &command) &&
        SendPacket(peer, 2, {0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00})) {
      while (ReadPacket(peer, &command) && !command.empty() && command[0] == kComQuery) {
        queries_.fetch_add(1, std::memory_order_acq_rel);
        SendOneColumnRow(peer);
      }
    }
    close(peer);
  }

  // One row, (Field, Type) = ("id", "int"): a live table of one column.
  static void SendOneColumnRow(int peer) {
    uint8_t sequence = 1;
    if (!SendPacket(peer, sequence++, {2})) return;
    for (const std::string column : {"Field", "Type"}) {
      std::vector<uint8_t> definition;
      for (const std::string& part :
           {std::string("def"), std::string(), std::string(), std::string(), column}) {
        definition.push_back(static_cast<uint8_t>(part.size()));
        definition.insert(definition.end(), part.begin(), part.end());
      }
      if (!SendPacket(peer, sequence++, definition)) return;
    }
    if (!SendPacket(peer, sequence++, {0xFE, 0x00, 0x00, 0x02, 0x00})) return;
    if (!SendPacket(peer, sequence++, {2, 'i', 'd', 3, 'i', 'n', 't'})) return;
    SendPacket(peer, sequence, {0xFE, 0x00, 0x00, 0x02, 0x00});
  }

  // Protocol41 + SecureConnection + PluginAuth, mysql_native_password.
  static std::vector<uint8_t> Handshake() {
    std::vector<uint8_t> payload{10};
    const std::string version = "8.4.0";
    payload.insert(payload.end(), version.begin(), version.end());
    payload.push_back(0);
    payload.insert(payload.end(), 4, 1);
    for (uint8_t i = 0; i < 8; ++i) payload.push_back(static_cast<uint8_t>('a' + i));
    payload.insert(payload.end(), {0, 0x00, 0x82, 45, 0x02, 0x00, 0x08, 0x00, 21});
    payload.insert(payload.end(), 10, 0);
    for (uint8_t i = 0; i < 12; ++i) payload.push_back(static_cast<uint8_t>('A' + i));
    payload.push_back(0);
    const std::string plugin = "mysql_native_password";
    payload.insert(payload.end(), plugin.begin(), plugin.end());
    payload.push_back(0);
    return payload;
  }

  static bool SendPacket(int peer, uint8_t sequence, const std::vector<uint8_t>& payload) {
    const size_t size = payload.size();
    std::vector<uint8_t> packet = {static_cast<uint8_t>(size), static_cast<uint8_t>(size >> 8),
                                   static_cast<uint8_t>(size >> 16), sequence};
    packet.insert(packet.end(), payload.begin(), payload.end());
    return send(peer, packet.data(), packet.size(), 0) == static_cast<ssize_t>(packet.size());
  }

  static bool ReadPacket(int peer, std::vector<uint8_t>* payload) {
    uint8_t header[4]{};
    if (recv(peer, header, sizeof(header), MSG_WAITALL) != static_cast<ssize_t>(sizeof(header))) {
      return false;
    }
    const size_t size = static_cast<size_t>(header[0]) | (static_cast<size_t>(header[1]) << 8) |
                        (static_cast<size_t>(header[2]) << 16);
    payload->assign(size, 0);
    return size == 0 ||
           recv(peer, payload->data(), size, MSG_WAITALL) == static_cast<ssize_t>(size);
  }

  int listener_ = -1;
  uint16_t port_ = 0;
  std::atomic<int> queries_{0};
  std::thread thread_;
};

#endif  // _WIN32

TEST(MetadataFetcherQueryTest, AColumnCountMismatchIsQueriedOncePerTableMapCount) {
#ifdef _WIN32
  GTEST_SKIP() << "local socket test is POSIX-only";
#else
  // A backlog replayed across an ALTER TABLE carries TABLE_MAPs whose column
  // count the live table no longer has, one per row event. The answer cannot
  // change until the schema does, so asking again per event only turns a
  // cache hit into a network round trip.
  ShowColumnsPeer peer;
  {
    MetadataFetcher fetcher;
    ASSERT_EQ(fetcher.Connect("127.0.0.1", peer.port(), "user", "", 2, 2), MES_OK);
    for (int event = 0; event < 3; ++event) {
      EXPECT_TRUE(fetcher.FetchColumnInfo("db", "t", 2).empty());
    }
    EXPECT_EQ(peer.queries(), 1);

    // Another count is another question, and invalidation asks again.
    EXPECT_EQ(fetcher.FetchColumnInfo("db", "t", 1).size(), 1u);
    EXPECT_EQ(peer.queries(), 2);
    fetcher.InvalidateCache("db", "t");
    EXPECT_TRUE(fetcher.FetchColumnInfo("db", "t", 2).empty());
    EXPECT_EQ(peer.queries(), 3);
  }
#endif
}

}  // namespace
}  // namespace mes
