// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * @file metadata_fetcher.h
 * @brief Fetches column metadata (names, unsigned flags) from MySQL
 *
 * Uses a dedicated MySQL connection to query SHOW COLUMNS and caches
 * results by "database.table" key.
 */

#ifndef MES_CLIENT_METADATA_FETCHER_H_
#define MES_CLIENT_METADATA_FETCHER_H_

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

#include "client/column_name_source.h"
#include "mes.h"
#include "protocol/mysql_connection.h"

namespace mes {

class MetadataFetcherTestAccess;

/**
 * @brief Fetches and caches column metadata from MySQL
 *
 * Uses a separate MySQL connection (not the binlog streaming connection)
 * to query column names and unsigned flags via SHOW COLUMNS FROM.
 */
class MetadataFetcher : public ColumnNameSource {
 public:
  /**
   * @brief Cached tables retained before the cache is dropped.
   *
   * Metadata is an optional enhancement, so a cold refetch is preferable to
   * unbounded growth in long-lived multi-tenant streams.
   */
  static constexpr size_t kMaxCacheEntries = 8192;

  /**
   * @brief Cached metadata bytes retained before the cache is dropped.
   *
   * The entry count alone cannot bound what this cache holds. Per-entry cost is
   * set by schema content the client does not choose -- how many columns a
   * table has and how long their identifiers are -- so a source of wide tables
   * retains orders of magnitude more per entry than a narrow one, and the
   * entry count says nothing about the memory.
   *
   * Sized to give an entry the same allowance TableMapRegistry does
   * (kMaxCacheEntries entries of 2 KiB), because the two bound the same
   * content: the schema of the tables one stream touches. That leaves the entry
   * count the binding bound in ordinary use and this one engaging only on the
   * wide-table case it exists for.
   */
  static constexpr size_t kMaxRetainedBytes = 16u * 1024 * 1024;

  MetadataFetcher();
  ~MetadataFetcher() override;

  // Non-copyable
  MetadataFetcher(const MetadataFetcher&) = delete;
  MetadataFetcher& operator=(const MetadataFetcher&) = delete;

  /**
   * @brief Connect to MySQL server
   * @return MES_OK on success, MES_ERR_CONNECT on failure
   */
  mes_error_t Connect(const std::string& host, uint16_t port, const std::string& user,
                      const std::string& password, uint32_t connect_timeout_s,
                      uint32_t read_timeout_s, uint32_t ssl_mode = 0,
                      const std::string& ssl_ca = "", const std::string& ssl_cert = "",
                      const std::string& ssl_key = "", bool allow_public_key_retrieval = false);

  /** @brief Disconnect and close the MySQL connection */
  void Disconnect();

  /**
   * @brief Fetch column info for a table (cached)
   *
   * Returns cached results if available and column count matches.
   * On cache miss, executes SHOW COLUMNS FROM `db`.`table`.
   * On failure, returns empty vector (column names remain unknown).
   *
   * @param database Database name
   * @param table Table name
   * @param expected_count Expected number of columns (from TABLE_MAP)
   * @return Column info vector, or empty on failure
   */
  std::vector<ColumnInfo> FetchColumnInfo(const std::string& database, const std::string& table,
                                          size_t expected_count) override;

  /**
   * @brief Remove cached entry for a table (e.g., after schema change)
   */
  void InvalidateCache(const std::string& database, const std::string& table);

  /**
   * @brief Remove all cached column info.
   *
   * Used when a DDL statement is observed in the binlog stream: a schema change
   * may rename/retype columns while preserving the column count, which the
   * per-table count guard cannot detect. Clearing forces a fresh SHOW COLUMNS
   * on the next row event.
   */
  void ClearCache() override;

  /** @brief Number of tables the cache holds, resolved and unresolvable alike. */
  size_t CacheEntryCount() const;

  /**
   * @brief Bytes of variable-length metadata the cached entries retain.
   *
   * Counts what a schema's own content sizes: each entry's database and table
   * name, its column array and every column name in it. Fixed per-entry
   * structure is not counted -- it is proportional to the entry count, which
   * kMaxCacheEntries bounds on its own.
   */
  size_t RetainedBytes() const;

 private:
  friend class MetadataFetcherTestAccess;

  static size_t IdentifierCharge(const std::string& database, const std::string& table);
  static size_t EntryCharge(const std::string& database, const std::string& table,
                            const std::vector<ColumnInfo>& columns);

  /**
   * @brief Cache a resolved table, dropping the cache first if it is full.
   *
   * Overflow drops every entry rather than evicting one: unlike
   * TableMapRegistry, which a ROWS event decodes against and must therefore
   * keep populated, nothing here is needed before the next SHOW COLUMNS can
   * repopulate it, which is what makes a cold refetch the cheaper trade. The
   * entry being stored is always kept, so a single table wider than the whole
   * byte budget leaves the cache holding just that one entry.
   */
  void StoreCacheEntry(const std::string& database, const std::string& table,
                       std::vector<ColumnInfo> columns);

  /** @brief Remember that a table could not be resolved; bounded as above. */
  void StoreNegativeEntry(const std::string& database, const std::string& table,
                          size_t expected_count);

  /** @brief Drop a cached table and its charge. @return true when one was held. */
  bool EraseCacheEntry(const std::string& database, const std::string& table);

  /** @brief Drop a negative entry and its charge. @return true when one was held. */
  bool EraseNegativeEntry(const std::string& database, const std::string& table);

  /** @brief Drop every entry and reset the retained-byte total. */
  void DropAllEntries();

  protocol::MysqlConnection conn_;
  std::unordered_map<std::string, std::unordered_map<std::string, std::vector<ColumnInfo>>> cache_;
  // Server-side SQL failures (for example missing SELECT privilege) are
  // stable until DDL/cache invalidation. Remember the expected column count
  // so repeated TABLE_MAP events do not cause a query/reconnect storm.
  std::unordered_map<std::string, std::unordered_map<std::string, size_t>> negative_cache_;
  // Both maps are charged against one total because they share one bound and
  // one overflow policy. A negative entry retains only the two identifiers, so
  // it is the resolved entries that can reach the byte bound.
  size_t retained_bytes_ = 0;

  // Stored connection parameters for reconnection
  std::string host_;
  uint16_t port_ = 0;
  std::string user_;
  std::string password_;
  uint32_t connect_timeout_s_ = 0;
  uint32_t read_timeout_s_ = 0;
  uint32_t ssl_mode_ = 0;
  std::string ssl_ca_;
  std::string ssl_cert_;
  std::string ssl_key_;
  bool allow_public_key_retrieval_ = false;
  std::chrono::steady_clock::time_point next_reconnect_attempt_{};

  bool Reconnect();
  std::string EscapeIdentifier(const std::string& id);
};

}  // namespace mes

#endif  // MES_CLIENT_METADATA_FETCHER_H_
