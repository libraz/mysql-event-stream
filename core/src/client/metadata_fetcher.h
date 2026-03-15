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

#ifdef MES_HAS_MYSQL

#include <mysql.h>

#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

#include "mes.h"

namespace mes {

/**
 * @brief Column info retrieved from SHOW COLUMNS
 */
struct ColumnInfo {
  std::string name;
  bool is_unsigned = false;
};

/**
 * @brief Fetches and caches column metadata from MySQL
 *
 * Uses a separate MySQL connection (not the binlog streaming connection)
 * to query column names and unsigned flags via SHOW COLUMNS FROM.
 */
class MetadataFetcher {
 public:
  MetadataFetcher();
  ~MetadataFetcher();

  // Non-copyable
  MetadataFetcher(const MetadataFetcher&) = delete;
  MetadataFetcher& operator=(const MetadataFetcher&) = delete;

  /**
   * @brief Connect to MySQL server
   * @return MES_OK on success, MES_ERR_CONNECT on failure
   */
  mes_error_t Connect(const std::string& host, uint16_t port, const std::string& user,
                      const std::string& password, uint32_t connect_timeout_s);

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
                                          size_t expected_count);

  /**
   * @brief Remove cached entry for a table (e.g., after schema change)
   */
  void InvalidateCache(const std::string& database, const std::string& table);

 private:
  MYSQL* conn_ = nullptr;
  std::unordered_map<std::string, std::vector<ColumnInfo>> cache_;

  static std::string MakeCacheKey(const std::string& db, const std::string& table);
  std::string EscapeIdentifier(const std::string& id);
};

}  // namespace mes

#endif  // MES_HAS_MYSQL
#endif  // MES_CLIENT_METADATA_FETCHER_H_
