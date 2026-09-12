// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * @file column_name_source.h
 * @brief Interface through which the engine resolves column names
 */

#ifndef MES_CLIENT_COLUMN_NAME_SOURCE_H_
#define MES_CLIENT_COLUMN_NAME_SOURCE_H_

#include <cstddef>
#include <string>
#include <vector>

namespace mes {

/**
 * @brief Column info for one column of a table.
 */
struct ColumnInfo {
  std::string name;
  bool is_unsigned = false;
};

/**
 * @brief Supplies the column names a TABLE_MAP event does not carry.
 *
 * CdcEngine resolves names through this interface rather than through the
 * concrete fetcher, so everything the engine asks of a metadata side-connection
 * is declared here and nothing else about the fetcher is reachable from the
 * engine. Resolution can therefore be driven -- and its success observed --
 * without a server to answer it.
 *
 * The implementation is owned by whoever installs it; CdcEngine holds a
 * non-owning pointer and must not outlive it.
 */
class ColumnNameSource {
 public:
  virtual ~ColumnNameSource() = default;

  ColumnNameSource(const ColumnNameSource&) = delete;
  ColumnNameSource& operator=(const ColumnNameSource&) = delete;

  /**
   * @brief Resolve column info for a table.
   *
   * @param database Database name.
   * @param table Table name.
   * @param expected_count Number of columns the TABLE_MAP declared.
   * @return One entry per column, or an empty vector when the names could not
   *         be resolved. A partial answer is never returned: a result that does
   *         not describe @p expected_count columns is a failure.
   */
  virtual std::vector<ColumnInfo> FetchColumnInfo(const std::string& database,
                                                  const std::string& table,
                                                  size_t expected_count) = 0;

  /**
   * @brief Forget everything resolved so far.
   *
   * Called when a DDL statement is observed in the stream: a schema change may
   * rename or retype columns while preserving the column count, which a
   * per-table count guard cannot detect.
   */
  virtual void ClearCache() = 0;

 protected:
  ColumnNameSource() = default;
};

}  // namespace mes

#endif  // MES_CLIENT_COLUMN_NAME_SOURCE_H_
