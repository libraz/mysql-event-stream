// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * @file mysql_query.h
 * @brief MySQL COM_QUERY execution and result set parsing
 *
 * Provides a blocking interface to execute SQL queries over an established
 * MySQL connection and retrieve the full result set. Supports both
 * result-returning queries (SELECT) and non-result commands (SET, etc.).
 */

#ifndef MES_PROTOCOL_MYSQL_QUERY_H_
#define MES_PROTOCOL_MYSQL_QUERY_H_

#include <cstdint>
#include <string>
#include <vector>

#include "mes.h"

namespace mes::protocol {

// Forward declaration; defined in protocol/mysql_socket.h
class SocketHandle;

/** @brief A single row from a query result set */
struct QueryResultRow {
  std::vector<std::string> values;
  std::vector<bool> is_null;
};

/** @brief Full result set from a COM_QUERY execution */
struct QueryResult {
  std::vector<std::string> column_names;
  std::vector<QueryResultRow> rows;
};

/**
 * @brief Execute a COM_QUERY and store the full result set
 *
 * Sends the query as a COM_QUERY command and reads the entire result set.
 * For commands with no result set (SET, USE, etc.), returns an empty
 * QueryResult with MES_OK.
 *
 * When @p deprecate_eof is true (default), the connection is assumed to use
 * CLIENT_DEPRECATE_EOF (MySQL 8.4 default) where EOF packets are replaced
 * by OK packets. When false, traditional EOF packets are expected between
 * column definitions and row data, and after the last row.
 *
 * @param sock           Connected socket handle
 * @param query          SQL query string
 * @param result         Output: populated with column names and row data
 * @param error_msg      Output: MySQL error message on failure
 * @param deprecate_eof  Whether CLIENT_DEPRECATE_EOF is negotiated (default:
 *                       true)
 * @return MES_OK on success, MES_ERR_STREAM on protocol or MySQL error
 */
mes_error_t ExecuteQuery(SocketHandle* sock, const std::string& query,
                         QueryResult* result, std::string* error_msg,
                         bool deprecate_eof = true);

}  // namespace mes::protocol

#endif  // MES_PROTOCOL_MYSQL_QUERY_H_
