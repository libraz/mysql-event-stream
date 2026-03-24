// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#include "client/connection_validator.h"

#ifdef MES_HAS_MYSQL

#include <cctype>
#include <cstdio>
#include <cstring>

#include <mysql.h>

namespace mes {

namespace {

/** @brief Case-insensitive string comparison */
bool EqualsIgnoreCase(const char* a, const char* b) {
  while (*a && *b) {
    if (std::tolower(static_cast<unsigned char>(*a)) !=
        std::tolower(static_cast<unsigned char>(*b))) {
      return false;
    }
    ++a;
    ++b;
  }
  return *a == *b;
}

}  // namespace

ValidationResult ConnectionValidator::Validate(MYSQL* conn) {
  ValidationResult result;

  if (conn == nullptr) {
    result.error = MES_ERR_VALIDATION;
    std::snprintf(result.message, sizeof(result.message),
                  "MySQL connection is null");
    return result;
  }

  // 1. gtid_mode must be ON
  if (!CheckVariable(conn, "gtid_mode", "ON", &result)) {
    return result;
  }

  // 2. binlog_format must be ROW
  if (!CheckVariable(conn, "binlog_format", "ROW", &result)) {
    return result;
  }

  // 3. binlog_row_image must be FULL
  if (!CheckVariable(conn, "binlog_row_image", "FULL", &result)) {
    return result;
  }

  // 4. binlog_transaction_compression must NOT be ON (may not exist)
  CheckVariableNot(conn, "binlog_transaction_compression", "ON", &result);
  if (result.error != MES_OK) {
    return result;
  }

  // 5. Retrieve server_uuid
  if (!FetchServerUuid(conn, &result)) {
    return result;
  }

  return result;
}

bool ConnectionValidator::CheckVariable(MYSQL* conn, const char* var_name,
                                        const char* expected,
                                        ValidationResult* result) {
  char query[256];
  std::snprintf(query, sizeof(query),
                "SHOW VARIABLES WHERE Variable_name = '%s'", var_name);

  if (mysql_query(conn, query) != 0) {
    result->error = MES_ERR_VALIDATION;
    std::snprintf(result->message, sizeof(result->message),
                  "Failed to query %s: %s", var_name, mysql_error(conn));
    return false;
  }

  MYSQL_RES* res = mysql_store_result(conn);
  if (res == nullptr) {
    result->error = MES_ERR_VALIDATION;
    std::snprintf(result->message, sizeof(result->message),
                  "Failed to store result for %s", var_name);
    return false;
  }

  MYSQL_ROW row = mysql_fetch_row(res);
  if (row == nullptr || row[1] == nullptr) {
    mysql_free_result(res);
    result->error = MES_ERR_VALIDATION;
    std::snprintf(result->message, sizeof(result->message),
                  "Variable %s not found", var_name);
    return false;
  }

  bool match = EqualsIgnoreCase(row[1], expected);
  if (!match) {
    result->error = MES_ERR_VALIDATION;
    std::snprintf(result->message, sizeof(result->message),
                  "%s must be %s, got %s", var_name, expected, row[1]);
  }

  mysql_free_result(res);
  return match;
}

bool ConnectionValidator::CheckVariableNot(MYSQL* conn, const char* var_name,
                                           const char* rejected,
                                           ValidationResult* result) {
  char query[256];
  std::snprintf(query, sizeof(query),
                "SHOW VARIABLES WHERE Variable_name = '%s'", var_name);

  if (mysql_query(conn, query) != 0) {
    // Variable may not exist on this MySQL version; that is acceptable
    return true;
  }

  MYSQL_RES* res = mysql_store_result(conn);
  if (res == nullptr) {
    return true;
  }

  MYSQL_ROW row = mysql_fetch_row(res);
  if (row == nullptr || row[1] == nullptr) {
    // Variable not found; acceptable
    mysql_free_result(res);
    return true;
  }

  bool is_rejected = EqualsIgnoreCase(row[1], rejected);
  if (is_rejected) {
    result->error = MES_ERR_VALIDATION;
    std::snprintf(result->message, sizeof(result->message),
                  "%s must not be %s", var_name, rejected);
  }

  mysql_free_result(res);
  return !is_rejected;
}

bool ConnectionValidator::FetchServerUuid(MYSQL* conn,
                                          ValidationResult* result) {
  if (mysql_query(conn, "SELECT @@server_uuid") != 0) {
    result->error = MES_ERR_VALIDATION;
    std::snprintf(result->message, sizeof(result->message),
                  "Failed to query server_uuid: %s", mysql_error(conn));
    return false;
  }

  MYSQL_RES* res = mysql_store_result(conn);
  if (res == nullptr) {
    result->error = MES_ERR_VALIDATION;
    std::snprintf(result->message, sizeof(result->message),
                  "Failed to store result for server_uuid");
    return false;
  }

  MYSQL_ROW row = mysql_fetch_row(res);
  if (row == nullptr || row[0] == nullptr) {
    mysql_free_result(res);
    result->error = MES_ERR_VALIDATION;
    std::snprintf(result->message, sizeof(result->message),
                  "server_uuid is empty");
    return false;
  }

  std::snprintf(result->server_uuid, sizeof(result->server_uuid), "%s",
                row[0]);
  mysql_free_result(res);
  return true;
}

}  // namespace mes

#endif  // MES_HAS_MYSQL
