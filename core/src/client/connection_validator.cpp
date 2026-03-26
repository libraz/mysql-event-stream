// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#include "client/connection_validator.h"

#include <cctype>
#include <cstdio>
#include <cstring>

#include "protocol/mysql_query.h"

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

ValidationResult ConnectionValidator::Validate(
    protocol::MysqlConnection* conn) {
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

bool ConnectionValidator::CheckVariable(protocol::MysqlConnection* conn,
                                        const char* var_name,
                                        const char* expected,
                                        ValidationResult* result) {
  char query[256];
  std::snprintf(query, sizeof(query),
                "SHOW VARIABLES WHERE Variable_name = '%s'", var_name);

  protocol::QueryResult qr;
  std::string err;
  if (protocol::ExecuteQuery(conn->Socket(), query, &qr, &err) != MES_OK) {
    result->error = MES_ERR_VALIDATION;
    std::snprintf(result->message, sizeof(result->message),
                  "Failed to query %s: %s", var_name, err.c_str());
    return false;
  }

  if (qr.rows.empty() || qr.rows[0].is_null[1]) {
    result->error = MES_ERR_VALIDATION;
    std::snprintf(result->message, sizeof(result->message),
                  "Variable %s not found", var_name);
    return false;
  }

  bool match = EqualsIgnoreCase(qr.rows[0].values[1].c_str(), expected);
  if (!match) {
    result->error = MES_ERR_VALIDATION;
    std::snprintf(result->message, sizeof(result->message),
                  "%s must be %s, got %s", var_name, expected,
                  qr.rows[0].values[1].c_str());
  }

  return match;
}

bool ConnectionValidator::CheckVariableNot(protocol::MysqlConnection* conn,
                                           const char* var_name,
                                           const char* rejected,
                                           ValidationResult* result) {
  char query[256];
  std::snprintf(query, sizeof(query),
                "SHOW VARIABLES WHERE Variable_name = '%s'", var_name);

  protocol::QueryResult qr;
  std::string err;
  if (protocol::ExecuteQuery(conn->Socket(), query, &qr, &err) != MES_OK) {
    // Variable may not exist on this MySQL version; that is acceptable
    return true;
  }

  if (qr.rows.empty() || qr.rows[0].is_null[1]) {
    // Variable not found; acceptable
    return true;
  }

  bool is_rejected = EqualsIgnoreCase(qr.rows[0].values[1].c_str(), rejected);
  if (is_rejected) {
    result->error = MES_ERR_VALIDATION;
    std::snprintf(result->message, sizeof(result->message),
                  "%s must not be %s", var_name, rejected);
  }

  return !is_rejected;
}

bool ConnectionValidator::FetchServerUuid(protocol::MysqlConnection* conn,
                                          ValidationResult* result) {
  protocol::QueryResult qr;
  std::string err;
  if (protocol::ExecuteQuery(conn->Socket(), "SELECT @@server_uuid", &qr,
                             &err) != MES_OK) {
    result->error = MES_ERR_VALIDATION;
    std::snprintf(result->message, sizeof(result->message),
                  "Failed to query server_uuid: %s", err.c_str());
    return false;
  }

  if (qr.rows.empty() || qr.rows[0].is_null[0]) {
    result->error = MES_ERR_VALIDATION;
    std::snprintf(result->message, sizeof(result->message),
                  "server_uuid is empty");
    return false;
  }

  std::snprintf(result->server_uuid, sizeof(result->server_uuid), "%s",
                qr.rows[0].values[0].c_str());
  return true;
}

}  // namespace mes
