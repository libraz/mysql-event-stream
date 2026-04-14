// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#include "client/connection_validator.h"

#include <cctype>
#include <cstdio>
#include <cstring>
#include <string>

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

/** @brief Query a single MySQL variable value via SHOW VARIABLES */
mes_error_t QueryVariable(protocol::MysqlConnection* conn, const char* var_name,
                          std::string& out_value) {
  std::string query =
      "SHOW VARIABLES WHERE Variable_name = '" + std::string(var_name) + "'";

  protocol::QueryResult qr;
  std::string err;
  mes_error_t rc = protocol::ExecuteQuery(conn->Socket(), query, &qr, &err);
  if (rc != MES_OK) {
    out_value.clear();
    return rc;
  }

  if (qr.rows.empty() || qr.rows[0].is_null[1]) {
    out_value.clear();
    return MES_ERR_VALIDATION;
  }

  out_value = qr.rows[0].values[1];
  return MES_OK;
}

}  // namespace

ValidationResult ConnectionValidator::Validate(
    protocol::MysqlConnection* conn, ServerFlavor flavor) {
  ValidationResult result;

  if (conn == nullptr) {
    result.error = MES_ERR_VALIDATION;
    std::snprintf(result.message, sizeof(result.message),
                  "MySQL connection is null");
    return result;
  }

  // 1. log_bin must be ON
  if (!CheckVariable(conn, "log_bin", "ON", &result)) {
    return result;
  }

  // 2. gtid_mode must be ON (MySQL only; MariaDB GTID is always active)
  if (flavor != ServerFlavor::kMariaDB) {
    if (!CheckVariable(conn, "gtid_mode", "ON", &result)) {
      return result;
    }
  }

  // 3. binlog_format must be ROW
  if (!CheckVariable(conn, "binlog_format", "ROW", &result)) {
    return result;
  }

  // 4. binlog_row_image must be FULL
  if (!CheckVariable(conn, "binlog_row_image", "FULL", &result)) {
    return result;
  }

  // 5. binlog_transaction_compression must NOT be ON
  // (MariaDB doesn't have this variable)
  if (flavor != ServerFlavor::kMariaDB) {
    CheckVariableNot(conn, "binlog_transaction_compression", "ON", &result);
    if (result.error != MES_OK) {
      return result;
    }
  }

  return result;
}

bool ConnectionValidator::CheckVariable(protocol::MysqlConnection* conn,
                                        const char* var_name,
                                        const char* expected,
                                        ValidationResult* result) {
  std::string value;
  mes_error_t rc = QueryVariable(conn, var_name, value);
  if (rc != MES_OK) {
    result->error = MES_ERR_VALIDATION;
    if (rc == MES_ERR_VALIDATION) {
      std::snprintf(result->message, sizeof(result->message),
                    "Variable %s not found", var_name);
    } else {
      std::snprintf(result->message, sizeof(result->message),
                    "Failed to query %s", var_name);
    }
    return false;
  }

  bool match = EqualsIgnoreCase(value.c_str(), expected);
  if (!match) {
    result->error = MES_ERR_VALIDATION;
    std::string msg = std::string(var_name) + " must be " + expected + ", got " + value;
    std::strncpy(result->message, msg.c_str(), sizeof(result->message) - 1);
    result->message[sizeof(result->message) - 1] = '\0';
  }

  return match;
}

bool ConnectionValidator::CheckVariableNot(protocol::MysqlConnection* conn,
                                           const char* var_name,
                                           const char* rejected,
                                           ValidationResult* result) {
  std::string value;
  mes_error_t rc = QueryVariable(conn, var_name, value);
  if (rc != MES_OK) {
    // Variable may not exist on this MySQL version; that is acceptable
    return true;
  }

  bool is_rejected = EqualsIgnoreCase(value.c_str(), rejected);
  if (is_rejected) {
    result->error = MES_ERR_VALIDATION;
    std::snprintf(result->message, sizeof(result->message),
                  "%s must not be %s", var_name, rejected);
  }

  return !is_rejected;
}

}  // namespace mes
