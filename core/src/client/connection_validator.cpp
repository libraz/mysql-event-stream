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

/** @brief Allow-list of variable names that may be passed to QueryVariable.
 *
 * var_name is embedded into a SQL literal, so it must never originate from
 * untrusted input. This allow-list enforces that contract at runtime and
 * provides a cheap defense-in-depth guard against future callers mistakenly
 * plumbing external input into this helper.
 */
bool IsAllowedVariableName(const char* var_name) {
  static constexpr const char* kAllowed[] = {
      "log_bin",
      "gtid_mode",
      "binlog_format",
      "binlog_row_image",
      "binlog_transaction_compression",
      "binlog_row_value_options",
  };
  if (var_name == nullptr) return false;
  for (const char* allowed : kAllowed) {
    if (std::strcmp(var_name, allowed) == 0) return true;
  }
  return false;
}

/** @brief Query a single MySQL variable value via SHOW VARIABLES
 *
 * @warning var_name is interpolated into a SQL literal. It MUST be one of
 * the compile-time constants enumerated in IsAllowedVariableName(); external
 * input MUST NOT be passed. The allow-list check enforces this at runtime.
 */
struct VariableQueryResult {
  detail::VariableQueryStatus status = detail::VariableQueryStatus::kQueryError;
  mes_error_t query_error = MES_OK;
  std::string value;
};

VariableQueryResult QueryVariable(protocol::MysqlConnection* conn, const char* var_name) {
  VariableQueryResult result;
  if (!IsAllowedVariableName(var_name)) {
    result.query_error = MES_ERR_INVALID_ARG;
    return result;
  }
  std::string query = "SHOW VARIABLES WHERE Variable_name = '" + std::string(var_name) + "'";

  protocol::QueryResult qr;
  std::string err;
  result.query_error =
      protocol::ExecuteQuery(conn->Socket(), query, &qr, &err, conn->DeprecateEofNegotiated());
  result.status = detail::ClassifyVariableQueryResult(result.query_error, qr);
  if (result.status == detail::VariableQueryStatus::kFound) {
    result.value = qr.rows[0].values[1];
  }
  return result;
}

}  // namespace

namespace detail {

VariableQueryStatus ClassifyVariableQueryResult(mes_error_t query_error,
                                                const protocol::QueryResult& result) {
  if (query_error != MES_OK) return VariableQueryStatus::kQueryError;
  if (result.rows.empty()) return VariableQueryStatus::kNotFound;
  if (result.rows[0].values.size() < 2 || result.rows[0].is_null.size() < 2 ||
      result.rows[0].is_null[1]) {
    return VariableQueryStatus::kMalformed;
  }
  return VariableQueryStatus::kFound;
}

}  // namespace detail

ValidationResult ConnectionValidator::Validate(protocol::MysqlConnection* conn,
                                               ServerFlavor flavor) {
  ValidationResult result;

  if (conn == nullptr) {
    result.error = MES_ERR_VALIDATION;
    std::snprintf(result.message, sizeof(result.message), "MySQL connection is null");
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

  // 6. binlog_row_value_options must NOT be PARTIAL_JSON (MySQL only).
  // PARTIAL_JSON emits JSON column updates as partial diff payloads rather than
  // full values, which the row decoder cannot interpret; reject it up front.
  if (flavor != ServerFlavor::kMariaDB) {
    CheckVariableNot(conn, "binlog_row_value_options", "PARTIAL_JSON", &result);
    if (result.error != MES_OK) {
      return result;
    }
  }

  return result;
}

bool ConnectionValidator::CheckVariable(protocol::MysqlConnection* conn, const char* var_name,
                                        const char* expected, ValidationResult* result) {
  VariableQueryResult query = QueryVariable(conn, var_name);
  if (query.status != detail::VariableQueryStatus::kFound) {
    result->error = MES_ERR_VALIDATION;
    if (query.status == detail::VariableQueryStatus::kNotFound) {
      std::snprintf(result->message, sizeof(result->message), "Variable %s not found", var_name);
    } else {
      std::snprintf(result->message, sizeof(result->message), "Failed to query %s", var_name);
    }
    return false;
  }

  bool match = EqualsIgnoreCase(query.value.c_str(), expected);
  if (!match) {
    result->error = MES_ERR_VALIDATION;
    std::string msg = std::string(var_name) + " must be " + expected + ", got " + query.value;
    std::strncpy(result->message, msg.c_str(), sizeof(result->message) - 1);
    result->message[sizeof(result->message) - 1] = '\0';
  }

  return match;
}

bool ConnectionValidator::CheckVariableNot(protocol::MysqlConnection* conn, const char* var_name,
                                           const char* rejected, ValidationResult* result) {
  VariableQueryResult query = QueryVariable(conn, var_name);
  if (query.status == detail::VariableQueryStatus::kNotFound) {
    // Optional safety variables may not exist on an older supported server.
    return true;
  }
  if (query.status != detail::VariableQueryStatus::kFound) {
    result->error = MES_ERR_VALIDATION;
    std::snprintf(result->message, sizeof(result->message), "Failed to query %s", var_name);
    return false;
  }

  bool is_rejected = EqualsIgnoreCase(query.value.c_str(), rejected);
  if (is_rejected) {
    result->error = MES_ERR_VALIDATION;
    std::snprintf(result->message, sizeof(result->message), "%s must not be %s", var_name,
                  rejected);
  }

  return !is_rejected;
}

}  // namespace mes
