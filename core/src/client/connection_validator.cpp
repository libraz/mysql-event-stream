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
      "log_bin_compress",
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
detail::VariableValue QueryVariable(protocol::MysqlConnection* conn, const char* var_name) {
  detail::VariableValue result;
  if (!IsAllowedVariableName(var_name)) {
    result.error_message = "Variable name is not allowed";
    return result;
  }
  std::string query = "SHOW VARIABLES WHERE Variable_name = '" + std::string(var_name) + "'";

  protocol::QueryResult qr;
  std::string err;
  const mes_error_t query_error =
      protocol::ExecuteQuery(conn->Socket(), query, &qr, &err, conn->DeprecateEofNegotiated());
  result.error_message = std::move(err);
  result.status = detail::ClassifyVariableQueryResult(query_error, qr);
  if (result.status == detail::VariableQueryStatus::kFound) {
    result.value = qr.rows[0].values[1];
  }
  return result;
}

detail::VariableValue LookupConnectionVariable(void* context, const char* var_name) {
  return QueryVariable(static_cast<protocol::MysqlConnection*>(context), var_name);
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

ValidationResult ValidateServerConfiguration(VariableLookup lookup, void* context,
                                             ServerFlavor flavor) {
  ValidationResult result;
  auto check_equal = [&](const char* var_name, const char* expected) {
    const VariableValue query = lookup(context, var_name);
    if (query.status != VariableQueryStatus::kFound) {
      result.error = MES_ERR_VALIDATION;
      if (query.status == VariableQueryStatus::kNotFound) {
        std::snprintf(result.message, sizeof(result.message), "Variable %s not found", var_name);
      } else {
        std::snprintf(result.message, sizeof(result.message), "Failed to query %s: %s", var_name,
                      query.error_message.empty() ? "unknown error" : query.error_message.c_str());
      }
      return false;
    }
    if (!EqualsIgnoreCase(query.value.c_str(), expected)) {
      result.error = MES_ERR_VALIDATION;
      std::snprintf(result.message, sizeof(result.message), "%s must be %s, got %s", var_name,
                    expected, query.value.c_str());
      return false;
    }
    return true;
  };
  auto check_not_equal = [&](const char* var_name, const char* rejected) {
    const VariableValue query = lookup(context, var_name);
    if (query.status == VariableQueryStatus::kNotFound) return true;
    if (query.status != VariableQueryStatus::kFound) {
      result.error = MES_ERR_VALIDATION;
      std::snprintf(result.message, sizeof(result.message), "Failed to query %s: %s", var_name,
                    query.error_message.empty() ? "unknown error" : query.error_message.c_str());
      return false;
    }
    if (EqualsIgnoreCase(query.value.c_str(), rejected)) {
      result.error = MES_ERR_VALIDATION;
      std::snprintf(result.message, sizeof(result.message), "%s must not be %s", var_name,
                    rejected);
      return false;
    }
    return true;
  };

  if (!check_equal("log_bin", "ON")) return result;
  if (flavor != ServerFlavor::kMariaDB && !check_equal("gtid_mode", "ON")) return result;
  if (!check_equal("binlog_format", "ROW")) return result;
  if (!check_equal("binlog_row_image", "FULL")) return result;
  if (flavor == ServerFlavor::kMariaDB) {
    if (!check_not_equal("log_bin_compress", "ON")) return result;
  } else {
    if (!check_not_equal("binlog_transaction_compression", "ON")) return result;
    if (!check_not_equal("binlog_row_value_options", "PARTIAL_JSON")) return result;
  }
  return result;
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

  return detail::ValidateServerConfiguration(LookupConnectionVariable, conn, flavor);
}

}  // namespace mes
