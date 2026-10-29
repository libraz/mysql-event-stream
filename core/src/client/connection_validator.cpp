// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#include "client/connection_validator.h"

#include <cstdio>
#include <cstring>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "mybinlog/server_settings.h"
#include "protocol/mysql_query.h"

namespace mes {

namespace {

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
  result.query_error = query_error;
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
                                             mybinlog::ServerFlavor flavor) {
  // Checks run in this order and the first failure returns, so a failed check
  // never queries the settings after it.
  static constexpr mybinlog::RuleId kRules[] = {
      mybinlog::RuleId::kLogBin,           mybinlog::RuleId::kGtidMode,
      mybinlog::RuleId::kBinlogFormat,     mybinlog::RuleId::kBinlogRowImage,
      mybinlog::RuleId::kNoLogBinCompress, mybinlog::RuleId::kNoTransactionCompression,
      mybinlog::RuleId::kNoPartialJson,
  };

  // A lookup that could not produce an answer is not a verdict on the setting,
  // so it is reported to the rule as absent and recorded here instead.
  struct LookupContext {
    VariableLookup lookup;
    void* context;
    VariableValue failure;
    std::string failed_variable;
    bool failed;
  };
  const mybinlog::VariableLookupFn adapter = [](void* ctx, std::string_view variable,
                                                std::string* value) {
    auto* self = static_cast<LookupContext*>(ctx);
    const std::string name(variable);
    VariableValue query = self->lookup(self->context, name.c_str());
    if (query.status == VariableQueryStatus::kFound) {
      *value = std::move(query.value);
      return true;
    }
    if (query.status != VariableQueryStatus::kNotFound) {
      self->failure = std::move(query);
      self->failed_variable = name;
      self->failed = true;
    }
    return false;
  };

  ValidationResult result;
  for (const mybinlog::RuleId id : kRules) {
    LookupContext ctx{lookup, context, {}, {}, false};
    std::vector<mybinlog::Violation> violations;
    mybinlog::EvaluateServerSettings(adapter, &ctx, flavor, &id, 1, &violations);
    if (ctx.failed) {
      const VariableValue& query = ctx.failure;
      // A failed round trip is not a verdict on the setting; keep its own code.
      result.error =
          query.status == VariableQueryStatus::kQueryError ? query.query_error : MES_ERR_VALIDATION;
      std::snprintf(result.message, sizeof(result.message), "Failed to query %s: %s",
                    ctx.failed_variable.c_str(),
                    query.error_message.empty() ? "unknown error" : query.error_message.c_str());
      return result;
    }
    if (violations.empty()) continue;

    const mybinlog::Violation& violation = violations.front();
    const mybinlog::ServerSettingRule& rule = mybinlog::GetRule(id);
    const char* name = violation.variable.c_str();
    result.error = MES_ERR_VALIDATION;
    if (violation.absent) {
      std::snprintf(result.message, sizeof(result.message), "Variable %s not found", name);
    } else if (rule.match == mybinlog::RuleMatch::kEquals) {
      std::snprintf(result.message, sizeof(result.message), "%s must be %s, got %s", name,
                    rule.value, violation.observed.c_str());
    } else if (rule.match == mybinlog::RuleMatch::kNotContains) {
      std::snprintf(result.message, sizeof(result.message), "%s must not contain %s", name,
                    rule.value);
    } else {
      std::snprintf(result.message, sizeof(result.message), "%s must not be %s", name, rule.value);
    }
    return result;
  }
  return result;
}

}  // namespace detail

ValidationResult ConnectionValidator::Validate(protocol::MysqlConnection* conn,
                                               mybinlog::ServerFlavor flavor) {
  ValidationResult result;

  if (conn == nullptr) {
    result.error = MES_ERR_VALIDATION;
    std::snprintf(result.message, sizeof(result.message), "MySQL connection is null");
    return result;
  }

  return detail::ValidateServerConfiguration(LookupConnectionVariable, conn, flavor);
}

}  // namespace mes
