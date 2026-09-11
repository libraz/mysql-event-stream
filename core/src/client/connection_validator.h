// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * @file connection_validator.h
 * @brief MySQL server configuration validation for binlog streaming
 */

#ifndef MES_CLIENT_CONNECTION_VALIDATOR_H_
#define MES_CLIENT_CONNECTION_VALIDATOR_H_

#include <string>

#include "mes.h"
#include "protocol/mysql_connection.h"
#include "protocol/mysql_query.h"
#include "server_flavor.h"

namespace mes {

/** @brief Result of server configuration validation. */
struct ValidationResult {
  mes_error_t error = MES_OK;
  char message[256] = {};
};

namespace detail {

enum class VariableQueryStatus {
  kFound,
  kNotFound,
  kMalformed,
  kQueryError,
};

/** Classify a SHOW VARIABLES result without conflating absence with failure. */
VariableQueryStatus ClassifyVariableQueryResult(mes_error_t query_error,
                                                const protocol::QueryResult& result);

/** A variable lookup result used by configuration validation. */
struct VariableValue {
  VariableQueryStatus status = VariableQueryStatus::kQueryError;
  std::string value;
  std::string error_message;
};

using VariableLookup = VariableValue (*)(void* context, const char* variable_name);

/** Validate required binlog variables using a caller-provided lookup function. */
ValidationResult ValidateServerConfiguration(VariableLookup lookup, void* context,
                                             ServerFlavor flavor);

}  // namespace detail

/**
 * @brief Validates MySQL/MariaDB server configuration for binlog streaming
 *
 * These are the settings that gate Connect() with MES_ERR_VALIDATION, in the
 * order they run; the first failure returns and the rest are never queried:
 * 1. log_bin must be ON
 * 2. gtid_mode must be ON (MySQL only: MariaDB has no such variable)
 * 3. binlog_format must be ROW
 * 4. binlog_row_image must be FULL
 * 5. log_bin_compress must not be ON (MariaDB only)
 * 6. binlog_transaction_compression must not be ON (MySQL only)
 * 7. binlog_row_value_options must not be PARTIAL_JSON (MySQL only)
 *
 * The two phrasings differ in how they treat a server that does not define the
 * variable at all. A must-be check fails, because the stream cannot proceed
 * without knowing the setting. A must-not-be check passes, because a server too
 * old to define the variable is also too old to have enabled what it rejects.
 */
class ConnectionValidator {
 public:
  /**
   * @brief Validate server configuration
   * @param conn Active MySQL connection
   * @param flavor Server flavor, which selects the flavor-specific checks
   * @return ValidationResult carrying MES_OK, or MES_ERR_VALIDATION and the
   *         message naming the setting that failed
   */
  static ValidationResult Validate(protocol::MysqlConnection* conn,
                                   ServerFlavor flavor = ServerFlavor::kMySQL);
};

}  // namespace mes

#endif  // MES_CLIENT_CONNECTION_VALIDATOR_H_
