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
 * @brief Validates MySQL server configuration for binlog streaming
 *
 * Checks:
 * 1. log_bin = ON
 * 2. gtid_mode = ON
 * 3. binlog_format = ROW
 * 4. binlog_row_image = FULL
 * 5. binlog_transaction_compression = OFF
 */
class ConnectionValidator {
 public:
  /**
   * @brief Validate server configuration
   * @param conn Active MySQL connection
   * @return ValidationResult with error/message/uuid
   */
  static ValidationResult Validate(protocol::MysqlConnection* conn,
                                   ServerFlavor flavor = ServerFlavor::kMySQL);
};

}  // namespace mes

#endif  // MES_CLIENT_CONNECTION_VALIDATOR_H_
