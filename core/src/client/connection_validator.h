// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * @file connection_validator.h
 * @brief MySQL server configuration validation for binlog streaming
 */

#ifndef MES_CLIENT_CONNECTION_VALIDATOR_H_
#define MES_CLIENT_CONNECTION_VALIDATOR_H_

#include "mes.h"
#include "protocol/mysql_connection.h"
#include "server_flavor.h"

namespace mes {

/**
 * @brief Result of server configuration validation
 */
struct ValidationResult {
  mes_error_t error = MES_OK;
  char message[256] = {};
};

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

 private:
  static bool CheckVariable(protocol::MysqlConnection* conn,
                             const char* var_name, const char* expected,
                             ValidationResult* result);
  static bool CheckVariableNot(protocol::MysqlConnection* conn,
                                const char* var_name, const char* rejected,
                                ValidationResult* result);
};

}  // namespace mes

#endif  // MES_CLIENT_CONNECTION_VALIDATOR_H_
