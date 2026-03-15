// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * @file connection_validator.h
 * @brief MySQL server configuration validation for binlog streaming
 */

#ifndef MES_CLIENT_CONNECTION_VALIDATOR_H_
#define MES_CLIENT_CONNECTION_VALIDATOR_H_

#ifdef MES_HAS_MYSQL

#include <mysql.h>

#include "mes.h"

namespace mes {

/**
 * @brief Result of server configuration validation
 */
struct ValidationResult {
  mes_error_t error = MES_OK;
  char message[256] = {};
  char server_uuid[37] = {};  // UUID string (36 chars + null)
};

/**
 * @brief Validates MySQL server configuration for binlog streaming
 *
 * Checks:
 * 1. gtid_mode = ON
 * 2. binlog_format = ROW
 * 3. binlog_row_image = FULL
 * 4. binlog_transaction_compression = OFF
 * 5. Retrieves server_uuid
 */
class ConnectionValidator {
 public:
  /**
   * @brief Validate server configuration
   * @param conn Active MySQL connection
   * @return ValidationResult with error/message/uuid
   */
  static ValidationResult Validate(MYSQL* conn);

 private:
  static bool CheckVariable(MYSQL* conn, const char* var_name,
                            const char* expected, ValidationResult* result);
  static bool CheckVariableNot(MYSQL* conn, const char* var_name,
                               const char* rejected, ValidationResult* result);
  static bool FetchServerUuid(MYSQL* conn, ValidationResult* result);
};

}  // namespace mes

#endif  // MES_HAS_MYSQL
#endif  // MES_CLIENT_CONNECTION_VALIDATOR_H_
