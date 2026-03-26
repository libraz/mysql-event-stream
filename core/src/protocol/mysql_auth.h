// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * @file mysql_auth.h
 * @brief MySQL authentication response generators
 *
 * Implements the client-side auth response computation for
 * mysql_native_password and caching_sha2_password plugins.
 */

#ifndef MES_PROTOCOL_MYSQL_AUTH_H_
#define MES_PROTOCOL_MYSQL_AUTH_H_

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "mes.h"

namespace mes::protocol {

/**
 * @brief Generate auth response for mysql_native_password
 *
 * Algorithm: XOR(SHA1(password), SHA1(salt + SHA1(SHA1(password))))
 *
 * @param password   User password (plaintext)
 * @param salt       Concatenated auth_plugin_data from server (20 bytes)
 * @param salt_len   Length of salt
 * @param response   Output: 20-byte auth response (empty for empty password)
 * @return MES_OK on success, MES_ERR_AUTH on hash failure
 */
mes_error_t AuthNativePassword(const std::string& password,
                               const uint8_t* salt, size_t salt_len,
                               std::vector<uint8_t>* response);

/**
 * @brief Generate auth response for caching_sha2_password (fast auth)
 *
 * Algorithm: XOR(SHA256(password), SHA256(SHA256(SHA256(password)) + salt))
 *
 * @param password   User password (plaintext)
 * @param salt       Concatenated auth_plugin_data from server (20 bytes)
 * @param salt_len   Length of salt
 * @param response   Output: 32-byte auth response (empty for empty password)
 * @return MES_OK on success, MES_ERR_AUTH on hash failure
 */
mes_error_t AuthCachingSha2Password(const std::string& password,
                                    const uint8_t* salt, size_t salt_len,
                                    std::vector<uint8_t>* response);

}  // namespace mes::protocol

#endif  // MES_PROTOCOL_MYSQL_AUTH_H_
