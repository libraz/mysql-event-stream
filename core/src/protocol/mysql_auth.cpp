// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#include "protocol/mysql_auth.h"

#include <openssl/crypto.h>
#include <openssl/sha.h>

#include <cstring>

namespace mes::protocol {

mes_error_t AuthNativePassword(const std::string& password,
                               const uint8_t* salt, size_t salt_len,
                               std::vector<uint8_t>* response) {
  response->clear();

  // Empty password produces empty auth response
  if (password.empty()) {
    return MES_OK;
  }

  // Step 1: hash_stage1 = SHA1(password)
  uint8_t hash_stage1[SHA_DIGEST_LENGTH];
  if (SHA1(reinterpret_cast<const uint8_t*>(password.data()), password.size(),
           hash_stage1) == nullptr) {
    return MES_ERR_AUTH;
  }

  // Step 2: hash_stage2 = SHA1(hash_stage1)
  uint8_t hash_stage2[SHA_DIGEST_LENGTH];
  if (SHA1(hash_stage1, SHA_DIGEST_LENGTH, hash_stage2) == nullptr) {
    return MES_ERR_AUTH;
  }

  // Step 3: scramble = SHA1(salt + hash_stage2)
  uint8_t scramble[SHA_DIGEST_LENGTH];
  SHA_CTX ctx;
  if (SHA1_Init(&ctx) != 1) {
    return MES_ERR_AUTH;
  }
  if (SHA1_Update(&ctx, salt, salt_len) != 1) {
    return MES_ERR_AUTH;
  }
  if (SHA1_Update(&ctx, hash_stage2, SHA_DIGEST_LENGTH) != 1) {
    return MES_ERR_AUTH;
  }
  if (SHA1_Final(scramble, &ctx) != 1) {
    return MES_ERR_AUTH;
  }

  // Step 4: response = XOR(hash_stage1, scramble)
  response->resize(SHA_DIGEST_LENGTH);
  for (int i = 0; i < SHA_DIGEST_LENGTH; ++i) {
    (*response)[i] = hash_stage1[i] ^ scramble[i];
  }

  OPENSSL_cleanse(hash_stage1, sizeof(hash_stage1));
  OPENSSL_cleanse(hash_stage2, sizeof(hash_stage2));
  return MES_OK;
}

mes_error_t AuthCachingSha2Password(const std::string& password,
                                    const uint8_t* salt, size_t salt_len,
                                    std::vector<uint8_t>* response) {
  response->clear();

  // Empty password produces empty auth response
  if (password.empty()) {
    return MES_OK;
  }

  // Step 1: hash1 = SHA256(password)
  uint8_t hash1[SHA256_DIGEST_LENGTH];
  if (SHA256(reinterpret_cast<const uint8_t*>(password.data()), password.size(),
             hash1) == nullptr) {
    return MES_ERR_AUTH;
  }

  // Step 2: hash2 = SHA256(hash1)
  uint8_t hash2[SHA256_DIGEST_LENGTH];
  if (SHA256(hash1, SHA256_DIGEST_LENGTH, hash2) == nullptr) {
    return MES_ERR_AUTH;
  }

  // Step 3: hash3 = SHA256(hash2 + salt)
  uint8_t hash3[SHA256_DIGEST_LENGTH];
  SHA256_CTX ctx;
  if (SHA256_Init(&ctx) != 1) {
    return MES_ERR_AUTH;
  }
  if (SHA256_Update(&ctx, hash2, SHA256_DIGEST_LENGTH) != 1) {
    return MES_ERR_AUTH;
  }
  if (SHA256_Update(&ctx, salt, salt_len) != 1) {
    return MES_ERR_AUTH;
  }
  if (SHA256_Final(hash3, &ctx) != 1) {
    return MES_ERR_AUTH;
  }

  // Step 4: response = XOR(hash1, hash3)
  response->resize(SHA256_DIGEST_LENGTH);
  for (int i = 0; i < SHA256_DIGEST_LENGTH; ++i) {
    (*response)[i] = hash1[i] ^ hash3[i];
  }

  OPENSSL_cleanse(hash1, sizeof(hash1));
  OPENSSL_cleanse(hash2, sizeof(hash2));
  return MES_OK;
}

}  // namespace mes::protocol
