// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#include "protocol/mysql_auth.h"

#include <openssl/crypto.h>
#include <openssl/evp.h>
#include <openssl/sha.h>

#include <cstring>

namespace mes::protocol {

namespace {

// RAII helper: guarantees OPENSSL_cleanse of sensitive buffers on every exit
// path (including early returns on SHA failures). Without this, intermediate
// password-derived hashes could remain in stack memory after an error return.
struct SecureCleanse {
  void* buf;
  size_t n;
  ~SecureCleanse() { OPENSSL_cleanse(buf, n); }
};

struct DigestPart {
  const void* data;
  size_t len;
};

bool ComputeDigest(const EVP_MD* digest, const DigestPart* parts, size_t part_count, uint8_t* out,
                   unsigned int expected_len) {
  EVP_MD_CTX* ctx = EVP_MD_CTX_new();
  if (!ctx) return false;

  bool ok = EVP_DigestInit_ex(ctx, digest, nullptr) == 1;
  for (size_t i = 0; ok && i < part_count; i++) {
    if (parts[i].len == 0) continue;
    ok = EVP_DigestUpdate(ctx, parts[i].data, parts[i].len) == 1;
  }

  unsigned int actual_len = 0;
  if (ok) {
    ok = EVP_DigestFinal_ex(ctx, out, &actual_len) == 1 && actual_len == expected_len;
  }
  EVP_MD_CTX_free(ctx);
  return ok;
}

}  // namespace

mes_error_t AuthNativePassword(const std::string& password, const uint8_t* salt, size_t salt_len,
                               std::vector<uint8_t>* response) {
  response->clear();

  // Empty password produces empty auth response
  if (password.empty()) {
    return MES_OK;
  }

  // Step 1: hash_stage1 = SHA1(password)
  uint8_t hash_stage1[SHA_DIGEST_LENGTH];
  SecureCleanse cleanse_stage1{hash_stage1, sizeof(hash_stage1)};
  DigestPart stage1_parts[] = {{password.data(), password.size()}};
  if (!ComputeDigest(EVP_sha1(), stage1_parts, 1, hash_stage1, SHA_DIGEST_LENGTH)) {
    return MES_ERR_AUTH;
  }

  // Step 2: hash_stage2 = SHA1(hash_stage1)
  uint8_t hash_stage2[SHA_DIGEST_LENGTH];
  SecureCleanse cleanse_stage2{hash_stage2, sizeof(hash_stage2)};
  DigestPart stage2_parts[] = {{hash_stage1, SHA_DIGEST_LENGTH}};
  if (!ComputeDigest(EVP_sha1(), stage2_parts, 1, hash_stage2, SHA_DIGEST_LENGTH)) {
    return MES_ERR_AUTH;
  }

  // Step 3: scramble = SHA1(salt + hash_stage2)
  uint8_t scramble[SHA_DIGEST_LENGTH];
  SecureCleanse cleanse_scramble{scramble, sizeof(scramble)};
  DigestPart scramble_parts[] = {{salt, salt_len}, {hash_stage2, SHA_DIGEST_LENGTH}};
  if (!ComputeDigest(EVP_sha1(), scramble_parts, 2, scramble, SHA_DIGEST_LENGTH)) {
    return MES_ERR_AUTH;
  }

  // Step 4: response = XOR(hash_stage1, scramble)
  response->resize(SHA_DIGEST_LENGTH);
  for (int i = 0; i < SHA_DIGEST_LENGTH; ++i) {
    (*response)[i] = hash_stage1[i] ^ scramble[i];
  }

  // hash_stage1 / hash_stage2 cleansed by SecureCleanse destructors.
  return MES_OK;
}

mes_error_t AuthCachingSha2Password(const std::string& password, const uint8_t* salt,
                                    size_t salt_len, std::vector<uint8_t>* response) {
  response->clear();

  // Empty password produces empty auth response
  if (password.empty()) {
    return MES_OK;
  }

  // Step 1: hash1 = SHA256(password)
  uint8_t hash1[SHA256_DIGEST_LENGTH];
  SecureCleanse cleanse_hash1{hash1, sizeof(hash1)};
  DigestPart hash1_parts[] = {{password.data(), password.size()}};
  if (!ComputeDigest(EVP_sha256(), hash1_parts, 1, hash1, SHA256_DIGEST_LENGTH)) {
    return MES_ERR_AUTH;
  }

  // Step 2: hash2 = SHA256(hash1)
  uint8_t hash2[SHA256_DIGEST_LENGTH];
  SecureCleanse cleanse_hash2{hash2, sizeof(hash2)};
  DigestPart hash2_parts[] = {{hash1, SHA256_DIGEST_LENGTH}};
  if (!ComputeDigest(EVP_sha256(), hash2_parts, 1, hash2, SHA256_DIGEST_LENGTH)) {
    return MES_ERR_AUTH;
  }

  // Step 3: hash3 = SHA256(hash2 + salt)
  uint8_t hash3[SHA256_DIGEST_LENGTH];
  SecureCleanse cleanse_hash3{hash3, sizeof(hash3)};
  DigestPart hash3_parts[] = {{hash2, SHA256_DIGEST_LENGTH}, {salt, salt_len}};
  if (!ComputeDigest(EVP_sha256(), hash3_parts, 2, hash3, SHA256_DIGEST_LENGTH)) {
    return MES_ERR_AUTH;
  }

  // Step 4: response = XOR(hash1, hash3)
  response->resize(SHA256_DIGEST_LENGTH);
  for (int i = 0; i < SHA256_DIGEST_LENGTH; ++i) {
    (*response)[i] = hash1[i] ^ hash3[i];
  }

  // hash1 / hash2 cleansed by SecureCleanse destructors.
  return MES_OK;
}

}  // namespace mes::protocol
