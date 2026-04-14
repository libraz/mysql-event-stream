// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#include <gtest/gtest.h>

#include <cstdint>
#include <vector>

#include "protocol/mysql_auth.h"

namespace mes::protocol {
namespace {

const uint8_t kZeroSalt[20] = {};

// --- AuthNativePassword ---

TEST(AuthNativePasswordTest, EmptyPasswordReturnsEmptyResponse) {
  std::vector<uint8_t> response;
  auto err = AuthNativePassword("", kZeroSalt, 20, &response);
  EXPECT_EQ(err, MES_OK);
  EXPECT_TRUE(response.empty());
}

TEST(AuthNativePasswordTest, KnownPasswordProduces20Bytes) {
  std::vector<uint8_t> response;
  auto err = AuthNativePassword("root", kZeroSalt, 20, &response);
  EXPECT_EQ(err, MES_OK);
  EXPECT_EQ(response.size(), 20u);

  // Verify at least one byte is non-zero
  bool all_zero = true;
  for (uint8_t b : response) {
    if (b != 0) {
      all_zero = false;
      break;
    }
  }
  EXPECT_FALSE(all_zero);
}

TEST(AuthNativePasswordTest, Determinism) {
  std::vector<uint8_t> r1, r2;
  AuthNativePassword("root", kZeroSalt, 20, &r1);
  AuthNativePassword("root", kZeroSalt, 20, &r2);
  EXPECT_EQ(r1, r2);
}

// --- AuthCachingSha2Password ---

TEST(AuthCachingSha2PasswordTest, EmptyPasswordReturnsEmptyResponse) {
  std::vector<uint8_t> response;
  auto err = AuthCachingSha2Password("", kZeroSalt, 20, &response);
  EXPECT_EQ(err, MES_OK);
  EXPECT_TRUE(response.empty());
}

TEST(AuthCachingSha2PasswordTest, KnownPasswordProduces32Bytes) {
  std::vector<uint8_t> response;
  auto err = AuthCachingSha2Password("root", kZeroSalt, 20, &response);
  EXPECT_EQ(err, MES_OK);
  EXPECT_EQ(response.size(), 32u);

  bool all_zero = true;
  for (uint8_t b : response) {
    if (b != 0) {
      all_zero = false;
      break;
    }
  }
  EXPECT_FALSE(all_zero);
}

TEST(AuthCachingSha2PasswordTest, Determinism) {
  std::vector<uint8_t> r1, r2;
  AuthCachingSha2Password("root", kZeroSalt, 20, &r1);
  AuthCachingSha2Password("root", kZeroSalt, 20, &r2);
  EXPECT_EQ(r1, r2);
}

// --- Cross-algorithm comparison ---

TEST(AuthCrossAlgorithmTest, DifferentAlgorithmsProduceDifferentResults) {
  std::vector<uint8_t> native_resp, caching_resp;
  AuthNativePassword("root", kZeroSalt, 20, &native_resp);
  AuthCachingSha2Password("root", kZeroSalt, 20, &caching_resp);

  // Different sizes (20 vs 32) already means different, but verify explicitly
  EXPECT_NE(native_resp.size(), caching_resp.size());
}

}  // namespace
}  // namespace mes::protocol
