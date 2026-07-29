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
  const std::vector<uint8_t> expected = {0xbc, 0xcb, 0x5c, 0xac, 0x49, 0xb1, 0x43,
                                         0x08, 0x78, 0xe1, 0xfe, 0x07, 0xdf, 0xf9,
                                         0x23, 0xde, 0x69, 0xb4, 0x77, 0xaf};
  EXPECT_EQ(response, expected);
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
  const std::vector<uint8_t> expected = {
      0xff, 0xba, 0xf7, 0x7a, 0xba, 0xca, 0xb5, 0x85, 0x9b, 0x43, 0x37,
      0x5f, 0x82, 0x9a, 0x96, 0x6e, 0xe7, 0x20, 0xd2, 0xdb, 0xb5, 0x2d,
      0x10, 0x6e, 0xa3, 0xde, 0x76, 0x1e, 0x5b, 0x7e, 0x5c, 0x8e,
  };
  EXPECT_EQ(response, expected);
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
