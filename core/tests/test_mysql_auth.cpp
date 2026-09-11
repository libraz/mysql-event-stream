// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#include <gtest/gtest.h>

#include <cstdint>
#include <cstring>
#include <vector>

#include "protocol/mysql_auth.h"

namespace mes::protocol {
namespace {

const uint8_t kZeroSalt[20] = {};

// A salt that is neither zero nor uniform. An all-zero salt cannot distinguish
// a correct implementation from one that drops the salt argument and hashes a
// zeroed local buffer, and a uniform salt cannot distinguish one that uses only
// the first byte; the golden responses below are computed over this one so that
// they are verifiably a function of the salt and not of the password alone.
const uint8_t kScrambleSalt[20] = {0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a,
                                   0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14};

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

/**
 * @brief The documented algorithm puts the salt ahead of the password hash.
 *
 * Computed independently of this codebase, with the independent implementation
 * first checked against the all-zero vector above. A response that dropped the
 * salt, used only part of it, or concatenated it after the password hash instead
 * of before would each differ from this.
 */
TEST(AuthNativePasswordTest, KnownPasswordAndSaltProduceTheDocumentedScramble) {
  std::vector<uint8_t> response;
  auto err = AuthNativePassword("root", kScrambleSalt, 20, &response);
  EXPECT_EQ(err, MES_OK);
  const std::vector<uint8_t> expected = {0x77, 0x62, 0xab, 0xf3, 0xfd, 0x98, 0x18,
                                         0xd9, 0xf6, 0x3e, 0x07, 0x9f, 0x85, 0x01,
                                         0x67, 0xcb, 0x14, 0x2a, 0x09, 0x65};
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

/**
 * @brief This algorithm puts the salt after the password hash, not before it.
 *
 * The mirror of the native vector, and the reason both are needed: the two
 * plugins concatenate the salt on opposite sides of the hash, so a single
 * implementation servicing both with one order would still satisfy whichever
 * plugin it happened to match.
 */
TEST(AuthCachingSha2PasswordTest, KnownPasswordAndSaltProduceTheDocumentedScramble) {
  std::vector<uint8_t> response;
  auto err = AuthCachingSha2Password("root", kScrambleSalt, 20, &response);
  EXPECT_EQ(err, MES_OK);
  const std::vector<uint8_t> expected = {
      0x4c, 0x37, 0xb7, 0xa3, 0xd2, 0xd0, 0x53, 0xb0, 0x48, 0x47, 0x43,
      0x5e, 0x71, 0xb1, 0x09, 0xa0, 0xb8, 0x2a, 0x26, 0x58, 0x57, 0x7c,
      0xbb, 0xcf, 0xea, 0xf0, 0xd7, 0x5c, 0xdb, 0xf9, 0xf5, 0xc8,
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

/**
 * @brief Every byte of the salt reaches the response, on both plugins.
 *
 * The golden vectors pin one salt each. This pins the dependency itself: an
 * implementation reading only the first eight bytes, or stopping at the first
 * zero byte, would still reproduce a vector whose dropped bytes happened not to
 * matter. Flipping each position in turn leaves a partial read nowhere to hide,
 * and it needs no precomputed answer to do it.
 */
TEST(AuthCrossAlgorithmTest, EveryByteOfTheSaltChangesBothResponses) {
  std::vector<uint8_t> native_base, caching_base;
  ASSERT_EQ(AuthNativePassword("root", kScrambleSalt, 20, &native_base), MES_OK);
  ASSERT_EQ(AuthCachingSha2Password("root", kScrambleSalt, 20, &caching_base), MES_OK);
  ASSERT_FALSE(native_base.empty());
  ASSERT_FALSE(caching_base.empty());

  for (size_t at = 0; at < sizeof(kScrambleSalt); ++at) {
    uint8_t altered[sizeof(kScrambleSalt)];
    std::memcpy(altered, kScrambleSalt, sizeof(altered));
    altered[at] ^= 0xff;

    std::vector<uint8_t> native_resp, caching_resp;
    ASSERT_EQ(AuthNativePassword("root", altered, sizeof(altered), &native_resp), MES_OK);
    ASSERT_EQ(AuthCachingSha2Password("root", altered, sizeof(altered), &caching_resp), MES_OK);
    EXPECT_NE(native_resp, native_base) << "salt byte " << at;
    EXPECT_NE(caching_resp, caching_base) << "salt byte " << at;
  }
}

}  // namespace
}  // namespace mes::protocol
