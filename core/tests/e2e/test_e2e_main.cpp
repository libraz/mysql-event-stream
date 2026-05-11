// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#include <gtest/gtest.h>

#include "test_e2e_helpers.h"

namespace {

class E2eEnvironment : public ::testing::Environment {
 public:
  void SetUp() override {
    if (!e2e::IsE2eServerAvailable()) {
      GTEST_SKIP() << "E2E database is not available at " << e2e::kHost << ":" << e2e::kPort;
    }
  }
};

}  // namespace

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  ::testing::AddGlobalTestEnvironment(new E2eEnvironment());
  return RUN_ALL_TESTS();
}
