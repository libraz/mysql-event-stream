// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#ifndef MES_NODE_ADDON_CONSTANTS_H_
#define MES_NODE_ADDON_CONSTANTS_H_

#include <cstdint>

// 2^53 - 1: JavaScript Number.MAX_SAFE_INTEGER
// Values above this lose precision when stored as IEEE 754 double
constexpr int64_t kMaxSafeInteger = 9007199254740991LL;

constexpr uint16_t kDefaultPort = 3306;
constexpr uint32_t kDefaultServerId = 1;
constexpr uint32_t kDefaultConnectTimeoutS = 10;
constexpr uint32_t kDefaultReadTimeoutS = 30;

#endif  // MES_NODE_ADDON_CONSTANTS_H_
