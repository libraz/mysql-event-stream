// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#ifndef MES_NODE_LOG_CALLBACK_H_
#define MES_NODE_LOG_CALLBACK_H_

#include <napi.h>

namespace mes_node {

/**
 * @brief Register the module-level `setLogCallback(cb, level)` export.
 *
 * Exposes the C core's process-wide structured-log callback to JavaScript.
 * Because the core may invoke the callback from its reader thread, delivery is
 * marshalled to the JS thread via a Napi::ThreadSafeFunction.
 */
void InitLogCallback(Napi::Env env, Napi::Object exports);

}  // namespace mes_node

#endif  // MES_NODE_LOG_CALLBACK_H_
