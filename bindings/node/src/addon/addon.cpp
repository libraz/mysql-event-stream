// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#include <napi.h>

#include "client_wrap.h"
#include "engine_wrap.h"
#include "log_callback.h"

Napi::Object Init(Napi::Env env, Napi::Object exports) {
  EngineWrap::Init(env, exports);
  ClientWrap::Init(env, exports);
  mes_node::InitLogCallback(env, exports);

  exports.Set("hasClient", Napi::Boolean::New(env, true));

  return exports;
}

NODE_API_MODULE(mes, Init)
