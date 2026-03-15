// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#include <napi.h>

#include "engine_wrap.h"

#ifdef MES_HAS_MYSQL
#include "client_wrap.h"
#endif

Napi::Object Init(Napi::Env env, Napi::Object exports) {
  EngineWrap::Init(env, exports);

#ifdef MES_HAS_MYSQL
  ClientWrap::Init(env, exports);
#endif

  exports.Set("hasClient",
              Napi::Boolean::New(env,
#ifdef MES_HAS_MYSQL
                                 true
#else
                                 false
#endif
                                 ));

  return exports;
}

NODE_API_MODULE(mes, Init)
