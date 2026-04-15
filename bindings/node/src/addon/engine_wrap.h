// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#ifndef MES_NODE_ENGINE_WRAP_H_
#define MES_NODE_ENGINE_WRAP_H_

#include <napi.h>

#include "mes.h"

class EngineWrap : public Napi::ObjectWrap<EngineWrap> {
 public:
  static Napi::Object Init(Napi::Env env, Napi::Object exports);
  explicit EngineWrap(const Napi::CallbackInfo& info);
  ~EngineWrap();

 private:
  Napi::Value Feed(const Napi::CallbackInfo& info);
  Napi::Value NextEvent(const Napi::CallbackInfo& info);
  Napi::Value HasEvents(const Napi::CallbackInfo& info);
  Napi::Value GetPosition(const Napi::CallbackInfo& info);
  void Reset(const Napi::CallbackInfo& info);
  void SetMaxQueueSize(const Napi::CallbackInfo& info);
  void SetMaxEventSize(const Napi::CallbackInfo& info);
  Napi::Value GetMaxEventSize(const Napi::CallbackInfo& info);
  void SetIncludeDatabases(const Napi::CallbackInfo& info);
  void SetIncludeTables(const Napi::CallbackInfo& info);
  void SetExcludeTables(const Napi::CallbackInfo& info);
  void SetStringFilter(const Napi::CallbackInfo& info,
                       mes_error_t (*setter)(mes_engine_t*, const char**, size_t),
                       const char* method_name);
  void Destroy(const Napi::CallbackInfo& info);

  Napi::Value ReadColumns(Napi::Env env, const mes_column_t* cols, uint32_t count);

  mes_engine_t* engine_;

  Napi::Value EnableMetadata(const Napi::CallbackInfo& info);
};

#endif  // MES_NODE_ENGINE_WRAP_H_
