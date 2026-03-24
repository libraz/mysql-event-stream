// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#ifndef MES_NODE_CLIENT_WRAP_H_
#define MES_NODE_CLIENT_WRAP_H_

#include <napi.h>

#include "mes.h"

class ClientWrap : public Napi::ObjectWrap<ClientWrap> {
 public:
  static Napi::Object Init(Napi::Env env, Napi::Object exports);
  explicit ClientWrap(const Napi::CallbackInfo& info);
  ~ClientWrap();

 private:
  void Connect(const Napi::CallbackInfo& info);
  void Start(const Napi::CallbackInfo& info);
  Napi::Value Poll(const Napi::CallbackInfo& info);
  void Stop(const Napi::CallbackInfo& info);
  void Disconnect(const Napi::CallbackInfo& info);
  void Destroy(const Napi::CallbackInfo& info);
  Napi::Value GetIsConnected(const Napi::CallbackInfo& info);
  Napi::Value GetLastError(const Napi::CallbackInfo& info);
  Napi::Value GetCurrentGtid(const Napi::CallbackInfo& info);

  mes_client_t* client_;
};

#endif  // MES_NODE_CLIENT_WRAP_H_
