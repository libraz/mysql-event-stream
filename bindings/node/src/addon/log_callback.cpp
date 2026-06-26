// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#include "log_callback.h"

#include <mutex>
#include <string>

#include "mes.h"

namespace mes_node {

namespace {

// The C core stores a single process-wide log callback, so this state is
// likewise process-wide. `g_tsfn` marshals calls arriving on the core's
// reader thread back to the JS thread; `g_mutex` guards install/teardown
// against a concurrent C callback.
std::mutex g_mutex;
Napi::ThreadSafeFunction g_tsfn;
bool g_active = false;

// Invoked by the C core, possibly from a non-JS thread. Copies the message and
// hands it to the thread-safe function for delivery on the JS thread.
void CLogTrampoline(mes_log_level_t level, const char* message, void* /*userdata*/) {
  std::lock_guard<std::mutex> lock(g_mutex);
  if (!g_active) return;
  std::string msg = message != nullptr ? message : "";
  int level_value = static_cast<int>(level);
  // NonBlockingCall: never stall the core's processing thread on a slow or
  // backed-up JS event loop. Dropped calls under saturation are acceptable for
  // diagnostics.
  g_tsfn.NonBlockingCall([level_value, msg](Napi::Env env, Napi::Function js_cb) {
    js_cb.Call({Napi::Number::New(env, level_value), Napi::String::New(env, msg)});
  });
}

// setLogCallback(callback | null, level)
Napi::Value SetLogCallback(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();

  mes_log_level_t level = MES_LOG_WARN;
  if (info.Length() >= 2 && info[1].IsNumber()) {
    level = static_cast<mes_log_level_t>(info[1].As<Napi::Number>().Int32Value());
  }

  // Detach when no function is supplied.
  if (info.Length() < 1 || info[0].IsNull() || info[0].IsUndefined()) {
    // Stop the core from calling us before tearing down the trampoline.
    mes_set_log_callback(nullptr, level, nullptr);
    {
      std::lock_guard<std::mutex> lock(g_mutex);
      g_active = false;
    }
    if (g_tsfn) {
      g_tsfn.Release();
      g_tsfn = Napi::ThreadSafeFunction();
    }
    return env.Undefined();
  }

  if (!info[0].IsFunction()) {
    Napi::TypeError::New(env, "setLogCallback expects a function or null")
        .ThrowAsJavaScriptException();
    return env.Undefined();
  }

  // Replace any prior registration. Detach the core first, then swap the TSFN.
  mes_set_log_callback(nullptr, level, nullptr);
  {
    std::lock_guard<std::mutex> lock(g_mutex);
    g_active = false;
  }
  if (g_tsfn) {
    g_tsfn.Release();
    g_tsfn = Napi::ThreadSafeFunction();
  }

  g_tsfn = Napi::ThreadSafeFunction::New(env, info[0].As<Napi::Function>(), "mesLogCallback",
                                         /*max_queue_size=*/0, /*initial_thread_count=*/1);
  {
    std::lock_guard<std::mutex> lock(g_mutex);
    g_active = true;
  }
  mes_set_log_callback(&CLogTrampoline, level, nullptr);
  return env.Undefined();
}

}  // namespace

void InitLogCallback(Napi::Env env, Napi::Object exports) {
  exports.Set("setLogCallback", Napi::Function::New(env, SetLogCallback));
}

}  // namespace mes_node
