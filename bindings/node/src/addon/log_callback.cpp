// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#include "log_callback.h"

#include <cstdint>
#include <limits>
#include <memory>
#include <mutex>
#include <string>

#include "mes.h"

namespace mes_node {

namespace {

// The C core stores a single process-wide log callback, so this state is
// likewise process-wide. `g_tsfn` marshals calls arriving on the core's
// reader thread back to the JS thread; `g_mutex` guards install/teardown
// against a concurrent C callback.
struct LogMessage {
  int level;
  std::string message;
  uint64_t dropped_before;
};

void CallJsLog(Napi::Env env, Napi::Function js_cb, void* /*context*/, LogMessage* message) {
  // N-API invokes the call-js callback with a null environment while aborting
  // a TSFN. Always delete the queued payload, but do not touch JS in that case.
  std::unique_ptr<LogMessage> owned(message);
  if (env == nullptr || js_cb.IsEmpty()) return;

  if (owned->dropped_before > 0) {
    js_cb.Call({Napi::Number::New(env, MES_LOG_WARN),
                Napi::String::New(env, "event=node_log_queue_overflow dropped=" +
                                           std::to_string(owned->dropped_before))});
  }
  js_cb.Call({Napi::Number::New(env, owned->level), Napi::String::New(env, owned->message)});
}

using LogTsfn = Napi::TypedThreadSafeFunction<void, LogMessage, CallJsLog>;

constexpr size_t kMaxPendingLogMessages = 256;

// Registration changes can originate from separate Worker environments.
// Serialize the multi-step detach/create/install sequence across them.
std::mutex g_registration_mutex;
std::mutex g_mutex;
LogTsfn g_tsfn;
bool g_active = false;
napi_env g_owner_env = nullptr;
void* g_registration_token = nullptr;
uintptr_t g_next_registration = 0;
uint64_t g_dropped_messages = 0;

LogTsfn DetachLocked() {
  if (g_active) {
    mes_set_log_callback(nullptr, MES_LOG_WARN, nullptr);
  }
  g_active = false;
  g_owner_env = nullptr;
  g_registration_token = nullptr;
  g_dropped_messages = 0;
  LogTsfn previous = g_tsfn;
  g_tsfn = LogTsfn();
  return previous;
}

void AbortTsfn(const LogTsfn& tsfn) {
  if (tsfn) {
    (void)tsfn.Abort();
  }
}

void CleanupEnvironment(napi_env env) {
  std::lock_guard<std::mutex> registration_lock(g_registration_mutex);
  LogTsfn previous;
  {
    std::lock_guard<std::mutex> lock(g_mutex);
    if (!g_active || g_owner_env != env) return;
    previous = DetachLocked();
  }
  AbortTsfn(previous);
}

// Invoked by the C core, possibly from a non-JS thread. Copies the message and
// hands it to the thread-safe function for delivery on the JS thread.
void CLogTrampoline(mes_log_level_t level, const char* message, void* userdata) {
  std::lock_guard<std::mutex> lock(g_mutex);
  if (!g_active || userdata != g_registration_token) return;

  auto* queued = new LogMessage{static_cast<int>(level), message != nullptr ? message : "",
                                g_dropped_messages};
  g_dropped_messages = 0;
  // NonBlockingCall: never stall the core's processing thread on a slow or
  // backed-up JS event loop. The queue is bounded; report accumulated drops
  // with the next successfully queued record.
  const napi_status status = g_tsfn.NonBlockingCall(queued);
  if (status != napi_ok) {
    const uint64_t restore = queued->dropped_before == std::numeric_limits<uint64_t>::max()
                                 ? queued->dropped_before
                                 : queued->dropped_before + 1;
    if (std::numeric_limits<uint64_t>::max() - g_dropped_messages < restore) {
      g_dropped_messages = std::numeric_limits<uint64_t>::max();
    } else {
      g_dropped_messages += restore;
    }
    delete queued;
  }
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
    std::lock_guard<std::mutex> registration_lock(g_registration_mutex);
    LogTsfn previous;
    {
      std::lock_guard<std::mutex> lock(g_mutex);
      previous = DetachLocked();
    }
    AbortTsfn(previous);
    return env.Undefined();
  }

  if (!info[0].IsFunction()) {
    Napi::TypeError::New(env, "setLogCallback expects a function or null")
        .ThrowAsJavaScriptException();
    return env.Undefined();
  }

  std::lock_guard<std::mutex> registration_lock(g_registration_mutex);

  // Replace any prior registration. Detach the core first, then abort its
  // bounded queue so queued message ownership is released by CallJsLog.
  LogTsfn previous;
  {
    std::lock_guard<std::mutex> lock(g_mutex);
    previous = DetachLocked();
  }
  AbortTsfn(previous);

  LogTsfn next = LogTsfn::New(env, info[0].As<Napi::Function>(), "mesLogCallback",
                              kMaxPendingLogMessages, /*initial_thread_count=*/1);
  if (!next) return env.Undefined();
  next.Unref(env);

  {
    std::lock_guard<std::mutex> lock(g_mutex);
    ++g_next_registration;
    if (g_next_registration == 0) ++g_next_registration;
    g_registration_token = reinterpret_cast<void*>(g_next_registration);
    g_tsfn = next;
    g_owner_env = env;
    g_dropped_messages = 0;
    g_active = true;
    mes_set_log_callback(&CLogTrampoline, level, g_registration_token);
  }
  return env.Undefined();
}

}  // namespace

void InitLogCallback(Napi::Env env, Napi::Object exports) {
  const napi_env raw_env = env;
  env.AddCleanupHook([raw_env]() { CleanupEnvironment(raw_env); });
  exports.Set("setLogCallback", Napi::Function::New(env, SetLogCallback));
}

}  // namespace mes_node
