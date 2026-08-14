// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#ifndef MES_NODE_CLIENT_WRAP_H_
#define MES_NODE_CLIENT_WRAP_H_

#include <napi.h>

#include <atomic>
#include <string>

#include "mes.h"

class ClientWrap : public Napi::ObjectWrap<ClientWrap> {
 public:
  static Napi::Object Init(Napi::Env env, Napi::Object exports);
  explicit ClientWrap(const Napi::CallbackInfo& info);
  ~ClientWrap();

  /** @brief Called by PollWorker on the main thread when work completes. */
  void OnPollWorkerComplete();

  /**
   * @brief Hold a terminal poll error until the next poll/pollBatch call.
   *
   * mes_client_poll_batch() reports a terminal condition as the final element
   * of a batch whose earlier elements are real events. Those events are
   * delivered to the caller, so the error has to wait rather than replace
   * them; the C ABI checkpoint advances as if the batch was consumed.
   */
  void LatchTerminalError(mes_error_t error, const std::string& message);

 private:
  void Connect(const Napi::CallbackInfo& info);
  void Start(const Napi::CallbackInfo& info);
  Napi::Value Poll(const Napi::CallbackInfo& info);
  Napi::Value PollBatch(const Napi::CallbackInfo& info);
  void Stop(const Napi::CallbackInfo& info);
  void Disconnect(const Napi::CallbackInfo& info);
  void Destroy(const Napi::CallbackInfo& info);
  Napi::Value GetIsConnected(const Napi::CallbackInfo& info);
  Napi::Value GetIsStreaming(const Napi::CallbackInfo& info);
  Napi::Value GetLastError(const Napi::CallbackInfo& info);
  Napi::Value GetCurrentGtid(const Napi::CallbackInfo& info);
  Napi::Value GetFlavor(const Napi::CallbackInfo& info);
  Napi::Value GetChecksumEnabled(const Napi::CallbackInfo& info);
  Napi::Value GetQueuedBytes(const Napi::CallbackInfo& info);
  Napi::Value GetMaxQueueBytes(const Napi::CallbackInfo& info);
  Napi::Value GetMaxEventSize(const Napi::CallbackInfo& info);
  Napi::Value GetCrcErrors(const Napi::CallbackInfo& info);

  /** @brief Finalize deferred destroy if no workers remain in flight. */
  void MaybeFinalizeDeferredDestroy();

  /** @brief Reject lifecycle changes while the native poll buffer is borrowed. */
  bool RejectIfPollInFlight(Napi::Env env, const char* operation) const;

  /** @brief Take the pending terminal error, if any, clearing it. */
  bool TakeLatchedError(mes_error_t* error, std::string* message);

  mes_client_t* client_;
  mes_error_t latched_error_ = MES_OK;
  std::string latched_error_message_;
  std::atomic<int> pending_workers_{0};
  // N-API callbacks are serialized on the JS thread, but keep as atomic to
  // defensively document the shared-state contract with PollWorker completion.
  std::atomic<bool> destroy_pending_{false};
};

#endif  // MES_NODE_CLIENT_WRAP_H_
