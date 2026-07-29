// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#include "client_wrap.h"

#include <cmath>
#include <cstring>
#include <string>
#include <vector>

#include "addon_constants.h"
#include "config_parser.h"
#include "mes_error_util.h"

/** @brief AsyncWorker for non-blocking poll() on the libuv thread pool. */
class PollWorker : public Napi::AsyncWorker {
 public:
  PollWorker(Napi::Env env, mes_client_t* client, ClientWrap* wrap,
             Napi::Promise::Deferred deferred)
      : Napi::AsyncWorker(env),
        client_(client),
        wrap_(wrap),
        deferred_(deferred),
        error_(MES_OK),
        is_heartbeat_(false) {}

  void Execute() override {
    mes_poll_result_t result = mes_client_poll(client_);
    error_ = result.error;
    is_heartbeat_ = result.is_heartbeat != 0;

    if (result.data && result.size > 0 && error_ == MES_OK) {
      data_.assign(result.data, result.data + result.size);
    }
  }

  void OnOK() override {
    Napi::Env env = Env();

    if (error_ != MES_OK) {
      const char* msg = mes_client_last_error(client_);
      std::string err_msg = msg ? msg : "poll failed";
      deferred_.Reject(
          mes_node::MakeMesError(env, "mes_client_poll failed: " + err_msg, error_).Value());
    } else {
      Napi::Object result = Napi::Object::New(env);

      if (!data_.empty()) {
        auto* moved = new std::vector<uint8_t>(std::move(data_));
        auto buffer = Napi::Buffer<uint8_t>::New(
            env, moved->data(), moved->size(),
            [](Napi::Env, uint8_t*, std::vector<uint8_t>* hint) { delete hint; }, moved);
        result.Set("data", buffer);
      } else {
        result.Set("data", env.Null());
      }

      result.Set("isHeartbeat", Napi::Boolean::New(env, is_heartbeat_));
      deferred_.Resolve(result);
    }

    wrap_->OnPollWorkerComplete();
  }

  void OnError(const Napi::Error& error) override {
    deferred_.Reject(error.Value());
    wrap_->OnPollWorkerComplete();
  }

 private:
  mes_client_t* client_;
  ClientWrap* wrap_;
  Napi::Promise::Deferred deferred_;
  mes_error_t error_;
  bool is_heartbeat_;
  std::vector<uint8_t> data_;
};

/** @brief AsyncWorker for a blocking poll followed by a queued-event drain. */
class PollBatchWorker : public Napi::AsyncWorker {
 public:
  PollBatchWorker(Napi::Env env, mes_client_t* client, ClientWrap* wrap,
                  Napi::Promise::Deferred deferred, size_t max_events)
      : Napi::AsyncWorker(env),
        client_(client),
        wrap_(wrap),
        deferred_(deferred),
        max_events_(max_events),
        error_(MES_OK) {}

  void Execute() override {
    std::vector<mes_poll_result_t> raw(max_events_);
    size_t count = 0;
    error_ = mes_client_poll_batch(client_, raw.data(), raw.size(), &count);
    if (error_ != MES_OK) return;

    results_.reserve(count);
    for (size_t i = 0; i < count; ++i) {
      if (raw[i].error != MES_OK) {
        error_ = raw[i].error;
        return;
      }
      BatchResult result;
      result.is_heartbeat = raw[i].is_heartbeat != 0;
      if (raw[i].data != nullptr && raw[i].size > 0) {
        result.data.assign(raw[i].data, raw[i].data + raw[i].size);
      }
      results_.push_back(std::move(result));
    }
  }

  void OnOK() override {
    Napi::Env env = Env();
    if (error_ != MES_OK) {
      const char* msg = mes_client_last_error(client_);
      std::string err_msg = msg ? msg : "poll batch failed";
      deferred_.Reject(
          mes_node::MakeMesError(env, "mes_client_poll_batch failed: " + err_msg, error_).Value());
    } else {
      Napi::Array output = Napi::Array::New(env, results_.size());
      for (size_t i = 0; i < results_.size(); ++i) {
        Napi::Object result = Napi::Object::New(env);
        if (!results_[i].data.empty()) {
          auto* moved = new std::vector<uint8_t>(std::move(results_[i].data));
          result.Set(
              "data",
              Napi::Buffer<uint8_t>::New(
                  env, moved->data(), moved->size(),
                  [](Napi::Env, uint8_t*, std::vector<uint8_t>* hint) { delete hint; }, moved));
        } else {
          result.Set("data", env.Null());
        }
        result.Set("isHeartbeat", Napi::Boolean::New(env, results_[i].is_heartbeat));
        output.Set(i, result);
      }
      deferred_.Resolve(output);
    }
    wrap_->OnPollWorkerComplete();
  }

  void OnError(const Napi::Error& error) override {
    deferred_.Reject(error.Value());
    wrap_->OnPollWorkerComplete();
  }

 private:
  struct BatchResult {
    bool is_heartbeat = false;
    std::vector<uint8_t> data;
  };

  mes_client_t* client_;
  ClientWrap* wrap_;
  Napi::Promise::Deferred deferred_;
  size_t max_events_;
  mes_error_t error_;
  std::vector<BatchResult> results_;
};

Napi::Object ClientWrap::Init(Napi::Env env, Napi::Object exports) {
  Napi::Function func =
      DefineClass(env, "BinlogClient",
                  {
                      InstanceMethod<&ClientWrap::Connect>("connect"),
                      InstanceMethod<&ClientWrap::Start>("start"),
                      InstanceMethod<&ClientWrap::Poll>("poll"),
                      InstanceMethod<&ClientWrap::PollBatch>("pollBatch"),
                      InstanceMethod<&ClientWrap::Stop>("stop"),
                      InstanceMethod<&ClientWrap::Disconnect>("disconnect"),
                      InstanceMethod<&ClientWrap::Destroy>("destroy"),
                      InstanceAccessor<&ClientWrap::GetIsConnected>("isConnected"),
                      InstanceAccessor<&ClientWrap::GetIsStreaming>("isStreaming"),
                      InstanceAccessor<&ClientWrap::GetLastError>("lastError"),
                      InstanceAccessor<&ClientWrap::GetCurrentGtid>("currentGtid"),
                      InstanceAccessor<&ClientWrap::GetFlavor>("flavor"),
                      InstanceAccessor<&ClientWrap::GetChecksumEnabled>("checksumEnabled"),
                      InstanceAccessor<&ClientWrap::GetQueuedBytes>("queuedBytes"),
                      InstanceAccessor<&ClientWrap::GetMaxQueueBytes>("maxQueueBytes"),
                      InstanceAccessor<&ClientWrap::GetMaxEventSize>("maxEventSize"),
                      InstanceAccessor<&ClientWrap::GetCrcErrors>("crcErrors"),
                  });

  exports.Set("BinlogClient", func);
  return exports;
}

ClientWrap::ClientWrap(const Napi::CallbackInfo& info)
    : Napi::ObjectWrap<ClientWrap>(info), client_(mes_client_create()) {
  if (!client_) {
    Napi::Error::New(info.Env(), "Failed to create mes client").ThrowAsJavaScriptException();
  }
}

ClientWrap::~ClientWrap() {
  if (client_) {
    mes_client_destroy(client_);
    client_ = nullptr;
  }
}

bool ClientWrap::RejectIfPollInFlight(Napi::Env env, const char* operation) const {
  if (pending_workers_.load(std::memory_order_acquire) == 0) return false;
  Napi::Error::New(env, std::string("Cannot ") + operation + " while poll() is in progress")
      .ThrowAsJavaScriptException();
  return true;
}

void ClientWrap::Connect(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  if (RejectIfPollInFlight(env, "connect")) return;
  if (!client_) {
    Napi::Error::New(env, "Client has been destroyed").ThrowAsJavaScriptException();
    return;
  }
  if (info.Length() < 1 || !info[0].IsObject()) {
    Napi::TypeError::New(env, "Expected config object").ThrowAsJavaScriptException();
    return;
  }

  Napi::Object config = info[0].As<Napi::Object>();

  mes_client_config_t c_config{};
  mes_node::ConfigStrings strings;
  if (!mes_node::ParseClientConfig(env, config, c_config, strings)) {
    return;  // JS exception already scheduled
  }

  // Client-specific fields not in the shared parser.
  // Single Get() per key: undefined returns env.Undefined(), which fails
  // IsString()/IsNumber() checks — same semantics as the prior Has()+Get().
  Napi::Value start_gtid_v = config.Get("startGtid");
  if (start_gtid_v.IsString()) {
    strings.start_gtid = start_gtid_v.As<Napi::String>().Utf8Value();
    c_config.start_gtid = strings.start_gtid.c_str();
    c_config.start_position_mode = MES_START_AT_GTID;
  }

  Napi::Value binlog_file_v = config.Get("startBinlogFile");
  Napi::Value binlog_position_v = config.Get("startBinlogPosition");
  if (!binlog_file_v.IsUndefined() || !binlog_position_v.IsUndefined()) {
    if (start_gtid_v.IsString()) {
      Napi::TypeError::New(env, "startGtid and startBinlogFile cannot be combined")
          .ThrowAsJavaScriptException();
      return;
    }
    if (!binlog_file_v.IsString() || !binlog_position_v.IsNumber()) {
      Napi::TypeError::New(
          env, "startBinlogFile (string) and startBinlogPosition (number) are required together")
          .ThrowAsJavaScriptException();
      return;
    }
    strings.binlog_file = binlog_file_v.As<Napi::String>().Utf8Value();
    const int64_t position = binlog_position_v.As<Napi::Number>().Int64Value();
    if (strings.binlog_file.empty() || position < 4 || position > UINT32_MAX) {
      Napi::RangeError::New(env, "startBinlogPosition must be between 4 and UINT32_MAX")
          .ThrowAsJavaScriptException();
      return;
    }
    c_config.start_position_mode = MES_START_AT_POSITION;
    c_config.binlog_file = strings.binlog_file.c_str();
    c_config.binlog_position = static_cast<uint64_t>(position);
  }

  Napi::Value max_queue_size_v = config.Get("maxQueueSize");
  if (max_queue_size_v.IsNumber()) {
    // max_queue_size is size_t (64-bit) in the C ABI. Read via Int64Value() to
    // avoid silently truncating large values, then reject anything negative.
    int64_t max_queue_size = max_queue_size_v.As<Napi::Number>().Int64Value();
    if (max_queue_size < 0) {
      Napi::RangeError::New(env, "maxQueueSize must be non-negative").ThrowAsJavaScriptException();
      return;
    }
    c_config.max_queue_size = static_cast<size_t>(max_queue_size);
  }

  uint32_t max_event_size = 32u * 1024u * 1024u;
  Napi::Value max_event_size_v = config.Get("maxEventSize");
  if (max_event_size_v.IsNumber()) {
    int64_t raw = max_event_size_v.As<Napi::Number>().Int64Value();
    if (raw < 0 || raw > UINT32_MAX) {
      Napi::RangeError::New(env, "maxEventSize must fit in uint32").ThrowAsJavaScriptException();
      return;
    }
    max_event_size = static_cast<uint32_t>(raw);
  }
  mes_error_t limit_err = mes_client_set_max_event_size(client_, max_event_size);
  if (limit_err != MES_OK) {
    mes_node::MakeMesError(env, "mes_client_set_max_event_size failed", limit_err)
        .ThrowAsJavaScriptException();
    return;
  }

  size_t max_queue_bytes = MES_DEFAULT_QUEUE_BYTES;
  Napi::Value max_queue_bytes_v = config.Get("maxQueueBytes");
  if (max_queue_bytes_v.IsNumber()) {
    int64_t raw = max_queue_bytes_v.As<Napi::Number>().Int64Value();
    if (raw < 0) {
      Napi::RangeError::New(env, "maxQueueBytes must be non-negative").ThrowAsJavaScriptException();
      return;
    }
    max_queue_bytes = static_cast<size_t>(raw);
  }
  limit_err = mes_client_set_max_queue_bytes(client_, max_queue_bytes);
  if (limit_err != MES_OK) {
    mes_node::MakeMesError(env, "mes_client_set_max_queue_bytes failed", limit_err)
        .ThrowAsJavaScriptException();
    return;
  }

  mes_error_t err = mes_client_connect(client_, &c_config);
  if (err != MES_OK) {
    // Note: verified last_error messages in core
    // (core/src/client/binlog_client.cpp and
    // core/src/protocol/mysql_connection.cpp) do not include credentials.
    // Only descriptive strings (optionally with host:port, auth plugin name,
    // or ssl_mode) are forwarded. Safe to surface directly.
    const char* msg = mes_client_last_error(client_);
    std::string err_msg = msg ? msg : "connection failed";
    mes_node::MakeMesError(env, "mes_client_connect failed: " + err_msg, err)
        .ThrowAsJavaScriptException();
  }
}

void ClientWrap::Start(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();

  if (RejectIfPollInFlight(env, "start")) return;

  if (!client_) {
    Napi::Error::New(env, "Client has been destroyed").ThrowAsJavaScriptException();
    return;
  }

  mes_error_t err = mes_client_start(client_);
  if (err != MES_OK) {
    const char* msg = mes_client_last_error(client_);
    std::string err_msg = msg ? msg : "start failed";
    mes_node::MakeMesError(env, "mes_client_start failed: " + err_msg, err)
        .ThrowAsJavaScriptException();
  }
}

Napi::Value ClientWrap::Poll(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();

  if (!client_ || destroy_pending_.load(std::memory_order_acquire)) {
    auto deferred = Napi::Promise::Deferred::New(env);
    deferred.Reject(Napi::Error::New(env, "Client has been destroyed").Value());
    return deferred.Promise();
  }

  // Only one poll() may be in flight at a time. The C ABI contract states
  // that the data pointer from mes_client_poll() is valid only until the
  // next call to mes_client_poll() on the same client.
  if (pending_workers_.load(std::memory_order_acquire) > 0) {
    auto deferred = Napi::Promise::Deferred::New(env);
    deferred.Reject(Napi::Error::New(env, "A poll() is already in progress").Value());
    return deferred.Promise();
  }

  auto deferred = Napi::Promise::Deferred::New(env);
  // acq_rel for consistency with the acquire load above; same-thread use
  // makes the ordering choice immaterial to performance.
  pending_workers_.fetch_add(1, std::memory_order_acq_rel);
  Ref();  // prevent GC while worker is in flight
  auto* worker = new PollWorker(env, client_, this, deferred);
  worker->Queue();
  return deferred.Promise();
}

Napi::Value ClientWrap::PollBatch(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  auto deferred = Napi::Promise::Deferred::New(env);
  if (!client_ || destroy_pending_.load(std::memory_order_acquire)) {
    deferred.Reject(Napi::Error::New(env, "Client has been destroyed").Value());
    return deferred.Promise();
  }
  if (pending_workers_.load(std::memory_order_acquire) > 0) {
    deferred.Reject(Napi::Error::New(env, "A poll() is already in progress").Value());
    return deferred.Promise();
  }

  size_t max_events = 64;
  if (info.Length() > 0) {
    if (!info[0].IsNumber()) {
      deferred.Reject(Napi::TypeError::New(env, "maxEvents must be a number").Value());
      return deferred.Promise();
    }
    const double value = info[0].As<Napi::Number>().DoubleValue();
    if (!std::isfinite(value) || value < 1 || value > 1024 || std::floor(value) != value) {
      deferred.Reject(
          Napi::RangeError::New(env, "maxEvents must be an integer between 1 and 1024").Value());
      return deferred.Promise();
    }
    max_events = static_cast<size_t>(value);
  }

  pending_workers_.fetch_add(1, std::memory_order_acq_rel);
  Ref();
  auto* worker = new PollBatchWorker(env, client_, this, deferred, max_events);
  worker->Queue();
  return deferred.Promise();
}

void ClientWrap::Stop(const Napi::CallbackInfo& info) {
  (void)info;
  if (client_) {
    mes_client_stop(client_);
  }
}

void ClientWrap::Disconnect(const Napi::CallbackInfo& info) {
  if (RejectIfPollInFlight(info.Env(), "disconnect")) return;
  if (client_) {
    mes_client_disconnect(client_);
  }
}

void ClientWrap::Destroy(const Napi::CallbackInfo& info) {
  (void)info;
  if (!client_) return;

  if (pending_workers_.load(std::memory_order_acquire) > 0) {
    // Workers are in flight on the thread pool. Stop the client to unblock
    // any blocking mes_client_poll() call, but defer the actual destroy
    // until the last worker completes on the main thread.
    mes_client_stop(client_);
    destroy_pending_.store(true, std::memory_order_release);
  } else {
    // Ensure the reader thread is stopped before destroying the client.
    // Without stop(), destroy() may block waiting for the thread to finish
    // a blocking network read.
    mes_client_stop(client_);
    mes_client_destroy(client_);
    client_ = nullptr;
  }
}

void ClientWrap::OnPollWorkerComplete() {
  pending_workers_.fetch_sub(1, std::memory_order_acq_rel);
  Unref();  // allow GC now that worker is done
  MaybeFinalizeDeferredDestroy();
}

void ClientWrap::MaybeFinalizeDeferredDestroy() {
  if (destroy_pending_.load(std::memory_order_acquire) && client_ &&
      pending_workers_.load(std::memory_order_acquire) == 0) {
    mes_client_destroy(client_);
    client_ = nullptr;
    destroy_pending_.store(false, std::memory_order_release);
  }
}

Napi::Value ClientWrap::GetIsConnected(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  if (!client_) return Napi::Boolean::New(env, false);
  return Napi::Boolean::New(env, mes_client_is_connected(client_) == 1);
}

Napi::Value ClientWrap::GetIsStreaming(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  if (!client_) return Napi::Boolean::New(env, false);
  return Napi::Boolean::New(env, mes_client_is_streaming(client_) == 1);
}

Napi::Value ClientWrap::GetLastError(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  if (!client_) return Napi::String::New(env, "");
  const char* msg = mes_client_last_error(client_);
  return Napi::String::New(env, msg ? msg : "");
}

Napi::Value ClientWrap::GetCurrentGtid(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  if (!client_) return Napi::String::New(env, "");
  const char* gtid = mes_client_current_gtid(client_);
  return Napi::String::New(env, gtid ? gtid : "");
}

Napi::Value ClientWrap::GetFlavor(const Napi::CallbackInfo& info) {
  const auto flavor = client_ == nullptr ? MES_SERVER_FLAVOR_MYSQL : mes_client_flavor(client_);
  return Napi::Number::New(info.Env(), static_cast<int>(flavor));
}

Napi::Value ClientWrap::GetChecksumEnabled(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  return Napi::Boolean::New(env, client_ != nullptr && mes_client_checksum_enabled(client_) != 0);
}

Napi::Value ClientWrap::GetQueuedBytes(const Napi::CallbackInfo& info) {
  return Napi::Number::New(info.Env(), static_cast<double>(mes_client_queued_bytes(client_)));
}

Napi::Value ClientWrap::GetMaxQueueBytes(const Napi::CallbackInfo& info) {
  return Napi::Number::New(info.Env(),
                           static_cast<double>(mes_client_get_max_queue_bytes(client_)));
}

Napi::Value ClientWrap::GetMaxEventSize(const Napi::CallbackInfo& info) {
  return Napi::Number::New(info.Env(), static_cast<double>(mes_client_get_max_event_size(client_)));
}

Napi::Value ClientWrap::GetCrcErrors(const Napi::CallbackInfo& info) {
  return Napi::Number::New(info.Env(), static_cast<double>(mes_client_crc_errors(client_)));
}
