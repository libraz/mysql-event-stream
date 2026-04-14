// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#include "client_wrap.h"

#include <cstring>
#include <string>
#include <vector>

#include "addon_constants.h"

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
          Napi::Error::New(env, "mes_client_poll failed: " + err_msg)
              .Value());
    } else {
      Napi::Object result = Napi::Object::New(env);

      if (!data_.empty()) {
        auto* moved = new std::vector<uint8_t>(std::move(data_));
        auto buffer = Napi::Buffer<uint8_t>::New(
            env, moved->data(), moved->size(),
            [](Napi::Env, uint8_t*, std::vector<uint8_t>* hint) {
              delete hint;
            },
            moved);
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

Napi::Object ClientWrap::Init(Napi::Env env, Napi::Object exports) {
  Napi::Function func = DefineClass(
      env, "BinlogClient",
      {
          InstanceMethod<&ClientWrap::Connect>("connect"),
          InstanceMethod<&ClientWrap::Start>("start"),
          InstanceMethod<&ClientWrap::Poll>("poll"),
          InstanceMethod<&ClientWrap::Stop>("stop"),
          InstanceMethod<&ClientWrap::Disconnect>("disconnect"),
          InstanceMethod<&ClientWrap::Destroy>("destroy"),
          InstanceAccessor<&ClientWrap::GetIsConnected>("isConnected"),
          InstanceAccessor<&ClientWrap::GetLastError>("lastError"),
          InstanceAccessor<&ClientWrap::GetCurrentGtid>("currentGtid"),
      });

  exports.Set("BinlogClient", func);
  return exports;
}

ClientWrap::ClientWrap(const Napi::CallbackInfo& info)
    : Napi::ObjectWrap<ClientWrap>(info), client_(mes_client_create()) {
  if (!client_) {
    Napi::Error::New(info.Env(), "Failed to create mes client")
        .ThrowAsJavaScriptException();
  }
}

ClientWrap::~ClientWrap() {
  if (client_) {
    mes_client_destroy(client_);
    client_ = nullptr;
  }
}

void ClientWrap::Connect(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();

  if (!client_) {
    Napi::Error::New(env, "Client has been destroyed")
        .ThrowAsJavaScriptException();
    return;
  }

  if (info.Length() < 1 || !info[0].IsObject()) {
    Napi::TypeError::New(env, "Expected config object")
        .ThrowAsJavaScriptException();
    return;
  }

  Napi::Object config = info[0].As<Napi::Object>();

  // Extract config values with defaults
  std::string host = "127.0.0.1";
  uint16_t port = kDefaultPort;
  std::string user = "root";
  std::string password;
  uint32_t server_id = kDefaultServerId;
  std::string start_gtid;
  uint32_t connect_timeout_s = kDefaultConnectTimeoutS;
  uint32_t read_timeout_s = kDefaultReadTimeoutS;

  if (config.Has("host") && config.Get("host").IsString()) {
    host = config.Get("host").As<Napi::String>().Utf8Value();
  }
  if (config.Has("port") && config.Get("port").IsNumber()) {
    port = static_cast<uint16_t>(
        config.Get("port").As<Napi::Number>().Uint32Value());
  }
  if (config.Has("user") && config.Get("user").IsString()) {
    user = config.Get("user").As<Napi::String>().Utf8Value();
  }
  if (config.Has("password") && config.Get("password").IsString()) {
    password = config.Get("password").As<Napi::String>().Utf8Value();
  }
  if (config.Has("serverId") && config.Get("serverId").IsNumber()) {
    server_id = config.Get("serverId").As<Napi::Number>().Uint32Value();
  }
  if (config.Has("startGtid") && config.Get("startGtid").IsString()) {
    start_gtid = config.Get("startGtid").As<Napi::String>().Utf8Value();
  }
  if (config.Has("connectTimeoutS") &&
      config.Get("connectTimeoutS").IsNumber()) {
    connect_timeout_s =
        config.Get("connectTimeoutS").As<Napi::Number>().Uint32Value();
  }
  if (config.Has("readTimeoutS") && config.Get("readTimeoutS").IsNumber()) {
    read_timeout_s =
        config.Get("readTimeoutS").As<Napi::Number>().Uint32Value();
  }

  uint32_t ssl_mode = 0;
  std::string ssl_ca;
  std::string ssl_cert;
  std::string ssl_key;

  if (config.Has("sslMode") && config.Get("sslMode").IsNumber()) {
    ssl_mode = config.Get("sslMode").As<Napi::Number>().Uint32Value();
    if (ssl_mode > 4) {
      Napi::TypeError::New(env, "sslMode must be 0-4")
          .ThrowAsJavaScriptException();
      return;
    }
  }
  if (config.Has("sslCa") && config.Get("sslCa").IsString()) {
    ssl_ca = config.Get("sslCa").As<Napi::String>().Utf8Value();
  }
  if (config.Has("sslCert") && config.Get("sslCert").IsString()) {
    ssl_cert = config.Get("sslCert").As<Napi::String>().Utf8Value();
  }
  if (config.Has("sslKey") && config.Get("sslKey").IsString()) {
    ssl_key = config.Get("sslKey").As<Napi::String>().Utf8Value();
  }

  uint32_t max_queue_size = 0;
  if (config.Has("maxQueueSize") && config.Get("maxQueueSize").IsNumber()) {
    max_queue_size = config.Get("maxQueueSize").As<Napi::Number>().Uint32Value();
  }

  mes_client_config_t c_config{};
  c_config.host = host.c_str();
  c_config.port = port;
  c_config.user = user.c_str();
  c_config.password = password.c_str();
  c_config.server_id = server_id;
  c_config.start_gtid = start_gtid.empty() ? nullptr : start_gtid.c_str();
  c_config.connect_timeout_s = connect_timeout_s;
  c_config.read_timeout_s = read_timeout_s;
  c_config.ssl_mode = static_cast<mes_ssl_mode_t>(ssl_mode);
  c_config.ssl_ca = ssl_ca.empty() ? nullptr : ssl_ca.c_str();
  c_config.ssl_cert = ssl_cert.empty() ? nullptr : ssl_cert.c_str();
  c_config.ssl_key = ssl_key.empty() ? nullptr : ssl_key.c_str();
  c_config.max_queue_size = max_queue_size;

  mes_error_t err = mes_client_connect(client_, &c_config);
  if (err != MES_OK) {
    const char* msg = mes_client_last_error(client_);
    std::string err_msg = msg ? msg : "connection failed";
    Napi::Error::New(env, "mes_client_connect failed: " + err_msg)
        .ThrowAsJavaScriptException();
  }
}

void ClientWrap::Start(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();

  if (!client_) {
    Napi::Error::New(env, "Client has been destroyed")
        .ThrowAsJavaScriptException();
    return;
  }

  mes_error_t err = mes_client_start(client_);
  if (err != MES_OK) {
    const char* msg = mes_client_last_error(client_);
    std::string err_msg = msg ? msg : "start failed";
    Napi::Error::New(env, "mes_client_start failed: " + err_msg)
        .ThrowAsJavaScriptException();
  }
}

Napi::Value ClientWrap::Poll(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();

  if (!client_ || destroy_pending_) {
    auto deferred = Napi::Promise::Deferred::New(env);
    deferred.Reject(
        Napi::Error::New(env, "Client has been destroyed").Value());
    return deferred.Promise();
  }

  // Only one poll() may be in flight at a time. The C ABI contract states
  // that the data pointer from mes_client_poll() is valid only until the
  // next call to mes_client_poll() on the same client.
  if (pending_workers_.load(std::memory_order_acquire) > 0) {
    auto deferred = Napi::Promise::Deferred::New(env);
    deferred.Reject(
        Napi::Error::New(env, "A poll() is already in progress").Value());
    return deferred.Promise();
  }

  auto deferred = Napi::Promise::Deferred::New(env);
  pending_workers_.fetch_add(1, std::memory_order_relaxed);
  Ref();  // prevent GC while worker is in flight
  auto* worker = new PollWorker(env, client_, this, deferred);
  worker->Queue();
  return deferred.Promise();
}

void ClientWrap::Stop(const Napi::CallbackInfo& info) {
  if (client_) {
    mes_client_stop(client_);
  }
}

void ClientWrap::Disconnect(const Napi::CallbackInfo& info) {
  if (client_) {
    mes_client_disconnect(client_);
  }
}

void ClientWrap::Destroy(const Napi::CallbackInfo& info) {
  if (!client_) return;

  if (pending_workers_.load(std::memory_order_acquire) > 0) {
    // Workers are in flight on the thread pool. Stop the client to unblock
    // any blocking mes_client_poll() call, but defer the actual destroy
    // until the last worker completes on the main thread.
    mes_client_stop(client_);
    destroy_pending_ = true;
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
  pending_workers_.fetch_sub(1, std::memory_order_release);
  Unref();  // allow GC now that worker is done
  MaybeFinalizeDeferredDestroy();
}

void ClientWrap::MaybeFinalizeDeferredDestroy() {
  if (destroy_pending_ && client_ &&
      pending_workers_.load(std::memory_order_acquire) == 0) {
    mes_client_destroy(client_);
    client_ = nullptr;
    destroy_pending_ = false;
  }
}

Napi::Value ClientWrap::GetIsConnected(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  if (!client_) return Napi::Boolean::New(env, false);
  return Napi::Boolean::New(env, mes_client_is_connected(client_) == 1);
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
