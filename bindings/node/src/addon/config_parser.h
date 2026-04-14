// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#ifndef MES_NODE_CONFIG_PARSER_H_
#define MES_NODE_CONFIG_PARSER_H_

#include <napi.h>
#include <mes.h>

#include <string>

namespace mes_node {

constexpr uint16_t kDefaultPort = 3306;
constexpr uint32_t kDefaultServerId = 1;
constexpr uint32_t kDefaultConnectTimeoutS = 10;

/** Holds std::string values whose lifetime must outlive the mes_client_config_t
 *  that references them via c_str() pointers. */
struct ConfigStrings {
  std::string host = "127.0.0.1";
  std::string user = "root";
  std::string password;
  std::string start_gtid;
  std::string ssl_ca;
  std::string ssl_cert;
  std::string ssl_key;
};

/** Parse common client config fields from a JS object into a C config struct.
 *  Returns false and schedules a JS exception on validation errors. */
inline bool ParseClientConfig(Napi::Env env, Napi::Object config,
                              mes_client_config_t& cfg,
                              ConfigStrings& strings) {
  if (config.Has("host") && config.Get("host").IsString()) {
    strings.host = config.Get("host").As<Napi::String>().Utf8Value();
  }
  cfg.host = strings.host.c_str();

  if (config.Has("port") && config.Get("port").IsNumber()) {
    uint32_t port = config.Get("port").As<Napi::Number>().Uint32Value();
    if (port < 1 || port > 65535) {
      Napi::RangeError::New(env, "port must be 1-65535")
          .ThrowAsJavaScriptException();
      return false;
    }
    cfg.port = static_cast<uint16_t>(port);
  } else {
    cfg.port = kDefaultPort;
  }

  if (config.Has("user") && config.Get("user").IsString()) {
    strings.user = config.Get("user").As<Napi::String>().Utf8Value();
  }
  cfg.user = strings.user.c_str();

  if (config.Has("password") && config.Get("password").IsString()) {
    strings.password = config.Get("password").As<Napi::String>().Utf8Value();
  }
  cfg.password = strings.password.c_str();

  if (config.Has("serverId") && config.Get("serverId").IsNumber()) {
    cfg.server_id = config.Get("serverId").As<Napi::Number>().Uint32Value();
  } else {
    cfg.server_id = kDefaultServerId;
  }

  if (config.Has("connectTimeoutS") &&
      config.Get("connectTimeoutS").IsNumber()) {
    cfg.connect_timeout_s =
        config.Get("connectTimeoutS").As<Napi::Number>().Uint32Value();
  } else {
    cfg.connect_timeout_s = kDefaultConnectTimeoutS;
  }

  uint32_t ssl_mode = 0;
  if (config.Has("sslMode") && config.Get("sslMode").IsNumber()) {
    ssl_mode = config.Get("sslMode").As<Napi::Number>().Uint32Value();
    if (ssl_mode > 4) {
      Napi::TypeError::New(env, "sslMode must be 0-4")
          .ThrowAsJavaScriptException();
      return false;
    }
  }
  cfg.ssl_mode = static_cast<mes_ssl_mode_t>(ssl_mode);

  if (config.Has("sslCa") && config.Get("sslCa").IsString()) {
    strings.ssl_ca = config.Get("sslCa").As<Napi::String>().Utf8Value();
    cfg.ssl_ca = strings.ssl_ca.c_str();
  }
  if (config.Has("sslCert") && config.Get("sslCert").IsString()) {
    strings.ssl_cert = config.Get("sslCert").As<Napi::String>().Utf8Value();
    cfg.ssl_cert = strings.ssl_cert.c_str();
  }
  if (config.Has("sslKey") && config.Get("sslKey").IsString()) {
    strings.ssl_key = config.Get("sslKey").As<Napi::String>().Utf8Value();
    cfg.ssl_key = strings.ssl_key.c_str();
  }

  return true;
}

}  // namespace mes_node

#endif  // MES_NODE_CONFIG_PARSER_H_
