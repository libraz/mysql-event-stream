// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#ifndef MES_NODE_CONFIG_PARSER_H_
#define MES_NODE_CONFIG_PARSER_H_

#include <mes.h>
#include <napi.h>

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
 *  Returns false and schedules a JS exception on validation errors.
 *  NOTE(review): Uses a single Get() per key — undefined keys return
 *  env.Undefined(), which fails the subsequent IsString()/IsNumber() check,
 *  matching the intent of the previous Has() + Get() pattern while halving
 *  N-API round-trips. */
inline bool ParseClientConfig(Napi::Env env, Napi::Object config, mes_client_config_t& cfg,
                              ConfigStrings& strings) {
  Napi::Value host_v = config.Get("host");
  if (host_v.IsString()) {
    strings.host = host_v.As<Napi::String>().Utf8Value();
  }
  cfg.host = strings.host.c_str();

  Napi::Value port_v = config.Get("port");
  if (port_v.IsNumber()) {
    uint32_t port = port_v.As<Napi::Number>().Uint32Value();
    if (port < 1 || port > 65535) {
      Napi::RangeError::New(env, "port must be 1-65535").ThrowAsJavaScriptException();
      return false;
    }
    cfg.port = static_cast<uint16_t>(port);
  } else {
    cfg.port = kDefaultPort;
  }

  Napi::Value user_v = config.Get("user");
  if (user_v.IsString()) {
    strings.user = user_v.As<Napi::String>().Utf8Value();
  }
  cfg.user = strings.user.c_str();

  Napi::Value password_v = config.Get("password");
  if (password_v.IsString()) {
    strings.password = password_v.As<Napi::String>().Utf8Value();
  }
  cfg.password = strings.password.c_str();

  Napi::Value server_id_v = config.Get("serverId");
  if (server_id_v.IsNumber()) {
    cfg.server_id = server_id_v.As<Napi::Number>().Uint32Value();
  } else {
    cfg.server_id = kDefaultServerId;
  }

  Napi::Value connect_timeout_v = config.Get("connectTimeoutS");
  if (connect_timeout_v.IsNumber()) {
    cfg.connect_timeout_s = connect_timeout_v.As<Napi::Number>().Uint32Value();
  } else {
    cfg.connect_timeout_s = kDefaultConnectTimeoutS;
  }

  uint32_t ssl_mode = 0;
  Napi::Value ssl_mode_v = config.Get("sslMode");
  if (ssl_mode_v.IsNumber()) {
    ssl_mode = ssl_mode_v.As<Napi::Number>().Uint32Value();
    if (ssl_mode > 4) {
      Napi::TypeError::New(env, "sslMode must be 0-4").ThrowAsJavaScriptException();
      return false;
    }
  }
  cfg.ssl_mode = static_cast<mes_ssl_mode_t>(ssl_mode);

  Napi::Value ssl_ca_v = config.Get("sslCa");
  if (ssl_ca_v.IsString()) {
    strings.ssl_ca = ssl_ca_v.As<Napi::String>().Utf8Value();
    cfg.ssl_ca = strings.ssl_ca.c_str();
  }
  Napi::Value ssl_cert_v = config.Get("sslCert");
  if (ssl_cert_v.IsString()) {
    strings.ssl_cert = ssl_cert_v.As<Napi::String>().Utf8Value();
    cfg.ssl_cert = strings.ssl_cert.c_str();
  }
  Napi::Value ssl_key_v = config.Get("sslKey");
  if (ssl_key_v.IsString()) {
    strings.ssl_key = ssl_key_v.As<Napi::String>().Utf8Value();
    cfg.ssl_key = strings.ssl_key.c_str();
  }

  return true;
}

}  // namespace mes_node

#endif  // MES_NODE_CONFIG_PARSER_H_
