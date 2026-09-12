// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#ifndef MES_NODE_CONFIG_PARSER_H_
#define MES_NODE_CONFIG_PARSER_H_

#include <mes.h>
#include <napi.h>
#include <secure_cleanse.h>

#include <cstdint>
#include <string>

#include "mes_error_util.h"

namespace mes_node {

constexpr uint16_t kDefaultPort = 3306;
constexpr uint32_t kDefaultServerId = 1;
constexpr uint32_t kDefaultConnectTimeoutS = 10;
constexpr uint32_t kDefaultReadTimeoutS = 30;

/** Holds std::string values whose lifetime must outlive the mes_client_config_t
 *  that references them via c_str() pointers.
 *
 *  `password` is a staging copy of the replication credential, so it is wiped
 *  when this struct goes out of scope. The destructor is what makes that reach
 *  every exit path, including the validation errors that return as soon as a
 *  JS exception is scheduled, and the shared wipe helper is one the compiler
 *  may not drop as a dead store. `ssl_key` names a private key file rather than
 *  carrying key material, so it is left alone. The JS string the copy was taken
 *  from lives on the V8 heap and is not wipeable from here. */
struct ConfigStrings {
  std::string host = "127.0.0.1";
  std::string user = "root";
  std::string password;
  std::string start_gtid;
  std::string binlog_file;
  std::string ssl_ca;
  std::string ssl_cert;
  std::string ssl_key;

  ~ConfigStrings() {
    if (!password.empty()) {
      mes::SecureWipe(password);
    }
  }
};

/** Runtime type a config option's value must have, as declared for that option
 *  in core/contracts/bindings.json. */
enum class ConfigValueType { kString, kNumber, kBoolean };

struct ConfigOption {
  const char* key;
  ConfigValueType type;
};

/** Every option a client config may carry, with the type its value must have.
 *  Options the calling wrapper parses itself are listed too: a wrongly-typed
 *  value has to be rejected before reaching a check that reads it as absent and
 *  substitutes the documented default. */
constexpr ConfigOption kConfigOptions[] = {
    {"host", ConfigValueType::kString},
    {"user", ConfigValueType::kString},
    {"password", ConfigValueType::kString},
    {"startGtid", ConfigValueType::kString},
    {"startBinlogFile", ConfigValueType::kString},
    {"sslCa", ConfigValueType::kString},
    {"sslCert", ConfigValueType::kString},
    {"sslKey", ConfigValueType::kString},
    {"port", ConfigValueType::kNumber},
    {"serverId", ConfigValueType::kNumber},
    {"startBinlogPosition", ConfigValueType::kNumber},
    {"connectTimeoutS", ConfigValueType::kNumber},
    {"readTimeoutS", ConfigValueType::kNumber},
    {"sslMode", ConfigValueType::kNumber},
    {"maxQueueSize", ConfigValueType::kNumber},
    {"maxQueueBytes", ConfigValueType::kNumber},
    {"maxEventSize", ConfigValueType::kNumber},
    {"allowPublicKeyRetrieval", ConfigValueType::kBoolean},
};

/** Reject every supplied option whose value is not of its declared type.
 *  Returns false and schedules a coded JS error on the first mismatch.
 *
 *  An undefined value means the option was not supplied and keeps its default;
 *  any other value must match, so a caller that passes the wrong type gets an
 *  error naming the option instead of a silently defaulted connection. The scan
 *  runs once per connect, not per event. */
inline bool ValidateConfigTypes(Napi::Env env, Napi::Object config) {
  for (const ConfigOption& option : kConfigOptions) {
    Napi::Value value = config.Get(option.key);
    if (value.IsUndefined()) continue;

    bool matches = false;
    const char* expected = "";
    switch (option.type) {
      case ConfigValueType::kString:
        matches = value.IsString();
        expected = "a string";
        break;
      case ConfigValueType::kNumber:
        matches = value.IsNumber();
        expected = "a number";
        break;
      case ConfigValueType::kBoolean:
        matches = value.IsBoolean();
        expected = "a boolean";
        break;
    }
    if (!matches) {
      mes_node::MakeMesError(env, std::string(option.key) + " must be " + expected,
                             MES_ERR_INVALID_ARG, mes_node::MesErrorClass::kType)
          .ThrowAsJavaScriptException();
      return false;
    }
  }
  return true;
}

/** Parse common client config fields from a JS object into a C config struct.
 *  Returns false and schedules a JS exception on validation errors.
 *  Note: Uses a single Get() per key — undefined keys return
 *  env.Undefined(), which fails the subsequent IsString()/IsNumber() check,
 *  matching the intent of the previous Has() + Get() pattern while halving
 *  N-API round-trips. Types are checked up front by ValidateConfigTypes, so a
 *  failing IsString()/IsNumber() here means the option was not supplied. */
inline bool ParseClientConfig(Napi::Env env, Napi::Object config, mes_client_config_t& cfg,
                              ConfigStrings& strings) {
  if (!ValidateConfigTypes(env, config)) {
    return false;
  }

  Napi::Value host_v = config.Get("host");
  if (host_v.IsString()) {
    strings.host = host_v.As<Napi::String>().Utf8Value();
  }
  cfg.host = strings.host.c_str();

  Napi::Value port_v = config.Get("port");
  if (port_v.IsNumber()) {
    // Read as int64 so the refusal can state the value the caller passed: a
    // negative port read as uint32 would be reported as a large positive one.
    int64_t port = port_v.As<Napi::Number>().Int64Value();
    if (port < 1 || port > 65535) {
      mes_node::MakeMesError(env, "port must be 1-65535, got " + std::to_string(port),
                             MES_ERR_INVALID_ARG, mes_node::MesErrorClass::kRange)
          .ThrowAsJavaScriptException();
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

  Napi::Value read_timeout_v = config.Get("readTimeoutS");
  if (read_timeout_v.IsNumber()) {
    cfg.read_timeout_s = read_timeout_v.As<Napi::Number>().Uint32Value();
  } else {
    cfg.read_timeout_s = kDefaultReadTimeoutS;
  }

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

  int64_t ssl_mode = MES_SSL_PREFERRED;
  Napi::Value ssl_mode_v = config.Get("sslMode");
  if (ssl_mode_v.IsNumber()) {
    ssl_mode = ssl_mode_v.As<Napi::Number>().Int64Value();
    if (ssl_mode < 0 || ssl_mode > 4) {
      mes_node::MakeMesError(env,
                             "sslMode must be between 0 and 4, got " + std::to_string(ssl_mode),
                             MES_ERR_INVALID_ARG, mes_node::MesErrorClass::kRange)
          .ThrowAsJavaScriptException();
      return false;
    }
  }
  cfg.ssl_mode = static_cast<mes_ssl_mode_t>(ssl_mode);

  Napi::Value allow_key_v = config.Get("allowPublicKeyRetrieval");
  cfg.allow_public_key_retrieval =
      allow_key_v.IsBoolean() && allow_key_v.As<Napi::Boolean>().Value() ? 1 : 0;

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
