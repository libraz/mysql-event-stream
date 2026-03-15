// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#include "engine_wrap.h"

#include <cstdint>
#include <string>

static const char* const kEventTypeNames[] = {"INSERT", "UPDATE", "DELETE"};

Napi::Object EngineWrap::Init(Napi::Env env, Napi::Object exports) {
  Napi::Function func = DefineClass(
      env, "CdcEngine",
      {
          InstanceMethod<&EngineWrap::Feed>("feed"),
          InstanceMethod<&EngineWrap::NextEvent>("nextEvent"),
          InstanceMethod<&EngineWrap::HasEvents>("hasEvents"),
          InstanceMethod<&EngineWrap::GetPosition>("getPosition"),
          InstanceMethod<&EngineWrap::Reset>("reset"),
          InstanceMethod<&EngineWrap::Destroy>("destroy"),
#ifdef MES_HAS_MYSQL
          InstanceMethod<&EngineWrap::EnableMetadata>("enableMetadata"),
#endif
      });

  exports.Set("CdcEngine", func);
  return exports;
}

EngineWrap::EngineWrap(const Napi::CallbackInfo& info)
    : Napi::ObjectWrap<EngineWrap>(info), engine_(mes_create()) {
  if (!engine_) {
    Napi::Error::New(info.Env(), "Failed to create mes engine")
        .ThrowAsJavaScriptException();
  }
}

EngineWrap::~EngineWrap() {
  if (engine_) {
    mes_destroy(engine_);
    engine_ = nullptr;
  }
}

Napi::Value EngineWrap::Feed(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();

  if (!engine_) {
    Napi::Error::New(env, "Engine has been destroyed")
        .ThrowAsJavaScriptException();
    return env.Undefined();
  }

  if (info.Length() < 1) {
    Napi::TypeError::New(env, "Expected Buffer or Uint8Array argument")
        .ThrowAsJavaScriptException();
    return env.Undefined();
  }

  // Accept both Buffer and Uint8Array
  const uint8_t* data = nullptr;
  size_t len = 0;

  if (info[0].IsBuffer()) {
    auto buf = info[0].As<Napi::Buffer<uint8_t>>();
    data = buf.Data();
    len = buf.Length();
  } else if (info[0].IsTypedArray()) {
    auto arr = info[0].As<Napi::Uint8Array>();
    data = arr.Data();
    len = arr.ByteLength();
  } else {
    Napi::TypeError::New(env, "Expected Buffer or Uint8Array argument")
        .ThrowAsJavaScriptException();
    return env.Undefined();
  }

  if (len == 0) {
    return Napi::Number::New(env, 0);
  }

  size_t consumed = 0;
  mes_error_t err = mes_feed(engine_, data, len, &consumed);
  if (err != MES_OK) {
    Napi::Error::New(env, "mes_feed failed with error code " +
                              std::to_string(err))
        .ThrowAsJavaScriptException();
    return env.Undefined();
  }

  return Napi::Number::New(env, static_cast<double>(consumed));
}

Napi::Value EngineWrap::NextEvent(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();

  if (!engine_) {
    Napi::Error::New(env, "Engine has been destroyed")
        .ThrowAsJavaScriptException();
    return env.Undefined();
  }

  const mes_event_t* event = nullptr;
  mes_error_t err = mes_next_event(engine_, &event);

  if (err == MES_ERR_NO_EVENT) {
    return env.Null();
  }
  if (err != MES_OK) {
    Napi::Error::New(env, "mes_next_event failed with error code " +
                              std::to_string(err))
        .ThrowAsJavaScriptException();
    return env.Undefined();
  }

  // Build the JS event object
  Napi::Object obj = Napi::Object::New(env);

  // type
  int type_idx = static_cast<int>(event->type);
  if (type_idx < 0 || type_idx > 2) type_idx = 0;
  obj.Set("type", Napi::String::New(env, kEventTypeNames[type_idx]));

  // database, table
  obj.Set("database",
          Napi::String::New(env, event->database ? event->database : ""));
  obj.Set("table", Napi::String::New(env, event->table ? event->table : ""));

  // before / after columns
  if (event->before_columns && event->before_count > 0) {
    obj.Set("before",
            ReadColumns(env, event->before_columns, event->before_count));
  } else {
    obj.Set("before", env.Null());
  }

  if (event->after_columns && event->after_count > 0) {
    obj.Set("after",
            ReadColumns(env, event->after_columns, event->after_count));
  } else {
    obj.Set("after", env.Null());
  }

  // timestamp
  obj.Set("timestamp", Napi::Number::New(env, event->timestamp));

  // position
  Napi::Object pos = Napi::Object::New(env);
  pos.Set("file",
          Napi::String::New(env, event->binlog_file ? event->binlog_file : ""));
  pos.Set("offset",
          Napi::Number::New(env, static_cast<double>(event->binlog_offset)));
  obj.Set("position", pos);

  return obj;
}

Napi::Value EngineWrap::HasEvents(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();

  if (!engine_) {
    Napi::Error::New(env, "Engine has been destroyed")
        .ThrowAsJavaScriptException();
    return env.Undefined();
  }

  return Napi::Boolean::New(env, mes_has_events(engine_) == 1);
}

Napi::Value EngineWrap::GetPosition(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();

  if (!engine_) {
    Napi::Error::New(env, "Engine has been destroyed")
        .ThrowAsJavaScriptException();
    return env.Undefined();
  }

  const char* file = nullptr;
  uint64_t offset = 0;
  mes_error_t err = mes_get_position(engine_, &file, &offset);
  if (err != MES_OK) {
    Napi::Error::New(env, "mes_get_position failed with error code " +
                              std::to_string(err))
        .ThrowAsJavaScriptException();
    return env.Undefined();
  }

  Napi::Object pos = Napi::Object::New(env);
  pos.Set("file", Napi::String::New(env, file ? file : ""));
  pos.Set("offset", Napi::Number::New(env, static_cast<double>(offset)));
  return pos;
}

void EngineWrap::Reset(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();

  if (!engine_) {
    Napi::Error::New(env, "Engine has been destroyed")
        .ThrowAsJavaScriptException();
    return;
  }

  mes_error_t err = mes_reset(engine_);
  if (err != MES_OK) {
    Napi::Error::New(env, "mes_reset failed with error code " +
                              std::to_string(err))
        .ThrowAsJavaScriptException();
  }
}

void EngineWrap::Destroy(const Napi::CallbackInfo& info) {
  if (engine_) {
    mes_destroy(engine_);
    engine_ = nullptr;
  }
}

#ifdef MES_HAS_MYSQL
Napi::Value EngineWrap::EnableMetadata(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();

  if (!engine_) {
    Napi::Error::New(env, "Engine has been destroyed")
        .ThrowAsJavaScriptException();
    return env.Undefined();
  }

  if (info.Length() < 1 || !info[0].IsObject()) {
    Napi::TypeError::New(env, "Expected config object")
        .ThrowAsJavaScriptException();
    return env.Undefined();
  }

  Napi::Object config = info[0].As<Napi::Object>();

  mes_client_config_t cfg{};
  std::string host = "127.0.0.1";
  std::string user;
  std::string password;

  if (config.Has("host") && config.Get("host").IsString()) {
    host = config.Get("host").As<Napi::String>().Utf8Value();
  }
  cfg.host = host.c_str();

  if (config.Has("port") && config.Get("port").IsNumber()) {
    cfg.port = static_cast<uint16_t>(
        config.Get("port").As<Napi::Number>().Uint32Value());
  } else {
    cfg.port = 3306;
  }

  if (config.Has("user") && config.Get("user").IsString()) {
    user = config.Get("user").As<Napi::String>().Utf8Value();
  }
  cfg.user = user.c_str();

  if (config.Has("password") && config.Get("password").IsString()) {
    password = config.Get("password").As<Napi::String>().Utf8Value();
  }
  cfg.password = password.c_str();

  if (config.Has("connectTimeoutS") &&
      config.Get("connectTimeoutS").IsNumber()) {
    cfg.connect_timeout_s =
        config.Get("connectTimeoutS").As<Napi::Number>().Uint32Value();
  } else {
    cfg.connect_timeout_s = 10;
  }

  mes_error_t rc = mes_engine_set_metadata_conn(engine_, &cfg);
  if (rc != MES_OK) {
    Napi::Error::New(env, "Failed to connect metadata (error " +
                              std::to_string(rc) + ")")
        .ThrowAsJavaScriptException();
    return env.Undefined();
  }

  return env.Undefined();
}
#endif

Napi::Value EngineWrap::ReadColumns(Napi::Env env,
                                    const mes_column_t* cols,
                                    uint32_t count) {
  Napi::Object record = Napi::Object::New(env);

  for (uint32_t i = 0; i < count; i++) {
    const mes_column_t& col = cols[i];

    // Key: column name if available, otherwise string index
    std::string key;
    if (col.col_name != nullptr && col.col_name[0] != '\0') {
      key = col.col_name;
    } else {
      key = std::to_string(i);
    }

    Napi::Value val;
    switch (col.type) {
      case MES_COL_NULL:
        val = env.Null();
        break;

      case MES_COL_INT: {
        int64_t v = col.int_val;
        if (v > 9007199254740991LL || v < -9007199254740991LL) {
          val = Napi::BigInt::New(env, v);
        } else {
          val = Napi::Number::New(env, static_cast<double>(v));
        }
        break;
      }

      case MES_COL_DOUBLE:
        val = Napi::Number::New(env, col.double_val);
        break;

      case MES_COL_STRING:
        if (col.str_data) {
          val = Napi::String::New(env, col.str_data, col.str_len);
        } else {
          val = Napi::String::New(env, "");
        }
        break;

      case MES_COL_BYTES:
        if (col.str_data && col.str_len > 0) {
          val = Napi::Buffer<uint8_t>::Copy(
              env, reinterpret_cast<const uint8_t*>(col.str_data),
              col.str_len);
        } else {
          val = Napi::Buffer<uint8_t>::New(env, 0);
        }
        break;

      default:
        val = env.Null();
        break;
    }

    record.Set(key, val);
  }

  return record;
}
