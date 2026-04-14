// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#include "engine_wrap.h"

#include <cstdint>
#include <string>

#include "addon_constants.h"
#include "config_parser.h"

static const char* const kEventTypeNames[] = {"INSERT", "UPDATE", "DELETE"};

namespace {
const char* MesErrorString(mes_error_t err) {
  switch (err) {
    case MES_OK:
      return "success";
    case MES_ERR_NULL_ARG:
      return "null argument";
    case MES_ERR_INVALID_ARG:
      return "invalid argument";
    case MES_ERR_INTERNAL:
      return "internal error";
    case MES_ERR_PARSE:
      return "parse error";
    case MES_ERR_CHECKSUM:
      return "checksum mismatch";
    case MES_ERR_DECODE:
      return "decode error";
    case MES_ERR_DECODE_COLUMN:
      return "column decode error";
    case MES_ERR_DECODE_ROW:
      return "row decode error";
    case MES_ERR_NO_EVENT:
      return "no event available";
    case MES_ERR_QUEUE_FULL:
      return "queue full";
    case MES_ERR_CONNECT:
      return "connection error";
    case MES_ERR_AUTH:
      return "authentication error";
    case MES_ERR_VALIDATION:
      return "validation error";
    case MES_ERR_STREAM:
      return "stream error";
    case MES_ERR_DISCONNECTED:
      return "disconnected";
    default:
      return "unknown error";
  }
}
}  // namespace

Napi::Object EngineWrap::Init(Napi::Env env, Napi::Object exports) {
  Napi::Function func = DefineClass(
      env, "CdcEngine",
      {
          InstanceMethod<&EngineWrap::Feed>("feed"),
          InstanceMethod<&EngineWrap::NextEvent>("nextEvent"),
          InstanceMethod<&EngineWrap::HasEvents>("hasEvents"),
          InstanceMethod<&EngineWrap::GetPosition>("getPosition"),
          InstanceMethod<&EngineWrap::Reset>("reset"),
          InstanceMethod<&EngineWrap::SetMaxQueueSize>("setMaxQueueSize"),
          InstanceMethod<&EngineWrap::SetIncludeDatabases>("setIncludeDatabases"),
          InstanceMethod<&EngineWrap::SetIncludeTables>("setIncludeTables"),
          InstanceMethod<&EngineWrap::SetExcludeTables>("setExcludeTables"),
          InstanceMethod<&EngineWrap::Destroy>("destroy"),
          InstanceMethod<&EngineWrap::EnableMetadata>("enableMetadata"),
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

  if (info[0].IsTypedArray()) {
    auto typed = info[0].As<Napi::TypedArray>();
    if (typed.TypedArrayType() != napi_uint8_array) {
      Napi::TypeError::New(env, "Expected Buffer or Uint8Array")
          .ThrowAsJavaScriptException();
      return Napi::Number::New(env, 0);
    }
    auto arr = info[0].As<Napi::Uint8Array>();
    data = arr.Data();
    len = arr.ByteLength();
  } else if (info[0].IsBuffer()) {
    auto buf = info[0].As<Napi::Buffer<uint8_t>>();
    data = buf.Data();
    len = buf.Length();
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
    Napi::Error::New(env, std::string("mes_feed failed: ") +
                              MesErrorString(err))
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
    Napi::Error::New(env, std::string("mes_next_event failed: ") +
                              MesErrorString(err))
        .ThrowAsJavaScriptException();
    return env.Undefined();
  }

  // Build the JS event object
  Napi::Object obj = Napi::Object::New(env);

  // type
  int type_idx = static_cast<int>(event->type);
  constexpr int kEventTypeCount = static_cast<int>(sizeof(kEventTypeNames) / sizeof(kEventTypeNames[0]));
  if (type_idx < 0 || type_idx >= kEventTypeCount) {
    Napi::Error::New(env, "Unknown event type: " + std::to_string(type_idx))
        .ThrowAsJavaScriptException();
    return env.Undefined();
  }
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
  uint64_t evt_offset = event->binlog_offset;
  if (evt_offset > static_cast<uint64_t>(kMaxSafeInteger)) {
    pos.Set("offset", Napi::BigInt::New(env, evt_offset));
  } else {
    pos.Set("offset",
            Napi::Number::New(env, static_cast<double>(evt_offset)));
  }
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
    Napi::Error::New(env, std::string("mes_get_position failed: ") +
                              MesErrorString(err))
        .ThrowAsJavaScriptException();
    return env.Undefined();
  }

  Napi::Object pos = Napi::Object::New(env);
  pos.Set("file", Napi::String::New(env, file ? file : ""));
  if (offset > static_cast<uint64_t>(kMaxSafeInteger)) {
    pos.Set("offset", Napi::BigInt::New(env, offset));
  } else {
    pos.Set("offset", Napi::Number::New(env, static_cast<double>(offset)));
  }
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
    Napi::Error::New(env, std::string("mes_reset failed: ") +
                              MesErrorString(err))
        .ThrowAsJavaScriptException();
  }
}

void EngineWrap::SetMaxQueueSize(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();

  if (!engine_) {
    Napi::Error::New(env, "Engine has been destroyed")
        .ThrowAsJavaScriptException();
    return;
  }

  if (info.Length() < 1 || !info[0].IsNumber()) {
    Napi::TypeError::New(env, "Expected number argument")
        .ThrowAsJavaScriptException();
    return;
  }

  int64_t max_size = info[0].As<Napi::Number>().Int64Value();
  if (max_size < 0) {
    Napi::TypeError::New(env, "maxQueueSize must be non-negative")
        .ThrowAsJavaScriptException();
    return;
  }
  mes_error_t err =
      mes_set_max_queue_size(engine_, static_cast<size_t>(max_size));
  if (err != MES_OK) {
    Napi::Error::New(env, std::string("mes_set_max_queue_size failed: ") +
                              MesErrorString(err))
        .ThrowAsJavaScriptException();
  }
}

// Helper to extract a string array from a JS Array argument.
static std::vector<std::string> ExtractStringArray(Napi::Env env,
                                                   const Napi::Value& val) {
  std::vector<std::string> result;
  if (!val.IsArray()) return result;
  auto arr = val.As<Napi::Array>();
  for (uint32_t i = 0; i < arr.Length(); i++) {
    Napi::Value item = arr[i];
    if (!item.IsString()) {
      Napi::TypeError::New(env, "Array must contain only strings")
          .ThrowAsJavaScriptException();
      return {};
    }
    result.push_back(item.As<Napi::String>().Utf8Value());
  }
  return result;
}

void EngineWrap::SetIncludeDatabases(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  if (!engine_) {
    Napi::Error::New(env, "Engine has been destroyed")
        .ThrowAsJavaScriptException();
    return;
  }
  if (info.Length() < 1 || !info[0].IsArray()) {
    Napi::TypeError::New(env, "Expected array of strings")
        .ThrowAsJavaScriptException();
    return;
  }
  auto dbs = ExtractStringArray(env, info[0]);
  if (env.IsExceptionPending()) return;
  std::vector<const char*> ptrs;
  for (const auto& s : dbs) ptrs.push_back(s.c_str());
  mes_error_t err = mes_set_include_databases(engine_, ptrs.data(), ptrs.size());
  if (err != MES_OK) {
    Napi::Error::New(env, std::string("mes_set_include_databases failed: ") +
                              MesErrorString(err))
        .ThrowAsJavaScriptException();
  }
}

void EngineWrap::SetIncludeTables(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  if (!engine_) {
    Napi::Error::New(env, "Engine has been destroyed")
        .ThrowAsJavaScriptException();
    return;
  }
  if (info.Length() < 1 || !info[0].IsArray()) {
    Napi::TypeError::New(env, "Expected array of strings")
        .ThrowAsJavaScriptException();
    return;
  }
  auto tables = ExtractStringArray(env, info[0]);
  if (env.IsExceptionPending()) return;
  std::vector<const char*> ptrs;
  for (const auto& s : tables) ptrs.push_back(s.c_str());
  mes_error_t err = mes_set_include_tables(engine_, ptrs.data(), ptrs.size());
  if (err != MES_OK) {
    Napi::Error::New(env, std::string("mes_set_include_tables failed: ") +
                              MesErrorString(err))
        .ThrowAsJavaScriptException();
  }
}

void EngineWrap::SetExcludeTables(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  if (!engine_) {
    Napi::Error::New(env, "Engine has been destroyed")
        .ThrowAsJavaScriptException();
    return;
  }
  if (info.Length() < 1 || !info[0].IsArray()) {
    Napi::TypeError::New(env, "Expected array of strings")
        .ThrowAsJavaScriptException();
    return;
  }
  auto tables = ExtractStringArray(env, info[0]);
  if (env.IsExceptionPending()) return;
  std::vector<const char*> ptrs;
  for (const auto& s : tables) ptrs.push_back(s.c_str());
  mes_error_t err = mes_set_exclude_tables(engine_, ptrs.data(), ptrs.size());
  if (err != MES_OK) {
    Napi::Error::New(env, std::string("mes_set_exclude_tables failed: ") +
                              MesErrorString(err))
        .ThrowAsJavaScriptException();
  }
}

void EngineWrap::Destroy(const Napi::CallbackInfo& info) {
  if (engine_) {
    mes_destroy(engine_);
    engine_ = nullptr;
  }
}

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
  mes_node::ConfigStrings strings;
  if (!mes_node::ParseClientConfig(env, config, cfg, strings)) {
    return env.Undefined();
  }

  mes_error_t rc = mes_engine_set_metadata_conn(engine_, &cfg);
  if (rc != MES_OK) {
    Napi::Error::New(env, std::string("Failed to connect metadata: ") +
                              MesErrorString(rc))
        .ThrowAsJavaScriptException();
    return env.Undefined();
  }

  return env.Undefined();
}

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
        if (v > kMaxSafeInteger || v < -kMaxSafeInteger) {
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
        if (col.str_data) {
          val = Napi::Buffer<uint8_t>::Copy(
              env, reinterpret_cast<const uint8_t*>(col.str_data),
              col.str_len);
        } else {
          // str_data is null: the column value is semantically absent
          val = env.Null();
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
