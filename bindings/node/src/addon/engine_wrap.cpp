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

Napi::Value EngineWrap::ReadColumns(Napi::Env env,
                                    const mes_column_t* cols,
                                    uint32_t count) {
  Napi::Array arr = Napi::Array::New(env, count);

  for (uint32_t i = 0; i < count; i++) {
    const mes_column_t& col = cols[i];
    Napi::Object obj = Napi::Object::New(env);

    switch (col.type) {
      case MES_COL_NULL:
        obj.Set("type", Napi::String::New(env, "null"));
        obj.Set("value", env.Null());
        break;

      case MES_COL_INT: {
        obj.Set("type", Napi::String::New(env, "int"));
        int64_t val = col.int_val;
        // Use BigInt for values outside Number.MAX_SAFE_INTEGER range
        if (val > 9007199254740991LL || val < -9007199254740991LL) {
          obj.Set("value", Napi::BigInt::New(env, val));
        } else {
          obj.Set("value", Napi::Number::New(env, static_cast<double>(val)));
        }
        break;
      }

      case MES_COL_DOUBLE:
        obj.Set("type", Napi::String::New(env, "double"));
        obj.Set("value", Napi::Number::New(env, col.double_val));
        break;

      case MES_COL_STRING:
        obj.Set("type", Napi::String::New(env, "string"));
        if (col.str_data) {
          obj.Set("value",
                  Napi::String::New(env, col.str_data, col.str_len));
        } else {
          obj.Set("value", Napi::String::New(env, ""));
        }
        break;

      case MES_COL_BYTES:
        obj.Set("type", Napi::String::New(env, "bytes"));
        if (col.str_data && col.str_len > 0) {
          obj.Set("value",
                  Napi::Buffer<uint8_t>::Copy(
                      env,
                      reinterpret_cast<const uint8_t*>(col.str_data),
                      col.str_len));
        } else {
          obj.Set("value", Napi::Buffer<uint8_t>::New(env, 0));
        }
        break;

      default:
        obj.Set("type", Napi::String::New(env, "null"));
        obj.Set("value", env.Null());
        break;
    }

    arr.Set(i, obj);
  }

  return arr;
}
