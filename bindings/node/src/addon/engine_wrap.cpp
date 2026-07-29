// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#include "engine_wrap.h"

#include <cstdint>
#include <cstring>
#include <string>

#include "addon_constants.h"
#include "config_parser.h"
#include "mes_error_util.h"

static const char* const kEventTypeNames[] = {"INSERT", "UPDATE", "DELETE"};

namespace {

void ThrowDestroyed(Napi::Env env) {
  mes_node::MakeMesError(env, "Engine has been destroyed", MES_ERR_INVALID_ARG)
      .ThrowAsJavaScriptException();
}

}  // namespace

Napi::Object EngineWrap::Init(Napi::Env env, Napi::Object exports) {
  Napi::Function func =
      DefineClass(env, "CdcEngine",
                  {
                      InstanceMethod<&EngineWrap::Feed>("feed"),
                      InstanceMethod<&EngineWrap::NextEvent>("nextEvent"),
                      InstanceMethod<&EngineWrap::HasEvents>("hasEvents"),
                      InstanceMethod<&EngineWrap::GetPosition>("getPosition"),
                      InstanceMethod<&EngineWrap::Reset>("reset"),
                      InstanceMethod<&EngineWrap::SetMaxQueueSize>("setMaxQueueSize"),
                      InstanceMethod<&EngineWrap::SetMaxEventSize>("setMaxEventSize"),
                      InstanceMethod<&EngineWrap::GetMaxEventSize>("getMaxEventSize"),
                      InstanceMethod<&EngineWrap::SetChecksumEnabled>("setChecksumEnabled"),
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
    Napi::Error::New(info.Env(), "Failed to create mes engine").ThrowAsJavaScriptException();
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
    ThrowDestroyed(env);
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
      Napi::TypeError::New(env, "Expected Buffer or Uint8Array").ThrowAsJavaScriptException();
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
    // After a feed error the engine parse state is undefined; reset() is the
    // only valid next operation. Re-feeding without reset duplicates events.
    mes_node::MakeMesError(env,
                           std::string("mes_feed failed: ") + mes_error_string(err) +
                               " (call reset() before feeding again)",
                           err)
        .ThrowAsJavaScriptException();
    return env.Undefined();
  }

  return Napi::Number::New(env, static_cast<double>(consumed));
}

Napi::Value EngineWrap::NextEvent(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();

  if (!engine_) {
    ThrowDestroyed(env);
    return env.Undefined();
  }

  const mes_event_t* event = nullptr;
  mes_error_t err = mes_next_event(engine_, &event);

  if (err == MES_ERR_NO_EVENT) {
    return env.Null();
  }
  if (err != MES_OK) {
    mes_node::MakeMesError(env, std::string("mes_next_event failed: ") + mes_error_string(err), err)
        .ThrowAsJavaScriptException();
    return env.Undefined();
  }

  // Build the JS event object
  Napi::Object obj = Napi::Object::New(env);

  // type
  constexpr size_t kEventTypeCount = sizeof(kEventTypeNames) / sizeof(kEventTypeNames[0]);
  int type_idx = static_cast<int>(event->type);
  if (type_idx < 0 || static_cast<size_t>(type_idx) >= kEventTypeCount) {
    Napi::Error::New(env, "Unknown event type: " + std::to_string(type_idx))
        .ThrowAsJavaScriptException();
    return env.Undefined();
  }
  obj.Set("type", Napi::String::New(env, kEventTypeNames[type_idx]));

  // database, table
  obj.Set("database", Napi::String::New(env, event->database ? event->database : ""));
  obj.Set("table", Napi::String::New(env, event->table ? event->table : ""));

  // before / after columns
  if (event->before_columns && event->before_count > 0) {
    obj.Set("before", ReadColumns(env, event->before_columns, event->before_count));
  } else {
    obj.Set("before", env.Null());
  }

  if (event->after_columns && event->after_count > 0) {
    obj.Set("after", ReadColumns(env, event->after_columns, event->after_count));
  } else {
    obj.Set("after", env.Null());
  }

  // timestamp
  obj.Set("timestamp", Napi::Number::New(env, event->timestamp));

  // position
  Napi::Object pos = Napi::Object::New(env);
  pos.Set("file", Napi::String::New(env, event->binlog_file ? event->binlog_file : ""));
  uint64_t evt_offset = event->binlog_offset;
  if (evt_offset > static_cast<uint64_t>(kMaxSafeInteger)) {
    pos.Set("offset", Napi::BigInt::New(env, evt_offset));
  } else {
    pos.Set("offset", Napi::Number::New(env, static_cast<double>(evt_offset)));
  }
  obj.Set("position", pos);
  obj.Set("sourceSql", Napi::String::New(env, event->source_sql ? event->source_sql : ""));

  // namesResolved: false when column names could not be resolved for this
  // event's table, so all column keys fall back to numeric indices.
  obj.Set("namesResolved", Napi::Boolean::New(env, event->names_resolved != 0));

  return obj;
}

Napi::Value EngineWrap::HasEvents(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();

  if (!engine_) {
    ThrowDestroyed(env);
    return env.Undefined();
  }

  return Napi::Boolean::New(env, mes_has_events(engine_) == 1);
}

Napi::Value EngineWrap::GetPosition(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();

  if (!engine_) {
    ThrowDestroyed(env);
    return env.Undefined();
  }

  const char* file = nullptr;
  uint64_t offset = 0;
  mes_error_t err = mes_get_position(engine_, &file, &offset);
  if (err != MES_OK) {
    mes_node::MakeMesError(env, std::string("mes_get_position failed: ") + mes_error_string(err),
                           err)
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
    ThrowDestroyed(env);
    return;
  }

  mes_error_t err = mes_reset(engine_);
  if (err != MES_OK) {
    mes_node::MakeMesError(env, std::string("mes_reset failed: ") + mes_error_string(err), err)
        .ThrowAsJavaScriptException();
  }
}

void EngineWrap::SetMaxQueueSize(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();

  if (!engine_) {
    ThrowDestroyed(env);
    return;
  }

  if (info.Length() < 1 || !info[0].IsNumber()) {
    Napi::TypeError::New(env, "Expected number argument").ThrowAsJavaScriptException();
    return;
  }

  int64_t max_size = info[0].As<Napi::Number>().Int64Value();
  if (max_size < 0) {
    Napi::TypeError::New(env, "maxQueueSize must be non-negative").ThrowAsJavaScriptException();
    return;
  }
  mes_error_t err = mes_set_max_queue_size(engine_, static_cast<size_t>(max_size));
  if (err != MES_OK) {
    mes_node::MakeMesError(
        env, std::string("mes_set_max_queue_size failed: ") + mes_error_string(err), err)
        .ThrowAsJavaScriptException();
  }
}

void EngineWrap::SetMaxEventSize(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();

  if (!engine_) {
    ThrowDestroyed(env);
    return;
  }
  if (info.Length() < 1 || !info[0].IsNumber()) {
    Napi::TypeError::New(env, "Expected number argument").ThrowAsJavaScriptException();
    return;
  }

  int64_t raw = info[0].As<Napi::Number>().Int64Value();
  if (raw < 0 || raw > UINT32_MAX) {
    Napi::RangeError::New(env, "maxEventSize must fit in uint32").ThrowAsJavaScriptException();
    return;
  }
  mes_error_t err = mes_set_max_event_size(engine_, static_cast<uint32_t>(raw));
  if (err != MES_OK) {
    mes_node::MakeMesError(
        env, std::string("mes_set_max_event_size failed: ") + mes_error_string(err), err)
        .ThrowAsJavaScriptException();
  }
}

Napi::Value EngineWrap::GetMaxEventSize(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  if (!engine_) {
    ThrowDestroyed(env);
    return env.Undefined();
  }
  uint32_t value = mes_get_max_event_size(engine_);
  return Napi::Number::New(env, static_cast<double>(value));
}

void EngineWrap::SetChecksumEnabled(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  if (!engine_) {
    ThrowDestroyed(env);
    return;
  }
  if (info.Length() < 1 || !info[0].IsBoolean()) {
    Napi::TypeError::New(env, "Expected boolean argument").ThrowAsJavaScriptException();
    return;
  }
  mes_error_t err = mes_set_checksum_enabled(engine_, info[0].As<Napi::Boolean>().Value() ? 1 : 0);
  if (err != MES_OK) {
    mes_node::MakeMesError(
        env, std::string("mes_set_checksum_enabled failed: ") + mes_error_string(err), err)
        .ThrowAsJavaScriptException();
  }
}

// Helper to extract a string array from a JS Array argument.
static std::vector<std::string> ExtractStringArray(Napi::Env env, const Napi::Value& val) {
  std::vector<std::string> result;
  if (!val.IsArray()) return result;
  auto arr = val.As<Napi::Array>();
  for (uint32_t i = 0; i < arr.Length(); i++) {
    Napi::Value item = arr[i];
    if (!item.IsString()) {
      Napi::TypeError::New(env, "Array must contain only strings").ThrowAsJavaScriptException();
      return {};
    }
    result.push_back(item.As<Napi::String>().Utf8Value());
  }
  return result;
}

void EngineWrap::SetStringFilter(const Napi::CallbackInfo& info,
                                 mes_error_t (*setter)(mes_engine_t*, const char**, size_t),
                                 const char* method_name) {
  Napi::Env env = info.Env();
  if (!engine_) {
    ThrowDestroyed(env);
    return;
  }
  if (info.Length() < 1 || !info[0].IsArray()) {
    Napi::TypeError::New(env, "Expected array of strings").ThrowAsJavaScriptException();
    return;
  }
  auto strings = ExtractStringArray(env, info[0]);
  if (env.IsExceptionPending()) return;
  std::vector<const char*> ptrs;
  for (const auto& s : strings) ptrs.push_back(s.c_str());
  mes_error_t err = setter(engine_, ptrs.data(), ptrs.size());
  if (err != MES_OK) {
    mes_node::MakeMesError(env, std::string(method_name) + " failed: " + mes_error_string(err), err)
        .ThrowAsJavaScriptException();
  }
}

void EngineWrap::SetIncludeDatabases(const Napi::CallbackInfo& info) {
  SetStringFilter(info, mes_set_include_databases, "mes_set_include_databases");
}

void EngineWrap::SetIncludeTables(const Napi::CallbackInfo& info) {
  SetStringFilter(info, mes_set_include_tables, "mes_set_include_tables");
}

void EngineWrap::SetExcludeTables(const Napi::CallbackInfo& info) {
  SetStringFilter(info, mes_set_exclude_tables, "mes_set_exclude_tables");
}

void EngineWrap::Destroy(const Napi::CallbackInfo& info) {
  (void)info;
  if (engine_) {
    mes_destroy(engine_);
    engine_ = nullptr;
  }
}

Napi::Value EngineWrap::EnableMetadata(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();

  if (!engine_) {
    ThrowDestroyed(env);
    return env.Undefined();
  }

  if (info.Length() < 1 || !info[0].IsObject()) {
    Napi::TypeError::New(env, "Expected config object").ThrowAsJavaScriptException();
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
    mes_node::MakeMesError(env, std::string("Failed to connect metadata: ") + mes_error_string(rc),
                           rc)
        .ThrowAsJavaScriptException();
    return env.Undefined();
  }

  return env.Undefined();
}

Napi::Value EngineWrap::ReadColumns(Napi::Env env, const mes_column_t* cols, uint32_t count) {
  Napi::Object record = Napi::Object::New(env);

  for (uint32_t i = 0; i < count; i++) {
    const mes_column_t& col = cols[i];

    Napi::String key = GetColumnKey(env, col, i);

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
        // Napi::String interprets the bytes as UTF-8. Text stored in a
        // non-UTF-8 charset (latin1, sjis, ...) is not transcoded; invalid
        // sequences become U+FFFD, so such columns may be lossy. Charset-aware
        // decoding is intentionally out of scope here.
        if (col.str_data) {
          val = Napi::String::New(env, col.str_data, col.str_len);
        } else {
          val = Napi::String::New(env, "");
        }
        break;

      case MES_COL_BYTES:
        // MES_COL_BYTES is by definition a bytes value; str_len == 0 is a
        // legitimate empty payload and maps to an empty Buffer. Truly-null
        // columns arrive with type == MES_COL_NULL (handled separately).
        // If str_data is null here (defensive; the C ABI always passes a
        // valid pointer even for empty vectors) we still return an empty
        // Buffer so the consumer's type expectation is preserved.
        // TODO(perf): future optimization -- Buffer::New with external
        // memory would avoid this copy, but requires careful lifetime
        // management tied to mes_event_t (valid only until the next
        // mes_next_event() call). Skipped due to crash risk.
        if (col.str_data) {
          val = Napi::Buffer<uint8_t>::Copy(env, reinterpret_cast<const uint8_t*>(col.str_data),
                                            col.str_len);
        } else {
          val = Napi::Buffer<uint8_t>::New(env, static_cast<size_t>(0));
        }
        break;

      default:
        val = env.Null();
        break;
    }

    // Define an own data property instead of performing ordinary [[Set]].
    // In particular, "__proto__" must remain a legal column key rather than
    // invoking Object.prototype.__proto__ and changing this row's prototype.
    record.DefineProperty(Napi::PropertyDescriptor::Value(
        key, val,
        static_cast<napi_property_attributes>(napi_writable | napi_enumerable |
                                              napi_configurable)));
  }

  return record;
}

Napi::String EngineWrap::GetColumnKey(Napi::Env env, const mes_column_t& col, uint32_t index) {
  if (col.col_name == nullptr || col.col_name[0] == '\0') {
    return Napi::String::New(env, std::to_string(index));
  }

  const size_t name_size = std::strlen(col.col_name);
  auto cached = column_name_cache_.find(col.col_name);
  if (cached != column_name_cache_.end() && cached->second.bytes.size() == name_size &&
      std::memcmp(cached->second.bytes.data(), col.col_name, name_size) == 0) {
    return cached->second.holder.Value().Get("value").As<Napi::String>();
  }

  // TABLE_MAP storage can be cleared and later reuse the same address. Keep a
  // byte copy and compare it above before returning a cached V8 string.
  if (column_name_cache_.size() >= 8192) column_name_cache_.clear();
  Napi::String key = Napi::String::New(env, col.col_name, name_size);
  Napi::Object holder = Napi::Object::New(env);
  holder.Set("value", key);
  ColumnNameCacheEntry entry{std::string(col.col_name, name_size), Napi::Persistent(holder)};
  column_name_cache_.insert_or_assign(col.col_name, std::move(entry));
  return key;
}
