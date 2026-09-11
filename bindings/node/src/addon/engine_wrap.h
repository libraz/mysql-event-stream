// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#ifndef MES_NODE_ENGINE_WRAP_H_
#define MES_NODE_ENGINE_WRAP_H_

#include <napi.h>

#include <cstdint>
#include <string>
#include <unordered_map>

#include "mes.h"

class EngineWrap : public Napi::ObjectWrap<EngineWrap> {
 public:
  static Napi::Object Init(Napi::Env env, Napi::Object exports);
  explicit EngineWrap(const Napi::CallbackInfo& info);
  ~EngineWrap();

 private:
  Napi::Value Feed(const Napi::CallbackInfo& info);
  Napi::Value NextEvent(const Napi::CallbackInfo& info);
  Napi::Value HasEvents(const Napi::CallbackInfo& info);
  Napi::Value GetPosition(const Napi::CallbackInfo& info);
  void Reset(const Napi::CallbackInfo& info);
  void SetMaxQueueSize(const Napi::CallbackInfo& info);
  void SetMaxEventSize(const Napi::CallbackInfo& info);
  Napi::Value GetMaxEventSize(const Napi::CallbackInfo& info);
  void SetChecksumEnabled(const Napi::CallbackInfo& info);
  void SetIncludeDatabases(const Napi::CallbackInfo& info);
  void SetIncludeTables(const Napi::CallbackInfo& info);
  void SetExcludeTables(const Napi::CallbackInfo& info);
  void SetStringFilter(const Napi::CallbackInfo& info,
                       mes_error_t (*setter)(mes_engine_t*, const char**, size_t),
                       const char* method_name);
  void Destroy(const Napi::CallbackInfo& info);

  Napi::Value ReadColumns(Napi::Env env, const mes_column_t* cols, uint32_t count);

  struct ColumnNameCacheEntry {
    std::string bytes;
    Napi::Reference<Napi::Object> holder;
  };

  /**
   * @brief The JS string for the statement the last converted event annotated.
   *
   * One ANNOTATE_ROWS statement covers every row of the ROWS event that
   * follows it, and each of those rows arrives as its own C event carrying the
   * same @c source_sql pointer, so a single entry serves the whole event.
   * @c address is only a fast-path key: the core reuses that storage across
   * events, so @c bytes is compared before the cached string is reused.
   */
  struct SourceSqlCacheEntry {
    const char* address = nullptr;
    std::string bytes;
    Napi::Reference<Napi::Object> holder;
  };

  Napi::String GetColumnKey(Napi::Env env, const mes_column_t& col, uint32_t index);
  Napi::String GetSourceSql(Napi::Env env, const char* source_sql);
  Napi::Value GetSourceSqlConversions(const Napi::CallbackInfo& info);

  mes_engine_t* engine_;
  std::unordered_map<const char*, ColumnNameCacheEntry> column_name_cache_;
  SourceSqlCacheEntry source_sql_cache_;
  /// Count of annotating statements converted to a JS string, so that the
  /// per-event sharing above is observable rather than inferred from timing.
  uint64_t source_sql_conversions_ = 0;

  Napi::Value EnableMetadata(const Napi::CallbackInfo& info);
};

#endif  // MES_NODE_ENGINE_WRAP_H_
