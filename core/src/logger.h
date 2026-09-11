// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#ifndef MES_LOGGER_H_
#define MES_LOGGER_H_

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "mes.h"

namespace mes {

/** @brief Immutable snapshot of the global log configuration. */
struct LogConfigSnapshot {
  mes_log_callback_t callback = nullptr;
  mes_log_level_t level = MES_LOG_ERROR;
  void* userdata = nullptr;
};

/**
 * @brief Global log configuration that dispatches to the registered callback.
 *
 * The configuration is published as an immutable snapshot behind a shared_ptr,
 * and GetSnapshot() is the only way to read it. That is what makes a reader's
 * (callback, level, userdata) a consistent triple: all three come from one
 * generation, so no reader can pair one generation's callback with another's
 * userdata. Single-field accessors would forfeit precisely that, because two
 * calls can straddle a republication, which is why none are offered.
 *
 * The snapshot and its mutex live in storage that is never reclaimed, so an
 * object with static storage duration can still log from its destructor during
 * process teardown, whatever the destruction order turns out to be.
 */
class LogConfig {
 public:
  static void SetCallback(mes_log_callback_t callback, mes_log_level_t log_level, void* userdata);

  /** @brief Get a consistent snapshot of the current configuration. */
  static std::shared_ptr<const LogConfigSnapshot> GetSnapshot();
};

/** @brief Structured log entry builder with fluent API.
 *
 * Usage:
 *   StructuredLog().Event("binlog_error").Field("gtid", gtid).Error();
 *
 * NOTE(perf): The constructor takes a single snapshot of the global log
 * configuration and caches it for the lifetime of the builder, so each
 * Field() performs no synchronization and costs only the emplace_back.
 * Acquiring the snapshot mutex once per Field() instead dominates hot-path
 * logging cost when many fields are attached in a reader-thread loop.
 */
class StructuredLog {
 public:
  StructuredLog() : snap_(LogConfig::GetSnapshot()) {}

  /** @brief Set event name (required). */
  StructuredLog& Event(const char* event_name) {
    event_ = event_name;
    return *this;
  }

  /** @brief Add a string field. */
  StructuredLog& Field(const char* key, const std::string& value) {
    if (Enabled()) fields_.emplace_back(key, value);
    return *this;
  }

  /** @brief Add a string literal field. */
  StructuredLog& Field(const char* key, const char* value) {
    if (Enabled()) fields_.emplace_back(key, std::string(value));
    return *this;
  }

  /** @brief Add an int64 field. */
  StructuredLog& Field(const char* key, int64_t value) {
    if (Enabled()) fields_.emplace_back(key, std::to_string(value));
    return *this;
  }

  /** @brief Add a uint64 field. */
  StructuredLog& Field(const char* key, uint64_t value) {
    if (Enabled()) fields_.emplace_back(key, std::to_string(value));
    return *this;
  }

  /** @brief Add an int field. */
  StructuredLog& Field(const char* key, int value) {
    if (Enabled()) fields_.emplace_back(key, std::to_string(value));
    return *this;
  }

  /** @brief Add a double field. */
  StructuredLog& Field(const char* key, double value) {
    if (Enabled()) fields_.emplace_back(key, std::to_string(value));
    return *this;
  }

  /** @brief Add a bool field. */
  StructuredLog& Field(const char* key, bool value) {
    if (Enabled()) fields_.emplace_back(key, value ? "true" : "false");
    return *this;
  }

  /** @brief Emit at ERROR level. */
  void Error() { Emit(MES_LOG_ERROR); }

  /** @brief Emit at WARN level. */
  void Warn() { Emit(MES_LOG_WARN); }

  /** @brief Emit at INFO level. */
  void Info() { Emit(MES_LOG_INFO); }

  /** @brief Emit at DEBUG level. */
  void Debug() { Emit(MES_LOG_DEBUG); }

 private:
  void Emit(mes_log_level_t level);

  bool Enabled() const { return snap_ && snap_->callback != nullptr; }

  // Snapshot captured at construction: guarantees all Field() calls and
  // the final Emit() see the same (callback, level, userdata) triple,
  // eliminating per-Field mutex traffic.
  std::shared_ptr<const LogConfigSnapshot> snap_;
  std::string event_;
  std::vector<std::pair<std::string, std::string>> fields_;
};

// Convenience helper functions (matching mygram-db patterns)

/** @brief Log MySQL connection error. */
inline void LogMySQLConnectionError(const std::string& host, int port,
                                    const std::string& error_msg) {
  StructuredLog()
      .Event("mysql_connection_error")
      .Field("host", host)
      .Field("port", port)
      .Field("error", error_msg)
      .Error();
}

/** @brief Log binlog streaming error. */
inline void LogBinlogError(const char* error_type, const std::string& gtid,
                           const std::string& error_msg, int retry_count = 0) {
  StructuredLog()
      .Event("binlog_error")
      .Field("type", error_type)
      .Field("gtid", gtid)
      .Field("error", error_msg)
      .Field("retry_count", retry_count)
      .Error();
}

}  // namespace mes

#endif  // MES_LOGGER_H_
