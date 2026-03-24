// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#ifndef MES_LOGGER_H_
#define MES_LOGGER_H_

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "mes.h"

namespace mes {

/** @brief Global log configuration that dispatches to the registered callback. */
class LogConfig {
 public:
  static void SetCallback(mes_log_callback_t callback, mes_log_level_t min_level, void* userdata);
  static mes_log_callback_t GetCallback();
  static mes_log_level_t GetMinLevel();
  static void* GetUserdata();

 private:
  static mes_log_callback_t callback_;
  static mes_log_level_t min_level_;
  static void* userdata_;
};

/** @brief Structured log entry builder with fluent API.
 *
 * Usage:
 *   StructuredLog().Event("binlog_error").Field("gtid", gtid).Error();
 */
class StructuredLog {
 public:
  StructuredLog() = default;

  /** @brief Set event name (required). */
  StructuredLog& Event(const char* event_name) {
    event_ = event_name;
    return *this;
  }

  /** @brief Add a string field. */
  StructuredLog& Field(const char* key, const std::string& value) {
    fields_.emplace_back(key, value);
    return *this;
  }

  /** @brief Add a string literal field. */
  StructuredLog& Field(const char* key, const char* value) {
    fields_.emplace_back(key, std::string(value));
    return *this;
  }

  /** @brief Add an int64 field. */
  StructuredLog& Field(const char* key, int64_t value) {
    fields_.emplace_back(key, std::to_string(value));
    return *this;
  }

  /** @brief Add a uint64 field. */
  StructuredLog& Field(const char* key, uint64_t value) {
    fields_.emplace_back(key, std::to_string(value));
    return *this;
  }

  /** @brief Add an int field. */
  StructuredLog& Field(const char* key, int value) {
    fields_.emplace_back(key, std::to_string(value));
    return *this;
  }

  /** @brief Add a double field. */
  StructuredLog& Field(const char* key, double value) {
    fields_.emplace_back(key, std::to_string(value));
    return *this;
  }

  /** @brief Add a bool field. */
  StructuredLog& Field(const char* key, bool value) {
    fields_.emplace_back(key, value ? "true" : "false");
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
