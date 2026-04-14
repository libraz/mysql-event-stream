// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#include "logger.h"

namespace mes {

std::atomic<mes_log_callback_t> LogConfig::callback_{nullptr};
std::atomic<mes_log_level_t> LogConfig::min_level_{MES_LOG_ERROR};
std::atomic<void*> LogConfig::userdata_{nullptr};

void LogConfig::SetCallback(mes_log_callback_t callback, mes_log_level_t min_level,
                             void* userdata) {
  userdata_.store(userdata, std::memory_order_relaxed);
  min_level_.store(min_level, std::memory_order_relaxed);
  callback_.store(callback, std::memory_order_release);
}

mes_log_callback_t LogConfig::GetCallback() {
  return callback_.load(std::memory_order_acquire);
}
mes_log_level_t LogConfig::GetMinLevel() {
  return min_level_.load(std::memory_order_relaxed);
}
void* LogConfig::GetUserdata() { return userdata_.load(std::memory_order_relaxed); }

void StructuredLog::Emit(mes_log_level_t level) {
  auto* callback = LogConfig::GetCallback();
  if (callback == nullptr || level > LogConfig::GetMinLevel()) {
    return;
  }

  // Build key=value format (matching mygram-db TEXT format)
  std::string message;
  if (!event_.empty()) {
    message += "event=";
    message += event_;
  }
  for (const auto& [key, value] : fields_) {
    if (!message.empty()) message += ' ';
    message += key;
    message += '=';
    message += value;
  }

  callback(level, message.c_str(), LogConfig::GetUserdata());
}

}  // namespace mes
