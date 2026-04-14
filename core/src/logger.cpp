// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#include "logger.h"

#include <mutex>

namespace mes {

namespace {

// Serializes updates to snapshot_. Reads take a local shared_ptr copy
// under the same lock, which is cheap (atomic refcount bump) and ensures
// that the observed (callback, level, userdata) triple is always internally
// consistent. std::atomic<std::shared_ptr> is C++20-only; this lock is the
// portable C++17 equivalent.
std::mutex& SnapshotMutex() {
  static std::mutex m;
  return m;
}

}  // namespace

std::shared_ptr<const LogConfigSnapshot> LogConfig::snapshot_ =
    std::make_shared<const LogConfigSnapshot>();

void LogConfig::SetCallback(mes_log_callback_t callback, mes_log_level_t log_level,
                            void* userdata) {
  auto next = std::make_shared<LogConfigSnapshot>();
  next->callback = callback;
  next->level = log_level;
  next->userdata = userdata;
  std::lock_guard<std::mutex> lock(SnapshotMutex());
  snapshot_ = std::move(next);
}

std::shared_ptr<const LogConfigSnapshot> LogConfig::GetSnapshot() {
  std::lock_guard<std::mutex> lock(SnapshotMutex());
  return snapshot_;
}

mes_log_callback_t LogConfig::GetCallback() { return GetSnapshot()->callback; }
mes_log_level_t LogConfig::GetLogLevel() { return GetSnapshot()->level; }
void* LogConfig::GetUserdata() { return GetSnapshot()->userdata; }

void StructuredLog::Emit(mes_log_level_t level) {
  // Single atomic read ensures the callback, level, and userdata we use
  // below are all from the same configuration generation.
  auto snap = LogConfig::GetSnapshot();
  if (snap->callback == nullptr || level > snap->level) {
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

  snap->callback(level, message.c_str(), snap->userdata);
}

}  // namespace mes
